package mirror

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Reasons written on the guest when its mirror is gone.
const (
	ReasonMirrorDeleted = "MirrorDeleted"
	ReasonGuestDeleted  = "GuestKubeletPodDeleted"
)

// TranslateStatus is the guest's status given its mirror's status: the real kubelet's view of
// the process, copied upward. It returns a new guest object; neither input is modified.
//
// Copied: phase, reason, message, podIP(s), hostIP(s), startTime, conditions, container
// statuses (state, lastState, restartCount, image, imageID, containerID, ready, started).
// Kept from the guest: qosClass (immutable once set, and the mirror's differs when its
// requests were capped), and any readiness-gate conditions the mirror cannot have.
func TranslateStatus(guest, m *corev1.Pod) *corev1.Pod {
	out := guest.DeepCopy()
	ms := m.Status.DeepCopy()
	st := corev1.PodStatus{
		Phase:                 ms.Phase,
		Reason:                ms.Reason,
		Message:               ms.Message,
		HostIP:                ms.HostIP,
		HostIPs:               ms.HostIPs,
		PodIP:                 ms.PodIP,
		PodIPs:                ms.PodIPs,
		StartTime:             ms.StartTime,
		Conditions:            ms.Conditions,
		ContainerStatuses:     ms.ContainerStatuses,
		InitContainerStatuses: ms.InitContainerStatuses,
		QOSClass:              guest.Status.QOSClass,
	}
	if st.Phase == "" {
		st.Phase = corev1.PodPending
	}
	// The mirror has no readiness gates, so its Ready ignores the guest's gates. Keep the
	// guest's gate conditions, and hold Ready false until every gate is true, which is what the
	// real kubelet does.
	for _, g := range guest.Spec.ReadinessGates {
		c := findCondition(guest.Status.Conditions, g.ConditionType)
		if c == nil {
			setReadyFalse(&st, "ReadinessGatesNotReady")
			continue
		}
		st.Conditions = append(st.Conditions, *c)
		if c.Status != corev1.ConditionTrue {
			setReadyFalse(&st, "ReadinessGatesNotReady")
		}
	}
	out.Status = st
	return out
}

// TerminalStatus is the guest's final status when its mirror is gone: every container
// terminated and the phase Succeeded (the guest was deleted) or Failed (someone else deleted
// the mirror). The library force-deletes a guest that has a deletionTimestamp and no running
// containers, so this is also what ends a guest deletion early.
func TerminalStatus(guest *corev1.Pod, lastMirror *corev1.Pod, reason string) *corev1.Pod {
	var out *corev1.Pod
	if lastMirror != nil {
		out = TranslateStatus(guest, lastMirror)
	} else {
		out = guest.DeepCopy()
	}
	now := metav1.Now()
	out.Status.Phase = corev1.PodFailed
	if reason == ReasonGuestDeleted {
		out.Status.Phase = corev1.PodSucceeded
	}
	out.Status.Reason = reason
	for i := range out.Status.Conditions {
		if t := out.Status.Conditions[i].Type; t == corev1.PodReady || t == corev1.ContainersReady {
			out.Status.Conditions[i].Status = corev1.ConditionFalse
			out.Status.Conditions[i].LastTransitionTime = now
		}
	}
	known := map[string]bool{}
	for i := range out.Status.ContainerStatuses {
		cs := &out.Status.ContainerStatuses[i]
		known[cs.Name] = true
		if cs.State.Terminated != nil {
			continue
		}
		var started metav1.Time
		if cs.State.Running != nil {
			started = cs.State.Running.StartedAt
		}
		cs.Ready, cs.Started = false, new(false)
		cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: reason, StartedAt: started, FinishedAt: now, ExitCode: 0,
		}}
	}
	for _, c := range guest.Spec.Containers {
		if !known[c.Name] {
			out.Status.ContainerStatuses = append(out.Status.ContainerStatuses, corev1.ContainerStatus{
				Name: c.Name, Image: c.Image, Started: new(false),
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: reason, FinishedAt: now}},
			})
		}
	}
	return out
}

func findCondition(conds []corev1.PodCondition, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

func setReadyFalse(st *corev1.PodStatus, reason string) {
	if c := findCondition(st.Conditions, corev1.PodReady); c != nil && c.Status == corev1.ConditionTrue {
		c.Status, c.Reason = corev1.ConditionFalse, reason
	}
}
