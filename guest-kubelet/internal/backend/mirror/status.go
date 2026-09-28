package mirror

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Reasons written on the guest when its mirror is gone.
const (
	ReasonMirrorDeleted = "MirrorDeleted"
	ReasonGuestDeleted  = "GuestKubeletPodDeleted"

	// ReasonContainersNotReady is the kubelet's reason when a readiness probe has not passed.
	ReasonContainersNotReady = "ContainersNotReady"
)

// Readiness is the guest kubelet's own readiness verdict per guest container (M2). The mirror
// carries no probes, so its ready flags say only "running"; a Readiness replaces them.
type Readiness interface {
	// ContainerReady returns the verdict for one container and whether there is one. With none,
	// the mirror's flag stands (ready once running, as for a container without a probe).
	ContainerReady(guest *corev1.Pod, container string) (bool, bool)
}

// TranslateStatus is the guest's status given its mirror's status: the real kubelet's view of
// the process, copied upward. It returns a new guest object; neither input is modified.
//
// Copied: phase, reason, message, podIP(s), hostIP(s), startTime, conditions, container
// statuses (state, lastState, restartCount, image, imageID, containerID, ready, started).
// Kept from the guest: qosClass (immutable once set, and the mirror's differs when its
// requests were capped), and any readiness-gate conditions the mirror cannot have.
func TranslateStatus(guest, m *corev1.Pod) *corev1.Pod {
	return TranslateStatusWith(guest, m, nil)
}

// TranslateStatusWith is TranslateStatus with the guest kubelet's readiness verdicts applied:
// each container's ready flag comes from rd (and is false unless the container runs), and the
// ContainersReady and Ready conditions are recomputed from those flags. A nil rd is M1: the
// mirror's flags and conditions are copied as they are.
func TranslateStatusWith(guest, mirrorPod *corev1.Pod, rd Readiness) *corev1.Pod {
	out := guest.DeepCopy()
	ms := mirrorPod.Status.DeepCopy()
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
	if rd != nil {
		applyReadiness(&st, guest, rd)
	}
	// The mirror has no readiness gates, so its Ready ignores the guest's gates. Keep the
	// guest's gate conditions, and hold Ready false until every gate is true, which is what the
	// real kubelet does.
	for _, g := range guest.Spec.ReadinessGates {
		c := findCondition(guest.Status.Conditions, g.ConditionType)
		if c == nil {
			setReadyFalse(&st, guest, "ReadinessGatesNotReady")
			continue
		}
		st.Conditions = append(st.Conditions, *c)
		if c.Status != corev1.ConditionTrue {
			setReadyFalse(&st, guest, "ReadinessGatesNotReady")
		}
	}
	applySuspendState(&st, guest, mirrorPod)
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

// setReadyFalse holds Ready false. The transition time is the guest's own if it was already
// false there, so a re-translation does not change the status.
func setReadyFalse(st *corev1.PodStatus, guest *corev1.Pod, reason string) {
	c := findCondition(st.Conditions, corev1.PodReady)
	if c == nil || c.Status != corev1.ConditionTrue {
		return
	}
	c.Status, c.Reason = corev1.ConditionFalse, reason
	if prev := findCondition(guest.Status.Conditions, corev1.PodReady); prev != nil && prev.Status == corev1.ConditionFalse {
		c.LastTransitionTime = prev.LastTransitionTime
	}
}

// applyReadiness sets each container's ready flag from rd and recomputes ContainersReady and
// Ready, as the kubelet's status manager does. A condition keeps its previous transition time on
// the guest while its status does not change.
func applyReadiness(st *corev1.PodStatus, guest *corev1.Pod, rd Readiness) {
	var unready []string
	for i := range st.ContainerStatuses {
		cs := &st.ContainerStatuses[i]
		if verdict, has := rd.ContainerReady(guest, cs.Name); has {
			cs.Ready = verdict && cs.State.Running != nil
		}
		if !cs.Ready {
			unready = append(unready, cs.Name)
		}
	}
	for i := range guest.Spec.Containers {
		if name := guest.Spec.Containers[i].Name; findContainerStatus(st.ContainerStatuses, name) == nil {
			unready = append(unready, name)
		}
	}
	if len(unready) > 0 {
		// The same NotReady as a suspend (M3), with the kubelet's reason and message.
		msg := fmt.Sprintf("containers with unready status: [%s]", strings.Join(unready, " "))
		MarkNotReady(st, guest.Status.Conditions, ReasonContainersNotReady, msg, metav1.Now())
		return
	}
	setCondition(st, guest, corev1.ContainersReady, true, "")
	setCondition(st, guest, corev1.PodReady, st.Phase == corev1.PodRunning, "")
}

func setCondition(st *corev1.PodStatus, guest *corev1.Pod, ct corev1.PodConditionType, isTrue bool, msg string) {
	want := corev1.PodCondition{Type: ct, Status: corev1.ConditionFalse}
	if isTrue {
		want.Status = corev1.ConditionTrue
	} else {
		want.Reason, want.Message = ReasonContainersNotReady, msg
	}
	want.LastTransitionTime = metav1.Now()
	if prev := findCondition(guest.Status.Conditions, ct); prev != nil && prev.Status == want.Status {
		want.LastTransitionTime = prev.LastTransitionTime
	}
	if cur := findCondition(st.Conditions, ct); cur != nil {
		want.LastProbeTime = cur.LastProbeTime
		*cur = want
		return
	}
	st.Conditions = append(st.Conditions, want)
}

func findContainerStatus(statuses []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}
