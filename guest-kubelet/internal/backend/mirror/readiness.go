package mirror

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MarkNotReady is the guest kubelet's one NotReady signal. It sets the Ready and
// ContainersReady conditions in st to False with reason and message (adding them if missing).
// A condition that prev (the guest's conditions as last written) already has False keeps that
// transition time, so re-translating the same state does not change the status; otherwise the
// transition time is at. Container ready flags are the caller's: the prober clears only the
// containers that failed, a suspend clears them all.
//
// Two callers: the readiness prober (M2, applyReadiness) when a guest container's
// readinessProbe has not passed, and the suspend path (M3, applySuspendState) while a guest is
// Suspending, Suspended or Resuming.
func MarkNotReady(st *corev1.PodStatus, prev []corev1.PodCondition, reason, message string, at metav1.Time) {
	for _, t := range []corev1.PodConditionType{corev1.PodReady, corev1.ContainersReady} {
		want := corev1.PodCondition{
			Type: t, Status: corev1.ConditionFalse, Reason: reason, Message: message, LastTransitionTime: at,
		}
		if p := findCondition(prev, t); p != nil && p.Status == corev1.ConditionFalse {
			want.LastTransitionTime = p.LastTransitionTime
		}
		if c := findCondition(st.Conditions, t); c != nil {
			want.LastProbeTime = c.LastProbeTime
			*c = want
			continue
		}
		st.Conditions = append(st.Conditions, want)
	}
}

// IsReady reports whether a pod's Ready condition is True.
func IsReady(p *corev1.Pod) bool {
	c := findCondition(p.Status.Conditions, corev1.PodReady)
	return c != nil && c.Status == corev1.ConditionTrue
}

// WaitNotReady waits until the guest's Ready=False is visible through the guest lister (the
// virtual-kubelet library's informer, so the API server has stored it). This is the "confirm
// NotReady" step that must come before any freeze: without it, the endpoint may still route
// requests to a process that is about to stop.
func (b *Backend) WaitNotReady(ctx context.Context, guest *corev1.Pod, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		cur, err := b.guests.Pods(guest.Namespace).Get(guest.Name)
		if err != nil || cur.UID != guest.UID {
			return fmt.Errorf("guest %s/%s is gone", guest.Namespace, guest.Name)
		}
		if !IsReady(cur) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("guest %s/%s still Ready after %s: %w", guest.Namespace, guest.Name, timeout, ctx.Err())
		case <-t.C:
		}
	}
}
