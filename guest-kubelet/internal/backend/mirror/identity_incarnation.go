package mirror

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// ReasonMirrorReplaced is written on a guest's Ready condition while the mirror of the
// previous incarnation is deleted and a new one starts (--mirror-identity=incarnation).
const ReasonMirrorReplaced = "MirrorReplaced"

// retirePrevious runs once at startup with --mirror-identity=incarnation, before the pod
// controller starts. Every live mirror it finds belongs to a previous incarnation: it marks the
// guest NotReady, deletes the mirror with normal grace (never grace 0) and waits until the
// mirror is gone, so the first GetPod finds nothing and the library creates the new mirror at
// once. If a mirror outlives the wait, the create still cannot run beside it: the mirror name
// is fixed, so the create hits AlreadyExists and adoptOrReplace returns a retry error.
func (b *Backend) retirePrevious(ctx context.Context) error {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return err
	}
	var wait []*corev1.Pod
	var maxGrace int64
	for _, pod := range ms {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue // its guest already shows the terminal state; nothing runs
		}
		guestUID := types.UID(pod.Labels[LabelMirrorOf])
		attempt, ok := AttemptOf(pod.Labels[LabelJobID], guestUID)
		if !ok {
			attempt = 1
		}
		b.mu.Lock()
		b.retired[pod.UID] = guestUID
		b.replacedFor[guestUID] = pod.UID
		b.attempts[guestUID] = max(b.attempts[guestUID], attempt)
		b.mu.Unlock()

		logger := log.G(ctx).WithField("mirror", pod.Namespace+"/"+pod.Name).WithField("mirrorUID", pod.UID).
			WithField("jobID", pod.Labels[LabelJobID])
		logger.Warn("retiring mirror from previous incarnation")
		b.markGuestNotReady(ctx, pod)
		if pod.DeletionTimestamp == nil {
			uid := pod.UID
			err := b.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &uid}, // normal grace: GracePeriodSeconds unset
			})
			if err != nil && !apierrors.IsNotFound(err) {
				logger.WithError(err).Warn("could not delete mirror from previous incarnation; the create retries until it is gone")
			}
		}
		grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
		if g := pod.Spec.TerminationGracePeriodSeconds; g != nil {
			grace = *g
		}
		maxGrace = max(maxGrace, grace)
		wait = append(wait, pod)
	}
	if len(wait) == 0 {
		return nil
	}

	timeout := b.opts.ReplaceWait
	if timeout == 0 {
		timeout = time.Duration(maxGrace)*time.Second + 30*time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		left := 0
		for _, m := range wait {
			if cur, err := b.mirrors.Pods(m.Namespace).Get(m.Name); err == nil && cur.UID == m.UID {
				left++
			}
		}
		if left == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			log.G(ctx).WithField("left", left).WithField("waited", timeout.String()).
				Warn("mirrors from previous incarnation still terminating; their guests' creates wait for them")
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// markGuestNotReady writes Ready=False on the live guest of a mirror about to be retired, so
// no Service sends it traffic while no process serves. Best effort: the guest's next status
// comes from the new mirror anyway.
func (b *Backend) markGuestNotReady(ctx context.Context, m *corev1.Pod) {
	g, err := b.client.CoreV1().Pods(m.Namespace).Get(ctx, m.Annotations[AnnotationGuestName], metav1.GetOptions{})
	if err != nil || string(g.UID) != m.Labels[LabelMirrorOf] || g.DeletionTimestamp != nil {
		return
	}
	c := findCondition(g.Status.Conditions, corev1.PodReady)
	if c == nil || c.Status != corev1.ConditionTrue {
		return
	}
	g = g.DeepCopy()
	setReadyFalse(&g.Status, ReasonMirrorReplaced)
	findCondition(g.Status.Conditions, corev1.PodReady).LastTransitionTime = metav1.Now()
	if _, err := b.client.CoreV1().Pods(g.Namespace).UpdateStatus(ctx, g, metav1.UpdateOptions{}); err != nil {
		log.G(ctx).WithError(err).WithField("guest", g.Namespace+"/"+g.Name).
			Warn("could not mark guest NotReady before replacing its mirror")
	}
}

// isRetired reports whether a mirror belongs to a previous incarnation. Its status is not
// copied to the guest, it never counts as the guest's mirror, and its deletion does not fail
// the guest.
func (b *Backend) isRetired(uid types.UID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.retired[uid]
	return ok
}

// forgetRetired drops a retired mirror once the informer has seen it deleted.
func (b *Backend) forgetRetired(uid types.UID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.retired[uid]
	delete(b.retired, uid)
	return ok
}

// logReplaced records the first mirror this incarnation created for a guest whose previous
// mirror it retired.
func (b *Backend) logReplaced(ctx context.Context, guest, m *corev1.Pod) {
	b.mu.Lock()
	old, ok := b.replacedFor[guest.UID]
	delete(b.replacedFor, guest.UID)
	b.mu.Unlock()
	if !ok {
		return
	}
	log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("old", old).
		WithField("new", m.UID).WithField("jobID", m.Labels[LabelJobID]).
		Warn("replaced mirror from previous incarnation")
}
