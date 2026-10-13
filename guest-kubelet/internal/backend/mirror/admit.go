package mirror

import (
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// EventNotAdmitted is recorded on a guest whose mirror would not fit the notice window.
const EventNotAdmitted = "NotAdmitted"

// admit checks, before a new mirror is created, that every mirror of this virtual Node (the new
// one included) can be checkpointed and one restored within N - K (Q6: admit a guest only
// while one restore plus the sum of checkpoints fits N - K). It is off while either estimate
// is zero. A refused guest stays Pending; the pod controller retries the create.
func (b *Backend) admit(guest *corev1.Pod) error {
	so := &b.opts.Suspend
	if so.CheckpointEstimate <= 0 || so.RestoreEstimate <= 0 {
		return nil
	}
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list mirrors for admission: %w", err)
	}
	n := 1 // the new mirror
	for _, m := range ms {
		if m.Labels[LabelMirrorOf] == string(guest.UID) || m.DeletionTimestamp != nil || killedReason(m) != "" ||
			m.Status.Phase == corev1.PodSucceeded || m.Status.Phase == corev1.PodFailed {
			continue
		}
		n++
	}
	need := so.RestoreEstimate + so.CheckpointEstimate*time.Duration(n)
	budget := so.NoticeWindow - so.KillBudget
	if need <= budget {
		return nil
	}
	msg := fmt.Sprintf("%d mirror(s) need %s (one restore %s + %d checkpoints of %s), more than N - K = %s",
		n, need, so.RestoreEstimate, n, so.CheckpointEstimate, budget)
	b.event(guest, corev1.EventTypeWarning, EventNotAdmitted, msg)
	return errors.New("not admitted: " + msg)
}
