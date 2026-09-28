package mirror

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// Reasons written on the guest's Ready condition while the orchestrator loop holds it false.
const (
	// ReasonWaitingForGrant: the guest has not been released since its mirror started or resumed.
	ReasonWaitingForGrant = "WaitingForGrant"
	// ReasonSuspending: a notice runs and the guest is about to be suspended.
	ReasonSuspending = "GuestSuspending"
	// ReasonSuspended: the guest is suspended until the next grant.
	ReasonSuspended = "GuestSuspended"
	// ReasonVacated: the mirror was deleted to vacate the accelerator; the guest gets a new
	// mirror on the next grant.
	ReasonVacated = "GuestVacated"
)

// casRetries bounds the compare-and-swap retries of a mirror annotation update.
const casRetries = 5

// Guest is a guest pod with the mirror that serves it, if any.
type Guest struct {
	Pod    *corev1.Pod
	Mirror *corev1.Pod // nil when no mirror serves the guest
}

// gateState is the orchestrator mode's view of each guest. The mirrors in the API stay the
// source of truth for what runs; this only decides what the guest reports.
type gateState struct {
	group    string
	released map[types.UID]bool   // Ready may follow the mirror
	reason   map[types.UID]string // why Ready is held false
	attempts map[types.UID]int    // mirrors created so far for the guest
	vacated  map[types.UID]bool   // mirror deleted to vacate; the guest is not failed
}

func newGateState(group string) gateState {
	return gateState{
		group:    group,
		released: map[types.UID]bool{},
		reason:   map[types.UID]string{},
		attempts: map[types.UID]int{},
		vacated:  map[types.UID]bool{},
	}
}

// SetGroup sets the group written on new mirrors. The orchestrator loop calls it once it has
// read the group from the real node.
func (b *Backend) SetGroup(group string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gate.group = group
}

func (b *Backend) buildConfig(guest *corev1.Pod) Config {
	cfg := b.opts.Config
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.opts.Gated {
		cfg.Group = b.gate.group
		cfg.Attempt = b.gate.attempts[guest.UID]
	}
	return cfg
}

// translate is translateProbed plus the orchestrator mode's Ready hold.
func (b *Backend) translate(guest, m *corev1.Pod) *corev1.Pod {
	out := b.translateProbed(guest, m)
	if !b.opts.Gated {
		return out
	}
	b.mu.Lock()
	released := b.gate.released[guest.UID]
	reason := b.gate.reason[guest.UID]
	b.mu.Unlock()
	if released {
		return out
	}
	if reason == "" {
		reason = ReasonWaitingForGrant
	}
	holdNotReady(&out.Status, reason)
	return out
}

// holdNotReady forces Ready false, as the real kubelet reports a pod whose readiness probe fails.
func holdNotReady(st *corev1.PodStatus, reason string) {
	now := metav1.Now()
	found := false
	for i := range st.Conditions {
		cond := &st.Conditions[i]
		if cond.Type != corev1.PodReady && cond.Type != corev1.ContainersReady {
			continue
		}
		if cond.Type == corev1.PodReady {
			found = true
		}
		if cond.Status == corev1.ConditionTrue {
			cond.Status, cond.LastTransitionTime = corev1.ConditionFalse, now
		}
		cond.Reason = reason
	}
	if !found {
		st.Conditions = append(st.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: reason, LastTransitionTime: now,
		})
	}
	for i := range st.ContainerStatuses {
		st.ContainerStatuses[i].Ready = false
	}
}

// VacatedStatus is the guest's status after its mirror was deleted to vacate the accelerator:
// Pending and NotReady, not Failed, because the loop gives it a new mirror on the next grant.
func VacatedStatus(guest *corev1.Pod) *corev1.Pod {
	out := guest.DeepCopy()
	out.Status.Phase = corev1.PodPending
	out.Status.Reason = ReasonVacated
	out.Status.Message = "mirror deleted to vacate the accelerator; it is re-created on the next grant"
	holdNotReady(&out.Status, ReasonVacated)
	for i := range out.Status.ContainerStatuses {
		cs := &out.Status.ContainerStatuses[i]
		cs.Started = new(false)
		cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: ReasonVacated}}
	}
	return out
}

func (b *Backend) takeVacated(uid types.UID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	v := b.gate.vacated[uid]
	delete(b.gate.vacated, uid)
	return v
}

// Guests returns every pod bound to the virtual node with its mirror (from the informer).
func (b *Backend) Guests() ([]Guest, error) {
	pods, err := b.guests.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list guests: %w", err)
	}
	out := make([]Guest, 0, len(pods))
	for _, p := range pods {
		g := Guest{Pod: p}
		if m, ok := b.mirrorOf(p); ok {
			g.Mirror = m
		}
		out = append(out, g)
	}
	return out, nil
}

// HoldNotReady holds the guest's Ready false with reason and reports the guest at once.
func (b *Backend) HoldNotReady(guest *corev1.Pod, reason string) {
	b.mu.Lock()
	delete(b.gate.released, guest.UID)
	b.gate.reason[guest.UID] = reason
	b.mu.Unlock()
	b.refresh(guest)
}

// EngineReady is the loop's engine check: the guest's Ready as it would be without the hold,
// that is the VK readiness prober's verdict (M2) or, with no prober, the mirror's own flags.
func (b *Backend) EngineReady(guest, m *corev1.Pod) bool {
	if m.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range b.translateProbed(guest, m).Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ReleaseReady lets the guest's Ready follow its mirror again and reports the guest at once.
func (b *Backend) ReleaseReady(guest *corev1.Pod) {
	b.mu.Lock()
	b.gate.released[guest.UID] = true
	delete(b.gate.reason, guest.UID)
	b.mu.Unlock()
	b.refresh(guest)
}

func (b *Backend) refresh(guest *corev1.Pod) {
	if m, ok := b.mirrorOf(guest); ok {
		b.emit(b.translate(guest, m))
	}
}

// ConfirmNotReady returns once the API shows the guest not Ready (or gone), or when ctx ends.
// The status write itself is the library's; this reads it back.
func (b *Backend) ConfirmNotReady(ctx context.Context, guest *corev1.Pod) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		cur, err := b.client.CoreV1().Pods(guest.Namespace).Get(ctx, guest.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return nil
		case err == nil && cur.UID != guest.UID:
			return nil
		case err == nil:
			if c := findCondition(cur.Status.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionTrue {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("guest %s/%s still Ready: %w", guest.Namespace, guest.Name, ctx.Err())
		case <-tick.C:
		}
	}
}

// MirrorNow reads the guest's mirror from the API, not the informer, so a mirror created a
// moment ago is not missed. found is false when no mirror of this guest exists.
func (b *Backend) MirrorNow(ctx context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error) {
	m, err := b.client.CoreV1().Pods(guest.Namespace).Get(ctx, Name(guest.Name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get mirror: %w", err)
	}
	if m.Labels[LabelMirrorOf] != string(guest.UID) {
		return nil, false, nil
	}
	return m, true, nil
}

// VacateMirror deletes the mirror with its normal grace period to free the accelerator. The
// guest is reported Pending, not Failed, and its next mirror gets a new job id.
func (b *Backend) VacateMirror(ctx context.Context, guest, m *corev1.Pod) error {
	b.mu.Lock()
	b.gate.vacated[guest.UID] = true
	b.gate.attempts[guest.UID]++
	b.mu.Unlock()
	if err := b.deleteMirror(ctx, m); err != nil {
		b.mu.Lock()
		delete(b.gate.vacated, guest.UID)
		b.mu.Unlock()
		return err
	}
	return nil
}

// KillMirror deletes the mirror with its normal grace period after a failed suspend or
// resume. The guest then shows Failed (ReasonMirrorDeleted). Never grace 0: the pod must stay
// until its processes are gone.
func (b *Backend) KillMirror(ctx context.Context, m *corev1.Pod) error {
	return b.deleteMirror(ctx, m)
}

func (b *Backend) deleteMirror(ctx context.Context, m *corev1.Pod) error {
	uid := m.UID
	err := b.client.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete mirror %s/%s: %w", m.Namespace, m.Name, err)
	}
	return nil
}

// ErrMirrorReplaced means the mirror changed identity while its annotations were updated.
var ErrMirrorReplaced = errors.New("mirror was replaced")

// BumpEpoch increments timeslice.io/guest-epoch on the mirror by compare-and-swap on
// resourceVersion and returns the updated mirror and the new epoch.
func (b *Backend) BumpEpoch(ctx context.Context, m *corev1.Pod) (*corev1.Pod, int64, error) {
	var epoch int64
	upd, err := b.updateMirror(ctx, m, func(cur *corev1.Pod) error {
		prev := int64(0)
		if v := cur.Annotations[AnnotationGuestEpoch]; v != "" {
			parsed, perr := strconv.ParseInt(v, 10, 64)
			if perr != nil {
				return fmt.Errorf("bad %s %q: %w", AnnotationGuestEpoch, v, perr)
			}
			prev = parsed
		}
		epoch = prev + 1
		cur.Annotations[AnnotationGuestEpoch] = strconv.FormatInt(epoch, 10)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return upd, epoch, nil
}

// AnnotateMirror sets one annotation on the mirror by compare-and-swap.
func (b *Backend) AnnotateMirror(ctx context.Context, m *corev1.Pod, key, value string) error {
	_, err := b.updateMirror(ctx, m, func(cur *corev1.Pod) error {
		cur.Annotations[key] = value
		return nil
	})
	return err
}

// updateMirror applies change to the latest mirror and writes it with its resourceVersion,
// retrying on conflict. It fails if the mirror was replaced (other UID).
func (b *Backend) updateMirror(ctx context.Context, m *corev1.Pod, change func(*corev1.Pod) error) (*corev1.Pod, error) {
	pods := b.client.CoreV1().Pods(m.Namespace)
	var lastErr error
	for range casRetries {
		cur, err := pods.Get(ctx, m.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get mirror: %w", err)
		}
		if cur.UID != m.UID {
			return nil, ErrMirrorReplaced
		}
		cur = cur.DeepCopy()
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		if err := change(cur); err != nil {
			return nil, err
		}
		upd, err := pods.Update(ctx, cur, metav1.UpdateOptions{})
		if err == nil {
			return upd, nil
		}
		if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("update mirror: %w", err)
		}
		lastErr = err
	}
	return nil, fmt.Errorf("update mirror: %w", lastErr)
}
