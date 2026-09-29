package mirror

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// Suspend state, kept on the mirror so that it survives a guest-kubelet restart and every
// replica derives the same guest status from it. No annotation means Running.
const (
	// AnnotationSuspendState is Suspending, Suspended or Resuming.
	AnnotationSuspendState = "timeslice.io/suspend-state"
	// AnnotationSuspendStateSince is when the current suspend state began (RFC 3339). For
	// Suspended it is the time the freeze completed.
	AnnotationSuspendStateSince = "timeslice.io/suspend-state-since"

	StateSuspending = "Suspending"
	StateSuspended  = "Suspended"
	StateResuming   = "Resuming"

	// ConditionSuspended is a guest pod condition: True while the guest is frozen, False with
	// reason Suspending or Resuming during a transition, absent while it runs.
	ConditionSuspended corev1.PodConditionType = "timeslice.io/suspended"

	// Event reasons on the guest.
	EventSuspended     = "Suspended"
	EventResumed       = "Resumed"
	EventSuspendFailed = "SuspendFailed"
	EventResumeFailed  = "ResumeFailed"
)

// SuspendOptions configures suspend and resume. Freezer nil disables both.
type SuspendOptions struct {
	// Freezer stops and restarts the mirror's processes: the cgroup freezer in M3, the
	// snapshot agent in M4.
	Freezer freeze.Backend
	// NotReadyTimeout bounds the wait for the guest's Ready=False to be visible in the API
	// before the freeze.
	NotReadyTimeout time.Duration
	// FreezeTimeout bounds each freeze and thaw.
	FreezeTimeout time.Duration
	// ReadyCheck confirms, after the thaw, that the engine serves again; ReadyTimeout bounds it.
	ReadyCheck   ReadyCheck
	ReadyTimeout time.Duration
	// Recorder records Suspended/Resumed events on the guest. Optional.
	Recorder record.EventRecorder
}

// Result reports what a suspend or resume did and how long each step took.
type Result struct {
	State string `json:"state"`
	Epoch int64  `json:"epoch"`
	// Suspend: time to confirm NotReady, then to freeze. Resume: time to thaw, then to pass
	// the ready check.
	NotReady   time.Duration `json:"notReady,omitempty"`
	Freeze     time.Duration `json:"freeze,omitempty"`
	Thaw       time.Duration `json:"thaw,omitempty"`
	ReadyCheck time.Duration `json:"readyCheck,omitempty"`
	Total      time.Duration `json:"total"`
	// Noop is true when the guest was already in the requested state.
	Noop bool `json:"noop,omitempty"`
}

// ErrBusy means another suspend or resume of the same guest is running.
var ErrBusy = errors.New("another suspend or resume of this guest is in flight")

// guestLocks allows one suspend or resume per guest at a time.
type guestLocks struct {
	mu    sync.Mutex
	inUse map[types.UID]bool
}

func (l *guestLocks) tryLock(uid types.UID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inUse == nil {
		l.inUse = map[types.UID]bool{}
	}
	if l.inUse[uid] {
		return false
	}
	l.inUse[uid] = true
	return true
}

func (l *guestLocks) unlock(uid types.UID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.inUse, uid)
}

// SuspendState returns the suspend state and epoch recorded on a mirror. "" means Running.
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func SuspendState(m *corev1.Pod) (string, int64) {
	var epoch int64 // missing or unparsable = 0
	if v, err := strconv.ParseInt(m.Annotations[AnnotationGuestEpoch], 10, 64); err == nil {
		epoch = v
	}
	return m.Annotations[AnnotationSuspendState], epoch
}

// Suspend takes a running guest out of service and freezes it. The order is fixed:
//  1. raise the epoch and record Suspending on the mirror (the guest turns NotReady);
//  2. wait until the API shows the guest NotReady, so its endpoint is withdrawn;
//  3. freeze;
//  4. record Suspended (the guest shows container state Waiting, reason Suspended).
//
// There is no drain: freezing without one lost no requests in Q3, and requests that reach a
// frozen engine are served after the thaw. If step 2 or 3 fails, the guest is thawed and put
// back to Running; if the thaw also fails it is left Suspending, which keeps it NotReady.
func (b *Backend) Suspend(ctx context.Context, namespace, name string) (Result, error) {
	start := time.Now()
	so := b.opts.Suspend
	guest, mirrorPod, err := b.lockGuest(namespace, name)
	if err != nil {
		return Result{}, err
	}
	defer b.locks.unlock(guest.UID)
	state, epoch := SuspendState(mirrorPod)
	switch state {
	case StateSuspended:
		return Result{State: state, Epoch: epoch, Noop: true}, nil
	case "", StateSuspending: // Suspending: a previous attempt stopped half way; redo it
	default:
		return Result{}, errdefs.InvalidInputf("guest %s/%s is %s; resume it first", namespace, name, state)
	}
	logger := log.G(ctx).WithField("guest", namespace+"/"+name)

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateSuspending, true); err != nil {
		return Result{}, err
	}
	_, epoch = SuspendState(mirrorPod)
	b.emit(b.translate(guest, mirrorPod))
	res := Result{State: StateSuspending, Epoch: epoch}

	t := time.Now()
	// The informer must hold the Suspending mirror first, or a late event for the previous
	// mirror version could re-emit Ready after the check below has passed.
	if err := b.waitInformerState(ctx, guest, StateSuspending, so.NotReadyTimeout); err != nil {
		return res, b.abortSuspend(ctx, guest, mirrorPod, epoch, err)
	}
	if err := b.WaitNotReady(ctx, guest, so.NotReadyTimeout); err != nil {
		return res, b.abortSuspend(ctx, guest, mirrorPod, epoch, fmt.Errorf("confirm NotReady: %w", err))
	}
	res.NotReady = time.Since(t)

	t = time.Now()
	fctx, cancel := context.WithTimeout(ctx, so.FreezeTimeout)
	err = so.Freezer.Suspend(fctx, mirrorPod, epoch)
	cancel()
	if err != nil {
		return res, b.abortSuspend(ctx, guest, mirrorPod, epoch, fmt.Errorf("freeze: %w", err))
	}
	res.Freeze = time.Since(t)

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateSuspended, false); err != nil {
		// Frozen, but not recorded. The guest stays NotReady (Suspending); a retry finishes it.
		return res, fmt.Errorf("frozen, but recording Suspended failed: %w", err)
	}
	b.emit(b.translate(guest, mirrorPod))
	// A resume right after this call reads the state from the informer: wait (bounded) until it
	// shows Suspended, so the resume is not refused as still Suspending.
	if err := b.waitInformerState(ctx, guest, StateSuspended, settleInformerWait); err != nil {
		logger.WithError(err).Debug("informer lags behind Suspended")
	}
	res.State, res.Total = StateSuspended, time.Since(start)
	b.event(guest, corev1.EventTypeNormal, EventSuspended,
		fmt.Sprintf("frozen (epoch %d): NotReady confirmed in %s, freeze %s", epoch, res.NotReady, res.Freeze))
	logger.WithField("epoch", epoch).WithField("notReadyMs", res.NotReady.Milliseconds()).
		WithField("freezeMs", res.Freeze.Milliseconds()).WithField("totalMs", res.Total.Milliseconds()).Info("guest suspended")
	return res, nil
}

// abortSuspend undoes a failed suspend: thaw (in case the freeze got partway), then Running.
func (b *Backend) abortSuspend(ctx context.Context, guest, m *corev1.Pod, epoch int64, cause error) error {
	b.event(guest, corev1.EventTypeWarning, EventSuspendFailed, cause.Error())
	tctx, cancel := context.WithTimeout(ctx, b.opts.Suspend.FreezeTimeout)
	defer cancel()
	if err := b.opts.Suspend.Freezer.Resume(tctx, m, epoch); err != nil && !errors.Is(err, freeze.ErrNoCgroup) {
		return fmt.Errorf("%w; thaw after the failure also failed, guest left %s: %w", cause, StateSuspending, err)
	}
	if m2, err := b.setSuspendState(ctx, guest, "", false); err == nil {
		b.emit(b.translate(guest, m2))
	} else {
		return fmt.Errorf("%w; could not clear %s: %w", cause, StateSuspending, err)
	}
	return cause
}

// Resume thaws a suspended guest and returns it to service:
//  1. raise the epoch and record Resuming (the guest stays NotReady);
//  2. thaw;
//  3. run the ready check (the engine answers its readiness probe);
//  4. clear the suspend state (the guest turns Ready again).
//
// Any failure leaves the guest Resuming, so it is never Ready on a process that did not come back.
func (b *Backend) Resume(ctx context.Context, namespace, name string) (Result, error) {
	start := time.Now()
	so := b.opts.Suspend
	guest, mirrorPod, err := b.lockGuest(namespace, name)
	if err != nil {
		return Result{}, err
	}
	defer b.locks.unlock(guest.UID)
	state, epoch := SuspendState(mirrorPod)
	switch state {
	case "":
		return Result{State: "Running", Epoch: epoch, Noop: true}, nil
	case StateSuspended, StateResuming:
	default:
		return Result{}, errdefs.InvalidInputf("guest %s/%s is %s; suspend it first", namespace, name, state)
	}
	logger := log.G(ctx).WithField("guest", namespace+"/"+name)

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateResuming, true); err != nil {
		return Result{}, err
	}
	_, epoch = SuspendState(mirrorPod)
	b.emit(b.translate(guest, mirrorPod))
	res := Result{State: StateResuming, Epoch: epoch}

	t := time.Now()
	fctx, cancel := context.WithTimeout(ctx, so.FreezeTimeout)
	err = so.Freezer.Resume(fctx, mirrorPod, epoch)
	cancel()
	if err != nil {
		b.event(guest, corev1.EventTypeWarning, EventResumeFailed, "thaw: "+err.Error())
		return res, fmt.Errorf("thaw: %w", err)
	}
	res.Thaw = time.Since(t)

	t = time.Now()
	if so.ReadyCheck != nil {
		rctx, rcancel := context.WithTimeout(ctx, so.ReadyTimeout)
		err = so.ReadyCheck(rctx, guest, mirrorPod)
		rcancel()
		if err != nil {
			b.event(guest, corev1.EventTypeWarning, EventResumeFailed, "ready check: "+err.Error())
			return res, fmt.Errorf("ready check: %w", err)
		}
	}
	res.ReadyCheck = time.Since(t)

	if mirrorPod, err = b.setSuspendState(ctx, guest, "", false); err != nil {
		return res, fmt.Errorf("thawed, but clearing %s failed: %w", StateResuming, err)
	}
	b.emit(b.translate(guest, mirrorPod))
	// Same for a suspend right after this call.
	if err := b.waitInformerState(ctx, guest, "", settleInformerWait); err != nil {
		logger.WithError(err).Debug("informer lags behind Running")
	}
	res.State, res.Total = "Running", time.Since(start)
	b.event(guest, corev1.EventTypeNormal, EventResumed,
		fmt.Sprintf("thawed (epoch %d): thaw %s, ready check %s", epoch, res.Thaw, res.ReadyCheck))
	logger.WithField("epoch", epoch).WithField("thawMs", res.Thaw.Milliseconds()).
		WithField("readyCheckMs", res.ReadyCheck.Milliseconds()).WithField("totalMs", res.Total.Milliseconds()).Info("guest resumed")
	return res, nil
}

// lockGuest finds the guest and its mirror and takes the guest's operation lock.
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func (b *Backend) lockGuest(namespace, name string) (*corev1.Pod, *corev1.Pod, error) {
	if b.opts.Suspend.Freezer == nil {
		return nil, nil, errdefs.InvalidInput("suspend is not configured (no freeze backend)")
	}
	guest, err := b.guests.Pods(namespace).Get(name)
	if err != nil {
		return nil, nil, errdefs.NotFoundf("guest %s/%s not found", namespace, name)
	}
	m, ok := b.mirrorOf(guest)
	if !ok {
		return nil, nil, errdefs.NotFoundf("guest %s/%s has no mirror", namespace, name)
	}
	if !b.locks.tryLock(guest.UID) {
		return nil, nil, ErrBusy
	}
	return guest, m, nil
}

// setSuspendState writes the suspend state (and, with bump, epoch+1) on the guest's mirror by
// compare-and-swap on its resourceVersion, retrying on conflicts. state "" clears it.
func (b *Backend) setSuspendState(ctx context.Context, guest *corev1.Pod, state string, bump bool) (*corev1.Pod, error) {
	pods := b.client.CoreV1().Pods(guest.Namespace)
	var lastErr error
	for range 5 {
		cur, err := pods.Get(ctx, Name(guest.Name), metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get mirror: %w", err)
		}
		if cur.Labels[LabelMirrorOf] != string(guest.UID) {
			return nil, fmt.Errorf("mirror %s/%s no longer belongs to guest %s", cur.Namespace, cur.Name, guest.UID)
		}
		upd := cur.DeepCopy()
		if upd.Annotations == nil {
			upd.Annotations = map[string]string{}
		}
		if bump {
			_, epoch := SuspendState(cur)
			upd.Annotations[AnnotationGuestEpoch] = strconv.FormatInt(epoch+1, 10)
		}
		if state == "" {
			delete(upd.Annotations, AnnotationSuspendState)
			delete(upd.Annotations, AnnotationSuspendStateSince)
		} else {
			upd.Annotations[AnnotationSuspendState] = state
			upd.Annotations[AnnotationSuspendStateSince] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		out, err := pods.Update(ctx, upd, metav1.UpdateOptions{}) // upd carries cur's resourceVersion
		if err == nil {
			return out, nil
		}
		if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("update mirror: %w", err)
		}
		lastErr = err
	}
	return nil, fmt.Errorf("update mirror: %w", lastErr)
}

func (b *Backend) event(guest *corev1.Pod, typ, reason, msg string) {
	if r := b.opts.Suspend.Recorder; r != nil {
		r.Event(guest, typ, reason, msg)
	}
}

// applySuspendState overlays the mirror's suspend state on the translated guest status:
//   - Suspending, Suspended, Resuming: NotReady (MarkNotReady) and condition timeslice.io/suspended;
//   - Suspended also: every running container shows Waiting with reason Suspended, so that
//     kubectl get pods prints 0/1 Suspended. Phase stays Running and restart counts are unchanged.
func applySuspendState(st *corev1.PodStatus, guest, m *corev1.Pod) {
	state, epoch := SuspendState(m)
	if state == "" {
		return
	}
	var since metav1.Time
	if t, err := time.Parse(time.RFC3339Nano, m.Annotations[AnnotationSuspendStateSince]); err == nil {
		since = metav1.NewTime(t)
	}
	msg := fmt.Sprintf("guest-kubelet: %s (epoch %d)", state, epoch)
	MarkNotReady(st, guest.Status.Conditions, state, msg, since)
	for i := range st.ContainerStatuses {
		st.ContainerStatuses[i].Ready = false
	}
	cond := corev1.PodCondition{
		Type: ConditionSuspended, Status: corev1.ConditionFalse, Reason: state, Message: msg, LastTransitionTime: since,
	}
	if state == StateSuspended {
		cond.Status = corev1.ConditionTrue
		for i := range st.ContainerStatuses {
			cs := &st.ContainerStatuses[i]
			if cs.State.Running == nil {
				continue
			}
			cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: StateSuspended, Message: "frozen by guest-kubelet since " + since.UTC().Format(time.RFC3339),
			}}
			cs.Started = new(false)
		}
	}
	st.Conditions = append(st.Conditions, cond)
}

// settleInformerWait bounds how long a finished suspend or resume waits for the mirror informer
// to show its final state. The next call reads the state from the informer: without the wait, a
// resume issued right after a suspend could be refused as still Suspending.
const settleInformerWait = time.Second

// waitInformerState waits until the mirror informer holds the guest's mirror in state.
func (b *Backend) waitInformerState(ctx context.Context, guest *corev1.Pod, state string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if m, ok := b.mirrorOf(guest); ok {
			if got, _ := SuspendState(m); got == state {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("mirror informer never showed %s: %w", state, ctx.Err())
		case <-tick.C:
		}
	}
}
