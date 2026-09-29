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
	// Suspended it is the time the agent reported the job suspended.
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
	EventKilled        = "Killed"
)

// SuspendOptions configures suspend and resume through the snapshot agent (M4). Agent nil
// disables both.
type SuspendOptions struct {
	// Agent is the node's snapshot agent. The guest kubelet never freezes a process itself.
	Agent freeze.Agent
	// NotReadyTimeout bounds the wait for the guest's Ready=False to be visible in the API
	// before the agent is called.
	NotReadyTimeout time.Duration
	// NoticeWindow (N) and KillBudget (K) come from the demo config (pending lead decision;
	// defaults 30 s and 3 s). A suspend or resume the caller gives no deadline gets
	// now + N - K; the kill sequence gives the agent K to confirm its Kill.
	NoticeWindow time.Duration
	KillBudget   time.Duration
	// CheckpointEstimate and RestoreEstimate are one guest's suspend and resume times on this
	// GPU. A new mirror is admitted only while one restore plus the checkpoints of every mirror
	// (the new one included) fit N - K (Q6). Zero turns the check off.
	CheckpointEstimate time.Duration
	RestoreEstimate    time.Duration
	// AgentStatusPoll is how often the agent's Status is read to keep the hold (and the
	// Suspended state of each guest) in line with the agent. Zero turns it off.
	AgentStatusPoll time.Duration
	// ReadyCheck confirms, after the resume, that the engine serves again; ReadyTimeout bounds it.
	ReadyCheck   ReadyCheck
	ReadyTimeout time.Duration
	// Recorder records Suspended/Resumed/Killed events on the guest. Optional.
	Recorder record.EventRecorder
	// OnHoldChange, if set, is called when a guest turns Suspending (before NotReady is
	// confirmed), when a suspend is rolled back, when a resume starts, when a guest is killed
	// and when the agent's Status changes the hold: the answer of Hold may have changed. It
	// must not block (the ns-cordon option of --cordon-while-held pokes its loop).
	OnHoldChange func()
}

// Result reports what a suspend or resume did and how long each step took.
type Result struct {
	State string `json:"state"`
	Epoch int64  `json:"epoch"`
	// Deadline is the absolute deadline given to the agent.
	Deadline time.Time `json:"deadline,omitzero"`
	// NotReady is the time to confirm NotReady (suspend). Agent is the agent call, from the
	// first send to the end of its operation. ReadyCheck is the engine check (resume).
	NotReady   time.Duration `json:"notReady,omitempty"`
	Agent      time.Duration `json:"agent,omitempty"`
	ReadyCheck time.Duration `json:"readyCheck,omitempty"`
	Total      time.Duration `json:"total"`
	// Outcome is the agent's outcome (SUSPENDED, RELEASED, RESUMED).
	Outcome string `json:"outcome,omitempty"`
	// Noop is true when the guest was already in the requested state.
	Noop bool `json:"noop,omitempty"`
	// Killed is set when the call ended in the kill sequence (or, for a guest with no
	// accelerator context, a plain delete); the guest is then Failed.
	Killed string `json:"killed,omitempty"`
}

// ErrBusy means another suspend or resume of the same guest is running.
var ErrBusy = errors.New("another suspend or resume of this guest is in flight")

// guestLocks allows one agent call per guest at a time. seq counts the unlocks, so the agent
// status loop can tell that an operation finished while it read the agent.
type guestLocks struct {
	mu    sync.Mutex
	inUse map[types.UID]bool
	seq   uint64
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
	l.seq++
}

func (l *guestLocks) unlocks() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
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

// deadlineOr is the given agent deadline, or now + N - K.
func (so *SuspendOptions) deadlineOr(deadline time.Time) time.Time {
	if !deadline.IsZero() {
		return deadline
	}
	return time.Now().Add(so.NoticeWindow - so.KillBudget)
}

// waitCtx bounds the wait for an agent operation: its deadline plus the RPC time for the
// agent's FAILED answer to arrive. Past it the guest kubelet stops waiting (a deadline miss).
func waitCtx(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, deadline.Add(agentAnswerGrace))
}

// agentAnswerGrace is how long past its deadline the guest kubelet waits for the agent's
// answer. The agent fails the operation at the deadline itself.
const agentAnswerGrace = time.Second

// Suspend takes a running guest out of service and has the snapshot agent suspend it (Q6):
//  1. raise the epoch by compare-and-swap and record Suspending on the mirror (the guest turns
//     NotReady);
//  2. wait until the API shows the guest NotReady, so its endpoint is withdrawn;
//  3. read the agent's Status: a job it does not list has no accelerator context yet, so the
//     mirror is deleted instead (normal grace) and the guest ends Failed;
//  4. agent Suspend with the epoch and the deadline (zero: now + N - K), then GetOperation
//     until it ends;
//  5. record Suspended (the guest shows container state Waiting, reason Suspended).
//
// If step 2 fails nothing was frozen: the guest is put back to Running. From step 3 on, any
// agent failure, deadline miss or Unimplemented runs the kill sequence (agent Kill, delete the
// mirror with normal grace, the guest Failed); a guest is never left running on a process the
// agent may have half frozen.
func (b *Backend) Suspend(ctx context.Context, namespace, name string, deadline time.Time) (Result, error) {
	start := time.Now()
	so := &b.opts.Suspend
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
	job := mirrorPod.Labels[LabelJobID]
	if job == "" {
		return Result{}, errdefs.InvalidInputf("mirror of %s/%s has no %s label", namespace, name, LabelJobID)
	}
	deadline = so.deadlineOr(deadline)
	logger := log.G(ctx).WithField("guest", namespace+"/"+name).WithField("job", job)

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateSuspending, bumpEpoch); err != nil {
		return Result{}, err
	}
	_, epoch = SuspendState(mirrorPod)
	b.emit(b.translate(guest, mirrorPod))
	res := Result{State: StateSuspending, Epoch: epoch, Deadline: deadline}

	stepStart := time.Now()
	// The informer must hold the Suspending mirror first, or a late event for the previous
	// mirror version could re-emit Ready after the check below has passed.
	if err := b.waitInformerState(ctx, guest, StateSuspending, so.NotReadyTimeout); err != nil {
		return res, b.abortSuspend(ctx, guest, err)
	}
	if so.OnHoldChange != nil {
		so.OnHoldChange() // the informer already shows Suspending: cordon before the agent call
	}
	if err := b.WaitNotReady(ctx, guest, so.NotReadyTimeout); err != nil {
		return res, b.abortSuspend(ctx, guest, fmt.Errorf("confirm NotReady: %w", err))
	}
	res.NotReady = time.Since(stepStart)

	stepStart = time.Now()
	wctx, cancel := waitCtx(ctx, deadline)
	defer cancel()
	jobs, err := so.Agent.Jobs(wctx)
	if err != nil {
		res.Killed = b.killGuest(ctx, guest, mirrorPod, "agent Status failed before Suspend: "+err.Error())
		return res, fmt.Errorf("agent status: %w", err)
	}
	if _, listed := jobs[job]; !listed {
		res.Killed = b.deleteNoContext(ctx, guest, mirrorPod)
		return res, nil
	}
	outcome, err := so.Agent.Suspend(wctx, job, epoch, deadline)
	res.Agent = time.Since(stepStart)
	if err != nil {
		res.Killed = b.killGuest(ctx, guest, mirrorPod, "agent Suspend failed: "+err.Error())
		return res, fmt.Errorf("agent suspend: %w", err)
	}
	res.Outcome = outcome

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateSuspended, keepEpoch); err != nil {
		// Suspended, but not recorded. The guest stays NotReady (Suspending) and held; a retry
		// (same state, next epoch) finishes it.
		return res, fmt.Errorf("suspended, but recording Suspended failed: %w", err)
	}
	b.emit(b.translate(guest, mirrorPod))
	// A resume right after this call reads the state from the informer: wait (bounded) until it
	// shows Suspended, so the resume is not refused as still Suspending.
	if err := b.waitInformerState(ctx, guest, StateSuspended, holdInformerWait); err != nil {
		logger.WithError(err).Debug("informer lags behind Suspended")
	}
	res.State, res.Total = StateSuspended, time.Since(start)
	b.event(guest, corev1.EventTypeNormal, EventSuspended,
		fmt.Sprintf("suspended by the snapshot agent (epoch %d, %s): NotReady confirmed in %s, agent %s",
			epoch, outcome, res.NotReady, res.Agent))
	logger.WithField("epoch", epoch).WithField("outcome", outcome).WithField("notReadyMs", res.NotReady.Milliseconds()).
		WithField("agentMs", res.Agent.Milliseconds()).WithField("totalMs", res.Total.Milliseconds()).Info("guest suspended")
	return res, nil
}

// abortSuspend undoes a suspend that failed before the agent was called: back to Running.
func (b *Backend) abortSuspend(ctx context.Context, guest *corev1.Pod, cause error) error {
	b.event(guest, corev1.EventTypeWarning, EventSuspendFailed, cause.Error())
	m2, err := b.setSuspendState(ctx, guest, "", keepEpoch)
	if err != nil {
		return fmt.Errorf("%w; could not clear %s: %w", cause, StateSuspending, err)
	}
	b.emit(b.translate(guest, m2))
	b.holdChanged(ctx, guest, "")
	return cause
}

// Resume has the snapshot agent resume a suspended guest and returns it to service (Q6):
//  1. raise the epoch by compare-and-swap and record Resuming (the guest stays NotReady);
//  2. agent Resume with the epoch and the deadline (zero: now + N - K), one guest at a time;
//  3. run the ready check (the engine answers its readiness probe);
//  4. clear the suspend state (the guest turns Ready again).
//
// A failure in step 2 or 3 runs the kill sequence: the guest is never Ready on a process that
// did not come back.
func (b *Backend) Resume(ctx context.Context, namespace, name string, deadline time.Time) (Result, error) {
	start := time.Now()
	so := &b.opts.Suspend
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
	job := mirrorPod.Labels[LabelJobID]
	if job == "" {
		return Result{}, errdefs.InvalidInputf("mirror of %s/%s has no %s label", namespace, name, LabelJobID)
	}
	// Resume one at a time (Q6): the restore budget in N - K counts one restore.
	b.resumeMu.Lock()
	defer b.resumeMu.Unlock()
	deadline = so.deadlineOr(deadline)
	logger := log.G(ctx).WithField("guest", namespace+"/"+name).WithField("job", job)

	if mirrorPod, err = b.setSuspendState(ctx, guest, StateResuming, bumpEpoch); err != nil {
		return Result{}, err
	}
	_, epoch = SuspendState(mirrorPod)
	b.emit(b.translate(guest, mirrorPod))
	b.holdChanged(ctx, guest, StateResuming)
	res := Result{State: StateResuming, Epoch: epoch, Deadline: deadline}

	stepStart := time.Now()
	wctx, cancel := waitCtx(ctx, deadline)
	err = so.Agent.Resume(wctx, job, epoch, deadline)
	cancel()
	res.Agent = time.Since(stepStart)
	if err != nil {
		res.Killed = b.killGuest(ctx, guest, mirrorPod, "agent Resume failed: "+err.Error())
		return res, fmt.Errorf("agent resume: %w", err)
	}
	res.Outcome = freeze.OutcomeResumed

	if err := b.readyCheck(ctx, guest, mirrorPod, &res); err != nil {
		res.Killed = b.killGuest(ctx, guest, mirrorPod, "engine did not serve after the resume: "+err.Error())
		return res, fmt.Errorf("ready check: %w", err)
	}

	if mirrorPod, err = b.setSuspendState(ctx, guest, "", keepEpoch); err != nil {
		return res, fmt.Errorf("resumed, but clearing %s failed: %w", StateResuming, err)
	}
	b.emit(b.translate(guest, mirrorPod))
	// Same for a suspend right after this call.
	if err := b.waitInformerState(ctx, guest, "", holdInformerWait); err != nil {
		logger.WithError(err).Debug("informer lags behind Running")
	}
	res.State, res.Total = "Running", time.Since(start)
	b.event(guest, corev1.EventTypeNormal, EventResumed,
		fmt.Sprintf("resumed by the snapshot agent (epoch %d): agent %s, ready check %s", epoch, res.Agent, res.ReadyCheck))
	logger.WithField("epoch", epoch).WithField("agentMs", res.Agent.Milliseconds()).
		WithField("readyCheckMs", res.ReadyCheck.Milliseconds()).WithField("totalMs", res.Total.Milliseconds()).Info("guest resumed")
	return res, nil
}

func (b *Backend) readyCheck(ctx context.Context, guest, mirrorPod *corev1.Pod, res *Result) error {
	so := &b.opts.Suspend
	if so.ReadyCheck == nil {
		return nil
	}
	stepStart := time.Now()
	rctx, cancel := context.WithTimeout(ctx, so.ReadyTimeout)
	defer cancel()
	err := so.ReadyCheck(rctx, guest, mirrorPod)
	res.ReadyCheck = time.Since(stepStart)
	return err
}

// lockGuest finds the guest and its mirror and takes the guest's operation lock.
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func (b *Backend) lockGuest(namespace, name string) (*corev1.Pod, *corev1.Pod, error) {
	if b.opts.Suspend.Agent == nil {
		return nil, nil, errdefs.InvalidInput("suspend is not configured (no snapshot agent)")
	}
	guest, err := b.guests.Pods(namespace).Get(name)
	if err != nil {
		return nil, nil, errdefs.NotFoundf("guest %s/%s not found", namespace, name)
	}
	m, ok := b.mirrorOf(guest)
	if !ok {
		return nil, nil, errdefs.NotFoundf("guest %s/%s has no mirror", namespace, name)
	}
	if killStarted(m) || m.DeletionTimestamp != nil {
		return nil, nil, errdefs.InvalidInputf("guest %s/%s is being killed or deleted", namespace, name)
	}
	if !b.locks.tryLock(guest.UID) {
		return nil, nil, ErrBusy
	}
	return guest, m, nil
}

// How setSuspendState moves the epoch.
const (
	keepEpoch int64 = 0
	bumpEpoch int64 = -1
)

// setSuspendState writes the suspend state on the guest's mirror by compare-and-swap on its
// resourceVersion, retrying on conflicts. state "" clears it. epoch is keepEpoch, bumpEpoch
// (epoch+1) or a value to write (a host-level call's epoch).
func (b *Backend) setSuspendState(ctx context.Context, guest *corev1.Pod, state string, epoch int64) (*corev1.Pod, error) {
	return b.mutateMirror(ctx, guest, func(cur *corev1.Pod, a map[string]string) {
		switch epoch {
		case keepEpoch:
		case bumpEpoch:
			_, e := SuspendState(cur)
			a[AnnotationGuestEpoch] = strconv.FormatInt(e+1, 10)
		default:
			a[AnnotationGuestEpoch] = strconv.FormatInt(epoch, 10)
		}
		if state == "" {
			delete(a, AnnotationSuspendState)
			delete(a, AnnotationSuspendStateSince)
		} else {
			a[AnnotationSuspendState] = state
			a[AnnotationSuspendStateSince] = time.Now().UTC().Format(time.RFC3339Nano)
		}
	})
}

// mutateMirror applies fn to the annotations of the guest's current mirror and writes them by
// compare-and-swap on its resourceVersion, retrying on conflicts.
func (b *Backend) mutateMirror(
	ctx context.Context, guest *corev1.Pod, fn func(cur *corev1.Pod, a map[string]string),
) (*corev1.Pod, error) {
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
		fn(cur, upd.Annotations)
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
				Reason: StateSuspended, Message: "suspended by the snapshot agent since " + since.UTC().Format(time.RFC3339),
			}}
			cs.Started = new(false)
		}
	}
	st.Conditions = append(st.Conditions, cond)
}

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
