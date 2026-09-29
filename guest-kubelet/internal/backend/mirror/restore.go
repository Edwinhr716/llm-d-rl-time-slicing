package mirror

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// Epoch arguments of SetMirrorSuspendState besides an explicit value (> 0).
const (
	// EpochKeep leaves timeslice.io/guest-epoch as it is.
	EpochKeep int64 = -1
	// EpochBump raises timeslice.io/guest-epoch by one.
	EpochBump int64 = -2
)

// AnnotationVacated marks a mirror deleted to vacate the accelerator (not killed), so the guest
// is reported vacated, not failed, even when the delete is seen only after a restart (M5).
const AnnotationVacated = "timeslice.io/vacated"

// SetMirrorSuspendState records a suspend state on the mirror m by compare-and-swap ("" clears
// it) and sets its guest epoch in the same write: epoch > 0 writes that value, EpochBump raises
// it by one, EpochKeep leaves it. It returns the updated mirror and its epoch. The host command
// server calls it around every freeze and thaw, so the state survives a guest-kubelet restart
// (M5), the guest shows Suspended (applySuspendState) and Hold sees it.
func (b *Backend) SetMirrorSuspendState(
	ctx context.Context, m *corev1.Pod, state string, epoch int64,
) (*corev1.Pod, int64, error) {
	var out int64
	upd, err := b.updateMirror(ctx, m, func(cur *corev1.Pod) error {
		_, prev := SuspendState(cur)
		switch {
		case epoch > 0:
			out = epoch
		case epoch == EpochBump:
			out = prev + 1
		default:
			out = prev
		}
		cur.Annotations[AnnotationGuestEpoch] = strconv.FormatInt(out, 10)
		if state == "" {
			delete(cur.Annotations, AnnotationSuspendState)
			delete(cur.Annotations, AnnotationSuspendStateSince)
			return nil
		}
		if cur.Annotations[AnnotationSuspendState] != state {
			cur.Annotations[AnnotationSuspendStateSince] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		cur.Annotations[AnnotationSuspendState] = state
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if f := b.opts.Suspend.OnHoldChange; f != nil {
		f() // must not block; the cordon loop re-reads Hold from the informer
	}
	return upd, out, nil
}

// JobAttempt returns the attempt counter in a mirror's job id ("<guest uid>-<attempt>").
func JobAttempt(m *corev1.Pod) (int, bool) {
	id := m.Labels[LabelJobID]
	uid := m.Labels[LabelMirrorOf]
	if uid == "" || !strings.HasPrefix(id, uid+"-") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(id, uid+"-"))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// Attempts returns a copy of the host-command mode's attempt counters (mirrors created per
// guest), for the host command journal.
func (b *Backend) Attempts() map[types.UID]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[types.UID]int, len(b.gate.attempts))
	for k, v := range b.gate.attempts {
		out[k] = v
	}
	return out
}

// RestoreAttempts rebuilds the attempt counters after a restart (M5), so that no job id is used
// twice. A guest with a mirror keeps the mirror's attempt (the agent knows the job by it). A
// guest without one gets one more than the journal recorded: the journal is written after each
// command, so it can lag by at most the one mirror deleted before a crash. Call it after Start
// and before any mirror is created.
func (b *Backend) RestoreAttempts(guests []Guest, saved map[types.UID]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, g := range guests {
		uid := g.Pod.UID
		if g.Mirror != nil {
			if n, ok := JobAttempt(g.Mirror); ok {
				b.gate.attempts[uid] = max(n, saved[uid])
				continue
			}
		}
		b.gate.attempts[uid] = saved[uid] + 1
	}
}

// RestoreGate sets a guest's Ready hold as the host command server re-derived it after a
// restart: released lets Ready follow the mirror; otherwise Ready is held with reason.
func (b *Backend) RestoreGate(guest *corev1.Pod, released bool, reason string) {
	b.mu.Lock()
	if released {
		b.gate.released[guest.UID] = true
		delete(b.gate.reason, guest.UID)
	} else {
		delete(b.gate.released, guest.UID)
		b.gate.reason[guest.UID] = reason
	}
	b.mu.Unlock()
}

// Reconciled is one suspend state ReconcileSuspend changed.
type Reconciled struct {
	Mirror   string `json:"mirror"`
	Recorded string `json:"recorded"` // "" = Running
	Frozen   bool   `json:"frozen"`
	Now      string `json:"now"`
}

// ReconcileSuspend makes each mirror's recorded suspend state agree with the freezer after a
// restart (M5, M3 path): the host wins. A frozen mirror is Suspended whatever was recorded (a
// crash between the freeze and the write left Suspending; a crash before the thaw left
// Resuming). A running mirror recorded Suspending or Resuming was never frozen or already thawed:
// its state is cleared and its Ready follows the readiness probe again. A running mirror
// recorded Suspended (thawed behind our back) is cleared too. Mirrors without a cgroup (not
// started, or gone) are left alone. Call it after Start, before the pod controller runs, and
// only when this process owns suspend (not in host-command mode, whose freezer is elsewhere).
func (b *Backend) ReconcileSuspend(ctx context.Context) ([]Reconciled, error) {
	fz := b.opts.Suspend.Freezer
	if fz == nil {
		return nil, nil
	}
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list mirrors: %w", err)
	}
	var out []Reconciled
	var errs []error
	for _, mp := range ms {
		if mp.DeletionTimestamp != nil || mp.Status.Phase != corev1.PodRunning {
			continue
		}
		recorded, _ := SuspendState(mp)
		frozen, ferr := fz.Frozen(mp)
		if ferr != nil {
			if !errors.Is(ferr, freeze.ErrNoCgroup) {
				errs = append(errs, fmt.Errorf("mirror %s/%s: %w", mp.Namespace, mp.Name, ferr))
			}
			continue
		}
		want := ""
		if frozen {
			want = StateSuspended
		}
		if want == recorded {
			continue
		}
		if _, _, err := b.SetMirrorSuspendState(ctx, mp, want, EpochKeep); err != nil {
			errs = append(errs, fmt.Errorf("mirror %s/%s: %w", mp.Namespace, mp.Name, err))
			continue
		}
		r := Reconciled{Mirror: mp.Namespace + "/" + mp.Name, Recorded: recorded, Frozen: frozen, Now: want}
		log.G(ctx).WithField("mirror", r.Mirror).WithField("recorded", recorded).WithField("frozen", frozen).
			WithField("now", want).Warn("suspend state reconciled with the freezer (the host wins)")
		out = append(out, r)
	}
	return out, errors.Join(errs...)
}

// Kill sequence annotations (M4, Q6). The kill sequence writes AnnotationKilling (its cause)
// before the agent's Kill and AnnotationKilled (the message) before it deletes the mirror.
// Relist reads them to finish a kill sequence a restart interrupted (FinishKills).
const (
	AnnotationKilling = "timeslice.io/killing"
	AnnotationKilled  = "timeslice.io/killed"
)

// KillFunc asks the snapshot agent to kill one job, bounded by ctx.
type KillFunc func(ctx context.Context, jobID, reason string) error

// FinishedKill is one kill sequence FinishKills finished after a restart.
type FinishedKill struct {
	Mirror string `json:"mirror"`
	Cause  string `json:"cause"`
	// Agent is what the repeated agent Kill did: ok, skipped (no agent, or the mirror has
	// stopped) or the error.
	Agent string `json:"agent"`
}

// FinishKills finishes the kill sequences a restart interrupted (M5). A mirror marked
// AnnotationKilling but not yet deleted had its kill sequence cut short somewhere between the
// mark and the delete: the agent's Kill may or may not have run. The sequence is repeated from
// the top, which is safe because each step is idempotent: agent Kill (no epoch, works from any
// state; skipped when kill is nil or the mirror has stopped), record AnnotationKilled, delete
// the mirror with normal grace (never force). The guest then fails when the delete is seen,
// like any killed guest. A mirror already being deleted needs nothing: the delete is the last
// step. Errors are returned joined; a mirror left marked keeps killStarted true (M4), so no
// suspend or resume touches it, and the next restart tries again.
func (b *Backend) FinishKills(
	ctx context.Context, guests []Guest, kill KillFunc, budget time.Duration,
) ([]FinishedKill, error) {
	var out []FinishedKill
	var errs []error
	for _, g := range guests {
		mp := g.Mirror
		if mp == nil || mp.DeletionTimestamp != nil {
			continue
		}
		cause := mp.Annotations[AnnotationKilling]
		if cause == "" {
			continue
		}
		name := mp.Namespace + "/" + mp.Name
		fk := FinishedKill{Mirror: name, Cause: cause, Agent: "skipped"}
		job := mp.Labels[LabelJobID]
		stopped := mp.Status.Phase == corev1.PodFailed || mp.Status.Phase == corev1.PodSucceeded
		if kill != nil && job != "" && !stopped {
			kctx, cancel := context.WithTimeout(ctx, budget)
			if err := kill(kctx, job, cause+" (finished after a guest-kubelet restart)"); err != nil {
				fk.Agent = err.Error()
			} else {
				fk.Agent = "ok"
			}
			cancel()
		}
		msg := mp.Annotations[AnnotationKilled]
		if msg == "" {
			msg = "killed after " + cause
			upd, err := b.updateMirror(ctx, mp, func(cur *corev1.Pod) error {
				if cur.Annotations[AnnotationKilled] == "" {
					cur.Annotations[AnnotationKilled] = msg
				}
				return nil
			})
			switch {
			case errors.Is(err, ErrMirrorReplaced) || apierrors.IsNotFound(err):
				continue // gone or replaced since the relist: nothing left to finish
			case err != nil:
				errs = append(errs, fmt.Errorf("mirror %s: record the kill: %w", name, err))
				continue
			}
			mp = upd
		}
		uid := mp.UID
		err := b.client.CoreV1().Pods(mp.Namespace).Delete(ctx, mp.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			errs = append(errs, fmt.Errorf("mirror %s: delete: %w", name, err))
			continue
		}
		log.G(ctx).WithField("mirror", name).WithField("cause", cause).WithField("agentKill", fk.Agent).
			Warn("kill sequence interrupted by a restart finished (M5)")
		out = append(out, fk)
	}
	return out, errors.Join(errs...)
}
