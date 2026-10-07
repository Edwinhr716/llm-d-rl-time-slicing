package mirror

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// holdInformerWait bounds how long a suspend-state change waits for the mirror informer before
// it pokes OnHoldChange. The cordon loop's own poll covers a slower informer.
const holdInformerWait = time.Second

// agentView is the last answer of the agent's Status.
type agentView struct {
	jobs map[string]freeze.Job
	at   time.Time
	err  error
}

// Hold reports whether the donor holds the GPU: at least one guest of this virtual Node is
// Suspending or Suspended on its mirror, or is SUSPENDED in the snapshot agent's last Status
// (M4: the agent's state is the truth about the process; the mirror records what the guest
// kubelet asked for). Resuming is not held unless the agent still reports the job SUSPENDED.
// A killed mirror counts until it is gone. It feeds the ns-cordon option of
// --cordon-while-held (pending lead decision D-NS-8).
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func (b *Backend) Hold() (bool, string) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return false, "mirror list failed: " + err.Error()
	}
	b.mu.Lock()
	jobs := b.agent.jobs
	b.mu.Unlock()
	n, agentOnly := 0, 0
	for _, m := range ms {
		switch state, _ := SuspendState(m); state {
		case StateSuspending, StateSuspended:
			n++
		default:
			if j, ok := jobs[m.Labels[LabelJobID]]; ok && m.Labels[LabelJobID] != "" && j.State == freeze.JobSuspended {
				n++
				agentOnly++
			}
		}
	}
	if n == 0 {
		return false, "no guest suspended"
	}
	if agentOnly > 0 {
		return true, fmt.Sprintf("%d guest(s) suspended (%d per the snapshot agent only)", n, agentOnly)
	}
	return true, fmt.Sprintf("%d guest(s) suspended", n)
}

// holdChanged tells OnHoldChange that a guest's suspend state became state, once the mirror
// informer shows it (at most holdInformerWait), so Hold already sees the new answer.
func (b *Backend) holdChanged(ctx context.Context, guest *corev1.Pod, state string) {
	f := b.opts.Suspend.OnHoldChange
	if f == nil {
		return
	}
	if err := b.waitInformerState(ctx, guest, state, holdInformerWait); err != nil {
		log.G(ctx).WithError(err).Debug("hold poke before the informer caught up; the cordon poll repairs it")
	}
	f()
}

// waitHoldInformer waits (at most holdInformerWait) until the mirror informer shows state, so a
// hold poke after it already sees the new answer. A slower informer is repaired by the cordon poll.
func (b *Backend) waitHoldInformer(ctx context.Context, guest *corev1.Pod, state string) {
	if err := b.waitInformerState(ctx, guest, state, holdInformerWait); err != nil {
		log.G(ctx).WithError(err).Debug("mirror informer behind; the cordon poll repairs the hold")
	}
}

// AgentJobs returns the agent's last Status, when it was read and the last read error.
func (b *Backend) AgentJobs() (map[string]freeze.Job, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return maps.Clone(b.agent.jobs), b.agent.at, b.agent.err
}

// agentStatusLoop reads the agent's Status every AgentStatusPoll, so the hold follows the
// agent's state, and repairs a mirror whose job the agent reports SUSPENDED while no suspend
// or resume of it runs (a guest kubelet that stopped between the agent's answer and its own
// write): the mirror is recorded Suspended, which keeps the guest NotReady.
func (b *Backend) agentStatusLoop(ctx context.Context) {
	t := time.NewTicker(b.opts.Suspend.AgentStatusPoll)
	defer t.Stop()
	for {
		b.refreshAgentState(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (b *Backend) refreshAgentState(ctx context.Context) {
	so := &b.opts.Suspend
	seq := b.locks.unlocks()
	rctx, cancel := context.WithTimeout(ctx, so.AgentStatusPoll)
	jobs, err := so.Agent.Jobs(rctx)
	cancel()
	b.mu.Lock()
	b.agent.err = err
	var changed bool
	if err == nil {
		changed = suspendedSet(b.agent.jobs) != suspendedSet(jobs)
		b.agent.jobs, b.agent.at = jobs, time.Now()
	}
	b.mu.Unlock()
	if err != nil {
		log.G(ctx).WithError(err).Debug("agent status read failed; the hold keeps the last answer")
		return
	}
	b.reconcileAgentState(ctx, jobs, seq)
	if changed && so.OnHoldChange != nil {
		so.OnHoldChange()
	}
}

// suspendedSet is a comparable summary of which jobs are SUSPENDED.
func suspendedSet(jobs map[string]freeze.Job) string {
	ids := make([]string, 0, len(jobs))
	for id, j := range jobs {
		if j.State == freeze.JobSuspended {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return fmt.Sprint(ids)
}

// reconcileAgentState records Suspended on at most one mirror per Status read: one whose job
// the agent reports SUSPENDED while the mirror shows Running or Suspending and no operation
// ran since the Status was read (seq), so the answer is not stale.
func (b *Backend) reconcileAgentState(ctx context.Context, jobs map[string]freeze.Job, seq uint64) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return
	}
	for _, m := range ms {
		job := m.Labels[LabelJobID]
		if j, ok := jobs[job]; job == "" || !ok || j.State != freeze.JobSuspended {
			continue
		}
		state, _ := SuspendState(m)
		if state != "" && state != StateSuspending {
			continue
		}
		guest := b.guestFor(m)
		if guest == nil || m.DeletionTimestamp != nil || killStarted(m) {
			continue
		}
		if !b.locks.tryLock(guest.UID) {
			continue
		}
		if b.locks.unlocks() != seq {
			b.locks.unlock(guest.UID)
			return // an operation finished since the read: wait for the next one
		}
		if cur, ok := b.mirrorOf(guest); ok {
			if st, _ := SuspendState(cur); st == state {
				if m2, err := b.setSuspendState(ctx, guest, StateSuspended, keepEpoch); err == nil {
					b.emit(b.translate(guest, m2))
					b.event(guest, corev1.EventTypeWarning, EventSuspended,
						"the snapshot agent reports the job SUSPENDED; recorded Suspended on the mirror")
					log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("job", job).
						Warn("mirror recorded Suspended from the agent's state")
				}
			}
		}
		b.locks.unlock(guest.UID)
		return
	}
}
