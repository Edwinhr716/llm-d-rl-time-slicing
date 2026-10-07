package mirror

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// HostResult reports a host-level suspend or resume (D-NS-5 ns-host): one agent operation for
// every background guest of the node.
type HostResult struct {
	Epoch    int64         `json:"epoch"`
	Deadline time.Time     `json:"deadline"`
	Agent    time.Duration `json:"agent,omitempty"`
	Total    time.Duration `json:"total"`
	// Guests by namespace/name.
	Guests map[string]Result `json:"guests"`
	// Error is the host-level failure, if any; the guests it hit were killed.
	Error string `json:"error,omitempty"`
}

// hostGuest is one guest taking part in a host-level call.
type hostGuest struct {
	guest, mirror *corev1.Pod
	job           string
	res           Result
	done          bool // finished (noop, killed or no context): no further step applies
}

func (item *hostGuest) key() string { return item.guest.Namespace + "/" + item.guest.Name }

// lockAll takes the operation lock of every live guest of this virtual Node whose mirror has a
// job id, and returns them sorted by name. If one is busy it takes none and returns ErrBusy.
func (b *Backend) lockAll() ([]*hostGuest, error) {
	if b.opts.Suspend.Agent == nil {
		return nil, errdefs.InvalidInput("suspend is not configured (no snapshot agent)")
	}
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list mirrors: %w", err)
	}
	var out []*hostGuest
	for _, m := range ms {
		g := b.guestFor(m)
		if g == nil || m.DeletionTimestamp != nil || killStarted(m) || m.Labels[LabelJobID] == "" {
			continue
		}
		if !b.locks.tryLock(g.UID) {
			b.unlockAll(out)
			return nil, ErrBusy
		}
		out = append(out, &hostGuest{guest: g, mirror: m, job: m.Labels[LabelJobID]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out, nil
}

func (b *Backend) unlockAll(hs []*hostGuest) {
	for _, item := range hs {
		b.locks.unlock(item.guest.UID)
	}
}

// hostEpoch is the epoch of the next host-level call: above every guest's epoch and every host
// epoch used before, so the agent's host fence and each job's fence accept it.
func (b *Backend) nextHostEpoch(hs []*hostGuest) int64 {
	b.mu.Lock()
	e := b.hostEpoch
	b.mu.Unlock()
	for _, item := range hs {
		if _, ge := SuspendState(item.mirror); ge > e {
			e = ge
		}
	}
	return e + 1
}

func (b *Backend) setHostEpoch(e int64) {
	b.mu.Lock()
	if e > b.hostEpoch {
		b.hostEpoch = e
	}
	b.mu.Unlock()
}

// SuspendAll vacates the host with one agent SuspendAll (D-NS-5 ns-host): every running guest
// turns Suspending with the host epoch (CAS on each mirror) and its NotReady is confirmed; a
// guest the agent does not list is deleted instead; then the agent suspends every background
// job of the node by the deadline (zero: now + N - K). A guest whose target did not reach
// SUSPENDED or RELEASED, and every guest when the call as a whole failed, gets the kill
// sequence. A STALE_EPOCH refusal is retried once with the agent's last epoch plus one.
func (b *Backend) SuspendAll(ctx context.Context, deadline time.Time) (HostResult, error) {
	start := time.Now()
	so := &b.opts.Suspend
	b.hostMu.Lock()
	defer b.hostMu.Unlock()
	hs, err := b.lockAll()
	if err != nil {
		return HostResult{}, err
	}
	defer b.unlockAll(hs)
	deadline = so.deadlineOr(deadline)
	epoch := b.nextHostEpoch(hs)
	out := HostResult{Epoch: epoch, Deadline: deadline, Guests: map[string]Result{}}
	logger := log.G(ctx).WithField("epoch", epoch).WithField("guests", len(hs))

	// 1. Suspending with the host epoch, then NotReady confirmed.
	for _, item := range hs {
		state, _ := SuspendState(item.mirror)
		item.res = Result{Epoch: epoch, Deadline: deadline}
		target := StateSuspending
		if state == StateSuspended {
			target = StateSuspended // already suspended: only the epoch moves
		}
		m, err := b.setSuspendState(ctx, item.guest, target, epoch)
		if err != nil {
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, "could not record Suspending: "+err.Error())
			item.done = true
			continue
		}
		item.mirror, item.res.State = m, target
		b.emit(b.translate(item.guest, m))
	}
	for _, item := range hs {
		if item.done {
			continue
		}
		if err := b.waitInformerState(ctx, item.guest, item.res.State, so.NotReadyTimeout); err != nil {
			logger.WithError(err).Debug("informer lags behind Suspending")
		}
	}
	if so.OnHoldChange != nil {
		so.OnHoldChange()
	}
	for _, item := range hs {
		if item.done || item.res.State == StateSuspended {
			continue
		}
		t := time.Now()
		if err := b.WaitNotReady(ctx, item.guest, so.NotReadyTimeout); err != nil {
			// The host must be vacated: a guest whose NotReady cannot be confirmed is killed,
			// not left serving.
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, "NotReady not confirmed before SuspendAll: "+err.Error())
			item.done = true
			continue
		}
		item.res.NotReady = time.Since(t)
	}

	// 2. Only the jobs the agent lists are suspended.
	wctx, cancel := waitCtx(ctx, deadline)
	defer cancel()
	jobs, err := so.Agent.Jobs(wctx)
	if err != nil {
		out.Error = "agent Status failed before SuspendAll: " + err.Error()
		b.killRest(ctx, hs, out.Error)
		return b.hostDone(out, hs, start), fmt.Errorf("agent status: %w", err)
	}
	live := 0
	for _, item := range hs {
		if item.done {
			continue
		}
		if _, listed := jobs[item.job]; !listed {
			item.res.Killed = b.deleteNoContext(ctx, item.guest, item.mirror)
			item.done = true
			continue
		}
		live++
	}
	if live == 0 {
		logger.Info("nothing to suspend on the host")
		return b.hostDone(out, hs, start), nil
	}

	// 3. One agent SuspendAll for the host.
	t := time.Now()
	hr, err := so.Agent.SuspendAll(wctx, RoleBackground, epoch, deadline)
	if last, stale := freeze.LastEpoch(err); stale && last >= epoch {
		epoch = last + 1
		out.Epoch = epoch
		logger.WithField("agentLastEpoch", last).Warn("host epoch stale; retrying SuspendAll once with a higher epoch")
		b.rewriteEpoch(ctx, hs, epoch)
		hr, err = so.Agent.SuspendAll(wctx, RoleBackground, epoch, deadline)
	}
	out.Agent = time.Since(t)
	b.setHostEpoch(epoch)
	if hr == nil {
		err = orNoResult(err)
		out.Error = "agent SuspendAll failed: " + err.Error()
		b.killRest(ctx, hs, out.Error)
		return b.hostDone(out, hs, start), fmt.Errorf("agent suspend-all: %w", err)
	}
	if hr.Error != "" {
		out.Error = hr.Error
	}

	// 4. Per target: Suspended, or the kill sequence.
	for _, item := range hs {
		if item.done {
			continue
		}
		item.res.Agent = out.Agent
		tg, ok := hr.Targets[item.job]
		if !ok || !tg.Done {
			why := "not a target of SuspendAll"
			if ok {
				why = "SuspendAll target failed: " + tg.Error
			}
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, why)
			item.done = true
			continue
		}
		item.res.Outcome = tg.Outcome
		m, err := b.setSuspendState(ctx, item.guest, StateSuspended, keepEpoch)
		if err != nil {
			logger.WithError(err).WithField("guest", item.key()).Warn("suspended, but recording Suspended failed")
			continue
		}
		item.mirror, item.res.State = m, StateSuspended
		b.emit(b.translate(item.guest, m))
		b.event(item.guest, corev1.EventTypeNormal, EventSuspended,
			fmt.Sprintf("suspended by the snapshot agent with the host (epoch %d, %s)", epoch, tg.Outcome))
	}
	for _, item := range hs {
		if !item.done && item.res.State == StateSuspended {
			b.waitHoldInformer(ctx, item.guest, StateSuspended)
		}
	}
	out = b.hostDone(out, hs, start)
	logger.WithField("agentMs", out.Agent.Milliseconds()).WithField("totalMs", out.Total.Milliseconds()).Info("host suspended")
	if out.Error != "" {
		return out, errors.New(out.Error)
	}
	return out, nil
}

// ResumeAll brings the host's suspended guests back with one agent ResumeAll: each turns
// Resuming with the host epoch, the agent resumes every background job, and each guest whose
// target reached RESUMED passes the ready check and turns Running. Any other guest gets the
// kill sequence; none is ever Ready on a process that did not come back.
func (b *Backend) ResumeAll(ctx context.Context, deadline time.Time) (HostResult, error) {
	start := time.Now()
	so := &b.opts.Suspend
	b.hostMu.Lock()
	defer b.hostMu.Unlock()
	hs, err := b.lockAll()
	if err != nil {
		return HostResult{}, err
	}
	defer b.unlockAll(hs)
	b.resumeMu.Lock()
	defer b.resumeMu.Unlock()
	deadline = so.deadlineOr(deadline)
	epoch := b.nextHostEpoch(hs)
	out := HostResult{Epoch: epoch, Deadline: deadline, Guests: map[string]Result{}}
	logger := log.G(ctx).WithField("epoch", epoch).WithField("guests", len(hs))

	live := 0
	for _, item := range hs {
		state, ge := SuspendState(item.mirror)
		item.res = Result{Epoch: ge, Deadline: deadline}
		if state != StateSuspended && state != StateResuming {
			item.res.State, item.res.Noop, item.done = "Running", true, true
			if state != "" {
				item.res.State = state
			}
			continue
		}
		m, err := b.setSuspendState(ctx, item.guest, StateResuming, epoch)
		if err != nil {
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, "could not record Resuming: "+err.Error())
			item.done = true
			continue
		}
		item.mirror, item.res.State, item.res.Epoch = m, StateResuming, epoch
		b.emit(b.translate(item.guest, m))
		live++
	}
	if live == 0 {
		return b.hostDone(out, hs, start), nil
	}
	for _, item := range hs {
		if !item.done {
			b.waitHoldInformer(ctx, item.guest, StateResuming)
		}
	}
	if so.OnHoldChange != nil {
		so.OnHoldChange()
	}

	t := time.Now()
	wctx, cancel := waitCtx(ctx, deadline)
	hr, err := so.Agent.ResumeAll(wctx, RoleBackground, epoch, deadline)
	if last, stale := freeze.LastEpoch(err); stale && last >= epoch {
		epoch = last + 1
		out.Epoch = epoch
		logger.WithField("agentLastEpoch", last).Warn("host epoch stale; retrying ResumeAll once with a higher epoch")
		b.rewriteEpoch(ctx, hs, epoch)
		hr, err = so.Agent.ResumeAll(wctx, RoleBackground, epoch, deadline)
	}
	cancel()
	out.Agent = time.Since(t)
	b.setHostEpoch(epoch)
	if hr == nil {
		err = orNoResult(err)
		out.Error = "agent ResumeAll failed: " + err.Error()
		b.killRest(ctx, hs, out.Error)
		return b.hostDone(out, hs, start), fmt.Errorf("agent resume-all: %w", err)
	}
	if hr.Error != "" {
		out.Error = hr.Error
	}
	for _, item := range hs {
		if item.done {
			continue
		}
		item.res.Agent = out.Agent
		tg, ok := hr.Targets[item.job]
		if !ok || !tg.Done {
			why := "not a target of ResumeAll"
			if ok {
				why = "ResumeAll target failed: " + tg.Error
			}
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, why)
			item.done = true
			continue
		}
		item.res.Outcome = tg.Outcome
		if err := b.readyCheck(ctx, item.guest, item.mirror, &item.res); err != nil {
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, "engine did not serve after ResumeAll: "+err.Error())
			item.done = true
			continue
		}
		m, err := b.setSuspendState(ctx, item.guest, "", keepEpoch)
		if err != nil {
			logger.WithError(err).WithField("guest", item.key()).Warn("resumed, but clearing Resuming failed")
			continue
		}
		item.mirror, item.res.State = m, "Running"
		b.emit(b.translate(item.guest, m))
		b.event(item.guest, corev1.EventTypeNormal, EventResumed,
			fmt.Sprintf("resumed by the snapshot agent with the host (epoch %d): ready check %s", epoch, item.res.ReadyCheck))
	}
	for _, item := range hs {
		if !item.done && item.res.State == "Running" {
			b.waitHoldInformer(ctx, item.guest, "")
		}
	}
	out = b.hostDone(out, hs, start)
	logger.WithField("agentMs", out.Agent.Milliseconds()).WithField("totalMs", out.Total.Milliseconds()).Info("host resumed")
	if out.Error != "" {
		return out, errors.New(out.Error)
	}
	return out, nil
}

// rewriteEpoch writes a new host epoch on every guest still taking part.
func (b *Backend) rewriteEpoch(ctx context.Context, hs []*hostGuest, epoch int64) {
	for _, item := range hs {
		if item.done {
			continue
		}
		state, _ := SuspendState(item.mirror)
		if m, err := b.setSuspendState(ctx, item.guest, state, epoch); err == nil {
			item.mirror, item.res.Epoch = m, epoch
		}
	}
}

// killRest runs the kill sequence on every guest still taking part.
func (b *Backend) killRest(ctx context.Context, hs []*hostGuest, cause string) {
	for _, item := range hs {
		if !item.done {
			item.res.Killed = b.killGuest(ctx, item.guest, item.mirror, cause)
			item.done = true
		}
	}
}

func (b *Backend) hostDone(out HostResult, hs []*hostGuest, start time.Time) HostResult {
	for _, item := range hs {
		item.res.Total = time.Since(start)
		out.Guests[item.key()] = item.res
	}
	out.Total = time.Since(start)
	return out
}

func orNoResult(err error) error {
	if err == nil {
		return errors.New("the agent returned no result")
	}
	return err
}
