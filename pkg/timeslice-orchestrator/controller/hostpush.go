// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

// Host command push (PENDING LEAD DECISION D-NS-4, option "hybrid").
//
// The background lock protocol is kept: each host's virtual kubelet is a
// ROLE_BACKGROUND participant (Acquire, Yield, participant_id heartbeat). On
// top of it the orchestrator pushes commands to each host:
//
//   - Lend: after a foreground Yield with a lend hint, once no job's context
//     is resident on the group's nodes, every waiting participant is granted
//     and sent Resume. Hosts start guests only on Resume.
//   - Notice: when a foreground Acquire starts a notice, every host that held
//     a grant or a claim is sent Vacate(epoch, deadline T). The foreground is
//     granted only after every one of them acked OUTCOME_VACATED for that
//     epoch (GroupSpec.PendingAcks is empty).
//   - Restart: a new process promotes and restores nothing for
//     hostStartupGrace, and a host's first Acquire, like its first
//     heartbeat, registers a claim, so a host that may still serve guests
//     is sent Vacate before the foreground gets the accelerator.
//
// There is no kill path here (ORCH-A4): a host that never acks keeps the
// foreground waiting, and is logged at T as not clear.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// HostCommander pushes host commands and returns the host's ack.
type HostCommander interface {
	Vacate(ctx context.Context, groupID, node string, epoch int64, deadline time.Time) (*hcpb.HostAck, error)
	Resume(ctx context.Context, groupID, node string, epoch int64, deadline time.Time) (*hcpb.HostAck, error)
}

// hostCommandRetry is the pause between attempts of a host command that
// failed or could not be delivered.
const hostCommandRetry = 250 * time.Millisecond

// hostCommandCallTimeout bounds each host command call that has no later
// deadline.
const hostCommandCallTimeout = 5 * time.Second

// hostStartupGrace is how long a new orchestrator process promotes and
// restores nothing, so that every host heartbeats (every 0.5 s) or calls
// Acquire, and one that may hold a grant from the earlier process registers a
// claim first (fail closed).
const hostStartupGrace = 2 * time.Second

// HostPushConfig configures the host command push.
type HostPushConfig struct {
	// Commander sends the commands. Nil disables the push, the lend and the
	// ack barrier: the controller behaves as without this option.
	Commander HostCommander
	// NoticeWindow (N) and KillBudget (K) give the vacate deadline
	// T = notice + N - K.
	NoticeWindow time.Duration
	KillBudget   time.Duration
}

// hostPush is the controller's in-memory host command state.
type hostPush struct {
	cfg HostPushConfig
	// startedAt is when the push was enabled, i.e. the process start.
	startedAt time.Time

	mu sync.Mutex
	// vacates holds the vacate commands in flight, keyed by group, node and
	// epoch.
	vacates map[pushKey]bool
	// started records, per group, the notice epoch whose fan-out was logged
	// and whose deadline timer is armed.
	started map[string]int64
	// resumeCancel cancels a group's resume commands in flight when a notice
	// supersedes them.
	resumeCancel map[string]context.CancelFunc
}

type pushKey struct {
	group string
	node  string
	epoch int64
}

// EnableHostPush turns on the host command push (D-NS-4 hybrid).
func (c *Controller) EnableHostPush(cfg HostPushConfig) {
	if cfg.Commander == nil {
		c.hostPush = nil
		return
	}
	c.hostPush = &hostPush{
		cfg:          cfg,
		startedAt:    time.Now(),
		vacates:      make(map[pushKey]bool),
		started:      make(map[string]int64),
		resumeCancel: make(map[string]context.CancelFunc),
	}
}

// HostPushEnabled reports whether the host command push is on.
func (c *Controller) HostPushEnabled() bool {
	return c.hostPush != nil
}

// reconcileHostCommands pushes the current notice's vacate to every host that
// has not acked it and reports whether the group must hold: while a
// background participant holds a grant or a claim, or a host has not acked
// the notice, nothing may be promoted or restored (fail closed).
func (c *Controller) reconcileHostCommands(ctx context.Context, group *store.Group) bool {
	spec := group.Spec()
	pending := spec.PendingAcks()
	hold := spec.BackgroundHeld() || len(pending) > 0
	hp := c.hostPush
	if wait := time.Until(hp.startedAt.Add(hostStartupGrace)); wait > 0 {
		// A new process does not know yet which hosts hold a grant from an
		// earlier one: hold until they had time to call in and claim.
		time.AfterFunc(wait, func() { c.EnqueueWork(group.ID()) })
		hold = true
	}
	noticeAt := spec.NoticeAt()
	epoch := spec.NoticeEpoch()
	if noticeAt.IsZero() || epoch == 0 {
		return hold
	}
	deadline := noticeAt.Add(hp.cfg.NoticeWindow - hp.cfg.KillBudget)

	hp.mu.Lock()
	first := hp.started[group.ID()] != epoch
	if first {
		hp.started[group.ID()] = epoch
		if cancel, ok := hp.resumeCancel[group.ID()]; ok {
			cancel()
			delete(hp.resumeCancel, group.ID())
		}
	}
	hp.mu.Unlock()

	if first {
		slog.InfoContext(ctx, "Vacate started",
			"group", group.ID(), "hosts", pending, "deadline", deadline, "epoch", epoch)
		c.armDeadline(ctx, group, epoch, deadline)
	}
	for _, node := range pending {
		c.pushVacate(ctx, group, node, epoch, deadline)
	}
	return hold
}

// armDeadline logs, at T, every host that has not acked the notice.
func (c *Controller) armDeadline(ctx context.Context, group *store.Group, epoch int64, deadline time.Time) {
	time.AfterFunc(time.Until(deadline), func() {
		if ctx.Err() != nil || group.Spec().NoticeEpoch() != epoch {
			return
		}
		for _, node := range group.Spec().PendingAcks() {
			slog.WarnContext(ctx, "Host not clear at deadline",
				"group", group.ID(), "node", node, "deadline", deadline, "epoch", epoch)
		}
	})
}

// pushVacate starts sending the vacate of the given epoch to node unless it
// is already in flight. The command is retried until the host acks it
// vacated, reports a newer epoch, or the notice changes.
func (c *Controller) pushVacate(ctx context.Context, group *store.Group, node string, epoch int64, deadline time.Time) {
	hp := c.hostPush
	key := pushKey{group: group.ID(), node: node, epoch: epoch}
	hp.mu.Lock()
	if hp.vacates[key] {
		hp.mu.Unlock()
		return
	}
	hp.vacates[key] = true
	hp.mu.Unlock()

	go func() {
		defer func() {
			hp.mu.Lock()
			delete(hp.vacates, key)
			hp.mu.Unlock()
		}()
		for {
			if ctx.Err() != nil || group.Spec().NoticeEpoch() != epoch {
				return
			}
			slog.InfoContext(ctx, "Host command sent",
				"group", group.ID(), "node", node, "command", "vacate", "deadline", deadline, "epoch", epoch)
			// A call may run until T + K; past it, each attempt gets
			// hostCommandCallTimeout, so a host that comes back is still
			// asked (there is no kill path to clear it otherwise).
			callDeadline := deadline.Add(hp.cfg.KillBudget)
			if minDeadline := time.Now().Add(hostCommandCallTimeout); callDeadline.Before(minDeadline) {
				callDeadline = minDeadline
			}
			callCtx, cancel := context.WithDeadline(ctx, callDeadline)
			ack, err := hp.cfg.Commander.Vacate(callCtx, group.ID(), node, epoch, deadline)
			cancel()
			if err != nil {
				slog.InfoContext(ctx, "Host ack",
					"group", group.ID(), "node", node, "command", "vacate", "outcome", "unreachable",
					"epoch", epoch, "error", err)
				waitRetry(ctx)
				continue
			}
			slog.InfoContext(ctx, "Host ack",
				"group", group.ID(), "node", node, "command", "vacate",
				"outcome", hostcmd.OutcomeName(ack.GetOutcome()), "epoch", ack.GetEpoch(), "error", ack.GetError())
			switch {
			case ack.GetOutcome() == hcpb.Outcome_OUTCOME_VACATED && ack.GetEpoch() == epoch &&
				ack.GetCommand() == hcpb.Command_COMMAND_VACATE:
				if group.Spec().AckVacate(node, epoch) {
					slog.InfoContext(ctx, "Host clear", "group", group.ID(), "node", node, "how", "ack", "epoch", epoch)
					c.EnqueueWork(group.ID())
				}
				return
			case ack.GetOutcome() == hcpb.Outcome_OUTCOME_STALE_EPOCH:
				// The host has seen a newer command than this notice: this
				// command can never be acked. The node stays pending.
				return
			default:
				waitRetry(ctx)
			}
		}
	}()
}

// reconcileLend lends the accelerator after a foreground Yield with a lend
// hint: it grants every waiting participant and sends each Resume. It runs
// only when no foreground job holds or waits for the lock and no notice runs,
// and only once no job context is resident on any node of the group
// (reconcileNode offloads it first because the active job is cleared).
func (c *Controller) reconcileLend(ctx context.Context, group *store.Group) {
	if !c.lendPending(group) {
		return
	}
	spec := group.Spec()
	nodes := spec.LendableParticipants()
	if len(nodes) == 0 {
		return
	}
	resident, err := c.contextResident(ctx, group)
	if err != nil {
		slog.WarnContext(ctx, "Cannot lend: failed to read job contexts", "group", group.ID(), "error", err)
		return
	}
	if resident {
		return
	}

	hp := c.hostPush
	epoch := time.Now().UnixNano()
	deadline := time.Now().Add(hp.cfg.NoticeWindow)
	// The next notice cancels this lend's resumes (reconcileHostCommands).
	resumeCtx, cancel := context.WithCancel(ctx)
	hp.mu.Lock()
	if prev, ok := hp.resumeCancel[group.ID()]; ok {
		hp.resumeCancel[group.ID()] = func() { prev(); cancel() }
	} else {
		hp.resumeCancel[group.ID()] = cancel
	}
	hp.mu.Unlock()

	for _, node := range nodes {
		if !spec.Grant(node) {
			continue
		}
		slog.InfoContext(ctx, "Background granted", "group", group.ID(), "node", node)
		slog.InfoContext(ctx, "Resume started", "group", group.ID(), "node", node, "epoch", epoch)
		go c.pushResume(resumeCtx, group, node, epoch, deadline)
	}
}

// lendPending reports whether the last foreground Yield asked to lend and
// nothing has taken the accelerator back since: no foreground job holds or
// waits for the lock and no notice runs.
func (c *Controller) lendPending(group *store.Group) bool {
	spec := group.Spec()
	return spec.Lend() && spec.LockingJob() == "" && spec.GetWaitingJobQueue().Len() == 0 && spec.NoticeAt().IsZero()
}

// pushResume sends Resume to node until the host acks it, the host reports a
// newer command, or ctx is cancelled (a notice started).
func (c *Controller) pushResume(ctx context.Context, group *store.Group, node string, epoch int64, deadline time.Time) {
	hp := c.hostPush
	for ctx.Err() == nil {
		if !group.Spec().NoticeAt().IsZero() || !group.Spec().Granted(node) {
			return
		}
		slog.InfoContext(ctx, "Host command sent",
			"group", group.ID(), "node", node, "command", "resume", "deadline", deadline, "epoch", epoch)
		ack, err := hp.cfg.Commander.Resume(ctx, group.ID(), node, epoch, deadline)
		if err != nil {
			slog.InfoContext(ctx, "Host ack",
				"group", group.ID(), "node", node, "command", "resume", "outcome", "unreachable",
				"epoch", epoch, "error", err)
			waitRetry(ctx)
			continue
		}
		slog.InfoContext(ctx, "Host ack",
			"group", group.ID(), "node", node, "command", "resume",
			"outcome", hostcmd.OutcomeName(ack.GetOutcome()), "epoch", ack.GetEpoch(), "error", ack.GetError())
		switch ack.GetOutcome() {
		case hcpb.Outcome_OUTCOME_RESUMED, hcpb.Outcome_OUTCOME_STALE_EPOCH:
			return
		default:
			waitRetry(ctx)
		}
	}
}

// contextResident reports whether any job's context is running or moving on
// any node of the group.
func (c *Controller) contextResident(ctx context.Context, group *store.Group) (bool, error) {
	jobs, err := c.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		return false, err
	}
	nodes := group.Status().Nodes()
	for _, job := range jobs {
		states := job.ContextState()
		for _, node := range nodes {
			switch states[node] {
			case pb.SnapshotAgentJobState_STATE_RUNNING, pb.SnapshotAgentJobState_STATE_TRANSITIONING:
				return true, nil
			}
		}
	}
	return false, nil
}

// waitRetry waits hostCommandRetry or until ctx is done.
func waitRetry(ctx context.Context) {
	t := time.NewTimer(hostCommandRetry)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
