package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
)

// This file is option block of D-NS-6 (--unconfirmed-kill=block), and the
// blocked-grant state that option escalate (unconfirmed_kill_escalate.go)
// builds on. A guest whose Kill is not confirmed keeps its host not clear, so
// the foreground is never granted over it: the grant waits until the agent
// confirms the guest gone (a later Kill confirmed, last_outcome
// OUTCOME_KILLED, JOB_STATE_SUSPENDED) or its mirror pod is gone. The Kill is
// retried every killRetryInterval meanwhile (kill.go). The block is held in
// memory only; after an orchestrator restart it is rebuilt from the agent by
// the next vacate barrier's Kill (D-NS-6 H8).

// grantBlockedLogEvery spaces the "Grant blocked" log lines of one guest.
const grantBlockedLogEvery = 5 * time.Second

// blockState is one guest, on one node, whose unconfirmed Kill holds back the
// foreground grant. Its fields are guarded by Controller.killMu.
type blockState struct {
	group, node, job string
	// since is when the first Kill of the guest in this barrier was sent; a
	// different since means a new barrier.
	since time.Time
	// decidedAt is when the unconfirmed-kill decision was taken (T + K).
	decidedAt time.Time
	lastLog   time.Time
	// released is set once the block is dropped; escalation steps check it.
	released bool
	// escalation holds the timers of option escalate, if any.
	escalation []*time.Timer
	// notLendable is set when escalation step 2 marked the node.
	notLendable bool
}

// blockUnconfirmed is option block: never grant without a confirmed Kill;
// alert once, then keep the grant blocked, logging every
// grantBlockedLogEvery.
func (c *Controller) blockUnconfirmed(
	ctx context.Context, group, node, job string, since, decidedAt time.Time,
) unconfirmedDecision {
	c.holdUnconfirmed(ctx, group, node, job, since, decidedAt, UnconfirmedKillBlock)
	return unconfirmedDecision{}
}

// holdUnconfirmed records or refreshes the block of one guest. The first time
// in a barrier it alerts ("Kill unconfirmed", timeslice_kill_unconfirmed_total,
// KillUnconfirmed and GrantBlocked events) and reports first = true.
func (c *Controller) holdUnconfirmed(
	ctx context.Context, group, node, job string, since, decidedAt time.Time, action string,
) (*blockState, bool) {
	key := killKey(group, node, job)
	now := time.Now()
	first, logNow := false, false
	var stale *blockState
	c.killMu.Lock()
	if c.blocks == nil {
		c.blocks = make(map[string]*blockState)
	}
	st, ok := c.blocks[key]
	if ok && !st.since.Equal(since) {
		stale = st
		ok = false
	}
	if !ok {
		st = &blockState{group: group, node: node, job: job, since: since, decidedAt: decidedAt, lastLog: now}
		c.blocks[key] = st
		first = true
	} else if now.Sub(st.lastLog) >= grantBlockedLogEvery {
		st.lastLog = now
		logNow = true
	}
	signal, attempts := "", 0
	if rec, ok := c.kills[key]; ok {
		signal, attempts = rec.signal, rec.attempts
	}
	c.killMu.Unlock()
	if stale != nil {
		c.dropBlocks(ctx, []*blockState{stale})
	}
	metrics.GrantBlocked.WithLabelValues(group, node).Set(1)

	if first {
		elapsed := now.Sub(since)
		metrics.KillUnconfirmedTotal.Inc()
		slog.WarnContext(ctx, "Kill unconfirmed", "group", group, "node", node, "job", job,
			"elapsed_ms", elapsed.Milliseconds(), "action", action, "signal", signal, "attempts", attempts,
			"vram_unconfirmed", false)
		c.kubeCall(ctx, "event "+eventKillUnconfirmed, func(ctx context.Context, k UnconfirmedKube) error {
			return k.GuestEvent(ctx, group, job, node, eventKillUnconfirmed, fmt.Sprintf(
				"Kill of guest %s on node %s not confirmed after %v (%s); the foreground grant is blocked "+
					"until it is (%s)", job, node, elapsed.Round(time.Millisecond), signal, action))
		})
		if fg := c.foregroundWaiter(ctx, group); fg != "" {
			c.kubeCall(ctx, "event "+eventGrantBlocked, func(ctx context.Context, k UnconfirmedKube) error {
				return k.ForegroundEvent(ctx, group, fg, eventGrantBlocked, fmt.Sprintf(
					"Grant held back: the Kill of guest %s on node %s is not confirmed", job, node))
			})
		}
	}
	if first || logNow {
		slog.WarnContext(ctx, "Grant blocked", "group", group, "node", node, "job", job,
			"since_ms", now.Sub(since).Milliseconds(), "action", action)
	}
	return st, first
}

// foregroundWaiter returns the foreground job the group's grant is for: the
// head of the waiting queue, else the lock holder, or "".
func (c *Controller) foregroundWaiter(ctx context.Context, group string) string {
	g, err := c.groupStore.Get(ctx, group)
	if err != nil {
		return ""
	}
	if job, ok := g.Spec().GetWaitingJobQueue().Peek(); ok {
		return job
	}
	return g.Spec().LockingJob()
}

// releaseBlocks drops the blocks of the group that keep reports false for.
func (c *Controller) releaseBlocks(ctx context.Context, group string, keep func(st *blockState) bool) {
	var drop []*blockState
	c.killMu.Lock()
	for key, st := range c.blocks {
		if st.group != group || keep(st) {
			continue
		}
		delete(c.blocks, key)
		drop = append(drop, st)
	}
	c.killMu.Unlock()
	c.dropBlocks(ctx, drop)
}

// releaseBlock drops the block of one guest, if any.
func (c *Controller) releaseBlock(ctx context.Context, group, node, job string) {
	key := killKey(group, node, job)
	c.killMu.Lock()
	st, ok := c.blocks[key]
	if ok {
		delete(c.blocks, key)
	}
	c.killMu.Unlock()
	if ok {
		c.dropBlocks(ctx, []*blockState{st})
	}
}

// dropBlocks ends blocks already removed from c.blocks: it stops their
// escalation, clears a not-lendable mark they set, and clears
// timeslice_grant_blocked for nodes no other block holds.
func (c *Controller) dropBlocks(ctx context.Context, drop []*blockState) {
	for _, st := range drop {
		c.killMu.Lock()
		st.released = true
		for _, t := range st.escalation {
			t.Stop()
		}
		notLendable := st.notLendable
		if notLendable {
			delete(c.notLendable, st.node)
		}
		stillBlocked := false
		for _, other := range c.blocks {
			if other.group == st.group && other.node == st.node {
				stillBlocked = true
				break
			}
		}
		c.killMu.Unlock()
		if notLendable {
			metrics.NodeFailed.DeleteLabelValues(st.node)
		}
		if !stillBlocked {
			metrics.GrantBlocked.DeleteLabelValues(st.group, st.node)
		}
		slog.InfoContext(ctx, "Grant block cleared", "group", st.group, "node", st.node, "job", st.job,
			"blocked_ms", time.Since(st.decidedAt).Milliseconds())
	}
}

// nodeNotLendable returns why node must not be lent, or "".
func (c *Controller) nodeNotLendable(node string) string {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	return c.notLendable[node]
}
