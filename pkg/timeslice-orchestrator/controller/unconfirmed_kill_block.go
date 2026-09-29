package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file is option "block" of decision D-NS-6 (--unconfirmed-kill=block),
// which "escalate" builds on: after a Kill that reached the agent is not
// confirmed, the node is never handed back to the foreground until the agent
// confirms the guest gone (it reports the guest SUSPENDED, or last_outcome =
// KILLED with no device memory, or no longer lists it). vram_unconfirmed is
// never set. The Kill is retried every second meanwhile.
//
// The guest is held: its node counts as busy (liveGuests), so the notice keeps
// running, the foreground's Acquire keeps waiting and the node is not lent
// again. The hold is kept in memory only and outlives the guest's mirror pod,
// because a mirror pod that is gone (for example after the escalation's
// graceful delete) says nothing about the device memory the agent still
// reports. After an orchestrator restart the hold is rebuilt from the agent:
// a guest whose mirror pod is still there is live until the agent says
// otherwise, is killed again at T, and is held again.

// unconfirmedRetryWait bounds how long a repeat Kill of a guest that is
// already unconfirmed is polled in one reconcile pass, so the pass (and the
// escalation steps and log lines that run in it) is not held for K each
// second. The agent is still given the deadline K.
const unconfirmedRetryWait = 500 * time.Millisecond

// grantBlockedLogInterval spaces the "Grant blocked" log lines of one hold.
const grantBlockedLogInterval = 5 * time.Second

// unconfirmedHold is one guest on one node held after an unconfirmed Kill.
// Only the reconcile of its group reads or writes its mutable fields; the map
// that holds it is guarded by killMu.
type unconfirmedHold struct {
	groupID string
	node    string
	jobID   string
	reason  string
	// since is when the Kill was first seen unconfirmed; firstSent is when
	// the first Kill was sent.
	since     time.Time
	firstSent time.Time
	// blockedSince is when the foreground was first kept waiting past N.
	blockedSince   time.Time
	lastBlockedLog time.Time
	// steps is how many escalation steps ran (escalate only).
	steps int
	// notLendable is true once this hold marked its node not lendable.
	notLendable bool
}

// unconfirmedBlock is option "block": hold the guest and never grant. Past N
// it reports the foreground as blocked (noteGrantBlocked).
func (c *Controller) unconfirmedBlock(
	ctx context.Context, group *store.Group, jobID, node string, rec *killRecord, pastN bool,
) unconfirmedDecision {
	hold := c.ensureHold(group.ID(), jobID, node, rec)
	if pastN {
		c.noteGrantBlocked(ctx, group, hold)
	}
	return unconfirmedDecision{}
}

// killPollBound is how long killGuest polls one Kill attempt: K, or at most
// unconfirmedRetryWait for a repeat Kill of a guest already seen unconfirmed
// under block or escalate.
func (c *Controller) killPollBound(rec *killRecord) time.Duration {
	bound := c.killBudget()
	if rec.counted && c.unconfirmedKillMode() != UnconfirmedKillGrant {
		bound = min(bound, unconfirmedRetryWait)
	}
	return bound
}

func (c *Controller) ensureHold(groupID, jobID, node string, rec *killRecord) *unconfirmedHold {
	key := killKey(groupID, node, jobID)
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.holds == nil {
		c.holds = make(map[string]*unconfirmedHold)
	}
	hold := c.holds[key]
	if hold == nil {
		since := rec.unconfirmedSince
		if since.IsZero() {
			since = time.Now()
		}
		hold = &unconfirmedHold{
			groupID: groupID, node: node, jobID: jobID, reason: rec.reason,
			since: since, firstSent: rec.firstSent,
		}
		c.holds[key] = hold
	}
	return hold
}

// groupHolds returns the holds of a group.
func (c *Controller) groupHolds(groupID string) map[string]*unconfirmedHold {
	prefix := groupID + "\x00"
	out := make(map[string]*unconfirmedHold)
	c.killMu.Lock()
	defer c.killMu.Unlock()
	for key, hold := range c.holds {
		if strings.HasPrefix(key, prefix) {
			out[key] = hold
		}
	}
	return out
}

// heldNodes returns the nodes of a group on which a guest is held.
func (c *Controller) heldNodes(groupID string) map[string]bool {
	nodes := make(map[string]bool)
	for _, hold := range c.groupHolds(groupID) {
		nodes[hold.node] = true
	}
	return nodes
}

// nodeNotLendable returns why node may not be lent, or "".
func (c *Controller) nodeNotLendable(node string) string {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	return c.notLendable[node]
}

// noteGrantBlocked reports that the foreground waits past N on a held guest:
// the timeslice_grant_blocked gauge, a GrantBlocked Warning event on the
// trainer's pods (once per hold) and "Grant blocked" every 5 s.
func (c *Controller) noteGrantBlocked(ctx context.Context, group *store.Group, hold *unconfirmedHold) {
	now := time.Now()
	if hold.blockedSince.IsZero() {
		hold.blockedSince = now
		metrics.GrantBlocked.WithLabelValues(hold.groupID, hold.node).Set(1)
		c.warnPods(ctx, hold.groupID, foregroundJobOf(group), "", EventGrantBlocked,
			fmt.Sprintf("Grant held: the kill of guest %s on node %s is not confirmed", hold.jobID, hold.node))
	}
	if !hold.lastBlockedLog.IsZero() && now.Sub(hold.lastBlockedLog) < grantBlockedLogInterval {
		return
	}
	hold.lastBlockedLog = now
	slog.WarnContext(ctx, "Grant blocked", "group", hold.groupID, "node", hold.node, "job", hold.jobID,
		"since_ms", now.Sub(hold.since).Milliseconds())
}

// foregroundJobOf returns the foreground job that holds or waits for the
// group lock, or "".
func foregroundJobOf(group *store.Group) string {
	if job := group.Spec().LockingJob(); job != "" {
		return job
	}
	job, _ := group.Spec().GetWaitingJobQueue().Peek()
	return job
}

// reconcileHolds runs once per reconcile of the group, before the kill loop.
// A held guest whose job is still known is released when the agent reports
// it vacated; one whose mirror pod is gone is checked against the agent
// directly and its Kill retried. Every hold still in place past N reports
// the foreground as blocked, and under escalate runs its ladder.
func (c *Controller) reconcileHolds(ctx context.Context, group *store.Group, jobs []*store.Job, pastN bool) {
	holds := c.groupHolds(group.ID())
	if len(holds) == 0 {
		return
	}
	byID := make(map[string]*store.Job, len(jobs))
	for _, job := range jobs {
		if job.Background() {
			byID[job.JobID()] = job
		}
	}
	for key, hold := range holds {
		if job := byID[hold.jobID]; job != nil {
			if guestVacated(job, job.ContextState(), hold.node) {
				c.releaseHold(ctx, key, "agent-status")
				continue
			}
		} else if c.releaseOrphanHold(ctx, key, hold) {
			continue
		}
		if pastN {
			c.noteGrantBlocked(ctx, group, hold)
		}
		if c.unconfirmedKillMode() == UnconfirmedKillEscalate {
			c.escalateHold(ctx, group, hold)
		}
	}
}

// releaseOrphanHold handles a hold whose guest has no mirror pod any more:
// it releases the hold when the agent no longer reports the guest holding
// the node, and otherwise retries the Kill every second. It reports whether
// the hold was released.
func (c *Controller) releaseOrphanHold(ctx context.Context, key string, hold *unconfirmedHold) bool {
	if c.agentClearedGuest(ctx, hold.node, hold.jobID) {
		c.releaseHold(ctx, key, "agent-status")
		return true
	}
	rec := c.killRecordFor(key)
	if rec == nil {
		rec = c.newKillRecord(key, hold.reason)
		rec.counted = true
	}
	if !rec.lastAttempt.IsZero() && time.Since(rec.lastAttempt) < killRetryInterval {
		return false
	}
	job := store.NewJob(hold.groupID, hold.jobID)
	job.SetRole(store.RoleBackground)
	c.killGuest(ctx, hold.groupID, job, hold.node, rec)
	if job.Killed(hold.node) {
		c.releaseHold(ctx, key, "kill")
		return true
	}
	return false
}

// agentClearedGuest reports whether the agent on node says the guest is off
// the accelerator: SUSPENDED, killed with no device memory, or not listed.
// An agent that cannot be reached clears nothing.
func (c *Controller) agentClearedGuest(ctx context.Context, node, jobID string) bool {
	status, err := c.agentStore.GetStatus(ctx, node)
	if err != nil {
		slog.WarnContext(ctx, "Agent unreachable, held guest stays held", "node", node, "job", jobID, "error", err)
		return false
	}
	for _, js := range status.GetJobStatuses() {
		if js.GetJobId() != jobID {
			continue
		}
		return js.GetState() == agentpb.JobState_JOB_STATE_SUSPENDED ||
			(js.GetLastOutcome() == agentpb.Outcome_OUTCOME_KILLED && js.GetDeviceBytes() == 0)
	}
	return true
}

// releaseHold drops the hold at key, if any, once the guest is confirmed
// gone; how says how ("kill" when a Kill was confirmed, which killGuest
// already logged, or "agent-status").
func (c *Controller) releaseHold(ctx context.Context, key, how string) {
	c.killMu.Lock()
	hold := c.holds[key]
	if hold == nil {
		c.killMu.Unlock()
		return
	}
	delete(c.holds, key)
	if hold.notLendable {
		delete(c.notLendable, hold.node)
	}
	c.killMu.Unlock()

	if !hold.blockedSince.IsZero() {
		metrics.GrantBlocked.WithLabelValues(hold.groupID, hold.node).Set(0)
	}
	if hold.notLendable {
		metrics.NodeFailed.WithLabelValues(hold.node).Set(0)
	}
	if how != "kill" {
		slog.InfoContext(ctx, "Kill confirmed", "group", hold.groupID, "node", hold.node, "job", hold.jobID,
			"elapsed_ms", time.Since(hold.firstSent).Milliseconds(), "how", how)
	}
	slog.InfoContext(ctx, "Held guest released", "group", hold.groupID, "node", hold.node, "job", hold.jobID,
		"held_ms", time.Since(hold.since).Milliseconds(), "how", how)
}

// nextHoldWake returns how long until a hold of the group needs a reconcile
// pass of its own (the next "Grant blocked" line or escalation step), and
// whether there is any hold.
func (c *Controller) nextHoldWake(groupID string) (time.Duration, bool) {
	holds := c.groupHolds(groupID)
	if len(holds) == 0 {
		return 0, false
	}
	now := time.Now()
	next := noticeRequeueInterval
	after := c.escalateAfter()
	for _, hold := range holds {
		if !hold.lastBlockedLog.IsZero() {
			next = min(next, hold.lastBlockedLog.Add(grantBlockedLogInterval).Sub(now))
		}
		if c.unconfirmedKillMode() == UnconfirmedKillEscalate && hold.steps < len(after) {
			next = min(next, hold.since.Add(after[hold.steps]).Sub(now))
		}
	}
	return max(next, minKillRequeue), true
}
