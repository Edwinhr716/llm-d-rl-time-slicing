package controller

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file is the kill path on the host-push path (ORCH-A4 on D-NS-4
// ns-push-vk). The hosts are commanded to vacate by hostcmd.Commander; the
// reconcile loop acts on a host that has not acked:
//
//   - at T = notice + N - K, and
//   - when the host's commands have failed for L (the VK is unseen),
//
// by sending the agent a Kill for every live guest on that host. A guest is
// vacated when the agent reports it SUSPENDED or killed (last_outcome
// OUTCOME_KILLED, or our own Kill confirmed), or its mirror pod is gone or
// terminal (the job leaves the store). Once every guest on the host is
// vacated, the host is marked clear (hostcmd ClearByOrchestrator) and the
// barrier can finish. A guest the agent reports FAULTED is killed at any time.
//
// Each Kill has the deadline now + K and is polled every KillPollInterval,
// bounded by K. A Kill the agent cannot be reached for is retried every
// killRetryInterval and the host stays not clear (fail closed). A Kill that
// reached the agent but was not confirmed is decided by onKillUnconfirmed
// (unconfirmed_kill.go): by default the host stays not clear and the grant is
// blocked; with --grant-unconfirmed the host is handed back at the end of the
// notice window.

// Kill reasons, sent to the agent and logged.
const (
	killReasonDeadline = "deadline"
	killReasonVKUnseen = "vk-unseen"
	killReasonFaulted  = "guest-faulted"
)

// How a host was cleared by the orchestrator, for ClearByOrchestrator.
const (
	clearHowKill            = "kill"
	clearHowUnconfirmedKill = "unconfirmed-kill"
	clearHowNoLiveGuest     = "no-live-guest"
)

// DefaultBackgroundLiveness is L, the time a host may go unseen during a
// vacate barrier before its guests are killed.
const DefaultBackgroundLiveness = 3 * time.Second

// killRetryInterval spaces Kill attempts for the same guest on the same node.
const killRetryInterval = 1 * time.Second

// pendingKillRequeue is how soon the group is looked at again while a host
// waits on a Kill or on the unconfirmed-kill decision.
const pendingKillRequeue = 100 * time.Millisecond

// killRecord is the kill state of one guest on one node. Only the reconcile of
// the guest's group touches it; the workqueue never runs two reconciles of the
// same group at once.
type killRecord struct {
	reason string
	// noticeAt is the barrier the record belongs to. A record of an older
	// barrier is started over.
	noticeAt    time.Time
	attempts    int
	firstSent   time.Time
	lastAttempt time.Time
	// unconfirmed is true when the last Kill that reached the agent was not
	// confirmed; signal says how (see unconfirmed_kill.go).
	unconfirmed bool
	signal      string
	// handedBack is set once handBackUnconfirmed ran for the guest.
	handedBack bool
}

func killKey(groupID, node, jobID string) string {
	return groupID + "\x00" + node + "\x00" + jobID
}

func (c *Controller) backgroundLiveness() time.Duration {
	if c.BackgroundLiveness <= 0 {
		return DefaultBackgroundLiveness
	}
	return c.BackgroundLiveness
}

// hostsGuardGuests reports whether job is a guest whose presence on a node is
// guarded by the host barrier rather than by the foreground snapshot and
// restore loop: with host commands on, guests are vacated by their host or
// killed, never snapshotted, and they never make a node look busy to the
// foreground.
func (c *Controller) hostsGuardGuests(job *store.Job) bool {
	return c.Hosts != nil && job.Background()
}

// markAgentSeen records that the agent of node answered a status call.
func (c *Controller) markAgentSeen(node string) {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.agentSeen == nil {
		c.agentSeen = make(map[string]time.Time)
	}
	c.agentSeen[node] = time.Now()
}

// agentSeenWithin reports whether the agent of node answered a status call in
// the last d.
func (c *Controller) agentSeenWithin(node string, d time.Duration) bool {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	seen, ok := c.agentSeen[node]
	return ok && time.Since(seen) <= d
}

// guestOnNode reports whether the background job may be on node: it has a
// non-terminal mirror pod there, or the agent of node reported it.
func guestOnNode(job *store.Job, states map[string]pb.SnapshotAgentJobState_State, node string) bool {
	if _, ok := states[node]; ok {
		return true
	}
	return slices.Contains(job.PodNodes(), node)
}

// guestVacated reports whether the guest is off node per the contract: the
// agent reports it SUSPENDED or killed.
func guestVacated(job *store.Job, states map[string]pb.SnapshotAgentJobState_State, node string) bool {
	return states[node] == pb.SnapshotAgentJobState_STATE_SUSPENDED || job.Killed(node)
}

// overdueReason returns why the kill path must act on a host that is not
// clear, or "" if it need not act yet.
func overdueReason(h hostcmd.HostStatus, deadline, now time.Time, liveness time.Duration) string {
	switch {
	case !now.Before(deadline):
		return killReasonDeadline
	case !h.FailingSince.IsZero() && now.Sub(h.FailingSince) >= liveness:
		return killReasonVKUnseen
	default:
		return ""
	}
}

// killOverdueHosts is the host-not-clear-at-deadline decision. For every host
// of the running barrier that is not clear at T, or whose commands have failed
// for L, it kills the live guests on that host and marks the host clear once
// they are all vacated. It reports whether every host of the group is clear
// afterwards.
func (c *Controller) killOverdueHosts(ctx context.Context, group *store.Group) bool {
	groupID := group.ID()
	bar, ok := c.Hosts.Barrier(groupID)
	if !ok {
		// No barrier: nothing holds a grant back any more.
		c.releaseBlocks(ctx, groupID, func(*blockState) bool { return false })
		return c.Hosts.AllClear(groupID)
	}
	jobs, err := c.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		slog.ErrorContext(ctx, "Kill path: failed to list jobs", "group", groupID, "error", err)
		c.queue.AddAfter(groupID, killRetryInterval)
		return false
	}
	c.pruneKills(groupID, jobs)
	c.releaseSettledBlocks(ctx, groupID, jobs, &bar)

	now := time.Now()
	liveness := c.backgroundLiveness()
	for _, h := range bar.NotClear {
		reason := overdueReason(h, bar.Deadline, now, liveness)
		if reason == "" {
			// Not due yet: look again when it is. The Commander also
			// enqueues the group at T.
			due := bar.Deadline
			if !h.FailingSince.IsZero() && h.FailingSince.Add(liveness).Before(due) {
				due = h.FailingSince.Add(liveness)
			}
			c.queue.AddAfter(groupID, max(time.Until(due), pendingKillRequeue))
			continue
		}
		res := c.vacateHost(ctx, group, jobs, h.Node, reason, &bar)
		if !res.done {
			c.queue.AddAfter(groupID, res.retry)
			continue
		}
		c.Hosts.ClearByOrchestrator(groupID, h.Node, res.how)
	}
	return c.Hosts.AllClear(groupID)
}

// hostVacate is what vacateHost found for one host.
type hostVacate struct {
	how   string        // how the host was cleared, when done
	done  bool          // every guest on the host is vacated
	retry time.Duration // how soon to look again, when not done
}

// vacateHost kills the live guests on node. It reports how the host was
// cleared and whether every guest on node is vacated, or else how soon to look
// again.
func (c *Controller) vacateHost(
	ctx context.Context, group *store.Group, jobs []*store.Job, node, reason string, bar *hostcmd.Barrier,
) hostVacate {
	groupID := group.ID()
	var guests []*store.Job
	handedBack := false
	for _, job := range jobs {
		if !job.Background() {
			continue
		}
		states := job.ContextState()
		if !guestOnNode(job, states, node) || guestVacated(job, states, node) {
			continue
		}
		if job.UnconfirmedKill(node) {
			handedBack = true
			continue
		}
		guests = append(guests, job)
	}

	if len(guests) == 0 {
		if handedBack {
			return hostVacate{how: clearHowUnconfirmedKill, done: true}
		}
		// No guest is known on the host. Only trust that when the agent
		// answered recently; otherwise a guest may be there that no
		// mirror pod shows (fail closed).
		if !c.agentSeenWithin(node, c.backgroundLiveness()) {
			if c.firstHoldLog(groupID, node, bar.NoticeAt) {
				slog.WarnContext(ctx, "Host not clear and its agent is not seen: holding the grant",
					"group", groupID, "node", node, "reason", reason)
			}
			return hostVacate{retry: killRetryInterval}
		}
		return hostVacate{how: clearHowNoLiveGuest, done: true}
	}

	allDone := true
	for _, job := range guests {
		key := killKey(groupID, node, job.JobID())
		rec := c.killRecordFor(key, reason, bar.NoticeAt)
		if rec.lastAttempt.IsZero() || time.Since(rec.lastAttempt) >= killRetryInterval {
			c.killGuest(ctx, groupID, job, node, rec, bar.KillBudget)
		}
		if job.Killed(node) {
			c.releaseBlock(ctx, groupID, node, job.JobID())
			continue
		}
		if !rec.unconfirmed {
			// Not delivered (agent unreachable): retried, and the host
			// stays not clear.
			allDone = false
			continue
		}
		// A Kill that reached the agent but was not confirmed.
		decision := c.onKillUnconfirmed(ctx, groupID, node, job.JobID(), rec.firstSent)
		if !decision.grant {
			allDone = false
			continue
		}
		c.handBackUnconfirmed(ctx, group, job, node, rec, decision.vramUnconfirmed)
		handedBack = true
	}
	if !allDone {
		return hostVacate{retry: pendingKillRequeue}
	}
	if handedBack {
		return hostVacate{how: clearHowUnconfirmedKill, done: true}
	}
	return hostVacate{how: clearHowKill, done: true}
}

// killFaultedGuests kills every guest the agent reports FAULTED on a node of
// the group, at any time. It does not touch the host registry.
func (c *Controller) killFaultedGuests(ctx context.Context, group *store.Group) {
	groupID := group.ID()
	jobs, err := c.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		slog.ErrorContext(ctx, "Kill path: failed to list jobs", "group", groupID, "error", err)
		return
	}
	killBudget := c.killBudgetFor(groupID)
	for _, job := range jobs {
		if !job.Background() {
			continue
		}
		for node, state := range job.ContextState() {
			if state != pb.SnapshotAgentJobState_STATE_FAULTED || job.Killed(node) {
				continue
			}
			rec := c.killRecordFor(killKey(groupID, node, job.JobID()), killReasonFaulted, time.Time{})
			if !rec.lastAttempt.IsZero() && time.Since(rec.lastAttempt) < killRetryInterval {
				continue
			}
			c.killGuest(ctx, groupID, job, node, rec, killBudget)
		}
	}
}

// killBudgetFor returns K from the group's running barrier, or the server
// default when none runs.
func (c *Controller) killBudgetFor(groupID string) time.Duration {
	if bar, ok := c.Hosts.Barrier(groupID); ok && bar.KillBudget > 0 {
		return bar.KillBudget
	}
	return defaultKillBudget
}

// defaultKillBudget is K when no barrier says otherwise. It matches
// server.DefaultKillBudget.
const defaultKillBudget = 3 * time.Second

// killGuest sends one Kill for the guest on node with deadline now + K and
// polls it, bounded by K.
func (c *Controller) killGuest(
	ctx context.Context, groupID string, job *store.Job, node string, rec *killRecord, killBudget time.Duration,
) {
	start := time.Now()
	deadline := start.Add(killBudget)
	if rec.firstSent.IsZero() {
		rec.firstSent = start
	}
	rec.lastAttempt = start
	rec.attempts++
	kctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	log := slog.With("group", groupID, "node", node, "job", job.JobID(), "reason", rec.reason, "attempt", rec.attempts)
	log.InfoContext(ctx, "Kill sent", "deadline", deadline)
	resp, err := c.agentStore.Kill(kctx, node, job.JobID(), rec.reason, deadline)
	if err != nil {
		log.WarnContext(ctx, "Kill not delivered: agent unreachable, will retry", "retryIn", killRetryInterval,
			"error", err)
		return
	}
	if signal, err := c.waitKillConfirmed(kctx, groupID, job.JobID(), node, resp.GetOperationId()); signal != "" {
		rec.unconfirmed = true
		rec.signal = signal
		log.WarnContext(ctx, "Kill attempt not confirmed", "signal", signal,
			"elapsed_ms", time.Since(start).Milliseconds(), "error", err)
		return
	}
	rec.unconfirmed = false
	rec.signal = ""
	job.SetKilled(node, true)
	log.InfoContext(ctx, "Kill confirmed", "elapsed_ms", time.Since(start).Milliseconds())
}

// killRecordFor returns the record for key, starting a new one when there is
// none or it belongs to another barrier.
func (c *Controller) killRecordFor(key, reason string, noticeAt time.Time) *killRecord {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.kills == nil {
		c.kills = make(map[string]*killRecord)
	}
	rec, ok := c.kills[key]
	if !ok || !rec.noticeAt.Equal(noticeAt) {
		rec = &killRecord{reason: reason, noticeAt: noticeAt}
		c.kills[key] = rec
	}
	return rec
}

// pruneKills drops the records of the group's guests that left the job store
// (their mirror pods are gone).
func (c *Controller) pruneKills(groupID string, jobs []*store.Job) {
	known := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		known[job.JobID()] = true
	}
	prefix := groupID + "\x00"
	c.killMu.Lock()
	defer c.killMu.Unlock()
	for key := range c.kills {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) == 3 && !known[parts[2]] {
			delete(c.kills, key)
		}
	}
}

// forgetKills drops every kill record and hold log mark of the group, when
// the group is deleted.
func (c *Controller) forgetKills(groupID string) {
	prefix := groupID + "\x00"
	c.killMu.Lock()
	defer c.killMu.Unlock()
	for key := range c.kills {
		if strings.HasPrefix(key, prefix) {
			delete(c.kills, key)
		}
	}
	for key := range c.holdLogged {
		if strings.HasPrefix(key, prefix) {
			delete(c.holdLogged, key)
		}
	}
}

// releaseSettledBlocks drops the blocked grants of the group that no longer
// hold: the host is clear or no longer in the barrier, or the guest left the
// store, is no longer on the node or is vacated.
func (c *Controller) releaseSettledBlocks(ctx context.Context, groupID string, jobs []*store.Job, bar *hostcmd.Barrier) {
	notClear := make(map[string]bool, len(bar.NotClear))
	for _, h := range bar.NotClear {
		notClear[h.Node] = true
	}
	byID := make(map[string]*store.Job, len(jobs))
	for _, job := range jobs {
		byID[job.JobID()] = job
	}
	c.releaseBlocks(ctx, groupID, func(st *blockState) bool {
		job, ok := byID[st.job]
		if !ok || !notClear[st.node] {
			return false
		}
		states := job.ContextState()
		return guestOnNode(job, states, st.node) && !guestVacated(job, states, st.node)
	})
}

// guestRef names a guest and the node it is on.
type guestRef struct {
	job, node string
}

// unconfirmedGuestOn returns a guest that was handed back after an
// unconfirmed Kill and that the agent does not yet report vacated, with its
// node, or an empty job when there is none.
func (c *Controller) unconfirmedGuestOn(ctx context.Context, group *store.Group) guestRef {
	jobs, err := c.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		// Fail closed: not knowing counts as a guest that may be there.
		return guestRef{job: "unknown"}
	}
	for _, job := range jobs {
		if !job.Background() {
			continue
		}
		states := job.ContextState()
		for _, node := range group.Status().Nodes() {
			if job.UnconfirmedKill(node) && guestOnNode(job, states, node) && !guestVacated(job, states, node) {
				return guestRef{job: job.JobID(), node: node}
			}
		}
	}
	return guestRef{}
}

// firstHoldLog reports whether this is the first time, for this barrier, that
// the host is logged as held because its agent is not seen.
func (c *Controller) firstHoldLog(groupID, node string, noticeAt time.Time) bool {
	key := groupID + "\x00" + node
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.holdLogged == nil {
		c.holdLogged = make(map[string]time.Time)
	}
	if last, ok := c.holdLogged[key]; ok && last.Equal(noticeAt) {
		return false
	}
	c.holdLogged[key] = noticeAt
	return true
}
