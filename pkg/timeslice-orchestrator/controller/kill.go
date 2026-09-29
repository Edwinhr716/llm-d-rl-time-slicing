package controller

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file is the kill path of the background participant protocol. The
// orchestrator sends the agent a Kill for a live guest
//
//   - at T = notice + N - K, for every guest still live on the group,
//   - when the agent reports the guest FAULTED, at any time,
//   - when the node's participant holds a grant or a claim and has not been
//     seen for L; its guests are killed and its grant is taken back.
//
// At T the orchestrator also takes back every grant and claim still held.
// Each Kill is polled every KillPollInterval and bounded by K. A confirmed
// Kill counts the guest as vacated at once. A Kill the agent cannot be reached
// for is retried every second. A Kill that reached the agent but was not
// confirmed is decided by onKillUnconfirmed (unconfirmed_kill.go, D-NS-6).

// Kill reasons, sent to the agent and logged.
const (
	killReasonDeadline = "deadline"
	killReasonFaulted  = "guest-faulted"
	killReasonVKUnseen = "vk-unseen"
)

// killRetryInterval spaces Kill attempts for the same guest on the same node.
const killRetryInterval = 1 * time.Second

// minKillRequeue is the shortest requeue delay for a Kill that is due.
const minKillRequeue = 10 * time.Millisecond

// killRecord is the kill state of one guest on one node. Only the reconcile of
// the guest's group touches it; the workqueue never runs two reconciles of the
// same group at once.
type killRecord struct {
	reason      string
	attempts    int
	lastAttempt time.Time
	// firstSent is when the first Kill attempt was sent.
	firstSent time.Time
	// reached is true once an attempt reached the agent (Kill returned an
	// operation). It stays true when a later attempt cannot reach it.
	reached bool
	// unconfirmed is true when the last Kill that reached the agent was not
	// confirmed; signal says how (see unconfirmed_kill.go).
	unconfirmed bool
	signal      string
	// counted is true once noteKillUnconfirmed ran for the guest, at
	// unconfirmedSince (the first Kill seen unconfirmed).
	counted          bool
	unconfirmedSince time.Time
	confirmed        bool
	// handedBack is true once handBackUnconfirmed ran for the guest.
	handedBack bool
	// doneAt is when the Kill was confirmed or the node handed back.
	doneAt time.Time
}

func killKey(groupID, node, jobID string) string {
	return groupID + "\x00" + node + "\x00" + jobID
}

// guestNodes returns the nodes a background job may be on: those with a
// non-terminal mirror pod and those the agent reported it on.
func guestNodes(job *store.Job, states map[string]pb.SnapshotAgentJobState_State) []string {
	nodes := job.PodNodes()
	for node := range states {
		if !slices.Contains(nodes, node) {
			nodes = append(nodes, node)
		}
	}
	slices.Sort(nodes)
	return nodes
}

// guestVacated reports whether the agent says the guest is off node: it
// reports it SUSPENDED, or killed.
func guestVacated(job *store.Job, states map[string]pb.SnapshotAgentJobState_State, node string) bool {
	return states[node] == pb.SnapshotAgentJobState_STATE_SUSPENDED || job.Killed(node)
}

// unconfirmedKillOn reports whether a guest that was handed back after an
// unconfirmed Kill may still be on node.
func unconfirmedKillOn(jobs []*store.Job, node string) bool {
	for _, job := range jobs {
		if !job.Background() || !job.UnconfirmedKill(node) {
			continue
		}
		if !guestVacated(job, job.ContextState(), node) {
			return true
		}
	}
	return false
}

func (c *Controller) killBudget() time.Duration {
	if c.KillBudget <= 0 {
		return defaultKillBudget
	}
	return c.KillBudget
}

func (c *Controller) backgroundLiveness() time.Duration {
	if c.BackgroundLiveness <= 0 {
		return DefaultBackgroundLiveness
	}
	return c.BackgroundLiveness
}

// reconcileKills takes back the grants that ended (at T, or with the
// participant unseen for L) and kills the guests that must go.
func (c *Controller) reconcileKills(ctx context.Context, group *store.Group) error {
	spec := group.Spec()
	groupID := group.ID()
	now := time.Now()
	noticeAt := spec.NoticeAt()
	pastT := !noticeAt.IsZero() && !now.Before(c.vacateDeadline(noticeAt))

	nodes := group.Status().Nodes()
	slices.Sort(nodes)
	revoked := make(map[string]string)
	for _, node := range nodes {
		if !spec.NodeHeld(node) {
			continue
		}
		reason := ""
		switch {
		case pastT:
			reason = killReasonDeadline
		case spec.HolderUnseen(node, now, c.backgroundLiveness()):
			reason = killReasonVKUnseen
		default:
		}
		if reason == "" || !spec.RevokeGrant(node) {
			continue
		}
		revoked[node] = reason
		if !noticeAt.IsZero() {
			track := c.noticeTrackFor(groupID, noticeAt)
			c.noticeMu.Lock()
			track.revoked[node] = true
			c.noticeMu.Unlock()
		}
		slog.WarnContext(ctx, "Background grant revoked", "group", groupID, "node", node, "reason", reason)
	}

	jobs, err := c.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to list jobs for group %s: %w", groupID, err)
	}
	// D-NS-6 block and escalate: guests held after an unconfirmed Kill.
	c.reconcileHolds(ctx, group, jobs, c.pastN(spec.NoticeAt()))
	known := make(map[string]bool)
	for _, job := range jobs {
		if !job.Background() {
			continue
		}
		states := job.ContextState()
		for _, node := range guestNodes(job, states) {
			key := killKey(groupID, node, job.JobID())
			known[key] = true
			if guestVacated(job, states, node) || job.UnconfirmedKill(node) {
				c.releaseHold(ctx, key, "agent-status")
				continue
			}
			rec := c.killRecordFor(key)
			reason := ""
			switch {
			case states[node] == pb.SnapshotAgentJobState_STATE_FAULTED:
				reason = killReasonFaulted
			case pastT:
				reason = killReasonDeadline
			case revoked[node] != "":
				reason = revoked[node]
			case rec != nil:
				// A Kill that has not succeeded yet is retried.
				reason = rec.reason
			default:
			}
			if reason == "" {
				continue
			}
			if rec == nil {
				rec = c.newKillRecord(key, reason)
			}
			if rec.lastAttempt.IsZero() || time.Since(rec.lastAttempt) >= killRetryInterval {
				c.killGuest(ctx, groupID, job, node, rec)
			}
			if job.Killed(node) {
				c.releaseHold(ctx, key, "kill")
				continue
			}
			if !rec.reached || !rec.unconfirmed {
				continue
			}
			if decision := c.onKillUnconfirmed(ctx, group, job, node, rec, c.pastN(spec.NoticeAt())); decision.grant {
				c.handBackUnconfirmed(ctx, group, job, node, rec, decision.vramUnconfirmed)
			}
		}
	}
	c.pruneKills(groupID, known)
	return nil
}

// killGuest sends one Kill for the guest on node with deadline now + K and
// polls it, bounded by K.
func (c *Controller) killGuest(ctx context.Context, groupID string, job *store.Job, node string, rec *killRecord) {
	start := time.Now()
	deadline := start.Add(c.killBudget())
	rec.lastAttempt = start
	rec.attempts++
	if rec.firstSent.IsZero() {
		rec.firstSent = start
	}
	kctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	// The agent always gets the deadline K; how long this pass polls for
	// the outcome may be shorter (killPollBound).
	pctx, pcancel := context.WithDeadline(kctx, start.Add(c.killPollBound(rec)))
	defer pcancel()

	log := slog.With("group", groupID, "node", node, "job", job.JobID(), "reason", rec.reason, "attempt", rec.attempts)
	log.InfoContext(ctx, "Kill sent", "deadline", deadline)
	resp, err := c.agentStore.Kill(kctx, node, job.JobID(), rec.reason, deadline)
	if err != nil {
		log.WarnContext(ctx, "Agent unreachable, kill will be retried", "retryIn", killRetryInterval, "error", err)
		return
	}
	rec.reached = true
	if signal, err := c.waitKillConfirmed(pctx, groupID, job.JobID(), node, resp.GetOperationId()); signal != "" {
		rec.unconfirmed = true
		rec.signal = signal
		log.WarnContext(ctx, "Kill not confirmed", "signal", signal,
			"elapsed_ms", time.Since(start).Milliseconds(), "error", err)
		c.noteKillUnconfirmed(ctx, groupID, job.JobID(), node, rec, err)
		return
	}
	rec.unconfirmed = false
	rec.signal = ""
	job.SetKilled(node, true)
	rec.confirmed = true
	rec.doneAt = time.Now()
	log.InfoContext(ctx, "Guest killed", "elapsed_ms", time.Since(start).Milliseconds())
	slog.InfoContext(ctx, "Kill confirmed", "group", groupID, "node", node, "job", job.JobID(),
		"elapsed_ms", time.Since(rec.firstSent).Milliseconds())
}

// pastN reports whether the notice that started at noticeAt has run past N.
func (c *Controller) pastN(noticeAt time.Time) bool {
	return !noticeAt.IsZero() && !time.Now().Before(noticeAt.Add(c.NoticeWindow))
}

func (c *Controller) killRecordFor(key string) *killRecord {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	return c.kills[key]
}

func (c *Controller) newKillRecord(key, reason string) *killRecord {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.kills == nil {
		c.kills = make(map[string]*killRecord)
	}
	rec := &killRecord{reason: reason}
	c.kills[key] = rec
	return rec
}

// pruneKills drops the records of the group's guests that are gone from the
// job store (their mirror pod is gone), except those still held after an
// unconfirmed Kill (D-NS-6 block and escalate).
func (c *Controller) pruneKills(groupID string, known map[string]bool) {
	prefix := groupID + "\x00"
	c.killMu.Lock()
	defer c.killMu.Unlock()
	for key := range c.kills {
		if strings.HasPrefix(key, prefix) && !known[key] && c.holds[key] == nil {
			delete(c.kills, key)
		}
	}
}

// nextKillRetry reports whether a Kill for one of the group's guests has
// neither been confirmed nor handed back, and how long until the earliest of
// them may be sent again (at least minKillRequeue).
func (c *Controller) nextKillRetry(groupID string) (time.Duration, bool) {
	prefix := groupID + "\x00"
	c.killMu.Lock()
	defer c.killMu.Unlock()
	pending := false
	next := killRetryInterval
	for key, rec := range c.kills {
		if !strings.HasPrefix(key, prefix) || rec.confirmed || rec.handedBack {
			continue
		}
		pending = true
		if d := time.Until(rec.lastAttempt.Add(killRetryInterval)); d < next {
			next = d
		}
	}
	return max(next, minKillRequeue), pending
}

// hostClearHow names how a notice host was cleared by the orchestrator
// during the current notice: "kill", "unconfirmed-kill" or "revoked". It is
// empty when the orchestrator did nothing to clear it.
func (c *Controller) hostClearHow(groupID, node string) string {
	noticeAt := time.Time{}
	c.noticeMu.Lock()
	track := c.notices[groupID]
	revoked := false
	if track != nil {
		noticeAt = track.noticeAt
		revoked = track.revoked[node]
	}
	c.noticeMu.Unlock()

	prefix := groupID + "\x00" + node + "\x00"
	how := ""
	c.killMu.Lock()
	for key, rec := range c.kills {
		if !strings.HasPrefix(key, prefix) || rec.doneAt.Before(noticeAt) {
			continue
		}
		switch {
		case rec.handedBack:
			how = "unconfirmed-kill"
		case rec.confirmed && how == "":
			how = "kill"
		default:
		}
	}
	c.killMu.Unlock()
	if how == "" && revoked {
		how = "revoked"
	}
	return how
}
