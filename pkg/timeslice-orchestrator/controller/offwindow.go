package controller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file tracks each guest's off-window: how long it has been suspended,
// from its snapshot agent's state. The orchestrator cannot cut the foreground
// short to honour a limit (the RL job has priority), so it exports the
// off-window as timeslice_guest_offwindow_seconds and alerts, with a warning
// log and timeslice_guest_offwindow_exceeded_total, once an off-window passes
// --max-serving-offwindow. Past about 4 minutes, stock AP ends requests held
// on a frozen guest with DEADLINE_EXCEEDED.
//
// An off-window starts when a reconcile first sees the agent report the guest
// SUSPENDED. It ends when the agent reports anything else, or the guest leaves
// the job store (its mirror pod is gone). Between reconciles, the off-window
// ticker asks the agents of the nodes with a suspended guest directly, so the
// end is seen within one tick.

// DefaultMaxServingOffwindow is the default --max-serving-offwindow: stock AP's
// 5 minute request timeout minus the longest request and a margin.
const DefaultMaxServingOffwindow = 4 * time.Minute

// OffwindowTick is how often the off-window gauges are refreshed.
const OffwindowTick = time.Second

// offwindowEntry is the off-window of one guest on one node.
type offwindowEntry struct {
	group, job, node string
	since            time.Time
	// alerted is set once the off-window passed the limit.
	alerted bool
}

// offwindows is the set of running off-windows.
type offwindows struct {
	mu      sync.Mutex
	entries map[string]*offwindowEntry
}

func offwindowKey(group, job, node string) string {
	return group + "\x00" + job + "\x00" + node
}

// noteGuestState records the agent state of a guest on a node as a reconcile
// observed it.
func (c *Controller) noteGuestState(group, job, node string, state pb.SnapshotAgentJobState_State) {
	metrics.InitGuestSeries(group, node)
	c.offwindow.mu.Lock()
	defer c.offwindow.mu.Unlock()
	key := offwindowKey(group, job, node)
	if state != pb.SnapshotAgentJobState_STATE_SUSPENDED {
		c.endOffwindowLocked(key)
		return
	}
	if c.offwindow.entries == nil {
		c.offwindow.entries = make(map[string]*offwindowEntry)
	}
	if _, ok := c.offwindow.entries[key]; !ok {
		c.offwindow.entries[key] = &offwindowEntry{group: group, job: job, node: node, since: time.Now()}
		metrics.GuestOffwindowSeconds.WithLabelValues(group, job, node).Set(0)
	}
}

// endOffwindowLocked drops an off-window and its series. offwindow.mu is held.
func (c *Controller) endOffwindowLocked(key string) {
	entry, ok := c.offwindow.entries[key]
	if !ok {
		return
	}
	delete(c.offwindow.entries, key)
	metrics.GuestOffwindowSeconds.DeleteLabelValues(entry.group, entry.job, entry.node)
}

// updateOffwindows refreshes every running off-window: it ends the ones whose
// guest left the store or whose agent no longer reports it SUSPENDED, sets
// the gauge of the others, and alerts once per off-window past
// MaxServingOffwindow.
func (c *Controller) updateOffwindows(ctx context.Context) {
	entries := c.offwindowSnapshot()
	if len(entries) == 0 {
		return
	}
	// One agent Status per node that has a suspended guest.
	states := make(map[string]map[string]agentpb.JobState)
	for _, e := range entries {
		if _, ok := states[e.node]; ok {
			continue
		}
		resp, err := c.agentStore.GetStatus(ctx, e.node)
		if err != nil {
			// Unknown: keep the off-window running (the guest may still be
			// frozen) and try again next tick.
			states[e.node] = nil
			continue
		}
		byJob := make(map[string]agentpb.JobState, len(resp.GetJobStatuses()))
		for _, js := range resp.GetJobStatuses() {
			byJob[js.GetJobId()] = js.GetState()
		}
		states[e.node] = byJob
	}

	now := time.Now()
	limit := c.MaxServingOffwindow
	c.offwindow.mu.Lock()
	defer c.offwindow.mu.Unlock()
	for _, e := range entries {
		key := offwindowKey(e.group, e.job, e.node)
		entry, ok := c.offwindow.entries[key]
		if !ok || !entry.since.Equal(e.since) {
			continue // ended or restarted meanwhile
		}
		if _, err := c.jobStore.Get(ctx, e.group, e.job); errors.Is(err, store.ErrNotFound) {
			c.endOffwindowLocked(key)
			continue
		}
		if byJob := states[e.node]; byJob != nil && byJob[e.job] != agentpb.JobState_JOB_STATE_SUSPENDED {
			c.endOffwindowLocked(key)
			continue
		}
		offwindow := now.Sub(entry.since)
		metrics.GuestOffwindowSeconds.WithLabelValues(e.group, e.job, e.node).Set(offwindow.Seconds())
		if limit > 0 && offwindow > limit && !entry.alerted {
			entry.alerted = true
			metrics.GuestOffwindowExceededTotal.WithLabelValues(e.group).Inc()
			slog.WarnContext(ctx, "Guest off-window over the limit: requests held on it may fail",
				"group", e.group, "job", e.job, "node", e.node,
				"offwindow_s", int64(offwindow.Seconds()), "limit_s", int64(limit.Seconds()))
		}
	}
}

// offwindowSnapshot copies the running off-windows.
func (c *Controller) offwindowSnapshot() []offwindowEntry {
	c.offwindow.mu.Lock()
	defer c.offwindow.mu.Unlock()
	out := make([]offwindowEntry, 0, len(c.offwindow.entries))
	for _, e := range c.offwindow.entries {
		out = append(out, *e)
	}
	return out
}
