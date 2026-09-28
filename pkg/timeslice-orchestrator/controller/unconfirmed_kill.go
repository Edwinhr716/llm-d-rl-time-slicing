package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file holds what happens when a Kill reached the agent but the guest is
// not confirmed gone (decision D-NS-6). Every such case goes through
// onKillUnconfirmed, so the decision can be swapped in one place.

// Signals that a Kill that reached the agent was not confirmed (D-NS-6 H4).
const (
	// unconfirmedKillTimeout: the kill operation was not COMPLETE within K.
	unconfirmedKillTimeout = "kill-timeout"
	// unconfirmedKillAgent: the agent failed the operation with
	// error_reason KILL_UNCONFIRMED.
	unconfirmedKillAgent = "agent-kill-unconfirmed"
	// unconfirmedKillFailed: the agent failed the operation for another
	// reason.
	unconfirmedKillFailed = "kill-failed"
	// unconfirmedDeviceBytes: the operation completed, but the agent status
	// still shows device_bytes > 0 for the guest.
	unconfirmedDeviceBytes = "device-bytes"
)

// unconfirmedDecision is what onKillUnconfirmed decided.
type unconfirmedDecision struct {
	// grant: hand the host back to the foreground now.
	grant bool
	// vramUnconfirmed: flag the next foreground grant with
	// AcquireResponse.vram_unconfirmed.
	vramUnconfirmed bool
}

// onKillUnconfirmed is the D-NS-6 H2 seam. It is called from vacateHost
// (kill.go), inside killOverdueHosts, the controller's decision for a host
// that is not clear at the deadline T (or unseen for L), for a guest whose
// Kill reached the agent but was not confirmed. since is when the first Kill
// for the guest in this barrier was sent. It decides whether the host is
// handed back to the foreground now and whether that grant carries
// AcquireResponse.vram_unconfirmed. It is called on every reconcile pass until
// it grants.
//
// Today's behaviour: grant once the notice window N has run out
// (noticeAt + N), and never before the Kill had its full budget K
// (since + K), with vram_unconfirmed = true.
func (c *Controller) onKillUnconfirmed(
	ctx context.Context, group, node, job string, since time.Time,
) unconfirmedDecision {
	bar, ok := c.Hosts.Barrier(group)
	if !ok {
		return unconfirmedDecision{}
	}
	noticeAt := bar.NoticeAt
	if g, err := c.groupStore.Get(ctx, group); err == nil {
		if n := g.Spec().NoticeAt(); !n.IsZero() {
			noticeAt = n
		}
	}
	grantAt := noticeAt.Add(bar.NoticeWindow)
	if floor := since.Add(bar.KillBudget); floor.After(grantAt) {
		grantAt = floor
	}
	if time.Now().Before(grantAt) {
		return unconfirmedDecision{}
	}
	slog.DebugContext(ctx, "Unconfirmed kill decided", "group", group, "node", node, "job", job,
		"grant", true, "vramUnconfirmed", true)
	return unconfirmedDecision{grant: true, vramUnconfirmed: true}
}

// handBackUnconfirmed applies a grant decided by onKillUnconfirmed: it counts
// the guest as vacated on node so the host can be marked clear, flags the next
// foreground grant with vram_unconfirmed when asked, and counts
// timeslice_kill_unconfirmed_total. The guest stays marked, so the host is not
// lent again while the agent still reports it (resumeIfLent).
func (c *Controller) handBackUnconfirmed(
	ctx context.Context, group *store.Group, job *store.Job, node string, rec *killRecord, vramUnconfirmed bool,
) {
	job.SetUnconfirmedKill(node)
	if vramUnconfirmed {
		group.Spec().SetVramUnconfirmed()
	}
	metrics.KillUnconfirmedTotal.Inc()
	rec.handedBack = true
	slog.WarnContext(ctx, "Kill unconfirmed", "group", group.ID(), "node", node, "job", job.JobID(),
		"elapsed_ms", time.Since(rec.firstSent).Milliseconds(), "action", "grant",
		"reason", rec.reason, "signal", rec.signal, "attempts", rec.attempts, "vram_unconfirmed", vramUnconfirmed)
}

// waitKillConfirmed polls the kill operation every KillPollInterval until it
// ends or ctx is done (ctx carries the deadline K). It returns "" when the
// Kill is confirmed, or the unconfirmed signal and the error behind it.
func (c *Controller) waitKillConfirmed(
	ctx context.Context, groupID, jobID, nodeName, operationID string,
) (string, error) {
	interval := c.KillPollInterval
	if interval <= 0 {
		interval = DefaultKillPollInterval
	}
	ctx = logging.WithNodeName(ctx, nodeName)
	ctx = logging.WithOperationID(ctx, operationID)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return unconfirmedKillTimeout, fmt.Errorf("kill operation %s not complete: %w", operationID, ctx.Err())
		case <-ticker.C:
		}
		resp, err := c.agentStore.GetOperation(ctx, nodeName, operationID)
		if err != nil {
			slog.WarnContext(ctx, "Failed to get kill operation status, will retry", "error", err)
			continue
		}
		switch resp.GetStatus() {
		case agentpb.OperationStatus_OPERATION_STATUS_COMPLETE:
			metrics.AgentOperationDuration.WithLabelValues(groupID, jobID, nodeName, "kill").
				Observe(float64(resp.GetElapsedMs()) / 1000.0)
			return c.checkDeviceBytes(ctx, jobID, nodeName)
		case agentpb.OperationStatus_OPERATION_STATUS_FAILED:
			err := fmt.Errorf("kill operation %s failed: %s (%s)", operationID, resp.GetError(), resp.GetErrorReason())
			if resp.GetErrorReason() == agentpb.ErrorReason_KILL_UNCONFIRMED {
				return unconfirmedKillAgent, err
			}
			return unconfirmedKillFailed, err
		default:
		}
	}
}

// checkDeviceBytes reads the agent status after a completed Kill. If the agent
// still reports device memory for the guest the Kill is unconfirmed. A status
// that cannot be read leaves the completed operation as the confirmation.
func (c *Controller) checkDeviceBytes(ctx context.Context, jobID, nodeName string) (string, error) {
	status, err := c.agentStore.GetStatus(ctx, nodeName)
	if err != nil {
		slog.WarnContext(ctx, "Could not read agent status after kill; the completed operation confirms it",
			"error", err)
		return "", nil
	}
	for _, js := range status.GetJobStatuses() {
		if js.GetJobId() == jobID && js.GetDeviceBytes() > 0 {
			return unconfirmedDeviceBytes, fmt.Errorf("agent reports %d device bytes after kill", js.GetDeviceBytes())
		}
	}
	return "", nil
}
