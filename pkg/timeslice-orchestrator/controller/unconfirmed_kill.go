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
// not confirmed gone (decision D-NS-6). Every such signal goes through
// onKillUnconfirmed, so the decision can be swapped in one place.

// Signals that a Kill that reached the agent was not confirmed.
const (
	// unconfirmedKillTimeout: the kill operation did not complete within K.
	unconfirmedKillTimeout = "kill-timeout"
	// unconfirmedKillAgent: the agent failed the operation with
	// ERROR_REASON_KILL_UNCONFIRMED.
	unconfirmedKillAgent = "agent-kill-unconfirmed"
	// unconfirmedKillFailed: the agent failed the operation for another
	// reason.
	unconfirmedKillFailed = "kill-failed"
	// unconfirmedDeviceBytes: the operation completed, but the agent status
	// still shows device_bytes > 0 for the guest.
	unconfirmedDeviceBytes = "device-bytes"
)

// unconfirmedDecision is the result of onKillUnconfirmed.
type unconfirmedDecision struct {
	// grant hands the node back to the foreground now.
	grant bool
	// vramUnconfirmed sets AcquireResponse.vram_unconfirmed on that grant.
	vramUnconfirmed bool
}

// onKillUnconfirmed decides, for a guest whose Kill reached the agent but was
// not confirmed (signal is one of the unconfirmedKill* values), whether the
// node is handed back to the foreground now and whether that grant carries
// AcquireResponse.vram_unconfirmed. pastN reports whether the notice window N
// has run out. It is called on every reconcile pass until it grants.
//
// Today's behaviour (D-NS-6 "keep"): grant at N with vram_unconfirmed = true,
// whatever the signal.
func (c *Controller) onKillUnconfirmed(
	ctx context.Context, groupID string, job *store.Job, node, signal string, pastN bool,
) unconfirmedDecision {
	if !pastN {
		return unconfirmedDecision{}
	}
	slog.DebugContext(ctx, "Unconfirmed kill decided", "group", groupID, "node", node, "job", job.JobID(),
		"signal", signal, "grant", true, "vramUnconfirmed", true)
	return unconfirmedDecision{grant: true, vramUnconfirmed: true}
}

// handBackUnconfirmed applies a grant decided by onKillUnconfirmed: it counts
// the guest as vacated so the foreground is granted, flags that grant with
// vram_unconfirmed when asked, counts timeslice_kill_unconfirmed_total, and
// marks the guest so the node is not lent again while the agent still reports
// it (grantIfVacant).
func (c *Controller) handBackUnconfirmed(
	ctx context.Context, group *store.Group, job *store.Job, node string, rec *killRecord, vramUnconfirmed bool,
) {
	job.SetUnconfirmedKill(node, true)
	if vramUnconfirmed {
		group.Spec().SetVramUnconfirmed()
	}
	metrics.KillUnconfirmedTotal.Inc()
	rec.handedBack = true
	rec.doneAt = time.Now()
	slog.WarnContext(ctx, "Node handed back with an unconfirmed kill", "group", group.ID(), "node", node,
		"job", job.JobID(), "reason", rec.reason, "signal", rec.signal, "attempts", rec.attempts,
		"vramUnconfirmed", vramUnconfirmed)
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
