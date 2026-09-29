package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file holds what happens when a Kill reached the agent but the guest is
// not confirmed gone (decision D-NS-6). Every such signal goes through
// onKillUnconfirmed, so the decision can be swapped in one place.
//
// PENDING LEAD DECISION D-NS-6, flag --unconfirmed-kill:
//   - grant (default, today's behaviour): grant at N with vram_unconfirmed
//     (unconfirmedGrant, below);
//   - block: never grant until the agent confirms the guest gone; alert
//     (unconfirmedBlock, unconfirmed_kill_block.go);
//   - escalate: block, plus an escalation ladder (unconfirmedEscalate,
//     unconfirmed_kill_escalate.go).

// Values of --unconfirmed-kill.
const (
	UnconfirmedKillGrant    = "grant"
	UnconfirmedKillBlock    = "block"
	UnconfirmedKillEscalate = "escalate"
)

// DefaultEscalateAfter is the default of --unconfirmed-escalate-after.
var DefaultEscalateAfter = [2]time.Duration{10 * time.Second, 40 * time.Second}

// DefaultEscalateAfterFlag is DefaultEscalateAfter as the flag string.
const DefaultEscalateAfterFlag = "10s,40s"

// ValidateUnconfirmedKill checks a --unconfirmed-kill value.
func ValidateUnconfirmedKill(mode string) error {
	switch mode {
	case UnconfirmedKillGrant, UnconfirmedKillBlock, UnconfirmedKillEscalate:
		return nil
	default:
		return fmt.Errorf("unknown unconfirmed kill action %q: must be %q, %q or %q",
			mode, UnconfirmedKillGrant, UnconfirmedKillBlock, UnconfirmedKillEscalate)
	}
}

// ParseEscalateAfter parses a --unconfirmed-escalate-after value: two
// positive, increasing durations separated by a comma, for example "10s,40s".
func ParseEscalateAfter(s string) ([2]time.Duration, error) {
	var out [2]time.Duration
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return out, fmt.Errorf("want two durations separated by a comma, got %q", s)
	}
	for i, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return out, fmt.Errorf("step %d: %w", i+1, err)
		}
		if d <= 0 {
			return out, fmt.Errorf("step %d must be positive, got %v", i+1, d)
		}
		out[i] = d
	}
	if out[1] <= out[0] {
		return out, fmt.Errorf("step 2 (%v) must come after step 1 (%v)", out[1], out[0])
	}
	return out, nil
}

// unconfirmedKillMode returns the configured --unconfirmed-kill value;
// empty means grant.
func (c *Controller) unconfirmedKillMode() string {
	if c.UnconfirmedKill == "" {
		return UnconfirmedKillGrant
	}
	return c.UnconfirmedKill
}

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
// not confirmed (rec.signal is one of the unconfirmedKill* values, and
// rec.unconfirmedSince is when that was first seen), whether the node is
// handed back to the foreground now and whether that grant carries
// AcquireResponse.vram_unconfirmed. pastN reports whether the notice window N
// has run out. It is called on every reconcile pass until it grants.
func (c *Controller) onKillUnconfirmed(
	ctx context.Context, group *store.Group, job *store.Job, node string, rec *killRecord, pastN bool,
) unconfirmedDecision {
	switch c.unconfirmedKillMode() {
	case UnconfirmedKillBlock:
		return c.unconfirmedBlock(ctx, group, job.JobID(), node, rec, pastN)
	case UnconfirmedKillEscalate:
		return c.unconfirmedEscalate(ctx, group, job.JobID(), node, rec, pastN)
	default:
		return c.unconfirmedGrant(ctx, group.ID(), job, node, rec.signal, pastN)
	}
}

// unconfirmedGrant is option "grant" (D-NS-6 "keep", today's behaviour):
// grant at N with vram_unconfirmed = true, whatever the signal.
func (c *Controller) unconfirmedGrant(
	ctx context.Context, groupID string, job *store.Job, node, signal string, pastN bool,
) unconfirmedDecision {
	if !pastN {
		return unconfirmedDecision{}
	}
	slog.DebugContext(ctx, "Unconfirmed kill decided", "group", groupID, "node", node, "job", job.JobID(),
		"signal", signal, "grant", true, "vramUnconfirmed", true)
	return unconfirmedDecision{grant: true, vramUnconfirmed: true}
}

// noteKillUnconfirmed runs once per guest and node, when a Kill that reached
// the agent is first seen unconfirmed, in every option: it counts
// timeslice_kill_unconfirmed_total, writes "Kill unconfirmed" with the
// configured action and records a KillUnconfirmed Warning event on the
// guest's mirror pod.
func (c *Controller) noteKillUnconfirmed(
	ctx context.Context, groupID, jobID, node string, rec *killRecord, err error,
) {
	if rec.counted {
		return
	}
	rec.counted = true
	rec.unconfirmedSince = time.Now()
	metrics.KillUnconfirmedTotal.Inc()
	action := c.unconfirmedKillMode()
	slog.WarnContext(ctx, "Kill unconfirmed", "group", groupID, "node", node, "job", jobID,
		"elapsed_ms", time.Since(rec.firstSent).Milliseconds(), "action", action, "signal", rec.signal,
		"reason", rec.reason, "error", err)
	c.warnPods(ctx, groupID, jobID, node, EventKillUnconfirmed,
		fmt.Sprintf("Kill of guest %s on node %s not confirmed (%s); action %s", jobID, node, rec.signal, action))
}

// handBackUnconfirmed applies a grant decided by onKillUnconfirmed: it counts
// the guest as vacated so the foreground is granted, flags that grant with
// vram_unconfirmed when asked, and marks the guest so the node is not lent
// again while the agent still reports it (grantIfVacant).
func (c *Controller) handBackUnconfirmed(
	ctx context.Context, group *store.Group, job *store.Job, node string, rec *killRecord, vramUnconfirmed bool,
) {
	job.SetUnconfirmedKill(node, true)
	if vramUnconfirmed {
		group.Spec().SetVramUnconfirmed()
	}
	rec.handedBack = true
	rec.doneAt = time.Now()
	slog.WarnContext(ctx, "Node handed back with an unconfirmed kill", "group", group.ID(), "node", node,
		"job", job.JobID(), "reason", rec.reason, "signal", rec.signal, "attempts", rec.attempts,
		"vramUnconfirmed", vramUnconfirmed)
}

// Kubernetes Warning event reasons written for D-NS-6.
const (
	// EventKillUnconfirmed goes on the guest's mirror pod, in every option.
	EventKillUnconfirmed = "KillUnconfirmed"
	// EventGrantBlocked goes on the waiting trainer's pods (block, escalate).
	EventGrantBlocked = "GrantBlocked"
	// EventNodeNotLendable goes on the trainer's and the guest's pods at the
	// second escalation step (escalate).
	EventNodeNotLendable = "NodeNotLendable"
)

// KubeActions is what the controller asks of Kubernetes after an unconfirmed
// kill. Pods are found by the labels timeslice.io/group and
// timeslice.io/job-id; node, when not empty, keeps only pods bound to it.
type KubeActions interface {
	// WarnPods records a Warning event with reason and message on each pod.
	WarnPods(ctx context.Context, groupID, jobID, node, reason, message string) error
	// DeletePodsGracefully deletes each pod with its own grace period (never
	// a grace period of 0) and reports how many deletes it issued.
	DeletePodsGracefully(ctx context.Context, groupID, jobID, node string) (int, error)
}

// kubeActionTimeout bounds each KubeActions call.
const kubeActionTimeout = 5 * time.Second

// warnPods records a Warning event through Kube, if set. A failure is logged
// and otherwise ignored.
func (c *Controller) warnPods(ctx context.Context, groupID, jobID, node, reason, message string) {
	if c.Kube == nil || jobID == "" {
		return
	}
	kctx, cancel := context.WithTimeout(ctx, kubeActionTimeout)
	defer cancel()
	if err := c.Kube.WarnPods(kctx, groupID, jobID, node, reason, message); err != nil {
		slog.WarnContext(ctx, "Failed to record a Warning event", "group", groupID, "job", jobID, "node", node,
			"eventReason", reason, "error", err)
	}
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
