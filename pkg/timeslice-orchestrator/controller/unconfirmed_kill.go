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
)

// This file holds what happens when a Kill reached the agent but the guest is
// not confirmed gone. The foreground is never granted over such a guest: its
// host stays not clear, the grant is blocked and the operator is alerted
// (unconfirmed_kill_block.go), and the block escalates on a ladder
// (unconfirmed_kill_escalate.go). The block ends only when the agent confirms
// the guest gone or its mirror pod is gone.

// Signals that a Kill that reached the agent was not confirmed.
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

// DefaultUnconfirmedEscalateAfter is the default of
// --unconfirmed-escalate-after: escalation step 1 and step 2, measured from
// the unconfirmed-kill decision (T + K).
const DefaultUnconfirmedEscalateAfter = "10s,40s"

// ParseUnconfirmedEscalateAfter parses a --unconfirmed-escalate-after value,
// "<E1>,<E2>" with 0 < E1 < E2.
func ParseUnconfirmedEscalateAfter(value string) ([2]time.Duration, error) {
	var out [2]time.Duration
	parts := strings.Split(value, ",")
	if len(parts) != 2 {
		return out, fmt.Errorf("want two durations <step 1>,<step 2>, got %q", value)
	}
	for i, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return out, fmt.Errorf("step %d: %w", i+1, err)
		}
		out[i] = d
	}
	if out[0] <= 0 || out[1] <= out[0] {
		return out, fmt.Errorf("want 0 < step 1 < step 2, got %v,%v", out[0], out[1])
	}
	return out, nil
}

// UnconfirmedKube is what the unconfirmed-kill path does through the
// Kubernetes API. It is implemented by infrastructure.KubeActions. It never
// deletes with grace period 0 and never deletes a Node.
type UnconfirmedKube interface {
	// GuestEvent records a Warning event on the guest's mirror pods on node.
	GuestEvent(ctx context.Context, group, job, node, reason, message string) error
	// ForegroundEvent records a Warning event on the pods of a foreground
	// job of the group.
	ForegroundEvent(ctx context.Context, group, job, reason, message string) error
	// NodeEvent records a Warning event on a Node.
	NodeEvent(ctx context.Context, node, reason, message string) error
	// DeleteGuestMirror deletes the guest's mirror pods on node gracefully
	// (the pod's own grace period) and reports how many it deleted.
	DeleteGuestMirror(ctx context.Context, group, job, node string) (int, error)
}

// Kubernetes Warning event reasons.
const (
	eventKillUnconfirmed = "KillUnconfirmed"
	eventGrantBlocked    = "GrantBlocked"
	eventNodeNotLendable = "NodeNotLendable"
)

// kubeCallTimeout bounds each UnconfirmedKube call.
const kubeCallTimeout = 5 * time.Second

// kubeCall runs one UnconfirmedKube call, bounded by kubeCallTimeout, and logs
// a failure. It does nothing when no UnconfirmedKube is set.
func (c *Controller) kubeCall(ctx context.Context, what string, call func(context.Context, UnconfirmedKube) error) {
	if c.Kube == nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, kubeCallTimeout)
	defer cancel()
	if err := call(cctx, c.Kube); err != nil {
		slog.WarnContext(ctx, "Kubernetes call for an unconfirmed kill failed", "call", what, "error", err)
	}
}

// onKillUnconfirmed is called from vacateHost (kill.go), inside
// killOverdueHosts, for a host that is not clear at the deadline T (or unseen
// for L), for a guest whose Kill reached the agent but was not confirmed.
// since is when the first Kill for the guest in this barrier was sent. The
// host is never handed back here: the caller keeps it not clear. Once the
// notice window N has run out (noticeAt + N), and never before the Kill had
// its full budget K (since + K), the grant is recorded as blocked, the
// operator is alerted and the escalation ladder starts. It is called on every
// reconcile pass until the guest is vacated.
func (c *Controller) onKillUnconfirmed(ctx context.Context, group, node, job string, since time.Time) {
	decideAt, ok := c.unconfirmedDecisionAt(ctx, group, since)
	if !ok || time.Now().Before(decideAt) {
		return
	}
	if st, first := c.holdUnconfirmed(ctx, group, node, job, since, decideAt); first {
		c.startEscalation(ctx, st)
	}
}

// unconfirmedDecisionAt returns when the unconfirmed-kill decision for a guest
// whose first Kill was sent at since is taken: at noticeAt + N, and not before
// since + K. It reports false when the group has no running barrier.
func (c *Controller) unconfirmedDecisionAt(ctx context.Context, group string, since time.Time) (time.Time, bool) {
	bar, ok := c.Hosts.Barrier(group)
	if !ok {
		return time.Time{}, false
	}
	noticeAt := bar.NoticeAt
	if g, err := c.groupStore.Get(ctx, group); err == nil {
		if n := g.Spec().NoticeAt(); !n.IsZero() {
			noticeAt = n
		}
	}
	decideAt := noticeAt.Add(bar.NoticeWindow)
	if floor := since.Add(bar.KillBudget); floor.After(decideAt) {
		decideAt = floor
	}
	return decideAt, true
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
