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
// not confirmed gone (decision D-NS-6). Every such case goes through
// onKillUnconfirmed, so the decision can be swapped in one place. The flag
// --unconfirmed-kill picks the option; each option has its own function:
//
//   - grant (the default, today's behaviour): grantUnconfirmed, here;
//   - block: blockUnconfirmed, unconfirmed_kill_block.go;
//   - escalate: escalateUnconfirmed, unconfirmed_kill_escalate.go (block plus
//     an escalation ladder).

// Values of --unconfirmed-kill (D-NS-6 H1).
const (
	// UnconfirmedKillGrant hands the host back at N with
	// AcquireResponse.vram_unconfirmed = true (today's behaviour, the
	// default).
	UnconfirmedKillGrant = "grant"
	// UnconfirmedKillBlock never hands the host back until the Kill is
	// confirmed: it alerts and keeps the foreground waiting.
	UnconfirmedKillBlock = "block"
	// UnconfirmedKillEscalate blocks like UnconfirmedKillBlock and escalates
	// after --unconfirmed-escalate-after.
	UnconfirmedKillEscalate = "escalate"
)

// DefaultUnconfirmedEscalateAfter is the default of
// --unconfirmed-escalate-after: escalation step 1 and step 2, measured from
// the unconfirmed-kill decision (T + K).
const DefaultUnconfirmedEscalateAfter = "10s,40s"

// ValidateUnconfirmedKill checks a --unconfirmed-kill value.
func ValidateUnconfirmedKill(mode string) error {
	switch mode {
	case UnconfirmedKillGrant, UnconfirmedKillBlock, UnconfirmedKillEscalate:
		return nil
	default:
		return fmt.Errorf("unknown unconfirmed-kill action %q: must be %q, %q or %q",
			mode, UnconfirmedKillGrant, UnconfirmedKillBlock, UnconfirmedKillEscalate)
	}
}

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

// UnconfirmedKube is what the unconfirmed-kill paths do through the
// Kubernetes API (D-NS-6 H7). It is implemented by
// infrastructure.KubeActions. It never deletes with grace period 0 and never
// deletes a Node.
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

// Kubernetes Warning event reasons (D-NS-6 H7).
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

// unconfirmedKillMode returns the --unconfirmed-kill value, grant when unset.
func (c *Controller) unconfirmedKillMode() string {
	if c.UnconfirmedKill == "" {
		return UnconfirmedKillGrant
	}
	return c.UnconfirmedKill
}

// onKillUnconfirmed is the D-NS-6 H2 seam. It is called from vacateHost
// (kill.go), inside killOverdueHosts, the controller's decision for a host
// that is not clear at the deadline T (or unseen for L), for a guest whose
// Kill reached the agent but was not confirmed. since is when the first Kill
// for the guest in this barrier was sent. It decides whether the host is
// handed back to the foreground now and whether that grant carries
// AcquireResponse.vram_unconfirmed. It is called on every reconcile pass until
// it grants or the guest is vacated.
//
// The decision is taken once the notice window N has run out
// (noticeAt + N), and never before the Kill had its full budget K
// (since + K). What it decides depends on --unconfirmed-kill.
func (c *Controller) onKillUnconfirmed(
	ctx context.Context, group, node, job string, since time.Time,
) unconfirmedDecision {
	decideAt, ok := c.unconfirmedDecisionAt(ctx, group, since)
	if !ok || time.Now().Before(decideAt) {
		return unconfirmedDecision{}
	}
	switch c.unconfirmedKillMode() {
	case UnconfirmedKillBlock:
		return c.blockUnconfirmed(ctx, group, node, job, since, decideAt)
	case UnconfirmedKillEscalate:
		return c.escalateUnconfirmed(ctx, group, node, job, since, decideAt)
	default:
		return c.grantUnconfirmed(ctx, group, node, job)
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

// grantUnconfirmed is option grant (today's behaviour): hand the host back
// now, with vram_unconfirmed = true. handBackUnconfirmed then logs
// "Kill unconfirmed" and counts timeslice_kill_unconfirmed_total.
func (c *Controller) grantUnconfirmed(ctx context.Context, group, node, job string) unconfirmedDecision {
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
	elapsed := time.Since(rec.firstSent)
	slog.WarnContext(ctx, "Kill unconfirmed", "group", group.ID(), "node", node, "job", job.JobID(),
		"elapsed_ms", elapsed.Milliseconds(), "action", UnconfirmedKillGrant,
		"reason", rec.reason, "signal", rec.signal, "attempts", rec.attempts, "vram_unconfirmed", vramUnconfirmed)
	c.kubeCall(ctx, "event "+eventKillUnconfirmed, func(ctx context.Context, k UnconfirmedKube) error {
		return k.GuestEvent(ctx, group.ID(), job.JobID(), node, eventKillUnconfirmed, fmt.Sprintf(
			"Kill of guest %s on node %s not confirmed after %v (%s); host handed back to the foreground "+
				"with vram_unconfirmed", job.JobID(), node, elapsed.Round(time.Millisecond), rec.signal))
	})
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
