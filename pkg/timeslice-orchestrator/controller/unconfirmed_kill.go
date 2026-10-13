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
// onKillUnconfirmed, which hands the host back with vram_unconfirmed once the
// decision is due.

// UnconfirmedKillGrant is the action logged when the host is handed back at N
// with AcquireResponse.vram_unconfirmed = true.
const UnconfirmedKillGrant = "grant"

// UnconfirmedKube is what the unconfirmed-kill path does through the
// Kubernetes API (D-NS-6 H7). It is implemented by
// infrastructure.KubeActions.
type UnconfirmedKube interface {
	// GuestEvent records a Warning event on the guest's mirror pods on node.
	GuestEvent(ctx context.Context, group, job, node, reason, message string) error
}

// eventKillUnconfirmed is the Kubernetes Warning event reason (D-NS-6 H7).
const eventKillUnconfirmed = "KillUnconfirmed"

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
	// decideAt, when it does not grant, is the earliest time it may grant.
	// A retry Kill ends by then, so it never holds a due hand-back. Zero
	// means unknown: the retry gets the full K.
	decideAt time.Time
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
// (since + K). It then grants with vram_unconfirmed.
func (c *Controller) onKillUnconfirmed(
	ctx context.Context, group, node, job string, since time.Time,
) unconfirmedDecision {
	decideAt, ok := c.unconfirmedDecisionAt(ctx, group, since)
	if !ok {
		return unconfirmedDecision{}
	}
	if time.Now().Before(decideAt) {
		// Not due yet: a retry Kill must end by decideAt (ORCH-A6).
		return unconfirmedDecision{decideAt: decideAt}
	}
	return c.grantUnconfirmed(ctx, group, node, job)
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

// grantUnconfirmed hands the host back
// back now, with vram_unconfirmed = true. handBackUnconfirmed then logs
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
