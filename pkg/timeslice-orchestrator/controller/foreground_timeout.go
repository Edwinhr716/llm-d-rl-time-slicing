package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// What reconcile does when a foreground snapshot or restore passes
// ForegroundOpTimeout (--foreground-op-timeout-action).
//
// PENDING LEAD DECISION (D-ORCH-3): the three values are the options. Each
// has its own branch in onForegroundOpTimeout, so the losing ones can be
// deleted cleanly.
const (
	// ForegroundOpTimeoutActionRetry returns an error, so the group is
	// retried with the controller's rate limiter. The next pass waits on the
	// same operation again and never starts a new one while it is pending.
	// The default: it is what the code did before the flag.
	ForegroundOpTimeoutActionRetry = "retry"
	// ForegroundOpTimeoutActionFaulted marks the job FAULTED on the node at
	// the first timeout.
	ForegroundOpTimeoutActionFaulted = "faulted"
	// ForegroundOpTimeoutActionBounded retries like "retry" until the same
	// operation has timed out ForegroundOpTimeoutRetries more times, then
	// marks the job FAULTED like "faulted".
	ForegroundOpTimeoutActionBounded = "bounded"

	// DefaultForegroundOpTimeoutRetries is the "bounded" retry count.
	DefaultForegroundOpTimeoutRetries = 3
)

// ErrForegroundOpTimeout is wrapped by waitForOperation when the wait passes
// ForegroundOpTimeout (not when the caller's context ends).
var ErrForegroundOpTimeout = errors.New("foreground operation timed out")

// ValidateForegroundOpTimeoutAction checks a --foreground-op-timeout-action value.
func ValidateForegroundOpTimeoutAction(action string) error {
	switch action {
	case ForegroundOpTimeoutActionRetry, ForegroundOpTimeoutActionFaulted, ForegroundOpTimeoutActionBounded:
		return nil
	default:
		return fmt.Errorf("unknown foreground operation timeout action %q: must be %q, %q or %q", action,
			ForegroundOpTimeoutActionRetry, ForegroundOpTimeoutActionFaulted, ForegroundOpTimeoutActionBounded)
	}
}

// timedOutOp is a foreground operation that passed ForegroundOpTimeout and may
// still be running on the agent. While one is recorded for a node, reconcile
// waits on it instead of starting another snapshot or restore there.
type timedOutOp struct {
	jobID       string
	operationID string
	opType      string
	// timeouts counts the waits on this operation that timed out.
	timeouts int
}

func timedOutOpKey(groupID, nodeName string) string {
	return groupID + "/" + nodeName
}

func (c *Controller) foregroundOpTimeoutAction() string {
	if c.ForegroundOpTimeoutAction == "" {
		return ForegroundOpTimeoutActionRetry
	}
	return c.ForegroundOpTimeoutAction
}

func (c *Controller) getTimedOutOp(groupID, nodeName string) *timedOutOp {
	c.timedOutMu.Lock()
	defer c.timedOutMu.Unlock()
	if op, ok := c.timedOutOps[timedOutOpKey(groupID, nodeName)]; ok {
		cp := *op
		return &cp
	}
	return nil
}

// recordTimeout counts one more timeout of operationID on the node and returns
// the count.
func (c *Controller) recordTimeout(groupID, nodeName, jobID, operationID, opType string) int {
	c.timedOutMu.Lock()
	defer c.timedOutMu.Unlock()
	if c.timedOutOps == nil {
		c.timedOutOps = make(map[string]*timedOutOp)
	}
	key := timedOutOpKey(groupID, nodeName)
	op, ok := c.timedOutOps[key]
	if !ok || op.operationID != operationID {
		op = &timedOutOp{jobID: jobID, operationID: operationID, opType: opType}
		c.timedOutOps[key] = op
	}
	op.timeouts++
	return op.timeouts
}

func (c *Controller) clearTimedOutOp(groupID, nodeName string) {
	c.timedOutMu.Lock()
	defer c.timedOutMu.Unlock()
	delete(c.timedOutOps, timedOutOpKey(groupID, nodeName))
}

// isForegroundOpTimeout reports whether err is a wait that passed
// ForegroundOpTimeout while ctx is still live.
func isForegroundOpTimeout(ctx context.Context, err error) bool {
	return ctx.Err() == nil && errors.Is(err, ErrForegroundOpTimeout)
}

// onForegroundOpTimeout applies the configured action to a foreground
// operation that passed ForegroundOpTimeout. It always returns an error, so the
// group is retried through the rate limiter.
func (c *Controller) onForegroundOpTimeout(
	ctx context.Context, groupID, nodeName, jobID, operationID, opType string, cause error,
) error {
	action := c.foregroundOpTimeoutAction()
	timeouts := c.recordTimeout(groupID, nodeName, jobID, operationID, opType)
	attrs := []any{
		"group", groupID, "job", jobID, "node", nodeName, "operation_id", operationID,
		"type", opType, "action", action, "timeouts", timeouts,
	}

	var fault bool
	switch action {
	case ForegroundOpTimeoutActionFaulted:
		fault = true
	case ForegroundOpTimeoutActionBounded:
		fault = timeouts > c.ForegroundOpTimeoutRetries
	default: // ForegroundOpTimeoutActionRetry
		fault = false
	}

	if !fault {
		slog.WarnContext(ctx, "Foreground operation timed out", append(attrs, "outcome", "retry")...)
		return fmt.Errorf("%w; the group is retried and waits on operation %s instead of starting a new one",
			cause, operationID)
	}

	c.clearTimedOutOp(groupID, nodeName)
	marked := c.markForegroundTimeoutFault(ctx, groupID, nodeName, jobID, operationID)
	slog.ErrorContext(ctx, "Foreground operation timed out", append(attrs, "outcome", "faulted", "marked", marked)...)
	return fmt.Errorf("%w; job %s marked FAULTED on node %s, requires human intervention", cause, jobID, nodeName)
}

// markForegroundTimeoutFault marks the job FAULTED on the node in the job
// store, where the server's group fault check and the reconcile loop read it.
// It reports false if the job is not in the store (it has no pods, so there is
// nothing to fault).
func (c *Controller) markForegroundTimeoutFault(ctx context.Context, groupID, nodeName, jobID, operationID string) bool {
	job, err := c.jobStore.Get(ctx, groupID, jobID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			slog.ErrorContext(ctx, "Failed to get job to mark it FAULTED", "job", jobID, "error", err)
		}
		return false
	}
	job.MarkForegroundTimeoutFault(nodeName, operationID)
	return true
}

// resumeTimedOutOp runs before reconcileNode starts any operation on the node.
// If an earlier foreground operation there timed out, it waits on that same
// operation again (bounded by ForegroundOpTimeout) and never issues a new one
// while it is pending. It returns nil once nothing is pending, after
// refreshing the node's agent states if the operation finished.
func (c *Controller) resumeTimedOutOp(ctx context.Context, groupID, nodeName string) error {
	op := c.getTimedOutOp(groupID, nodeName)
	if op == nil {
		return nil
	}
	attrs := []any{"group", groupID, "job", op.jobID, "node", nodeName, "operation_id", op.operationID, "type", op.opType}

	if _, err := c.jobStore.Get(ctx, groupID, op.jobID); errors.Is(err, store.ErrNotFound) {
		slog.InfoContext(ctx, "Job of a timed-out foreground operation is gone, forgetting the operation", attrs...)
		c.clearTimedOutOp(groupID, nodeName)
		return nil
	}

	// One poll first: an operation the agent no longer knows (the agent
	// restarted) must not pin the group, and a finished one needs no wait.
	resp, err := c.agentStore.GetOperation(ctx, nodeName, op.operationID)
	switch {
	case status.Code(err) == codes.NotFound:
		slog.WarnContext(ctx, "Timed-out foreground operation is unknown to the agent, forgetting it", attrs...)
		c.clearTimedOutOp(groupID, nodeName)
		return nil
	case err == nil && resp.Status == agentpb.OperationStatus_OPERATION_STATUS_COMPLETE:
		slog.InfoContext(ctx, "Timed-out foreground operation finished", attrs...)
		c.clearTimedOutOp(groupID, nodeName)
		return c.observeNodeJobContext(ctx, groupID, nodeName)
	case err == nil && resp.Status == agentpb.OperationStatus_OPERATION_STATUS_FAILED:
		c.clearTimedOutOp(groupID, nodeName)
		return fmt.Errorf("timed-out %s operation %s for job %s on node %s failed: %s",
			op.opType, op.operationID, op.jobID, nodeName, resp.GetError())
	}

	slog.InfoContext(ctx, "Waiting again on a timed-out foreground operation", append(attrs, "timeouts", op.timeouts)...)
	err = c.waitForOperation(ctx, groupID, op.jobID, nodeName, op.operationID, op.opType)
	switch {
	case err == nil:
		c.clearTimedOutOp(groupID, nodeName)
		return c.observeNodeJobContext(ctx, groupID, nodeName)
	case isForegroundOpTimeout(ctx, err):
		return c.onForegroundOpTimeout(ctx, groupID, nodeName, op.jobID, op.operationID, op.opType, err)
	case ctx.Err() != nil:
		// Shutting down: keep the record.
		return err
	default:
		// The operation failed on the agent: nothing is pending any more.
		c.clearTimedOutOp(groupID, nodeName)
		return fmt.Errorf("timed-out %s operation %s for job %s on node %s: %w",
			op.opType, op.operationID, op.jobID, nodeName, err)
	}
}
