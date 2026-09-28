package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
)

// Foreground wait option B ("async-requeue", design section 6): reconcile
// starts a foreground snapshot or restore, keeps its operation ID in memory
// and returns. The group is requeued every foregroundCheckInterval and each
// pass checks the operation with a single GetOperation, so no worker ever
// blocks on an agent operation. The trainer's Acquire is unchanged: it keeps
// blocking and returns once the restore has completed and the job is loaded.

// foregroundCheckInterval is how often a group with a foreground operation in
// flight is requeued to check it. It matches the blocking poll interval.
const foregroundCheckInterval = operationPollInterval

// foregroundCheckRPCTimeout bounds each GetOperation made by a check, so a
// wedged RPC cannot hold the worker either.
const foregroundCheckRPCTimeout = 5 * time.Second

// errForegroundPending is returned by reconcileNode while a foreground
// operation on the node is still running. It is not a failure: the group is
// requeued after foregroundCheckInterval, not rate limited.
var errForegroundPending = errors.New("foreground operation in flight")

// foregroundOp is the in-memory record of one foreground operation on a node.
// It is lost on restart; see checkForegroundOp for how that is handled.
type foregroundOp struct {
	jobID       string
	operationID string // empty when adopted after a restart
	opType      string // snapshot, restore, or unknown when adopted
	started     time.Time
	adopted     bool // tracked by agent state, not by operation ID
	timedOut    bool // --foreground-op-timeout passed; back to the base path
}

func (c *Controller) asyncForeground() bool {
	return c.ForegroundWait == ForegroundWaitAsyncRequeue || c.ForegroundWait == ForegroundWaitAsync
}

func foregroundKey(groupID, nodeName string) string {
	return groupID + "/" + nodeName
}

func (c *Controller) getForegroundOp(groupID, nodeName string) *foregroundOp {
	c.fgMu.Lock()
	defer c.fgMu.Unlock()
	return c.fgOps[foregroundKey(groupID, nodeName)]
}

func (c *Controller) setForegroundOp(groupID, nodeName string, op *foregroundOp) {
	c.fgMu.Lock()
	defer c.fgMu.Unlock()
	if c.fgOps == nil {
		c.fgOps = make(map[string]*foregroundOp)
	}
	c.fgOps[foregroundKey(groupID, nodeName)] = op
}

func (c *Controller) deleteForegroundOp(groupID, nodeName string) {
	c.fgMu.Lock()
	defer c.fgMu.Unlock()
	delete(c.fgOps, foregroundKey(groupID, nodeName))
}

// pruneForegroundOps drops records for nodes that left the group.
func (c *Controller) pruneForegroundOps(groupID string, nodes []string) {
	keep := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		keep[foregroundKey(groupID, n)] = true
	}
	prefix := groupID + "/"
	c.fgMu.Lock()
	defer c.fgMu.Unlock()
	for k := range c.fgOps {
		if strings.HasPrefix(k, prefix) && !keep[k] {
			delete(c.fgOps, k)
		}
	}
}

// requeueAfter schedules the group again after d without touching the rate
// limiter.
func (c *Controller) requeueAfter(groupID string, d time.Duration) {
	if dq, ok := c.queue.(interface {
		AddAfter(item string, d time.Duration)
	}); ok {
		dq.AddAfter(groupID, d)
		return
	}
	time.AfterFunc(d, func() { c.queue.Add(groupID) })
}

func logForegroundStarted(ctx context.Context, groupID, jobID, nodeName, operationID, opType string) {
	slog.InfoContext(ctx, "Foreground operation started",
		"group", groupID, "job", jobID, "node", nodeName, "operation_id", operationID, "type", opType)
}

func logForegroundFinished(ctx context.Context, groupID, jobID, nodeName, operationID, opType, outcome string,
	elapsed time.Duration,
) {
	slog.InfoContext(ctx, "Foreground operation finished",
		"group", groupID, "job", jobID, "node", nodeName, "operation_id", operationID, "type", opType,
		"outcome", outcome, "elapsed", elapsed)
}

// startForegroundOp records an operation that was just started on the agent
// and returns errForegroundPending so the node is checked on the next requeue.
func (c *Controller) startForegroundOp(ctx context.Context, groupID, jobID, nodeName, operationID, opType string) error {
	c.setForegroundOp(groupID, nodeName, &foregroundOp{
		jobID:       jobID,
		operationID: operationID,
		opType:      opType,
		started:     time.Now(),
	})
	logForegroundStarted(ctx, groupID, jobID, nodeName, operationID, opType)
	return errForegroundPending
}

// checkForegroundOp runs at the top of reconcileNode in async mode.
//
//   - No record and some job is TRANSITIONING on the node: the operation was
//     started by a previous process (restart) whose record is gone. The agent
//     reports no operation ID in Status, so it cannot be adopted by ID; it is
//     logged "Foreground operation lost" and tracked by agent state instead.
//     Nothing is re-issued, so there is never a duplicate operation.
//   - A record in flight: one GetOperation (or a state check for a lost
//     one). Pending returns errForegroundPending; complete refreshes the agent
//     state and returns refreshed = true so the node continues this pass;
//     failed returns an error.
//   - Past --foreground-op-timeout: logged outcome=timeout and an error
//     wrapping context.DeadlineExceeded, as the blocking wait does. The record
//     stays, marked timed out, so later passes take the base path (TRANSITIONING
//     returns an error and a rate-limited retry) until the job leaves
//     TRANSITIONING. What happens after a timeout is decision D-ORCH-3.
func (c *Controller) checkForegroundOp(ctx context.Context, groupID, nodeName string,
	states map[string]pb.SnapshotAgentJobState_State,
) (bool, error) {
	op := c.getForegroundOp(groupID, nodeName)
	if op == nil {
		for jobID, state := range states {
			if state != pb.SnapshotAgentJobState_STATE_TRANSITIONING {
				continue
			}
			c.setForegroundOp(groupID, nodeName, &foregroundOp{
				jobID: jobID, opType: "unknown", started: time.Now(), adopted: true,
			})
			slog.WarnContext(ctx, "Foreground operation lost",
				"group", groupID, "job", jobID, "node", nodeName, "operation_id", "",
				"reason", "job is TRANSITIONING with no in-memory operation record (orchestrator restarted?); "+
					"tracking it by agent state, not re-issuing it")
			return false, errForegroundPending
		}
		return false, nil
	}

	if op.timedOut {
		if states[op.jobID] != pb.SnapshotAgentJobState_STATE_TRANSITIONING {
			c.deleteForegroundOp(groupID, nodeName)
		}
		return false, nil
	}

	elapsed := time.Since(op.started)
	if c.ForegroundOpTimeout > 0 && elapsed > c.ForegroundOpTimeout {
		op.timedOut = true
		logForegroundFinished(ctx, groupID, op.jobID, nodeName, op.operationID, op.opType, "timeout", elapsed)
		return false, fmt.Errorf("foreground %s operation %q for job %s on node %s still running after %v: %w",
			op.opType, op.operationID, op.jobID, nodeName, c.ForegroundOpTimeout, context.DeadlineExceeded)
	}

	if op.adopted {
		switch states[op.jobID] {
		case pb.SnapshotAgentJobState_STATE_TRANSITIONING:
			return false, errForegroundPending
		case pb.SnapshotAgentJobState_STATE_FAULTED:
			c.deleteForegroundOp(groupID, nodeName)
			logForegroundFinished(ctx, groupID, op.jobID, nodeName, "", op.opType, "failed", elapsed)
			return false, fmt.Errorf("adopted foreground operation for job %s on node %s ended FAULTED", op.jobID, nodeName)
		default:
			// The states were read from the agent at the start of this pass.
			c.deleteForegroundOp(groupID, nodeName)
			logForegroundFinished(ctx, groupID, op.jobID, nodeName, "", op.opType, "complete", elapsed)
			return false, nil
		}
	}

	rpcCtx, cancel := context.WithTimeout(ctx, foregroundCheckRPCTimeout)
	resp, err := c.agentStore.GetOperation(rpcCtx, nodeName, op.operationID)
	cancel()
	if err != nil {
		slog.WarnContext(ctx, "Failed to get foreground operation status, will check again on requeue",
			"group", groupID, "job", op.jobID, "node", nodeName, "operation_id", op.operationID, "error", err)
		return false, errForegroundPending
	}

	switch resp.Status {
	case agentpb.OperationStatus_OPERATION_STATUS_COMPLETE:
		c.deleteForegroundOp(groupID, nodeName)
		logForegroundFinished(ctx, groupID, op.jobID, nodeName, op.operationID, op.opType, "complete", elapsed)
		metrics.AgentOperationDuration.WithLabelValues(groupID, op.jobID, nodeName, op.opType).
			Observe(float64(resp.ElapsedMs) / 1000.0)
		if err := c.observeNodeJobContext(ctx, groupID, nodeName); err != nil {
			return false, fmt.Errorf("failed to refresh agent state after %s: %w", op.opType, err)
		}
		return true, nil
	case agentpb.OperationStatus_OPERATION_STATUS_FAILED:
		c.deleteForegroundOp(groupID, nodeName)
		logForegroundFinished(ctx, groupID, op.jobID, nodeName, op.operationID, op.opType, "failed", elapsed)
		errStr := "unknown error"
		if resp.Error != nil {
			errStr = *resp.Error
		}
		return false, fmt.Errorf("operation %s failed: %s", op.operationID, errStr)
	default:
		slog.DebugContext(ctx, "Foreground operation still pending",
			"group", groupID, "job", op.jobID, "node", nodeName, "operation_id", op.operationID,
			"status", resp.Status, "elapsedMs", resp.ElapsedMs)
		return false, errForegroundPending
	}
}
