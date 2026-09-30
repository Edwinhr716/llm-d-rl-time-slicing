package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// ErrForegroundOpTimeout is wrapped by the error reconcile returns when a
// foreground snapshot or restore did not finish within ForegroundOpTimeout and
// the job was marked FAULTED on the node.
var ErrForegroundOpTimeout = errors.New("foreground operation timed out")

// isForegroundOpTimeout reports whether err, returned by waitForOperation, is
// a wait that passed ForegroundOpTimeout while ctx (the reconcile's own
// context) is still live.
func (c *Controller) isForegroundOpTimeout(ctx context.Context, err error) bool {
	return c.ForegroundOpTimeout > 0 && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded)
}

// onForegroundOpTimeout marks the job FAULTED on the node because its
// foreground snapshot or restore operationID passed ForegroundOpTimeout. The
// operation may still be running on the agent, so the orchestrator never
// starts another one there for this job: the mark holds until the job's pods
// change (see store.Job.MarkForegroundTimeoutFault). It always returns an
// error wrapping ErrForegroundOpTimeout and cause.
func (c *Controller) onForegroundOpTimeout(
	ctx context.Context, groupID, nodeName, jobID, operationID, opType string, cause error,
) error {
	marked := c.markForegroundTimeoutFault(ctx, groupID, nodeName, jobID, operationID)
	slog.ErrorContext(ctx, "Foreground operation timed out",
		"group", groupID, "job", jobID, "node", nodeName, "operation_id", operationID, "type", opType,
		"timeout", c.ForegroundOpTimeout, "outcome", "faulted", "marked", marked)
	return fmt.Errorf("%w: %s operation %s did not finish within %s; job %s marked FAULTED on node %s, "+
		"requires human intervention: %w",
		ErrForegroundOpTimeout, opType, operationID, c.ForegroundOpTimeout, jobID, nodeName, cause)
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
