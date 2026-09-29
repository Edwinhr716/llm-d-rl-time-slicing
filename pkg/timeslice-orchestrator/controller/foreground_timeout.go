package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// ErrForegroundOpTimeout is wrapped by waitForOperation when the wait passes
// ForegroundOpTimeout (not when the caller's context ends).
var ErrForegroundOpTimeout = errors.New("foreground operation timed out")

// isForegroundOpTimeout reports whether err is a wait that passed
// ForegroundOpTimeout while ctx is still live.
func isForegroundOpTimeout(ctx context.Context, err error) bool {
	return ctx.Err() == nil && errors.Is(err, ErrForegroundOpTimeout)
}

// onForegroundOpTimeout marks the job FAULTED on the node when its foreground
// snapshot or restore passed ForegroundOpTimeout. The operation never
// finished, so the job may still hold the accelerator: the group stays faulted
// until the job's pods are replaced. It always returns an error.
func (c *Controller) onForegroundOpTimeout(
	ctx context.Context, groupID, nodeName, jobID, operationID, opType string, cause error,
) error {
	marked := c.markForegroundTimeoutFault(ctx, groupID, nodeName, jobID, operationID)
	slog.ErrorContext(ctx, "Foreground operation timed out",
		"group", groupID, "job", jobID, "node", nodeName, "operation_id", operationID,
		"type", opType, "outcome", "faulted", "marked", marked)
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
