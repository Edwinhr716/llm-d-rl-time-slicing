package backends

import "context"

// Backend defines the interface for checkpoint and restore operations.
type Backend interface {
	// Snapshot triggers a snapshot of the accelerator context for a job.
	Snapshot(ctx context.Context, jobID, group string) (string, error)

	// Restore triggers a restoration of the accelerator context for a job.
	Restore(ctx context.Context, jobID, group string) (string, error)
}
