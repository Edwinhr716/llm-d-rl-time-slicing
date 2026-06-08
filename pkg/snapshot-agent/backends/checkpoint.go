package backends

import "context"

// GPUStatus represents the status of a single accelerator.
type GPUStatus struct {
	ID               string
	MemoryUsedBytes  uint64
	MemoryTotalBytes uint64
}

// Backend defines the interface for checkpoint and restore operations.
type Backend interface {
	// Snapshot triggers a snapshot of the accelerator context for a job.
	// Returns storageBytes, deviceBytes, and error.
	Snapshot(ctx context.Context, pids []string) (error)

	// Restore triggers a restoration of the accelerator context for a job.
	Restore(ctx context.Context, pids []string) (error)

	// Discover checks if the backend is available and discovers available GPUs.
	Discover(ctx context.Context) error

	// GetAcceleratorStatuses returns the status of accelerators managed by this backend.
	GetAcceleratorStatuses(ctx context.Context) ([]GPUStatus, error)
}
