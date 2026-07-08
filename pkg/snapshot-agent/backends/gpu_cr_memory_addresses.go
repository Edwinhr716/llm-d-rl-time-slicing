package backends

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// GpuCrMemoryAddresses implements the Backend interface using cr_client with memory addresses.
type GpuCrMemoryAddresses struct {
	*GpuCr
}

// NewGpuCrMemoryAddresses creates a new GpuCrMemoryAddresses backend.
func NewGpuCrMemoryAddresses() *GpuCrMemoryAddresses {
	return &GpuCrMemoryAddresses{
		GpuCr: NewGpuCr(),
	}
}

// target format: "pid:addr:size"
func parseTargets(targets []string) (map[string][]string, error) {
	grouped := make(map[string][]string)
	for _, t := range targets {
		parts := strings.SplitN(t, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid target format %q, expected pid:spec", t)
		}
		pid := parts[0]
		spec := parts[1] // addr:size
		grouped[pid] = append(grouped[pid], spec)
	}
	return grouped, nil
}

// Snapshot triggers a selective snapshot of the accelerator context for a job.
func (g *GpuCrMemoryAddresses) Snapshot(ctx context.Context, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	slog.InfoContext(ctx, "Snapshotting memory addresses using GPU-CR", "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	t0 := time.Now()
	for pid, specs := range grouped {
		specStr := strings.Join(specs, ",")
		if err := g.checkpointMemoryAddresses(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %s with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective checkpoint took", "duration", time.Since(t0))
	return nil
}

// Restore triggers a selective restoration of the accelerator context for a job.
func (g *GpuCrMemoryAddresses) Restore(ctx context.Context, targets []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	slog.InfoContext(ctx, "Restoring memory addresses using GPU-CR", "targets", targets)

	grouped, err := parseTargets(targets)
	if err != nil {
		return err
	}

	t0 := time.Now()
	for pid, specs := range grouped {
		specStr := strings.Join(specs, ",")
		if err := g.restoreMemoryAddresses(ctx, pid, specStr); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %s with specs %s: %w", pid, specStr, err)
		}
	}
	slog.InfoContext(ctx, "GPU-CR selective restore took", "duration", time.Since(t0))
	return nil
}

func (g *GpuCrMemoryAddresses) checkpointMemoryAddresses(ctx context.Context, pid string, spec string) error {
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-c", "-p", pid, "-s", spec); err != nil {
		return err
	}
	return nil
}

func (g *GpuCrMemoryAddresses) restoreMemoryAddresses(ctx context.Context, pid string, spec string) error {
	binaryPath := g.getCrClientPath()
	if err := g.runCommand(ctx, binaryPath, "-r", "-p", pid, "-s", spec); err != nil {
		return err
	}
	return nil
}
