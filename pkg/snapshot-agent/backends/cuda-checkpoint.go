package backends

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

type nvmlClient interface {
	Init() nvml.Return
	Shutdown() nvml.Return
	DeviceGetCount() (int, nvml.Return)
}

type defaultNvmlClient struct{}

func (d *defaultNvmlClient) Init() nvml.Return {
	return nvml.Init()
}

func (d *defaultNvmlClient) Shutdown() nvml.Return {
	return nvml.Shutdown()
}

func (d *defaultNvmlClient) DeviceGetCount() (int, nvml.Return) {
	return nvml.DeviceGetCount()
}

// CudaCheckpoint implements the Backend interface using cuda-checkpoint and optionally CRIU.
type CudaCheckpoint struct {
	// lock is the node lock: one cuda-checkpoint action at a time per node.
	// Waiting for it honours the caller's context.
	lock        *NodeLock
	execCommand func(ctx context.Context, name string, args ...string) ([]byte, error)
	nvml        nvmlClient
	lookPath    func(string) (string, error)
}

// NewCudaCheckpoint creates a new CudaCheckpoint backend.
func NewCudaCheckpoint() *CudaCheckpoint {
	return &CudaCheckpoint{
		lock: NewNodeLock(),
		execCommand: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		},
		nvml:     &defaultNvmlClient{},
		lookPath: exec.LookPath,
	}
}

// Acquire takes the node lock, or returns an error when ctx ends first.
// The caller must Release it. The guest pipeline holds it across its own
// checks and SnapshotLocked or RestoreLocked.
func (c *CudaCheckpoint) Acquire(ctx context.Context) error {
	return c.lock.Acquire(ctx)
}

// Release frees the node lock taken with Acquire.
func (c *CudaCheckpoint) Release() {
	c.lock.Release()
}

// Snapshot triggers a snapshot of the accelerator context for a job. It
// waits for the node lock only as long as ctx allows.
func (c *CudaCheckpoint) Snapshot(ctx context.Context, req Request) error {
	if len(ExtractPIDStrings(req.Config)) == 0 {
		return fmt.Errorf("at least one PID is required for CUDA snapshot")
	}
	if err := c.lock.Acquire(ctx); err != nil {
		return err
	}
	defer c.lock.Release()
	return c.SnapshotLocked(ctx, req)
}

// SnapshotLocked is Snapshot for a caller that already holds the node lock.
func (c *CudaCheckpoint) SnapshotLocked(ctx context.Context, req Request) error {
	pids := ExtractPIDStrings(req.Config)
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required for CUDA snapshot")
	}

	slog.InfoContext(ctx, "Snapshotting PIDs", "pids", pids)

	t0 := time.Now()
	if err := c.checkpointPIDs(ctx, pids); err != nil {
		return fmt.Errorf("cuda-checkpoint checkpoint failed: %w", err)
	}
	slog.InfoContext(ctx, "cuda-checkpoint action took", "duration", time.Since(t0))
	return nil
}

// Restore triggers a restoration of the accelerator context for a job. It
// waits for the node lock only as long as ctx allows.
func (c *CudaCheckpoint) Restore(ctx context.Context, req Request) error {
	if len(ExtractPIDStrings(req.Config)) == 0 {
		return fmt.Errorf("at least one PID is required for CUDA restore")
	}
	if err := c.lock.Acquire(ctx); err != nil {
		return err
	}
	defer c.lock.Release()
	return c.RestoreLocked(ctx, req)
}

// RestoreLocked is Restore for a caller that already holds the node lock.
func (c *CudaCheckpoint) RestoreLocked(ctx context.Context, req Request) error {
	pids := ExtractPIDStrings(req.Config)
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required for CUDA restore")
	}

	slog.InfoContext(ctx, "Restoring PIDs", "pids", pids)
	t0 := time.Now()
	if err := c.restorePIDs(ctx, pids); err != nil {
		return fmt.Errorf("cuda-checkpoint toggle failed: %w", err)
	}
	slog.InfoContext(ctx, "cuda-checkpoint toggle took", "duration", time.Since(t0), "pids", pids)
	return nil
}

// ExtractPIDStrings extracts PID strings from a BackendConfig.
func ExtractPIDStrings(config *pb.BackendConfig) []string {
	if config == nil {
		return nil
	}
	cuda := config.GetCuda()
	if cuda == nil {
		return nil
	}
	target := cuda.GetExplicitTarget()
	if target == nil {
		return nil
	}
	pids := make([]string, 0, len(target.GetPids()))
	for _, pid := range target.GetPids() {
		pids = append(pids, strconv.Itoa(int(pid)))
	}
	return pids
}

// BuildCudaConfig wraps PID strings into a BackendConfig.
func BuildCudaConfig(pidStrings []string) *pb.BackendConfig {
	pids := make([]int32, 0, len(pidStrings))
	for _, s := range pidStrings {
		if pid, err := strconv.ParseInt(s, 10, 32); err == nil {
			pids = append(pids, int32(pid))
		}
	}
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_Cuda{
			Cuda: &pb.CudaBackendConfig{
				ExplicitTarget: &pb.ProcessTarget{Pids: pids},
			},
		},
	}
}

func (c *CudaCheckpoint) getCudaCheckpointPath() string {
	// First check if it's in the PATH
	if path, err := exec.LookPath("cuda-checkpoint"); err == nil {
		return path
	}
	// Fallback to the relative path used in development
	return "/usr/local/bin/cuda-checkpoint"
}

func (c *CudaCheckpoint) runSudoCommand(ctx context.Context, name string, args ...string) error {
	if out, err := c.execCommand(ctx, name, args...); err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(out))
	}
	return nil
}

func (c *CudaCheckpoint) checkpointPIDs(ctx context.Context, pids []string) error {
	binaryPath := c.getCudaCheckpointPath()
	pidArgs := make([]string, 0, 2*len(pids))
	for _, pid := range pids {
		pidArgs = append(pidArgs, "--pid", pid)
	}
	if err := c.runSudoCommand(ctx, binaryPath, append([]string{"--action", "lock"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint lock failed: %w", err)
	}
	if err := c.runSudoCommand(ctx, binaryPath, append([]string{"--action", "checkpoint"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint checkpoint failed: %w", err)
	}
	return nil
}

func (c *CudaCheckpoint) restorePIDs(ctx context.Context, pids []string) error {
	binaryPath := c.getCudaCheckpointPath()
	pidArgs := make([]string, 0, 2*len(pids))
	for _, pid := range pids {
		pidArgs = append(pidArgs, "--pid", pid)
	}
	if err := c.runSudoCommand(ctx, binaryPath, append([]string{"--toggle"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint toggle failed: %w", err)
	}
	return nil
}

// HealthCheck checks if the cuda-checkpoint backend is healthy by initializing the backend
// and the discovery provider.
func (c *CudaCheckpoint) HealthCheck(ctx context.Context) error {
	// 1. Check if cuda-checkpoint executable is available
	binaryPath := c.getCudaCheckpointPath()
	if _, err := c.lookPath(binaryPath); err != nil {
		return fmt.Errorf("cuda-checkpoint executable not found: %w", err)
	}

	// 2. Initialize NVML
	if ret := c.nvml.Init(); ret != nvml.SUCCESS {
		return fmt.Errorf("failed to initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer func() {
		if ret := c.nvml.Shutdown(); ret != nvml.SUCCESS {
			slog.ErrorContext(ctx, "Failed to shutdown NVML", "error", nvml.ErrorString(ret))
		}
	}()

	// 3. Check if there are any GPUs attached to the system
	count, ret := c.nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("failed to get device count: %v", nvml.ErrorString(ret))
	}

	if count == 0 {
		return fmt.Errorf("no GPUs found on the system")
	}

	return nil
}
