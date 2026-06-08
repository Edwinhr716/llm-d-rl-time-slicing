package backends

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

// CudaCheckpoint implements the Backend interface using cuda-checkpoint and optionally CRIU.
type CudaCheckpoint struct {
	mu sync.Mutex
}

// NewCudaCheckpoint creates a new CudaCheckpoint backend.
func NewCudaCheckpoint() *CudaCheckpoint {
	return &CudaCheckpoint{}
}

// Discover checks if the backend is available and discovers available GPUs.
func (c *CudaCheckpoint) Discover(ctx context.Context) error {
	// 1. Check if cuda-checkpoint is executable
	path := c.getCudaCheckpointPath()
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("cuda-checkpoint binary not found at %s: %w", path, err)
	}
	if info.Mode()&0111 == 0 {
		return fmt.Errorf("cuda-checkpoint binary at %s is not executable", path)
	}

	// 2. Discover GPUs using NVML
	_, err = c.discoverGPUs()
	if err != nil {
		return fmt.Errorf("GPU discovery failed: %w", err)
	}

	return nil
}

func (c *CudaCheckpoint) discoverGPUs() (int, error) {
	log.Printf("Initializing NVML...")
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return 0, fmt.Errorf("failed to initialize NVML: %v", ret)
	}
	log.Printf("NVML initialized successfully")
	defer nvml.Shutdown()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return 0, fmt.Errorf("failed to get device count: %v", ret)
	}

	if count == 0 {
		return 0, fmt.Errorf("no GPUs found on the system")
	}

	return count, nil
}

// GetAcceleratorStatuses returns the status of accelerators managed by this backend.
func (c *CudaCheckpoint) GetAcceleratorStatuses(ctx context.Context) ([]GPUStatus, error) {
	log.Printf("Initializing NVML for status discovery...")
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to initialize NVML: %v", ret)
	}
	defer nvml.Shutdown()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to get device count: %v", ret)
	}

	var statuses []GPUStatus
	for i := 0; i < count; i++ {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			log.Printf("Failed to get handle for device %d: %v", i, ret)
			continue
		}

		uuid, ret := device.GetUUID()
		if ret != nvml.SUCCESS {
			log.Printf("Failed to get UUID for device %d: %v", i, ret)
			uuid = fmt.Sprintf("gpu-%d", i)
		}

		memory, ret := device.GetMemoryInfo()
		if ret != nvml.SUCCESS {
			log.Printf("Failed to get memory info for device %d: %v", i, ret)
			continue
		}

		statuses = append(statuses, GPUStatus{
			ID:               uuid,
			MemoryUsedBytes:  memory.Used,
			MemoryTotalBytes: memory.Total,
		})
	}

	return statuses, nil
}

// Snapshot triggers a snapshot of the accelerator context for a job.
func (c *CudaCheckpoint) Snapshot(ctx context.Context, pids []string) (error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Snapshotting PIDs %v", pids)

	// 1. Lock and Checkpoint CUDA
	t0 := time.Now()
	binaryPath := c.getCudaCheckpointPath()

	var pidArgs []string
	for _, pid := range pids {
		pidArgs = append(pidArgs, "--pid", pid)
	}

	if err := c.runSudoCommand(binaryPath, append([]string{"--action", "lock"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint lock failed: %w", err)
	}
	if err := c.runSudoCommand(binaryPath, append([]string{"--action", "checkpoint"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint checkpoint failed: %w", err)
	}
	log.Printf("[Metric] cuda-checkpoint action took %v", time.Since(t0))

	return  nil
}

// Restore triggers a restoration of the accelerator context for a job.
func (c *CudaCheckpoint) Restore(ctx context.Context, pids []string) (error) {
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Restoring PIDs %v", pids)
	t0 := time.Now()
	binaryPath := c.getCudaCheckpointPath()
	var pidArgs []string
	for _, pid := range pids {
		pidArgs = append(pidArgs, "--pid", pid)
	}

	if err := c.runSudoCommand(binaryPath, append([]string{"--toggle"}, pidArgs...)...); err != nil {
		return fmt.Errorf("cuda-checkpoint toggle failed: %w", err)
	}
	log.Printf("[Metric] cuda-checkpoint toggle took %v for PIDs %v", time.Since(t0), pids)

	return nil
}

func (c *CudaCheckpoint) getCudaCheckpointPath() string {
	// First check if it's in the PATH
	if path, err := exec.LookPath("cuda-checkpoint"); err == nil {
		return path
	}
	// Fallback to the relative path used in development
	return "/usr/local/bin/cuda-checkpoint"
}

func (c *CudaCheckpoint) runSudoCommand(name string, args ...string) error {
	// Check if 'sudo' exists in PATH
	_, err := exec.LookPath("sudo")
	var cmd *exec.Cmd
	if err != nil {
		log.Printf("'sudo' not found in PATH, attempting to run command directly: %s %v", name, args)
		cmd = exec.Command(name, args...)
	} else {
		cmd = exec.Command("sudo", append([]string{name}, args...)...)
	}

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("command failed: %v, output: %s", err, string(out))
	}
	return nil
}
