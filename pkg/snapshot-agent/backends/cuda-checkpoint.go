package backends

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// CudaCheckpoint implements the Backend interface using cuda-checkpoint and optionally CRIU.
type CudaCheckpoint struct {
	useCriu     bool
	yieldedPids map[string]bool
	dumpedPids  map[string]bool
	mu          sync.Mutex
}

// NewCudaCheckpoint creates a new CudaCheckpoint backend.
func NewCudaCheckpoint(useCriu bool) *CudaCheckpoint {
	return &CudaCheckpoint{
		useCriu:     useCriu,
		yieldedPids: make(map[string]bool),
		dumpedPids:  make(map[string]bool),
	}
}

// Snapshot triggers a snapshot of the accelerator context for a job.
func (c *CudaCheckpoint) Snapshot(ctx context.Context, pids []string) (int64, int64, error) {

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
		return 0, 0, fmt.Errorf("cuda-checkpoint lock failed: %w", err)
	}
	if err := c.runSudoCommand(binaryPath, append([]string{"--action", "checkpoint"}, pidArgs...)...); err != nil {
		return 0, 0, fmt.Errorf("cuda-checkpoint checkpoint failed: %w", err)
	}
	log.Printf("[Metric] cuda-checkpoint action took %v", time.Since(t0))

	for _, pid := range pids {
		c.yieldedPids[pid] = true
	}

	// 2. CRIU Dump (Optional)
	if c.useCriu {
		for _, pid := range pids {
			imgDir := filepath.Join("checkpoint", "pid_"+pid)
			if err := os.MkdirAll(imgDir, 0755); err != nil {
				return 0, 0, fmt.Errorf("failed to create image directory: %w", err)
			}

			// Cleanup shared memory semaphores
			sems, _ := filepath.Glob("/dev/shm/sem.*")
			for _, sem := range sems {
				os.Remove(sem)
			}

			t0Dump := time.Now()
			// Use --leave-running to keep process alive in RAM after dump
			err := c.runSudoCommand("criu", "dump", "--shell-job", "--tcp-established", "--file-locks", "--link-remap", "--ext-unix-sk", "--external", "vdso32", "--leave-running", "--images-dir", imgDir, "--tree", pid)
			if err != nil {
				log.Printf("CRIU dump failed for PID %s: %v", pid, err)
				return 0, 0, fmt.Errorf("criu dump failed: %w", err)
			}
			log.Printf("[Metric] dump took %v for PID %s", time.Since(t0Dump), pid)
		}
	}

	return 1024 * 1024, 2048 * 1024, nil
}

// Restore triggers a restoration of the accelerator context for a job.
func (c *CudaCheckpoint) Restore(ctx context.Context, pids []string) error {
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Restoring PIDs %v", pids)

	var togglePIDs []string

	for _, pid := range pids {
		if c.dumpedPids[pid] {
			imgDir := filepath.Join("checkpoint", "pid_"+pid)
			t0Restore := time.Now()
			err := c.runSudoCommand("criu", "restore", "--shell-job", "--tcp-established", "--restore-detached", "--file-locks", "--link-remap", "--ext-unix-sk", "--external", "vdso32", "--images-dir", imgDir)
			if err != nil {
				log.Printf("CRIU restore failed for PID %s: %v", pid, err)
				return fmt.Errorf("criu restore failed for PID %s: %w", pid, err)
			}
			delete(c.dumpedPids, pid)
			log.Printf("[Metric] restore took %v for PID %s", time.Since(t0Restore), pid)
		} else if c.yieldedPids[pid] {
			togglePIDs = append(togglePIDs, pid)
		}
	}

	if len(togglePIDs) > 0 {
		t0 := time.Now()
		binaryPath := c.getCudaCheckpointPath()
		var pidArgs []string
		for _, pid := range togglePIDs {
			pidArgs = append(pidArgs, "--pid", pid)
		}

		if err := c.runSudoCommand(binaryPath, append([]string{"--toggle"}, pidArgs...)...); err != nil {
			return fmt.Errorf("cuda-checkpoint toggle failed: %w", err)
		}

		for _, pid := range togglePIDs {
			delete(c.yieldedPids, pid)
		}
		log.Printf("[Metric] cuda-checkpoint toggle took %v for PIDs %v", time.Since(t0), togglePIDs)
	}

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
