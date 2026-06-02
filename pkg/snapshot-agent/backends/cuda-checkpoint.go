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
func (c *CudaCheckpoint) Snapshot(ctx context.Context, pid string) (int64, int64, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Snapshotting PID %s", pid)

	// 1. Lock and Checkpoint CUDA
	t0 := time.Now()
	binaryPath := c.getCudaCheckpointPath()
	if err := c.runSudoCommand(binaryPath, "--action", "lock", "--pid", pid); err != nil {
		return 0, 0, fmt.Errorf("cuda-checkpoint lock failed: %w", err)
	}
	if err := c.runSudoCommand(binaryPath, "--action", "checkpoint", "--pid", pid); err != nil {
		return 0, 0, fmt.Errorf("cuda-checkpoint checkpoint failed: %w", err)
	}
	log.Printf("[Metric] cuda-checkpoint action took %v", time.Since(t0))

	c.yieldedPids[pid] = true

	// 2. CRIU Dump (Optional)
	if c.useCriu {
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

	return 1024 * 1024, 2048 * 1024, nil
}

// Restore triggers a restoration of the accelerator context for a job.
func (c *CudaCheckpoint) Restore(ctx context.Context, pid string) error {
	if pid == "" {
		return fmt.Errorf("jobID (PID) is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Restoring PID %s", pid)

	if c.dumpedPids[pid] {
		imgDir := filepath.Join("checkpoint", "pid_"+pid)
		t0Restore := time.Now()
		err := c.runSudoCommand("criu", "restore", "--shell-job", "--tcp-established", "--restore-detached", "--file-locks", "--link-remap", "--ext-unix-sk", "--external", "vdso32", "--images-dir", imgDir)
		if err != nil {
			log.Printf("CRIU restore failed for PID %s: %v", pid, err)
			return fmt.Errorf("criu restore failed: %w", err)
		}
		delete(c.dumpedPids, pid)
		log.Printf("[Metric] restore took %v for PID %s", time.Since(t0Restore), pid)
	} else if c.yieldedPids[pid] {
		t0 := time.Now()
		binaryPath := c.getCudaCheckpointPath()
		if err := c.runSudoCommand(binaryPath, "--toggle", "--pid", pid); err != nil {
			return fmt.Errorf("cuda-checkpoint toggle failed: %w", err)
		}
		delete(c.yieldedPids, pid)
		log.Printf("[Metric] cuda-checkpoint toggle took %v for PID %s", time.Since(t0), pid)
	}

	return nil
}

func (c *CudaCheckpoint) getCudaCheckpointPath() string {
	// First check if it's in the PATH
	if path, err := exec.LookPath("cuda-checkpoint"); err == nil {
		return path
	}
	// Fallback to the relative path used in development
	return "./bin/x86_64_Linux/cuda-checkpoint"
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
