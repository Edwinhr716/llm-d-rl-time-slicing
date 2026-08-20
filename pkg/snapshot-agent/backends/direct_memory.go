package backends

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// crClientPath is the only location cr_client is expected at: the
// snapshot-agent image installs it there, so anything else is a broken
// deployment and should fail loudly rather than resolve to a dangling path.
const crClientPath = "/usr/local/bin/cr_client"

// DirectMemory implements the Backend interface using cr_client.
type DirectMemory struct {
	mu          sync.Mutex
	execCommand func(ctx context.Context, name string, args ...string) ([]byte, error)
	statFunc    func(string) (os.FileInfo, error)
}

// NewDirectMemory creates a new DirectMemory backend.
func NewDirectMemory() *DirectMemory {
	return &DirectMemory{
		execCommand: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		},
		statFunc: os.Stat,
	}
}

// Snapshot triggers a snapshot of the target processes for a job using cr_client.
func (d *DirectMemory) Snapshot(ctx context.Context, req Request) error {
	pids := ExtractDirectMemoryPIDStrings(req.Config)
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required for Direct Memory snapshot")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	slog.InfoContext(ctx, "Snapshotting PIDs using Direct Memory", "pids", pids)

	t0 := time.Now()
	for _, pid := range pids {
		if err := d.checkpointPID(ctx, pid); err != nil {
			return fmt.Errorf("cr_client checkpoint failed for PID %s: %w", pid, err)
		}
	}
	slog.InfoContext(ctx, "cr_client checkpoint took", "duration", time.Since(t0))
	return nil
}

// Restore triggers a restoration of the target processes for a job using cr_client.
func (d *DirectMemory) Restore(ctx context.Context, req Request) error {
	pids := ExtractDirectMemoryPIDStrings(req.Config)
	if len(pids) == 0 {
		return fmt.Errorf("at least one PID is required for Direct Memory restore")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	slog.InfoContext(ctx, "Restoring PIDs using Direct Memory", "pids", pids)
	t0 := time.Now()
	for _, pid := range pids {
		if err := d.restorePID(ctx, pid); err != nil {
			return fmt.Errorf("cr_client restore failed for PID %s: %w", pid, err)
		}
	}
	slog.InfoContext(ctx, "cr_client restore took", "duration", time.Since(t0), "pids", pids)
	return nil
}

func (d *DirectMemory) getCrClientPath() (string, error) {
	if _, err := d.statFunc(crClientPath); err != nil {
		return "", fmt.Errorf("cr_client not found at %s: %w", crClientPath, err)
	}
	return crClientPath, nil
}

func (d *DirectMemory) runCommand(ctx context.Context, name string, args ...string) error {
	// A workload that dies mid-operation can leave cr_client blocked on its
	// shared-memory control channel forever; without a deadline that wedges
	// the job in TRANSITIONING and holds d.mu across all future requests.
	ctx, cancel := context.WithTimeout(ctx, opTimeout())
	defer cancel()
	if out, err := d.execCommand(ctx, name, args...); err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(out))
	}
	return nil
}

// opTimeout is the per-cr_client-invocation deadline, configurable via
// GPU_CR_OP_TIMEOUT_SEC (default 120).
func opTimeout() time.Duration {
	if v := os.Getenv("GPU_CR_OP_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 120 * time.Second
}

func (d *DirectMemory) checkpointPID(ctx context.Context, pid string) error {
	binaryPath, err := d.getCrClientPath()
	if err != nil {
		return err
	}
	return d.runCommand(ctx, binaryPath, "-c", "-p", pid)
}

func (d *DirectMemory) restorePID(ctx context.Context, pid string) error {
	binaryPath, err := d.getCrClientPath()
	if err != nil {
		return err
	}
	return d.runCommand(ctx, binaryPath, "-r", "-p", pid)
}

// HealthCheck checks if the Direct Memory backend is healthy.
func (d *DirectMemory) HealthCheck(ctx context.Context) error {
	_, err := d.getCrClientPath()
	return err
}

// ExtractDirectMemoryPIDStrings extracts PID strings from a DirectMemory BackendConfig.
func ExtractDirectMemoryPIDStrings(config *pb.BackendConfig) []string {
	if config == nil {
		return nil
	}
	dm := config.GetDirectMemory()
	if dm == nil {
		return nil
	}
	target := dm.GetExplicitTarget()
	if target == nil {
		return nil
	}
	pids := make([]string, 0, len(target.GetPids()))
	for _, pid := range target.GetPids() {
		pids = append(pids, strconv.Itoa(int(pid)))
	}
	return pids
}

// BuildDirectMemoryConfig wraps PID strings into a DirectMemory BackendConfig.
func BuildDirectMemoryConfig(pidStrings []string) (*pb.BackendConfig, error) {
	pids := make([]int32, 0, len(pidStrings))
	for _, s := range pidStrings {
		pid, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid PID string %q: %w", s, err)
		}
		pids = append(pids, int32(pid))
	}
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_DirectMemory{
			DirectMemory: &pb.DirectMemoryBackendConfig{
				ExplicitTarget: &pb.ProcessTarget{Pids: pids},
			},
		},
	}, nil
}
