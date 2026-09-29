package backends

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// CUDA process states printed by `cuda-checkpoint --get-state`.
const (
	CudaStateRunning      = "running"
	CudaStateLocked       = "locked"
	CudaStateCheckpointed = "checkpointed"
	CudaStateFailed       = "failed"
)

// unlockTimeout bounds the best-effort unlock on a failure path, which may run
// after the caller's deadline has passed.
const unlockTimeout = 5 * time.Second

// ErrNotCudaProcess is returned by GetState for a process that has no CUDA
// context (cuda-checkpoint refuses it).
var ErrNotCudaProcess = errors.New("not a CUDA process")

// GetState returns the cuda-checkpoint state of one process: "running",
// "locked", "checkpointed" or "failed". A process without a CUDA context
// returns ErrNotCudaProcess. GetState does not take the node lock: it only
// reads.
func (c *CudaCheckpoint) GetState(ctx context.Context, pid int) (string, error) {
	out, err := c.execCommand(ctx, c.getCudaCheckpointPath(), "--get-state", "--pid", strconv.Itoa(pid))
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("cuda-checkpoint --get-state %d: %w", pid, ctx.Err())
		}
		return "", fmt.Errorf("pid %d: %w: %w: %s", pid, ErrNotCudaProcess, err, strings.TrimSpace(string(out)))
	}
	state := strings.ToLower(strings.TrimSpace(string(out)))
	switch state {
	case CudaStateRunning, CudaStateLocked, CudaStateCheckpointed, CudaStateFailed:
		return state, nil
	default:
		return "", fmt.Errorf("cuda-checkpoint --get-state %d: unexpected output %q", pid, state)
	}
}

// GuestCheckpoint locks and then checkpoints every process in pids, one
// process at a time, under the node lock. It waits for the lock only as long
// as ctx allows, so a hung checkpoint elsewhere cannot hold it past its
// deadline. beforeRun, if not nil, runs after the lock is taken and before
// any process is touched; an error from it aborts the checkpoint without
// touching the processes (the suspend pipeline re-checks its deadline budget
// there).
//
// A process that is already locked is only checkpointed. If a later step
// fails, processes that were locked but not checkpointed are unlocked again
// so the guest keeps running.
func (c *CudaCheckpoint) GuestCheckpoint(
	ctx context.Context, pids []int, alreadyLocked map[int]bool, beforeRun func() error,
) error {
	if len(pids) == 0 {
		return errors.New("at least one PID is required for CUDA checkpoint")
	}
	if err := c.lock.Acquire(ctx); err != nil {
		return err
	}
	defer c.lock.Release()

	if beforeRun != nil {
		if err := beforeRun(); err != nil {
			return err
		}
	}
	bin := c.getCudaCheckpointPath()
	t0 := time.Now()
	var locked []int
	for _, pid := range pids {
		if alreadyLocked[pid] {
			continue
		}
		if err := c.runSudoCommand(ctx, bin, "--action", "lock", "--pid", strconv.Itoa(pid)); err != nil {
			c.unlockAll(ctx, bin, locked)
			return fmt.Errorf("cuda-checkpoint lock %d: %w", pid, withCtx(ctx, err))
		}
		locked = append(locked, pid)
	}
	for i, pid := range pids {
		if err := c.runSudoCommand(ctx, bin, "--action", "checkpoint", "--pid", strconv.Itoa(pid)); err != nil {
			// Processes after i are still only locked; release them.
			c.unlockAll(ctx, bin, intersect(locked, pids[i+1:]))
			return fmt.Errorf("cuda-checkpoint checkpoint %d: %w", pid, withCtx(ctx, err))
		}
	}
	slog.InfoContext(ctx, "cuda-checkpoint guest checkpoint", "pids", pids, "duration", time.Since(t0))
	return nil
}

// GuestRestore brings every process in pids back to running under the node
// lock (waiting only as long as ctx allows): a checkpointed process is
// restored and then unlocked, a locked one is only unlocked, a running one is
// left alone. states gives the state of each process as read by GetState.
func (c *CudaCheckpoint) GuestRestore(ctx context.Context, pids []int, states map[int]string) error {
	if err := c.lock.Acquire(ctx); err != nil {
		return err
	}
	defer c.lock.Release()

	bin := c.getCudaCheckpointPath()
	t0 := time.Now()
	for _, pid := range pids {
		p := strconv.Itoa(pid)
		switch states[pid] {
		case CudaStateCheckpointed:
			if err := c.runSudoCommand(ctx, bin, "--action", "restore", "--pid", p); err != nil {
				return fmt.Errorf("cuda-checkpoint restore %d: %w", pid, withCtx(ctx, err))
			}
			fallthrough
		case CudaStateLocked:
			if err := c.runSudoCommand(ctx, bin, "--action", "unlock", "--pid", p); err != nil {
				return fmt.Errorf("cuda-checkpoint unlock %d: %w", pid, withCtx(ctx, err))
			}
		case CudaStateRunning:
		default:
			return fmt.Errorf("cuda-checkpoint restore %d: cannot restore from state %q", pid, states[pid])
		}
	}
	slog.InfoContext(ctx, "cuda-checkpoint guest restore", "pids", pids, "duration", time.Since(t0))
	return nil
}

// unlockAll is best effort: it runs on a failure path whose error is already
// being returned.
func (c *CudaCheckpoint) unlockAll(ctx context.Context, bin string, pids []int) {
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	for _, pid := range pids {
		if err := c.runSudoCommand(uctx, bin, "--action", "unlock", "--pid", strconv.Itoa(pid)); err != nil {
			slog.WarnContext(ctx, "cuda-checkpoint unlock after failure", "pid", pid, "error", err)
		}
	}
}

func intersect(a, b []int) []int {
	in := map[int]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []int
	for _, x := range a {
		if in[x] {
			out = append(out, x)
		}
	}
	return out
}

// withCtx wraps err with the context error when the context ended, so the
// caller can tell a deadline from a backend failure.
func withCtx(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return err
}

// Available reports whether the cuda-checkpoint binary can be found.
func (c *CudaCheckpoint) Available() error {
	if _, err := c.lookPath(c.getCudaCheckpointPath()); err != nil {
		return fmt.Errorf("cuda-checkpoint executable not found: %w", err)
	}
	return nil
}
