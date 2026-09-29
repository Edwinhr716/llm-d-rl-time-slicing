package backends_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
)

func TestNodeLock(t *testing.T) {
	t.Run("a waiter leaves the queue when its context ends", func(t *testing.T) {
		lock := backends.NewNodeLock()
		if err := lock.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := lock.Acquire(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("waited %s behind a held lock", waited)
		}
		lock.Release()
		if err := lock.Acquire(context.Background()); err != nil {
			t.Fatalf("lock not free after release: %v", err)
		}
		lock.Release()
	})

	t.Run("cancel wakes a waiter", func(t *testing.T) {
		lock := backends.NewNodeLock()
		if err := lock.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer lock.Release()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- lock.Acquire(ctx) }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("want Canceled, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled waiter did not return")
		}
	})

	t.Run("an ended context never takes a free lock", func(t *testing.T) {
		lock := backends.NewNodeLock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 100 {
			if err := lock.Acquire(ctx); err == nil {
				t.Fatal("took the lock with a cancelled context")
			}
		}
	})

	t.Run("release hands the lock to one waiter", func(t *testing.T) {
		lock := backends.NewNodeLock()
		if err := lock.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		var holders atomic.Int32
		done := make(chan struct{}, 2)
		for range 2 {
			go func() {
				if err := lock.Acquire(context.Background()); err != nil {
					t.Error(err)
					done <- struct{}{}
					return
				}
				if holders.Add(1) != 1 {
					t.Error("two holders at once")
				}
				time.Sleep(10 * time.Millisecond)
				holders.Add(-1)
				lock.Release()
				done <- struct{}{}
			}()
		}
		lock.Release()
		for range 2 {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("waiter did not get the lock")
			}
		}
	})

	t.Run("release of an unlocked lock panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("want a panic")
			}
		}()
		backends.NewNodeLock().Release()
	})
}

// TestCudaCheckpoint_NodeLockHonoursContext checks that Snapshot and
// Restore stuck behind a hung holder of the node lock give up at their
// deadline without running cuda-checkpoint.
func TestCudaCheckpoint_NodeLockHonoursContext(t *testing.T) {
	cuda := backends.NewCudaCheckpoint()
	var calls atomic.Int32
	cuda.SetExecCommand(func(context.Context, string, ...string) ([]byte, error) {
		calls.Add(1)
		return nil, nil
	})
	if err := cuda.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	for name, call := range map[string]func(context.Context, backends.Request) error{
		"Snapshot": cuda.Snapshot,
		"Restore":  cuda.Restore,
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := call(ctx, backends.Request{JobID: "job", Config: cudaConfig(123)})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s behind a held lock: want DeadlineExceeded, got %v", name, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("cuda-checkpoint ran %d times without the lock", calls.Load())
	}

	// The locked variants run while the caller holds the lock.
	if err := cuda.SnapshotLocked(context.Background(), backends.Request{JobID: "job", Config: cudaConfig(123)}); err != nil {
		t.Fatal(err)
	}
	if err := cuda.RestoreLocked(context.Background(), backends.Request{JobID: "job", Config: cudaConfig(123)}); err != nil {
		t.Fatal(err)
	}
	cuda.Release()

	if err := cuda.Snapshot(context.Background(), backends.Request{JobID: "job", Config: cudaConfig(123)}); err != nil {
		t.Fatalf("Snapshot after release: %v", err)
	}
	// lock + checkpoint, toggle, lock + checkpoint.
	if got := calls.Load(); got != 5 {
		t.Fatalf("want 5 cuda-checkpoint calls, got %d", got)
	}
}
