package backends_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
)

// recorder fakes cuda-checkpoint: it records each call as "action pid" and
// fails the calls listed in fail.
type recorder struct {
	mu    sync.Mutex
	calls []string
	fail  map[string]bool
	out   map[string]string
}

func (r *recorder) exec(_ context.Context, _ string, args ...string) ([]byte, error) {
	var action, pid string
	for i := 0; i+1 < len(args); i++ {
		switch args[i] {
		case "--action":
			action = args[i+1]
		case "--pid":
			pid = args[i+1]
		}
	}
	if len(args) > 0 && args[0] == "--get-state" {
		action = "get-state"
	}
	call := action + " " + pid
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
	if r.fail[call] {
		return []byte("boom"), errors.New("exit status 1")
	}
	return []byte(r.out[call]), nil
}

func TestGetState(t *testing.T) {
	r := &recorder{
		out:  map[string]string{"get-state 1": "running\n", "get-state 2": "Checkpointed\n", "get-state 4": "weird\n"},
		fail: map[string]bool{"get-state 3": true},
	}
	c := backends.NewCudaCheckpoint()
	c.SetExecCommand(r.exec)
	ctx := context.Background()

	if s, err := c.GetState(ctx, 1); err != nil || s != backends.CudaStateRunning {
		t.Errorf("pid 1: %q, %v", s, err)
	}
	if s, err := c.GetState(ctx, 2); err != nil || s != backends.CudaStateCheckpointed {
		t.Errorf("pid 2: %q, %v", s, err)
	}
	if _, err := c.GetState(ctx, 3); !errors.Is(err, backends.ErrNotCudaProcess) {
		t.Errorf("pid 3: want ErrNotCudaProcess, got %v", err)
	}
	if _, err := c.GetState(ctx, 4); err == nil || errors.Is(err, backends.ErrNotCudaProcess) {
		t.Errorf("pid 4: want an unexpected-output error, got %v", err)
	}
}

func TestGuestCheckpoint(t *testing.T) {
	t.Run("locks then checkpoints", func(t *testing.T) {
		r := &recorder{}
		c := backends.NewCudaCheckpoint()
		c.SetExecCommand(r.exec)
		ran := false
		err := c.GuestCheckpoint(context.Background(), []int{10, 20}, map[int]bool{20: true}, func() error {
			ran = true
			if len(r.calls) != 0 {
				t.Error("beforeRun must run before any cuda-checkpoint call")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !ran {
			t.Error("beforeRun not called")
		}
		want := []string{"lock 10", "checkpoint 10", "checkpoint 20"}
		if !reflect.DeepEqual(r.calls, want) {
			t.Errorf("calls %v, want %v", r.calls, want)
		}
	})
	t.Run("beforeRun refuses", func(t *testing.T) {
		r := &recorder{}
		c := backends.NewCudaCheckpoint()
		c.SetExecCommand(r.exec)
		refuse := errors.New("no budget")
		if err := c.GuestCheckpoint(context.Background(), []int{10}, nil, func() error { return refuse }); !errors.Is(err, refuse) {
			t.Errorf("want refuse, got %v", err)
		}
		if len(r.calls) != 0 {
			t.Errorf("calls %v, want none", r.calls)
		}
	})
	t.Run("checkpoint failure unlocks the rest", func(t *testing.T) {
		r := &recorder{fail: map[string]bool{"checkpoint 10": true}}
		c := backends.NewCudaCheckpoint()
		c.SetExecCommand(r.exec)
		err := c.GuestCheckpoint(context.Background(), []int{10, 20}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "checkpoint 10") {
			t.Fatalf("got %v", err)
		}
		want := []string{"lock 10", "lock 20", "checkpoint 10", "unlock 20"}
		if !reflect.DeepEqual(r.calls, want) {
			t.Errorf("calls %v, want %v", r.calls, want)
		}
	})
	t.Run("lock failure unlocks locked", func(t *testing.T) {
		r := &recorder{fail: map[string]bool{"lock 20": true}}
		c := backends.NewCudaCheckpoint()
		c.SetExecCommand(r.exec)
		if err := c.GuestCheckpoint(context.Background(), []int{10, 20}, nil, nil); err == nil {
			t.Fatal("want an error")
		}
		want := []string{"lock 10", "lock 20", "unlock 10"}
		if !reflect.DeepEqual(r.calls, want) {
			t.Errorf("calls %v, want %v", r.calls, want)
		}
	})
	t.Run("deadline is reported", func(t *testing.T) {
		r := &recorder{fail: map[string]bool{"lock 10": true}}
		c := backends.NewCudaCheckpoint()
		c.SetExecCommand(r.exec)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.GuestCheckpoint(ctx, []int{10}, nil, nil); !errors.Is(err, context.Canceled) {
			t.Errorf("want Canceled, got %v", err)
		}
	})
	t.Run("no pids", func(t *testing.T) {
		if err := backends.NewCudaCheckpoint().GuestCheckpoint(context.Background(), nil, nil, nil); err == nil {
			t.Error("want an error")
		}
	})
}

func TestGuestRestore(t *testing.T) {
	r := &recorder{}
	c := backends.NewCudaCheckpoint()
	c.SetExecCommand(r.exec)
	states := map[int]string{1: backends.CudaStateCheckpointed, 2: backends.CudaStateLocked, 3: backends.CudaStateRunning}
	if err := c.GuestRestore(context.Background(), []int{1, 2, 3}, states); err != nil {
		t.Fatal(err)
	}
	want := []string{"restore 1", "unlock 1", "unlock 2"}
	if !reflect.DeepEqual(r.calls, want) {
		t.Errorf("calls %v, want %v", r.calls, want)
	}

	r2 := &recorder{fail: map[string]bool{"restore 1": true}}
	c.SetExecCommand(r2.exec)
	if err := c.GuestRestore(context.Background(), []int{1}, states); err == nil {
		t.Error("want an error on restore failure")
	}
	if err := c.GuestRestore(context.Background(), []int{9}, map[int]string{9: backends.CudaStateFailed}); err == nil {
		t.Error("want an error for a failed process")
	}
}

func TestAvailable(t *testing.T) {
	c := backends.NewCudaCheckpoint()
	c.SetLookPath(func(string) (string, error) { return "", errors.New("missing") })
	if err := c.Available(); err == nil {
		t.Error("want an error when the binary is missing")
	}
	c.SetLookPath(func(p string) (string, error) { return p, nil })
	if err := c.Available(); err != nil {
		t.Errorf("want nil, got %v", err)
	}
}
