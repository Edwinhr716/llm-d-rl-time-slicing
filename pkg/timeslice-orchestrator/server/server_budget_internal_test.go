package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

type countingWriter struct {
	mu     sync.Mutex
	writes []string
}

func (w *countingWriter) SetKey(_ context.Context, _, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, value)
	return nil
}

func (w *countingWriter) Close() error { return nil }

func (w *countingWriter) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.writes...)
}

// TestRunBudgetPublisher_WritesTheKeyAtStartup covers the sharpest edge of the
// consuming gate: an absent key reads as full capacity. The publisher must
// therefore establish the key before anything polls it, with no group present
// and no traffic of any kind.
func TestRunBudgetPublisher_WritesTheKeyAtStartup(t *testing.T) {
	w := &countingWriter{}
	srv := NewServer(nil, store.NewGroupStore(store.NewMemLockStore()), store.NewJobStore(),
		WithDispatchBudgetPublisher(budget.NewPublisher(w, "dispatch-gate-budget", "shadow-vllm")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.runBudgetPublisher(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(w.snapshot()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	got := w.snapshot()
	if len(got) == 0 {
		t.Fatal("publisher wrote nothing at startup; the gate would have failed open on an absent key")
	}
	if got[0] != budget.Blocked {
		t.Errorf("first write = %q, want %q with no group serving", got[0], budget.Blocked)
	}
}

func TestPublishDispatchBudget_NoPublisherIsANoOp(t *testing.T) {
	srv := NewServer(nil, store.NewGroupStore(store.NewMemLockStore()), store.NewJobStore())
	// Must not panic.
	srv.publishDispatchBudget(context.Background())
}

func TestQuietAdvertisedWaiterDepth(t *testing.T) {
	ctx := context.Background()
	gs := store.NewGroupStore(store.NewMemLockStore())
	g, _, err := gs.GetOrCreate(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	g.Spec().RequestLock("shadow-vllm")
	if _, err := g.Spec().TryPromote(ctx); err != nil {
		t.Fatalf("failed to promote: %v", err)
	}
	g.Status().SetLoadedJob("shadow-vllm")
	g.Spec().RequestLock("rl-trainer")

	snap := g.Snapshot()

	withQuantum := NewServer(nil, gs, store.NewJobStore(), WithServingQuantum(time.Hour))
	if got := withQuantum.quietAdvertisedWaiterDepth(snap); got != 0 {
		t.Errorf("quietAdvertisedWaiterDepth() inside the quantum = %d, want 0", got)
	}

	noQuantum := NewServer(nil, gs, store.NewJobStore())
	if got := noQuantum.quietAdvertisedWaiterDepth(snap); got != 1 {
		t.Errorf("quietAdvertisedWaiterDepth() with no quantum = %d, want 1", got)
	}
}
