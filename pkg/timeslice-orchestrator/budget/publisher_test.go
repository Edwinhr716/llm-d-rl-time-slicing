package budget_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

const batchJob = "shadow-vllm"

// fakeWriter records every write and can be made to fail.
type fakeWriter struct {
	mu     sync.Mutex
	writes []string
	keys   []string
	err    error
	closed bool
}

func (w *fakeWriter) SetKey(_ context.Context, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.keys = append(w.keys, key)
	w.writes = append(w.writes, value)
	return nil
}

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *fakeWriter) recorded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.writes...)
}

func (w *fakeWriter) fail(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = err
}

// snapshot builds a GroupSnapshot in the shape the store would produce.
func snapshot(lockingJob, loadedJob string, restored bool, waiters int) *store.GroupSnapshot {
	snap := &store.GroupSnapshot{
		ID:               "trainers",
		LockingJob:       lockingJob,
		LoadedJob:        loadedJob,
		WaiterQueueDepth: waiters,
	}
	if lockingJob != "" {
		snap.LockedAt = time.Now().Add(-time.Minute)
	}
	if restored {
		snap.LoadedAt = time.Now().Add(-30 * time.Second)
	}
	return snap
}

func TestFor(t *testing.T) {
	tests := []struct {
		name      string
		snap      *store.GroupSnapshot
		batchJob  string
		waiters   int
		want      string
		rationale string
	}{
		{
			name:      "batch tenant serving with no pressure",
			snap:      snapshot(batchJob, batchJob, true, 0),
			batchJob:  batchJob,
			want:      budget.Available,
			rationale: "the only case in which dispatch is safe",
		},
		{
			name:     "trainer holds the lock",
			snap:     snapshot("rl-trainer", "rl-trainer", true, 0),
			batchJob: batchJob,
			want:     budget.Blocked,
		},
		{
			name:     "nobody holds the lock",
			snap:     snapshot("", batchJob, true, 0),
			batchJob: batchJob,
			want:     budget.Blocked,
		},
		{
			name:      "batch tenant granted but context not yet restored",
			snap:      snapshot(batchJob, "", false, 0),
			batchJob:  batchJob,
			want:      budget.Blocked,
			rationale: "the engine cannot serve during the cuda-checkpoint restore",
		},
		{
			name:      "batch tenant holds the lock while the trainer's context is loaded",
			snap:      snapshot(batchJob, "rl-trainer", true, 0),
			batchJob:  batchJob,
			want:      budget.Blocked,
			rationale: "ServingSince requires the holder to own the loaded context",
		},
		{
			name:      "pre-emption advertised to the batch tenant",
			snap:      snapshot(batchJob, batchJob, true, 1),
			batchJob:  batchJob,
			waiters:   1,
			want:      budget.Blocked,
			rationale: "this is the edge the explicit signal exists to get ahead of",
		},
		{
			name:      "waiters queued but withheld by the quantum",
			snap:      snapshot(batchJob, batchJob, true, 1),
			batchJob:  batchJob,
			waiters:   0,
			want:      budget.Available,
			rationale: "the tenant has not been told to yield, so it can still serve",
		},
		{
			name:      "lock recovered at startup has no grant time",
			snap:      &store.GroupSnapshot{ID: "trainers", LockingJob: batchJob, LoadedJob: batchJob},
			batchJob:  batchJob,
			want:      budget.Blocked,
			rationale: "a recovered holder's ServingSince is zero; do not dispatch on an unknown",
		},
		{
			name:     "nil snapshot",
			snap:     nil,
			batchJob: batchJob,
			want:     budget.Blocked,
		},
		{
			name:     "no batch job configured",
			snap:     snapshot(batchJob, batchJob, true, 0),
			batchJob: "",
			want:     budget.Blocked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := budget.For(tt.snap, tt.batchJob, tt.waiters)
			if got != tt.want {
				t.Errorf("For() = %q, want %q (%s)", got, tt.want, tt.rationale)
			}
		})
	}
}

func TestPublisher_PublishWritesEveryCall(t *testing.T) {
	// The consuming gate fails OPEN on an absent key, so a publisher that
	// deduplicated unchanged values would never recreate an evicted key.
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "dispatch-gate-budget", batchJob)

	for range 3 {
		if err := p.Publish(context.Background(), budget.Available); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	got := w.recorded()
	if len(got) != 3 {
		t.Fatalf("got %d writes, want 3 (unchanged values must still be rewritten)", len(got))
	}
	for i, v := range got {
		if v != budget.Available {
			t.Errorf("write %d = %q, want %q", i, v, budget.Available)
		}
	}
}

func TestPublisher_PublishTracksLastValue(t *testing.T) {
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "k", batchJob)

	if p.Last() != "" {
		t.Errorf("Last() before any publish = %q, want empty", p.Last())
	}
	if err := p.Publish(context.Background(), budget.Available); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if p.Last() != budget.Available {
		t.Errorf("Last() = %q, want %q", p.Last(), budget.Available)
	}
	if err := p.Publish(context.Background(), budget.Blocked); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if p.Last() != budget.Blocked {
		t.Errorf("Last() = %q, want %q", p.Last(), budget.Blocked)
	}
}

func TestPublisher_PublishError(t *testing.T) {
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "k", batchJob)
	if err := p.Publish(context.Background(), budget.Available); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	sentinel := errors.New("redis down")
	w.fail(sentinel)
	err := p.Publish(context.Background(), budget.Blocked)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, sentinel)
	}
	// A failed write must not be remembered as published, or the next attempt
	// at the same value would be treated as a no-change.
	if p.Last() != budget.Available {
		t.Errorf("Last() after failed write = %q, want the last SUCCESSFUL value %q", p.Last(), budget.Available)
	}
}

func TestPublisher_PublishSurvivesCancelledCallerContext(t *testing.T) {
	// A Blocked write must land even if the poll that prompted it is gone.
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "k", batchJob)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.Publish(ctx, budget.Blocked); err != nil {
		t.Fatalf("Publish() with cancelled context error = %v", err)
	}
	if got := w.recorded(); len(got) != 1 || got[0] != budget.Blocked {
		t.Errorf("writes = %v, want one %q", got, budget.Blocked)
	}
}

func TestPublisher_DefaultKey(t *testing.T) {
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "", batchJob)
	if p.Key() != budget.DefaultKey {
		t.Errorf("Key() = %q, want %q", p.Key(), budget.DefaultKey)
	}
	if p.BatchJob() != batchJob {
		t.Errorf("BatchJob() = %q, want %q", p.BatchJob(), batchJob)
	}
}

func TestPublisher_NilIsSafe(t *testing.T) {
	var p *budget.Publisher
	if err := p.Publish(context.Background(), budget.Blocked); err != nil {
		t.Errorf("Publish() on nil publisher = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close() on nil publisher = %v, want nil", err)
	}
}

func TestPublisher_Close(t *testing.T) {
	w := &fakeWriter{}
	p := budget.NewPublisher(w, "k", batchJob)
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !w.closed {
		t.Error("Close() did not close the underlying writer")
	}
}
