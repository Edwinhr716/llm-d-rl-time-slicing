package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// TestAcquire_DefaultPollSeesGrantWithinOnePoll: a foreground Acquire whose
// grant becomes possible 250 ms in returns within one 100 ms poll of it. With
// the former 1 s poll it returned at 1 s, and a reclaim that was done at 2.8 s
// was granted at 4.0 s.
func TestAcquire_DefaultPollSeesGrantWithinOnePoll(t *testing.T) {
	ctx := context.Background()
	gs := store.NewGroupStore(store.NewMemLockStore())
	if _, _, err := gs.GetOrCreate(ctx, "group-1"); err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	srv := NewServer(nil, gs, store.NewJobStore())
	if srv.acquirePollInterval != 100*time.Millisecond {
		t.Fatalf("default acquirePollInterval = %v, want 100ms", srv.acquirePollInterval)
	}

	readyAt := time.Now().Add(250 * time.Millisecond)
	srv.checkAcquire = func(_ context.Context, _, _ string, startTime time.Time) (*pb.AcquireResponse, error, bool) {
		if time.Now().Before(readyAt) {
			return nil, nil, false //nolint:nilnil // not done yet
		}
		return &pb.AcquireResponse{Success: true, WaitedMs: time.Since(startTime).Milliseconds()}, nil, true
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	resp, err := srv.Acquire(ctx, &pb.AcquireRequest{GroupId: "group-1", JobId: "job-1"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Acquire success = false, want true")
	}
	if elapsed < 250*time.Millisecond || elapsed >= 600*time.Millisecond {
		t.Errorf("Acquire returned after %v, want within one 100ms poll of 250ms", elapsed)
	}
}
