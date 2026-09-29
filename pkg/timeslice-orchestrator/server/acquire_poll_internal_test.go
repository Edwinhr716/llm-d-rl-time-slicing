package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc/metadata"
)

func TestForegroundWait_AsyncPoll_Tracker(t *testing.T) {
	var tracker pollTracker
	t0 := time.Unix(1000, 0)

	first, isNew := tracker.mark("g", "j", t0)
	if !isNew || !first.Equal(t0) {
		t.Fatalf("first mark = %v, %v; want %v, true", first, isNew, t0)
	}
	first, isNew = tracker.mark("g", "j", t0.Add(time.Second))
	if isNew || !first.Equal(t0) {
		t.Fatalf("second mark = %v, %v; want %v, false", first, isNew, t0)
	}
	// Another job in the same group waits on its own.
	if _, isNew := tracker.mark("g", "other", t0.Add(time.Second)); !isNew {
		t.Error("another job's first mark is not new")
	}

	// A job that stops polling for longer than pollStaleAfter starts over.
	late := t0.Add(time.Second + pollStaleAfter + time.Millisecond)
	first, isNew = tracker.mark("g", "j", late)
	if !isNew || !first.Equal(late) {
		t.Fatalf("mark after a gap = %v, %v; want %v, true", first, isNew, late)
	}

	tracker.clear("g", "j")
	next := late.Add(time.Second)
	if first, isNew := tracker.mark("g", "j", next); !isNew || !first.Equal(next) {
		t.Fatalf("mark after clear = %v, %v; want %v, true", first, isNew, next)
	}
}

// TestForegroundWait_AsyncPoll_StaleWithdrawn: a job whose client stops
// polling leaves the waiter queue once the wait is stale, so a later Yield
// does not promote it for a caller that has gone. A job that keeps polling
// stays queued.
func TestForegroundWait_AsyncPoll_StaleWithdrawn(t *testing.T) {
	ctx := context.Background()
	gs := store.NewGroupStore(store.NewMemLockStore())
	group, _, err := gs.GetOrCreate(ctx, "g")
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	group.Spec().RequestLock("holder")
	if _, err := group.Spec().TryPromote(ctx); err != nil {
		t.Fatalf("promote holder: %v", err)
	}
	group.Status().SetLoadedJob("holder")

	srv := NewServer(nil, gs, store.NewJobStore(), WithForegroundWait(controller.ForegroundWaitAsyncPoll))
	srv.polls.stale = 200 * time.Millisecond
	pollIn := metadata.NewIncomingContext(ctx, metadata.Pairs(AcquireModeMetadataKey, AcquireModePoll))
	poll := func(job string) {
		t.Helper()
		resp, err := srv.Acquire(pollIn, &pb.AcquireRequest{JobId: job, GroupId: "g"})
		if err != nil || resp.GetSuccess() {
			t.Fatalf("poll %s = %v, %v; want success=false", job, resp, err)
		}
	}
	queued := group.Spec().GetWaitingJobQueue().Exists

	poll("gone")
	poll("steady")
	for range 8 {
		time.Sleep(60 * time.Millisecond)
		poll("steady")
	}
	if queued("gone") {
		t.Error("a job that stopped polling is still queued after the stale window")
	}
	if !queued("steady") {
		t.Error("a job that keeps polling was withdrawn")
	}

	// expire only removes a wait that is stale.
	var tr pollTracker
	now := time.Unix(2000, 0)
	tr.mark("g", "j", now)
	if tr.expire("g", "j", now.Add(time.Second)) {
		t.Error("expire removed a fresh wait")
	}
	if !tr.expire("g", "j", now.Add(pollStaleAfter)) {
		t.Error("expire kept a stale wait")
	}
}
