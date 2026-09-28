package server_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// pollCtx returns a context that carries the poll metadata and ends after d.
func pollCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		metadata.AppendToOutgoingContext(context.Background(), server.AcquireModeMetadataKey, server.AcquireModePoll), d)
	t.Cleanup(cancel)
	return ctx
}

func asyncPoll() server.Option {
	return server.WithForegroundWait(controller.ForegroundWaitAsyncPoll)
}

func TestForegroundWait_AsyncPoll_NotGrantedReturnsAtOnce(t *testing.T) {
	gs, group := backgroundGroup(t, "job-1", true)
	client := backgroundClient(t, gs, asyncPoll())
	req := &pb.AcquireRequest{JobId: "job-2", GroupId: bgGroup}

	for i := range 3 {
		start := time.Now()
		resp, err := client.Acquire(pollCtx(t, 5*time.Second), req)
		if err != nil {
			t.Fatalf("poll %d: Acquire = %v", i+1, err)
		}
		if resp.GetSuccess() {
			t.Fatalf("poll %d: Acquire succeeded while job-1 holds the lock", i+1)
		}
		// The blocking Acquire checks only after a 1 s tick.
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Errorf("poll %d: Acquire took %v, want an immediate answer", i+1, elapsed)
		}
		// Polling again does not queue the job twice.
		if depth := group.Snapshot().WaiterQueueDepth; depth != 1 {
			t.Errorf("poll %d: waiter queue depth = %d, want 1", i+1, depth)
		}
	}
}

func TestForegroundWait_AsyncPoll_GrantedAfterPolls(t *testing.T) {
	// job-1 holds the lock, but its context is not loaded yet.
	gs, group := backgroundGroup(t, "job-1", false)
	client := backgroundClient(t, gs, asyncPoll())
	req := &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup}

	resp, err := client.Acquire(pollCtx(t, 5*time.Second), req)
	if err != nil || resp.GetSuccess() {
		t.Fatalf("first poll = %v, %v; want success=false", resp, err)
	}
	time.Sleep(200 * time.Millisecond)
	resp, err = client.Acquire(pollCtx(t, 5*time.Second), req)
	if err != nil || resp.GetSuccess() {
		t.Fatalf("second poll = %v, %v; want success=false", resp, err)
	}
	if resp.GetWaitedMs() < 200 {
		t.Errorf("waited_ms = %d, want it counted from the first poll (>= 200)", resp.GetWaitedMs())
	}

	group.Status().SetLoadedJob("job-1")
	resp, err = client.Acquire(pollCtx(t, 5*time.Second), req)
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("poll after the restore = %v, %v; want success", resp, err)
	}
	if !resp.GetContextRestored() {
		t.Error("granted poll lost context_restored")
	}
	if resp.GetWaitedMs() < 200 {
		t.Errorf("granted waited_ms = %d, want it counted from the first poll (>= 200)", resp.GetWaitedMs())
	}
}

func TestForegroundWait_AsyncPoll_WithoutMetadataBlocks(t *testing.T) {
	gs, _ := backgroundGroup(t, "job-1", false)
	client := backgroundClient(t, gs, asyncPoll())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := client.Acquire(ctx, &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup})
	assertCode(t, err, codes.DeadlineExceeded)
}

func TestForegroundWait_AsyncPoll_BlockingServerIgnoresMetadata(t *testing.T) {
	gs, group := backgroundGroup(t, "job-1", false)
	client := backgroundClient(t, gs) // default: blocking
	req := &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup}

	_, err := client.Acquire(pollCtx(t, 300*time.Millisecond), req)
	assertCode(t, err, codes.DeadlineExceeded)

	// A polling client against a blocking server gets one blocking answer.
	group.Status().SetLoadedJob("job-1")
	resp, err := client.Acquire(pollCtx(t, 5*time.Second), req)
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v; want success", resp, err)
	}
}

func TestForegroundWait_AsyncPoll_FailsClosedOverBackgroundGrant(t *testing.T) {
	gs, group := backgroundGroup(t, "job-1", true)
	client := backgroundClient(t, gs, server.WithBackgroundRole(true), asyncPoll())
	group.Spec().RegisterParticipant(bgNode, bgParticipant, time.Now())
	group.Spec().Grant(bgNode)
	group.Spec().UnregisterParticipant(bgNode)

	resp, err := client.Acquire(pollCtx(t, 5*time.Second), &pb.AcquireRequest{JobId: "job-1", GroupId: bgGroup})
	if err != nil {
		t.Fatalf("Acquire = %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("Acquire granted the accelerator over a held background grant")
	}
	// The poll started the notice, as the blocking Acquire does.
	st := groupStatus(t, client, "")
	if st.GetGroupState() != pb.GroupStatus_STATE_VACATING || st.GetVacateWithin() == nil {
		t.Errorf("state = %v, vacate_within = %v; want STATE_VACATING with vacate_within set",
			st.GetGroupState(), st.GetVacateWithin())
	}
}
