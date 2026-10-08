package server_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// servingGroup builds a group whose holder holds the lock, optionally has its
// context loaded, and has waiters queued behind it.
func servingGroup(t *testing.T, ctx context.Context, holder string, loaded bool, waiters ...string) server.GroupStore {
	t.Helper()
	gs := store.NewGroupStore(store.NewMemLockStore())
	g, _, err := gs.GetOrCreate(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	g.Spec().RequestLock(holder)
	if _, err := g.Spec().TryPromote(ctx); err != nil {
		t.Fatalf("failed to promote %s: %v", holder, err)
	}
	if loaded {
		g.Status().SetLoadedJob(holder)
	}
	for _, w := range waiters {
		g.Spec().RequestLock(w)
	}
	g.Status().SetState(pb.GroupStatus_STATE_LOCKED)
	return gs
}

func statusClient(t *testing.T, gs server.GroupStore, opts ...server.Option) pb.TimeSliceOrchestratorServiceClient {
	t.Helper()
	_, _, cleanup := server.InitGRPCServer(gs, store.NewJobStore(), opts...)
	t.Cleanup(cleanup)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(server.BufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewTimeSliceOrchestratorServiceClient(conn)
}

func TestServer_GetGroupStatus_ServingQuantum(t *testing.T) {
	tests := []struct {
		name          string
		quantum       time.Duration
		loaded        bool
		settle        time.Duration
		expectedDepth int64
	}{
		{
			// Regression guard for the default: the previous behaviour must be
			// bit-for-bit unchanged unless the quantum is opted into.
			name:          "disabled reports waiters immediately",
			quantum:       0,
			loaded:        true,
			expectedDepth: 2,
		},
		{
			name:          "withholds waiters inside the quantum",
			quantum:       time.Hour,
			loaded:        true,
			expectedDepth: 0,
		},
		{
			name:          "reports waiters once the quantum expires",
			quantum:       20 * time.Millisecond,
			loaded:        true,
			settle:        60 * time.Millisecond,
			expectedDepth: 2,
		},
		{
			// Fail open: a grant whose context never loads must not withhold
			// pre-emption pressure forever.
			name:          "does not withhold before the context is loaded",
			quantum:       time.Hour,
			loaded:        false,
			expectedDepth: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			gs := servingGroup(t, ctx, "holder", tc.loaded, "waiter-1", "waiter-2")
			client := statusClient(t, gs, server.WithServingQuantum(tc.quantum))

			time.Sleep(tc.settle)

			resp, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
			if err != nil {
				t.Fatalf("GetGroupStatus failed: %v", err)
			}
			if got := resp.GetGroup().GetWaiterQueueDepth(); got != tc.expectedDepth {
				t.Errorf("waiter_queue_depth = %d, want %d", got, tc.expectedDepth)
			}
			// The quantum must only mask the advertisement; every other field
			// still describes the real state of the group.
			if got := resp.GetGroup().GetLockingJob(); got != "holder" {
				t.Errorf("locking_job = %q, want %q", got, "holder")
			}
		})
	}
}

func TestServer_GetGroupStatus_QuantumDoesNotDelayEarlyRelease(t *testing.T) {
	ctx := context.Background()
	gs := servingGroup(t, ctx, "holder", true, "waiter-1")
	client := statusClient(t, gs, server.WithServingQuantum(time.Hour))

	resp, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
	if err != nil {
		t.Fatalf("GetGroupStatus failed: %v", err)
	}
	if got := resp.GetGroup().GetWaiterQueueDepth(); got != 0 {
		t.Fatalf("expected the quantum to withhold the waiter, got depth %d", got)
	}

	// A holder that releases voluntarily while still inside its quantum must
	// hand the lock over at once: the quantum delays the pre-emption signal,
	// never the promotion itself.
	if _, err := client.Yield(ctx, &pb.YieldRequest{GroupId: "group-1", JobId: "holder"}); err != nil {
		t.Fatalf("Yield failed: %v", err)
	}

	group, err := gs.Get(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to get group: %v", err)
	}
	promoted, err := group.Spec().TryPromote(ctx)
	if err != nil {
		t.Fatalf("TryPromote failed: %v", err)
	}
	if !promoted {
		t.Fatal("expected the waiter to be promotable immediately after an early release")
	}
	if got := group.Spec().LockingJob(); got != "waiter-1" {
		t.Errorf("locking_job = %q, want %q", got, "waiter-1")
	}
}

func TestServer_GetGroupStatus_QuantumRestartsPerHolder(t *testing.T) {
	ctx := context.Background()
	gs := servingGroup(t, ctx, "holder", true, "waiter-1")
	client := statusClient(t, gs, server.WithServingQuantum(30*time.Millisecond))

	time.Sleep(60 * time.Millisecond)
	resp, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
	if err != nil {
		t.Fatalf("GetGroupStatus failed: %v", err)
	}
	if got := resp.GetGroup().GetWaiterQueueDepth(); got != 1 {
		t.Fatalf("expected the expired quantum to expose the waiter, got depth %d", got)
	}

	// Hand the lock to the waiter: the new holder gets its own fresh quantum.
	group, err := gs.Get(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to get group: %v", err)
	}
	if err := group.Spec().Yield(ctx, "holder"); err != nil {
		t.Fatalf("Yield failed: %v", err)
	}
	group.Status().SetLoadedJob("")
	if _, err := group.Spec().TryPromote(ctx); err != nil {
		t.Fatalf("TryPromote failed: %v", err)
	}
	group.Status().SetLoadedJob("waiter-1")
	group.Spec().RequestLock("holder")

	resp, err = client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
	if err != nil {
		t.Fatalf("GetGroupStatus failed: %v", err)
	}
	if got := resp.GetGroup().GetWaiterQueueDepth(); got != 0 {
		t.Errorf("expected the new holder's quantum to withhold the waiter, got depth %d", got)
	}
}
