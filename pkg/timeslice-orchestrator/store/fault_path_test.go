package store_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func queuedIDs(q *store.WaitingJobQueue) []string {
	jobs := q.List()
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.JobID)
	}
	return ids
}

func TestWaitingJobQueue_Remove(t *testing.T) {
	tests := []struct {
		name     string
		enqueue  []string
		remove   string
		wantOK   bool
		wantJobs []string
	}{
		{name: "front", enqueue: []string{"a", "b", "c"}, remove: "a", wantOK: true, wantJobs: []string{"b", "c"}},
		{name: "middle keeps FIFO order", enqueue: []string{"a", "b", "c"}, remove: "b", wantOK: true, wantJobs: []string{"a", "c"}},
		{name: "back", enqueue: []string{"a", "b", "c"}, remove: "c", wantOK: true, wantJobs: []string{"a", "b"}},
		{name: "absent", enqueue: []string{"a"}, remove: "z", wantOK: false, wantJobs: []string{"a"}},
		{name: "empty queue", enqueue: nil, remove: "a", wantOK: false, wantJobs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := store.NewWaitingJobQueue()
			for _, id := range tt.enqueue {
				q.Enqueue(id)
			}
			if got := q.Remove(tt.remove); got != tt.wantOK {
				t.Errorf("Remove(%q) = %v, want %v", tt.remove, got, tt.wantOK)
			}
			if got := queuedIDs(q); !reflect.DeepEqual(got, tt.wantJobs) {
				t.Errorf("queue = %v, want %v", got, tt.wantJobs)
			}
			if q.Exists(tt.remove) {
				t.Errorf("Exists(%q) = true after Remove", tt.remove)
			}
			if q.Len() != len(tt.wantJobs) {
				t.Errorf("Len() = %d, want %d", q.Len(), len(tt.wantJobs))
			}
		})
	}
}

func TestWaitingJobQueue_RemoveThenEnqueueGoesToBack(t *testing.T) {
	q := store.NewWaitingJobQueue()
	q.Enqueue("a")
	q.Enqueue("b")
	if !q.Remove("a") {
		t.Fatal("Remove(a) = false, want true")
	}
	if !q.Enqueue("a") {
		t.Fatal("Enqueue(a) after Remove = false, want true (not a duplicate any more)")
	}
	if got, want := queuedIDs(q), []string{"b", "a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("queue = %v, want %v", got, want)
	}
}

func TestGroupSpec_CancelRequest(t *testing.T) {
	ctx := context.Background()
	group, err := store.NewGroup(ctx, "g", store.NewGroupLockStoreWrapper(store.NewMemLockStore(), "g"))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	spec := group.Spec()

	spec.RequestLock("a")
	spec.RequestLock("b")
	if !spec.CancelRequest("b") {
		t.Error("CancelRequest(b) = false for a queued job, want true")
	}
	if spec.CancelRequest("b") {
		t.Error("second CancelRequest(b) = true, want false")
	}
	if promoted, err := spec.TryPromote(ctx); err != nil || !promoted {
		t.Fatalf("TryPromote = %v, %v; want true, nil", promoted, err)
	}
	// A granted lock is not withdrawn by a cancel; only Yield releases it.
	if spec.CancelRequest("a") {
		t.Error("CancelRequest(a) = true for the lock holder, want false")
	}
	if got := spec.LockingJob(); got != "a" {
		t.Errorf("LockingJob() = %q, want a", got)
	}
}

// TestGroupSpec_CancelledWaiterNotPromotedAfterYield is Q13 scenario S3: a
// waiter whose Acquire had already failed was promoted 0.021 s after the holder
// yielded, and the controller then snapshotted the running trainer for nobody.
// Once the failed Acquire withdraws its request, a Yield promotes no one.
func TestGroupSpec_CancelledWaiterNotPromotedAfterYield(t *testing.T) {
	ctx := context.Background()
	group, err := store.NewGroup(ctx, "g", store.NewGroupLockStoreWrapper(store.NewMemLockStore(), "g"))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	spec := group.Spec()

	spec.RequestLock("trainer")
	if _, err := spec.TryPromote(ctx); err != nil {
		t.Fatalf("TryPromote: %v", err)
	}
	spec.RequestLock("zombie")
	spec.CancelRequest("zombie")

	if err := spec.Yield(ctx, "trainer"); err != nil {
		t.Fatalf("Yield: %v", err)
	}
	promoted, err := spec.TryPromote(ctx)
	if err != nil {
		t.Fatalf("TryPromote: %v", err)
	}
	if promoted || spec.LockingJob() != "" {
		t.Errorf("TryPromote after Yield promoted %q, want no one", spec.LockingJob())
	}
	if got := spec.ActiveJob(); got != "trainer" {
		t.Errorf("ActiveJob() = %q, want trainer (its context stays loaded)", got)
	}
}

// hangingAgent never answers until the call's context ends.
type hangingAgent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer
}

func (hangingAgent) Status(ctx context.Context, _ *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (hangingAgent) GetOperation(
	ctx context.Context, _ *agentpb.GetOperationRequest,
) (*agentpb.GetOperationResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (hangingAgent) Snapshot(ctx context.Context, _ *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (hangingAgent) Restore(ctx context.Context, _ *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func startHangingAgent(t *testing.T) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, hangingAgent{})
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("fake agent: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

// TestGRPCSnapshotAgentStore_RPCTimeout is Q13 scenario S5c: one wedged
// GetOperation held a controller worker for 15 s after the operation had
// finished in 0.06 s. With a per-call timeout every agent call, including each
// status and operation poll, returns DeadlineExceeded on time.
func TestGRPCSnapshotAgentStore_RPCTimeout(t *testing.T) {
	addr := startHangingAgent(t)
	const timeout = 200 * time.Millisecond
	agents := store.NewGRPCSnapshotAgentStore(0, 9001).WithRPCTimeout(timeout)
	if got := agents.RPCTimeout(); got != timeout {
		t.Fatalf("RPCTimeout() = %v, want %v", got, timeout)
	}

	calls := map[string]func(ctx context.Context) error{
		"GetStatus": func(ctx context.Context) error {
			_, err := agents.GetStatus(ctx, addr)
			return err
		},
		"GetOperation": func(ctx context.Context) error {
			_, err := agents.GetOperation(ctx, addr, "op-1")
			return err
		},
		"Snapshot": func(ctx context.Context) error {
			_, err := agents.Snapshot(ctx, addr, "job-1", "g")
			return err
		},
		"Restore": func(ctx context.Context) error {
			_, err := agents.Restore(ctx, addr, "job-1", "g")
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			// The caller's own context is unbounded, as in the controller.
			start := time.Now()
			err := call(context.Background())
			elapsed := time.Since(start)
			if status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("err = %v, want DeadlineExceeded", err)
			}
			if elapsed < timeout || elapsed > 3*time.Second {
				t.Errorf("returned after %v, want about %v", elapsed, timeout)
			}
		})
	}
}

// TestGRPCSnapshotAgentStore_NoRPCTimeoutByDefault keeps the library default:
// without WithRPCTimeout only the caller's context bounds a call.
func TestGRPCSnapshotAgentStore_NoRPCTimeoutByDefault(t *testing.T) {
	addr := startHangingAgent(t)
	agents := store.NewGRPCSnapshotAgentStore(0, 9001)
	if got := agents.RPCTimeout(); got != 0 {
		t.Fatalf("RPCTimeout() = %v, want 0", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := agents.GetOperation(ctx, addr, "op-1")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded from the caller's context", err)
	}
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("returned after %v, want the caller's 600ms", elapsed)
	}
}
