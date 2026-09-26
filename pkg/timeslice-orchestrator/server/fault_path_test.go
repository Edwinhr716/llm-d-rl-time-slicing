package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const faultGroup = "group-1"

// faultFixture is a group whose lock is held by holder and, if loaded, whose
// holder's context is loaded.
type faultFixture struct {
	groups *store.GroupStore
	jobs   *store.JobStore
	group  *store.Group
}

func newFaultFixture(t *testing.T, holder string, loaded bool) *faultFixture {
	t.Helper()
	ctx := context.Background()
	lockStore := store.NewMemLockStore()
	if holder != "" {
		if err := lockStore.Lock(ctx, faultGroup, holder); err != nil {
			t.Fatalf("failed to lock: %v", err)
		}
	}
	groups := store.NewGroupStore(lockStore)
	group, _, err := groups.GetOrCreate(ctx, faultGroup)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	if loaded {
		group.Status().SetLoadedJob(holder)
	}
	return &faultFixture{groups: groups, jobs: store.NewJobStore(), group: group}
}

// putFaulted stores a job that is FAULTED on node-1 with the given role.
func (f *faultFixture) putFaulted(t *testing.T, jobID string, role store.Role) {
	t.Helper()
	job := store.NewJob(faultGroup, jobID)
	job.SetRole(role)
	job.UpdateContextState("node-1", pb.SnapshotAgentJobState_STATE_FAULTED)
	if err := f.jobs.Put(context.Background(), job); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}
}

func (f *faultFixture) client(t *testing.T) pb.TimeSliceOrchestratorServiceClient {
	t.Helper()
	_, _, cleanup := server.InitGRPCServer(f.groups, f.jobs)
	t.Cleanup(cleanup)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(server.BufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("closing client connection: %v", err)
		}
	})
	return pb.NewTimeSliceOrchestratorServiceClient(conn)
}

// waitForEmptyQueue polls because the server handler finishes its cleanup
// after the client has already seen a deadline or a cancel.
func (f *faultFixture) waitForEmptyQueue(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.group.Spec().GetWaitingJobQueue().Len() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waiting queue = %v after a failed Acquire, want empty", f.group.Spec().GetWaitingJobQueue().List())
}

func acquire(
	ctx context.Context, client pb.TimeSliceOrchestratorServiceClient, jobID string,
) (*pb.AcquireResponse, error) {
	return client.Acquire(ctx, &pb.AcquireRequest{GroupId: faultGroup, JobId: jobID})
}

// TestAcquire_FaultedGuestDoesNotFaultGroup is Q13 scenario S1: a guest's
// snapshot error left the guest FAULTED, and the trainer's Acquire failed with
// Unavailable at 1.7 s. Now the trainer's Acquire does not fail; it waits,
// because the guest may still hold accelerator memory.
func TestAcquire_FaultedGuestDoesNotFaultGroup(t *testing.T) {
	fx := newFaultFixture(t, "trainer", true)
	fx.putFaulted(t, "guest", store.RoleBackground)
	client := fx.client(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := acquire(ctx, client, "trainer")
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("trainer Acquire code = %v (%v), want DeadlineExceeded: a FAULTED guest must hold the grant, not fail it",
			got, err)
	}
}

// TestAcquire_FaultedGuestClearedThenGranted checks the wait ends once the
// FAULTED guest is gone.
func TestAcquire_FaultedGuestClearedThenGranted(t *testing.T) {
	fx := newFaultFixture(t, "trainer", true)
	fx.putFaulted(t, "guest", store.RoleBackground)
	client := fx.client(t)

	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := fx.jobs.Delete(context.Background(), faultGroup, "guest"); err != nil {
			t.Errorf("failed to delete guest: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	resp, err := acquire(ctx, client, "trainer")
	if err != nil {
		t.Fatalf("trainer Acquire after the guest was cleared: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("Success = false, want true")
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Errorf("granted after %v, before the guest was cleared", elapsed)
	}
}

// TestAcquire_FaultedGuestOwnAcquireFails keeps the fault visible to the guest
// itself: only its own Acquire fails.
func TestAcquire_FaultedGuestOwnAcquireFails(t *testing.T) {
	fx := newFaultFixture(t, "trainer", true)
	fx.putFaulted(t, "guest", store.RoleBackground)
	client := fx.client(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := acquire(ctx, client, "guest")
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("guest Acquire code = %v (%v), want Unavailable", got, err)
	}
	if !strings.Contains(err.Error(), "job guest is faulted") {
		t.Errorf("error = %v, want it to name the guest job, not the group", err)
	}
	fx.waitForEmptyQueue(t)
}

// TestAcquire_ForegroundFaultStillFaultsGroup keeps today's behaviour for a
// foreground job: the group is FAULTED and Acquire fails fast.
func TestAcquire_ForegroundFaultStillFaultsGroup(t *testing.T) {
	fx := newFaultFixture(t, "", false)
	fx.putFaulted(t, "trainer", store.RoleForeground)
	client := fx.client(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := acquire(ctx, client, "job-1")
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("Acquire code = %v (%v), want Unavailable", got, err)
	}
	if !strings.Contains(err.Error(), "group group-1 is faulted") {
		t.Errorf("error = %v, want the group fault message", err)
	}
}

// TestAcquire_FailedAcquireLeavesNoWaiter is Q13 scenario S2: Acquires that
// failed left their job in the waiting queue. Every failed return now removes
// it, whether the failure is a fault, a deadline or a cancel.
func TestAcquire_FailedAcquireLeavesNoWaiter(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, fx *faultFixture)
		ctx      func() (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{
			name: "unavailable on a foreground fault",
			setup: func(t *testing.T, fx *faultFixture) {
				t.Helper()
				fx.putFaulted(t, "trainer", store.RoleForeground)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 2*time.Second)
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "deadline exceeded while the holder keeps the lock",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 100*time.Millisecond)
			},
			wantCode: codes.DeadlineExceeded,
		},
		{
			name: "cancelled by the caller",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(100*time.Millisecond, cancel)
				return ctx, cancel
			},
			wantCode: codes.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newFaultFixture(t, "trainer", true)
			if tt.setup != nil {
				tt.setup(t, fx)
			}
			client := fx.client(t)

			ctx, cancel := tt.ctx()
			defer cancel()
			_, err := acquire(ctx, client, "job-1")
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("Acquire code = %v (%v), want %v", got, err, tt.wantCode)
			}
			fx.waitForEmptyQueue(t)
			if got := fx.group.Spec().LockingJob(); got != "trainer" {
				t.Errorf("LockingJob() = %q, want trainer untouched", got)
			}
		})
	}
}

// TestAcquire_FailedWaiterNotPromotedOnYield is Q13 scenario S3 end to end: a
// waiter whose Acquire has failed must not be granted the lock when the holder
// yields, or the controller evicts the holder for a caller that has gone.
func TestAcquire_FailedWaiterNotPromotedOnYield(t *testing.T) {
	fx := newFaultFixture(t, "trainer", true)
	client := fx.client(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := acquire(ctx, client, "zombie"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("zombie Acquire = %v, want DeadlineExceeded", err)
	}
	fx.waitForEmptyQueue(t)

	yieldCtx, yieldCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer yieldCancel()
	resp, err := client.Yield(yieldCtx, &pb.YieldRequest{GroupId: faultGroup, JobId: "trainer"})
	if err != nil {
		t.Fatalf("Yield: %v", err)
	}
	if resp.GetPendingWaiters() != 0 {
		t.Errorf("PendingWaiters = %d, want 0", resp.GetPendingWaiters())
	}
	promoted, err := fx.group.Spec().TryPromote(context.Background())
	if err != nil {
		t.Fatalf("TryPromote: %v", err)
	}
	if promoted {
		t.Errorf("TryPromote promoted %q after Yield, want no one", fx.group.Spec().LockingJob())
	}
}
