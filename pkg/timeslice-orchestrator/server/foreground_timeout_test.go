package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestForegroundOpTimeout_FaultedJobFailsAcquire: a foreground job the
// controller marked FAULTED after a foreground operation timeout faults the
// group for Acquire, although the agent still reports it TRANSITIONING. A pod
// replacement clears it, and the trainer then waits instead of failing.
func TestForegroundOpTimeout_FaultedJobFailsAcquire(t *testing.T) {
	fx := newFaultFixture(t, "trainer", false)
	job := store.NewJob(faultGroup, "trainer")
	job.SetPods([]string{"trainer-pod-1"})
	job.UpdateContextState("node-1", pb.SnapshotAgentJobState_STATE_TRANSITIONING)
	job.MarkForegroundTimeoutFault("node-1", "restore-1")
	if err := fx.jobs.Put(context.Background(), job); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}
	client := fx.client(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := acquire(ctx, client, "trainer")
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("trainer Acquire code = %v (%v), want Unavailable", got, err)
	}
	if !strings.Contains(err.Error(), "group group-1 is faulted") {
		t.Errorf("error = %v, want the group fault message", err)
	}
	fx.waitForEmptyQueue(t)

	job.SetPods([]string{"trainer-pod-2"})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	_, err = acquire(ctx2, client, "trainer")
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("trainer Acquire after the pod was replaced: code = %v (%v), want DeadlineExceeded (waits, not faulted)",
			got, err)
	}
}
