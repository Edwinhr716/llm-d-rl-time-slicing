package controller_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
)

// Foreground wait option B (async-requeue, D-ORCH-1) with the D-NS-4 host
// commands: the hosts never resume, and no job is reported loaded, while a
// foreground snapshot or restore is in flight.

// heldOperations makes every agent operation PENDING until release is set.
func heldOperations(release *atomic.Bool) func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
	return func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		if release.Load() {
			return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
		}
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
	}
}

// TestForegroundWait_Async_PushLendResumesAfterSnapshot: on a lend the hosts
// resume only once the trainer's snapshot operation has completed, even
// though the agent already reports the trainer SAVED.
func TestForegroundWait_Async_PushLendResumesAfterSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_RUNNING)
	fix.ctrl.ForegroundWait = controller.ForegroundWaitAsyncRequeue
	var release atomic.Bool
	fix.agent.OperationFunc = heldOperations(&release)
	fix.hosts.setAllClear()
	spec := fix.group.Spec()
	spec.RequestLock("trainer")
	if _, err := spec.TryPromote(ctx); err != nil {
		t.Fatal(err)
	}
	if err := spec.Yield(ctx, "trainer"); err != nil {
		t.Fatal(err)
	}
	spec.SetLend(true)
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return len(fix.events.list()) > 0 }, 3*time.Second); err != nil {
		t.Fatalf("no snapshot started: %v", err)
	}
	// A few 1 s checks with the snapshot pending.
	time.Sleep(2500 * time.Millisecond)
	if n := fix.hosts.resumeCount(); n != 0 {
		t.Fatalf("hosts resumed %d times while the trainer's snapshot is in flight", n)
	}

	release.Store(true)
	if err := waitWithTimeout(func() bool { return fix.hosts.resumeCount() == 1 }, 5*time.Second); err != nil {
		t.Fatalf("hosts never resumed after the snapshot completed: %v", err)
	}
	got := fix.events.list()
	if len(got) != 2 || got[0] != "snapshot trainer" || got[1] != "resume" {
		t.Fatalf("events = %v, want one snapshot of the trainer, then resume", got)
	}
}

// TestForegroundWait_Async_PushHoldThenRestore: promotion waits for the hosts
// to clear, then the restore is tracked on a requeue and the trainer is only
// reported loaded once it has completed.
func TestForegroundWait_Async_PushHoldThenRestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fix := newPushFixture(t, ctx, pb.SnapshotAgentJobState_STATE_SAVED)
	fix.ctrl.ForegroundWait = controller.ForegroundWaitAsyncRequeue
	var release atomic.Bool
	fix.agent.OperationFunc = heldOperations(&release)
	fix.group.Spec().RequestLock("trainer")
	fix.run(t, ctx)
	fix.queue.Add(pushGroup)

	if err := waitWithTimeout(func() bool { return fix.hosts.vacateCount() > 0 }, 3*time.Second); err != nil {
		t.Fatalf("no vacate started while a foreground job waits: %v", err)
	}
	if got := fix.group.Spec().LockingJob(); got != "" {
		t.Fatalf("trainer promoted to %q while hosts are not clear", got)
	}

	fix.hosts.setAllClear()
	fix.queue.Add(pushGroup)
	restored := func() bool {
		for _, ev := range fix.events.list() {
			if ev == "restore trainer" {
				return true
			}
		}
		return false
	}
	if err := waitWithTimeout(restored, 3*time.Second); err != nil {
		t.Fatalf("trainer restore not started once hosts are clear: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	if fix.group.Status().LoadedJob() == "trainer" {
		t.Fatal("trainer reported loaded while its restore is in flight")
	}

	release.Store(true)
	if err := waitWithTimeout(func() bool { return fix.group.Status().LoadedJob() == "trainer" }, 5*time.Second); err != nil {
		t.Fatalf("trainer not loaded after its restore completed: %v", err)
	}
}
