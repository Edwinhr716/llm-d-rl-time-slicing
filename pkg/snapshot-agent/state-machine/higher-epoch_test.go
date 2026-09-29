package statemachine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A higher-epoch Suspend or Resume aborts the running guest operation. These
// tests race that abort against a Kill of the same job.

// higherEpochOrdering drives the deterministic racing Suspend and Kill: a
// Suspend with epoch 1 runs, then a Suspend with epoch 2 and a Kill arrive
// in the given order. It returns the gRPC code of the epoch-2 call.
func higherEpochOrdering(t *testing.T, killFirst bool) codes.Code {
	t.Helper()
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	first := newGuestStub(suspendedResult)
	second := newGuestStub(suspendedResult)
	killStub := newGuestStub(statemachine.GuestResult{})

	firstID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, first.run)
	waitStarted(t, first)

	suspend2 := func() codes.Code {
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 2, future(), second.run)
		return status.Code(err)
	}
	var code codes.Code
	var killID string
	if killFirst {
		killID = startKill(t, sm, killStub.kill)
		waitStarted(t, killStub)
		code = suspend2()
		if code != codes.Aborted {
			t.Errorf("Suspend e=2 while Kill runs: expected Aborted, got %s", code)
		}
	} else {
		code = suspend2()
		killID = startKill(t, sm, killStub.kill)
		waitStarted(t, killStub)
	}

	close(killStub.release)
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	close(first.release)
	close(second.release)
	waitReturned(t, first)
	time.Sleep(50 * time.Millisecond)

	checkKilled(t, sm)
	if op := getOp(t, sm, firstID); op.Status != pb.OperationStatus_OPERATION_STATUS_FAILED {
		t.Errorf("Suspend e=1: expected FAILED, got %s", op.Status)
	}
	sm.InternalMu().RLock()
	for id, op := range sm.InternalOperations() {
		if op.Status == pb.OperationStatus_OPERATION_STATUS_PENDING {
			t.Errorf("operation %s (%s) left PENDING", id, op.Type)
		}
	}
	sm.InternalMu().RUnlock()
	return code
}

func TestHigherEpoch_RacingSuspendAndKillOrdered(t *testing.T) {
	if code := higherEpochOrdering(t, false); code != codes.OK {
		t.Errorf("Suspend e=2 first: expected OK (aborts e=1), got %s", code)
	}
	higherEpochOrdering(t, true)
}

// TestHigherEpoch_RacingSuspendAndKill fires a Suspend with epoch 2 and a
// Kill concurrently while a Suspend with epoch 1 runs, many times. Every
// iteration must settle IDLE + KILLED with the Kill COMPLETE, and no guest
// worker may start after the Kill confirmed.
func TestHigherEpoch_RacingSuspendAndKill(t *testing.T) {
	const iterations = 200
	for iter := range iterations {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		first := newGuestStub(suspendedResult)
		var killed atomic.Bool
		var lateWorkers atomic.Int32
		guest := func(ctx context.Context) (statemachine.GuestResult, error) {
			// A superseded worker gets a cancelled context and is not counted.
			if killed.Load() && ctx.Err() == nil {
				lateWorkers.Add(1)
			}
			return suspendedResult, ctx.Err()
		}
		startGuest(t, sm, statemachine.OpTypeSuspend, 1, first.run)
		waitStarted(t, first)

		var wg sync.WaitGroup
		var killID string
		var killErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			// OK (aborts e=1), or Aborted when the Kill runs first.
			if _, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 2, future(), guest); err != nil &&
				status.Code(err) != codes.Aborted {
				t.Errorf("Suspend e=2: unexpected error %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			killID, killErr = sm.StartKill(guestJob, future(), "race", func(context.Context) error {
				killed.Store(true)
				return nil
			})
		}()
		wg.Wait()
		if killErr != nil {
			t.Fatalf("iteration %d: Kill refused: %v", iter, killErr)
		}
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		close(first.release)
		waitReturned(t, first)

		st := jobStatus(t, sm)
		if st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetLastOutcome() != pb.Outcome_OUTCOME_KILLED {
			t.Fatalf("iteration %d: expected IDLE/KILLED, got %s/%s", iter, st.GetState(), st.GetLastOutcome())
		}
		sm.InternalMu().RLock()
		for id, op := range sm.InternalOperations() {
			if op.Status == pb.OperationStatus_OPERATION_STATUS_PENDING {
				t.Fatalf("iteration %d: operation %s (%s) left PENDING", iter, id, op.Type)
			}
		}
		sm.InternalMu().RUnlock()
		if n := lateWorkers.Load(); n != 0 {
			t.Fatalf("iteration %d: %d guest workers started after the Kill confirmed", iter, n)
		}
	}
}
