package statemachine_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A higher-epoch Suspend or Resume that arrives while a lower-epoch Suspend
// or Resume of the same job runs is refused with Aborted, and the running
// operation continues.

func TestHigherEpoch_RefusedAndRunningContinues(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	suspendStub := newGuestStub(suspendedResult)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 3, suspendStub.run)
	suspendCtx := waitStarted(t, suspendStub)

	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 4, future(), mustNotRun(t))
	requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	var refusal *statemachine.RefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("expected a RefusalError, got %T", err)
	}
	msg := status.Convert(err).Message()
	if !strings.Contains(msg, "a Suspend operation with epoch 3 is running") {
		t.Errorf("refusal message %q does not name the running operation and its epoch", msg)
	}

	// The running operation is untouched.
	if op := getOp(t, sm, suspendID); op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		t.Errorf("running operation: expected PENDING, got %s", op.Status)
	}
	if suspendCtx.Err() != nil {
		t.Errorf("running operation's context was cancelled: %v", suspendCtx.Err())
	}
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_TRANSITIONING)
	// The refused call's epoch is recorded.
	if got := jobStatus(t, sm).GetEpoch(); got != 4 {
		t.Errorf("expected epoch 4 after the refusal, got %d", got)
	}

	// A retry with the same epoch while the operation runs is refused again.
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 4, future(), mustNotRun(t))
	requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	close(suspendStub.release)
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)

	// Once it finishes, the retry with the same epoch is accepted.
	resumeID := startGuest(t, sm, statemachine.OpTypeResume, 4, instant(statemachine.GuestResult{}, nil))
	checkComplete(t, waitForOperation(t, sm, resumeID), pb.Outcome_OUTCOME_RESUMED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_RUNNING)
}

func TestHigherEpoch_FencingUnchanged(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 5, stub.run)
	waitStarted(t, stub)

	// Same epoch, same call: the running operation.
	if id := startGuest(t, sm, statemachine.OpTypeSuspend, 5, mustNotRun(t)); id != suspendID {
		t.Errorf("same epoch and call: expected %s, got %s", suspendID, id)
	}
	// Same epoch, different call, and a lower epoch: STALE_EPOCH, not Aborted.
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 5, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 4, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	// A past deadline is still infeasible.
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 6, time.Now().Add(-time.Second), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE)

	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
}

func TestHigherEpoch_KillStillSupersedes(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)
	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
	suspendCtx := waitStarted(t, stub)

	killID := startKill(t, sm, func(context.Context) error { return nil })
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	checkFailed(t, getOp(t, sm, suspendID), pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if !errors.Is(suspendCtx.Err(), context.Canceled) {
		t.Errorf("superseded operation's context: expected Canceled, got %v", suspendCtx.Err())
	}
	close(stub.release)
	waitReturned(t, stub)
	checkKilled(t, sm)
}

// higherEpochOrdering drives the deterministic racing Suspend and Kill: a
// Suspend with epoch 1 runs, then a Suspend with epoch 2 and a Kill arrive
// in the given order. The epoch-2 call is refused with Aborted either way.
func higherEpochOrdering(t *testing.T, killFirst bool) {
	t.Helper()
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	first := newGuestStub(suspendedResult)
	killStub := newGuestStub(statemachine.GuestResult{})

	firstID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, first.run)
	waitStarted(t, first)

	suspend2 := func() {
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 2, future(), mustNotRun(t))
		if code := status.Code(err); code != codes.Aborted {
			t.Errorf("Suspend e=2 (kill first %v): expected Aborted, got %s", killFirst, code)
		}
	}
	var killID string
	if killFirst {
		killID = startKill(t, sm, killStub.kill)
		waitStarted(t, killStub)
		suspend2()
	} else {
		suspend2()
		killID = startKill(t, sm, killStub.kill)
		waitStarted(t, killStub)
	}

	close(killStub.release)
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	close(first.release)
	waitReturned(t, first)

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
}

func TestHigherEpoch_RacingSuspendAndKillOrdered(t *testing.T) {
	higherEpochOrdering(t, false)
	higherEpochOrdering(t, true)
}

// TestHigherEpoch_RacingSuspendAndKill fires a Suspend with epoch 2 and a
// Kill concurrently while a Suspend with epoch 1 runs, many times. Every
// iteration must settle IDLE + KILLED with the Kill COMPLETE, and no guest
// worker may start for the epoch-2 call.
func TestHigherEpoch_RacingSuspendAndKill(t *testing.T) {
	const iterations = 200
	for iter := range iterations {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		first := newGuestStub(suspendedResult)
		var started atomic.Int32
		guest := func(ctx context.Context) (statemachine.GuestResult, error) {
			started.Add(1)
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
			// Aborted while an operation runs; accepted at once (RELEASED, no
			// worker) when the Kill has already finished.
			if _, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 2, future(), guest); err != nil &&
				status.Code(err) != codes.Aborted {
				t.Errorf("Suspend e=2: unexpected error %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			killID, killErr = sm.StartKill(guestJob, future(), "race", func(context.Context) error { return nil })
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
		if n := started.Load(); n != 0 {
			t.Fatalf("iteration %d: %d guest workers started for the epoch-2 call", iter, n)
		}
	}
}
