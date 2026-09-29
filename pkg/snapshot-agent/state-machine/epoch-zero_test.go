package statemachine_test

import (
	"fmt"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

// A Suspend or Resume needs epoch >= 1: epoch 0 is the proto default of a
// caller that never set the field.

func operationCount(sm *statemachine.StateManager) int {
	sm.InternalMu().RLock()
	defer sm.InternalMu().RUnlock()
	return len(sm.InternalOperations())
}

// TestEpochZero_Refused: Suspend and Resume with epoch 0 or a negative
// epoch are InvalidArgument with no ErrorReason; the worker never runs, no
// operation is created and the seeded epoch is unchanged.
func TestEpochZero_Refused(t *testing.T) {
	for _, intent := range []statemachine.OpType{statemachine.OpTypeSuspend, statemachine.OpTypeResume} {
		for _, epoch := range []int64{0, -1} {
			t.Run(fmt.Sprintf("%s epoch %d", intent, epoch), func(t *testing.T) {
				sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED)
				sm.SeedEpoch(guestJob, 2)
				_, err := sm.StartGuestOp(guestJob, intent, epoch, future(), mustNotRun(t))
				requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
				if got := jobStatus(t, sm).GetEpoch(); got != 2 {
					t.Errorf("epoch %d refusal changed the epoch: got %d, want 2", epoch, got)
				}
				if n := operationCount(sm); n != 0 {
					t.Errorf("epoch %d refusal created %d operations", epoch, n)
				}
				checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
			})
		}
	}
}

// TestEpochZero_RefusedBeforeFencing: with no seed (last epoch 0), epoch 0
// would pass fencing; it is refused, and a later epoch 1 is still accepted.
func TestEpochZero_RefusedBeforeFencing(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	if got := jobStatus(t, sm).GetEpoch(); got != 1 {
		t.Errorf("expected epoch 1, got %d", got)
	}
}

// TestEpochZero_UnknownJob: epoch 0 for an unknown job is refused too (no
// RELEASED), and neither a job nor an operation is created.
func TestEpochZero_UnknownJob(t *testing.T) {
	sm := statemachine.NewStateManager()
	for _, intent := range []statemachine.OpType{statemachine.OpTypeSuspend, statemachine.OpTypeResume} {
		_, err := sm.StartGuestOp("unknown-job", intent, 0, future(), mustNotRun(t))
		requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	}
	if n := operationCount(sm); n != 0 {
		t.Errorf("epoch 0 refusal created %d operations", n)
	}
	if n := len(sm.GetJobStatus()); n != 0 {
		t.Errorf("epoch 0 refusal created %d jobs", n)
	}
}

// TestEpochZero_KillUnaffected: Kill has no epoch and still works.
func TestEpochZero_KillUnaffected(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(statemachine.GuestResult{})
	close(stub.release)
	opID := startKill(t, sm, stub.kill)
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_KILLED)
	checkKilled(t, sm)
}

// TestEpochZero_SeedZeroIsNoop: SeedEpoch(job, 0) never changes the epoch.
func TestEpochZero_SeedZeroIsNoop(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	sm.SeedEpoch(guestJob, 0)
	if got := jobStatus(t, sm).GetEpoch(); got != 0 {
		t.Errorf("SeedEpoch(0) on a fresh job: got epoch %d, want 0", got)
	}
	sm.SeedEpoch(guestJob, 3)
	sm.SeedEpoch(guestJob, 0)
	if got := jobStatus(t, sm).GetEpoch(); got != 3 {
		t.Errorf("SeedEpoch(0) after 3: got epoch %d, want 3", got)
	}
}
