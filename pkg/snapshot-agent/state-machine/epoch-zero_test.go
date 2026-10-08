package statemachine_test

import (
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

// Tests for PENDING LEAD DECISION D-AGENT-5 (--epoch-zero). The collapse
// keeps the tests of the chosen option and deletes the others.

func operationCount(sm *statemachine.StateManager) int {
	sm.InternalMu().RLock()
	defer sm.InternalMu().RUnlock()
	return len(sm.InternalOperations())
}

func TestEpochZero_Values(t *testing.T) {
	for _, mode := range []string{statemachine.EpochZeroAccept, statemachine.EpochZeroRequire} {
		if !statemachine.ValidEpochZero(mode) {
			t.Errorf("ValidEpochZero(%q) = false", mode)
		}
	}
	for _, mode := range []string{"", "Accept", "reject", "1"} {
		if statemachine.ValidEpochZero(mode) {
			t.Errorf("ValidEpochZero(%q) = true", mode)
		}
	}
}

// TestEpochZero_Accept_FencesZeroLikeAnyEpoch: the default and an explicit
// "accept" run a Suspend with epoch 0, and a different call with the same
// epoch 0 is STALE_EPOCH.
func TestEpochZero_Accept_FencesZeroLikeAnyEpoch(t *testing.T) {
	for name, opts := range map[string][]statemachine.Option{
		"default":  nil,
		"explicit": {statemachine.WithEpochZero(statemachine.EpochZeroAccept)},
		"unknown":  {statemachine.WithEpochZero("bogus")},
	} {
		t.Run(name, func(t *testing.T) {
			sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, opts...)
			opID := startGuest(t, sm, statemachine.OpTypeSuspend, 0, instant(suspendedResult, nil))
			checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)

			_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 0, future(), mustNotRun(t))
			requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
			checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
		})
	}
}

// TestEpochZero_Accept_BelowSeedIsStale: with the mirror seeded, epoch 0 is
// refused as STALE_EPOCH by fencing.
func TestEpochZero_Accept_BelowSeedIsStale(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, statemachine.WithEpochZero(statemachine.EpochZeroAccept))
	sm.SeedEpoch(guestJob, 2)
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	if got := jobStatus(t, sm).GetEpoch(); got != 2 {
		t.Errorf("expected epoch 2, got %d", got)
	}
}

// TestEpochZero_Accept_UnknownJobReleased: a Suspend with epoch 0 of an
// unknown job completes at once with RELEASED, as for any epoch.
func TestEpochZero_Accept_UnknownJobReleased(t *testing.T) {
	sm := statemachine.NewStateManager(statemachine.WithEpochZero(statemachine.EpochZeroAccept))
	opID, err := sm.StartGuestOp("unknown-job", statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checkComplete(t, getOp(t, sm, opID), pb.Outcome_OUTCOME_RELEASED)
}

// TestEpochZero_Require_RefusesZero: Suspend and Resume with epoch 0 are
// InvalidArgument with no ErrorReason; the worker never runs, no operation
// is created and the seeded epoch is unchanged.
func TestEpochZero_Require_RefusesZero(t *testing.T) {
	for _, intent := range []statemachine.OpType{statemachine.OpTypeSuspend, statemachine.OpTypeResume} {
		t.Run(string(intent), func(t *testing.T) {
			sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED, statemachine.WithEpochZero(statemachine.EpochZeroRequire))
			sm.SeedEpoch(guestJob, 2)
			_, err := sm.StartGuestOp(guestJob, intent, 0, future(), mustNotRun(t))
			requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
			if got := jobStatus(t, sm).GetEpoch(); got != 2 {
				t.Errorf("epoch 0 refusal changed the epoch: got %d, want 2", got)
			}
			if n := operationCount(sm); n != 0 {
				t.Errorf("epoch 0 refusal created %d operations", n)
			}
			checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
		})
	}
}

// TestEpochZero_Require_RefusesZeroBeforeFencing: with no seed (last epoch
// 0), epoch 0 would pass fencing under "accept"; under "require" it is
// refused, and a later epoch 1 is still accepted.
func TestEpochZero_Require_RefusesZeroBeforeFencing(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, statemachine.WithEpochZero(statemachine.EpochZeroRequire))
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	if got := jobStatus(t, sm).GetEpoch(); got != 1 {
		t.Errorf("expected epoch 1, got %d", got)
	}
}

// TestEpochZero_Require_UnknownJob: epoch 0 for an unknown job is refused
// too (no RELEASED), and neither a job nor an operation is created.
func TestEpochZero_Require_UnknownJob(t *testing.T) {
	sm := statemachine.NewStateManager(statemachine.WithEpochZero(statemachine.EpochZeroRequire))
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

// TestEpochZero_Require_KillUnaffected: Kill has no epoch and still works.
func TestEpochZero_Require_KillUnaffected(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, statemachine.WithEpochZero(statemachine.EpochZeroRequire))
	stub := newGuestStub(statemachine.GuestResult{})
	close(stub.release)
	opID := startKill(t, sm, stub.kill)
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_KILLED)
	checkKilled(t, sm)
}

// SeedEpoch(job, 0) is a no-op under both options.
func testSeedZeroIsNoop(t *testing.T, mode string) {
	t.Helper()
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, statemachine.WithEpochZero(mode))
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

func TestEpochZero_Accept_SeedZeroIsNoop(t *testing.T) {
	testSeedZeroIsNoop(t, statemachine.EpochZeroAccept)
}

func TestEpochZero_Require_SeedZeroIsNoop(t *testing.T) {
	testSeedZeroIsNoop(t, statemachine.EpochZeroRequire)
}
