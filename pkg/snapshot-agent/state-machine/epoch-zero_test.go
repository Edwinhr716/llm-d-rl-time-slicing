package statemachine_test

import (
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

// Epoch 0, the proto default when a caller never sets the field, is fenced
// like any other epoch.

// TestEpochZero_FencedLikeAnyEpoch: a Suspend with epoch 0 runs, and a
// different call with the same epoch 0 is STALE_EPOCH.
func TestEpochZero_FencedLikeAnyEpoch(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 0, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)

	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 0, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
}

// TestEpochZero_BelowSeedIsStale: with the mirror seeded, epoch 0 is
// refused as STALE_EPOCH by fencing.
func TestEpochZero_BelowSeedIsStale(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	sm.SeedEpoch(guestJob, 2)
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	if got := jobStatus(t, sm).GetEpoch(); got != 2 {
		t.Errorf("expected epoch 2, got %d", got)
	}
}

// TestEpochZero_UnknownJobReleased: a Suspend with epoch 0 of an unknown
// job completes at once with RELEASED, as for any epoch.
func TestEpochZero_UnknownJobReleased(t *testing.T) {
	sm := statemachine.NewStateManager()
	opID, err := sm.StartGuestOp("unknown-job", statemachine.OpTypeSuspend, 0, future(), mustNotRun(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checkComplete(t, getOp(t, sm, opID), pb.Outcome_OUTCOME_RELEASED)
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
