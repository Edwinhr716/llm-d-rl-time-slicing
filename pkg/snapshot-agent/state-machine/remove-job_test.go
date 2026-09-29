package statemachine_test

import (
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

func jobKnown(sm *statemachine.StateManager) bool {
	for _, st := range sm.GetJobStatus() {
		if st.GetJobId() == guestJob {
			return true
		}
	}
	return false
}

func TestRemoveJob(t *testing.T) {
	t.Run("clears a FAULTED job; a new pod starts clean", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_FAULTED)
		sm.SeedEpoch(guestJob, 9)
		sm.RemoveJob(guestJob)
		if jobKnown(sm) {
			t.Fatal("job still known after RemoveJob")
		}
		sm.RegisterJob(guestJob, guestGroup)
		st := jobStatus(t, sm)
		if st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetEpoch() != 0 ||
			st.GetLastOutcome() != pb.Outcome_OUTCOME_UNSPECIFIED {
			t.Fatalf("re-registered job is not clean: %v", st)
		}
	})

	t.Run("aborts a running suspend; its worker writes nothing", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(suspendedResult)
		opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
		ctx := waitStarted(t, stub)

		sm.RemoveJob(guestJob)
		if ctx.Err() == nil {
			t.Error("the running suspend's context was not cancelled")
		}
		checkFailed(t, getOp(t, sm, opID), pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

		close(stub.release)
		waitReturned(t, stub)
		if jobKnown(sm) {
			t.Fatal("a returning worker re-created the removed job")
		}
		checkFailed(t, getOp(t, sm, opID), pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})

	t.Run("a running kill fails unconfirmed", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(statemachine.GuestResult{})
		killID := startKill(t, sm, stub.kill)
		waitStarted(t, stub)

		sm.RemoveJob(guestJob)
		close(stub.release)
		waitReturned(t, stub)
		checkFailed(t, getOp(t, sm, killID), pb.ErrorReason_KILL_UNCONFIRMED)
		if jobKnown(sm) {
			t.Fatal("job still known after RemoveJob")
		}
	})

	t.Run("unknown job is ignored", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		sm.RemoveJob("nope")
	})
}
