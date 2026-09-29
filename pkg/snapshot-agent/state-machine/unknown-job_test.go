package statemachine_test

import (
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// TestUnknownJob_SuspendReleased checks the idempotent answer to a Suspend
// of a job the agent does not know (for example right after an agent
// restart): the call completes at once with RELEASED, the operation carries
// the call's type, job and epoch, and no job record is created.
func TestUnknownJob_SuspendReleased(t *testing.T) {
	sm := statemachine.NewStateManager()
	opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	op := getOp(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_RELEASED)
	if op.Type != statemachine.OpTypeSuspend || op.Epoch != 7 || op.JobID != guestJob {
		t.Errorf("unexpected operation: %+v", op)
	}
	if n := len(sm.GetJobStatus()); n != 0 {
		t.Errorf("a Suspend of an unknown job created %d jobs", n)
	}
}

// TestUnknownJob_SuspendRunsOnceKnown is the agent-restart case: once the
// watcher registers the job and seeds its epoch, the next Suspend runs the
// worker instead of answering RELEASED.
func TestUnknownJob_SuspendRunsOnceKnown(t *testing.T) {
	sm := statemachine.NewStateManager()
	opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checkComplete(t, getOp(t, sm, opID), pb.Outcome_OUTCOME_RELEASED)

	sm.RegisterJob(guestJob, guestGroup)
	if err := sm.TransitionToRunning(guestJob, []int{42}); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	sm.SeedEpoch(guestJob, 7)

	opID = startGuest(t, sm, statemachine.OpTypeSuspend, 8, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
}
