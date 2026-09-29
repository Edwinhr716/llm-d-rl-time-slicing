package statemachine_test

import (
	"strings"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func checkNoJobs(t *testing.T, sm *statemachine.StateManager) {
	t.Helper()
	if n := len(sm.GetJobStatus()); n != 0 {
		t.Errorf("a call for an unknown job created %d jobs", n)
	}
}

func TestUnknownJob_RefusesSuspend(t *testing.T) {
	sm := statemachine.NewStateManager()
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "unknown to this agent") {
		t.Errorf("refusal message %q does not contain %q", msg, "unknown to this agent")
	}
	if n := len(sm.InternalOperations()); n != 0 {
		t.Errorf("the refusal created %d operations", n)
	}
	checkNoJobs(t, sm)
}

// TestUnknownJob_AcceptedOnceKnown is the agent-restart case: the Suspend is
// refused until the watcher registers the job, then the same call (same
// epoch) runs.
func TestUnknownJob_AcceptedOnceKnown(t *testing.T) {
	sm := statemachine.NewStateManager()
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	// The watcher relists the mirror pod and seeds the epoch from its
	// guest-epoch annotation.
	sm.RegisterJob(guestJob, guestGroup)
	if err := sm.TransitionToRunning(guestJob, []int{42}); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	sm.SeedEpoch(guestJob, 7)

	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 7, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
}
