package statemachine_test

import (
	"context"
	"strings"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Tests for PENDING LEAD DECISION D-AGENT-4 (--unknown-job-suspend).
// Released tests cover UnknownJobSuspendReleased (default), Precondition
// tests cover UnknownJobSuspendPrecondition.

func TestUnknownJob_Validate(t *testing.T) {
	for _, ok := range []string{statemachine.UnknownJobSuspendReleased, statemachine.UnknownJobSuspendPrecondition} {
		if err := statemachine.ValidateUnknownJobSuspend(ok); err != nil {
			t.Errorf("%q: unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Released", "failed-precondition", "released "} {
		if err := statemachine.ValidateUnknownJobSuspend(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func checkNoJobs(t *testing.T, sm *statemachine.StateManager) {
	t.Helper()
	if n := len(sm.GetJobStatus()); n != 0 {
		t.Errorf("a call for an unknown job created %d jobs", n)
	}
}

func TestUnknownJob_Released_DefaultAndExplicit(t *testing.T) {
	for name, opts := range map[string][]statemachine.Option{
		"default":  nil,
		"explicit": {statemachine.WithUnknownJobSuspend(statemachine.UnknownJobSuspendReleased)},
	} {
		t.Run(name, func(t *testing.T) {
			sm := statemachine.NewStateManager(opts...)
			opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			op := getOp(t, sm, opID)
			checkComplete(t, op, pb.Outcome_OUTCOME_RELEASED)
			if op.Type != statemachine.OpTypeSuspend || op.Epoch != 7 || op.JobID != guestJob {
				t.Errorf("unexpected operation: %+v", op)
			}
			checkNoJobs(t, sm)
		})
	}
}

func TestUnknownJob_Precondition_RefusesSuspend(t *testing.T) {
	sm := statemachine.NewStateManager(
		statemachine.WithUnknownJobSuspend(statemachine.UnknownJobSuspendPrecondition))
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "job unknown") {
		t.Errorf("refusal message %q does not contain %q", msg, "job unknown")
	}
	if n := len(sm.InternalOperations()); n != 0 {
		t.Errorf("the refusal created %d operations", n)
	}
	checkNoJobs(t, sm)
}

func TestUnknownJob_Precondition_ResumeAndKillUnchanged(t *testing.T) {
	sm := statemachine.NewStateManager(
		statemachine.WithUnknownJobSuspend(statemachine.UnknownJobSuspendPrecondition))

	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 7, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	killID, err := sm.StartKill(guestJob, future(), "unknown", func(context.Context) error {
		t.Error("kill worker must not run for an unknown job")
		return nil
	})
	if err != nil {
		t.Fatalf("Kill of an unknown job: unexpected error: %v", err)
	}
	checkComplete(t, getOp(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	checkNoJobs(t, sm)
}

// TestUnknownJob_Precondition_AcceptedOnceKnown is the agent-restart case:
// the Suspend is refused until the watcher registers the job, then the same
// call (same epoch) runs.
func TestUnknownJob_Precondition_AcceptedOnceKnown(t *testing.T) {
	sm := statemachine.NewStateManager(
		statemachine.WithUnknownJobSuspend(statemachine.UnknownJobSuspendPrecondition))
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

// TestUnknownJob_Precondition_KnownJobUnchanged checks that the policy only
// applies to unknown jobs: a known IDLE job whose last outcome is KILLED
// still completes a Suspend with RELEASED.
func TestUnknownJob_Precondition_KnownJobUnchanged(t *testing.T) {
	sm := statemachine.NewStateManager(
		statemachine.WithUnknownJobSuspend(statemachine.UnknownJobSuspendPrecondition))
	setJob(t, sm, pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_KILLED)
	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, mustNotRun(t))
	checkComplete(t, getOp(t, sm, opID), pb.Outcome_OUTCOME_RELEASED)
}
