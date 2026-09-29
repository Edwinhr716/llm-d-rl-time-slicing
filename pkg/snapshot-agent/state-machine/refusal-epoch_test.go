package statemachine_test

import (
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

// Only a Suspend or Resume that is accepted stores its epoch; a call that
// passes epoch fencing and is then refused leaves the stored epoch as it
// was. These tests pin that rule for every kind of refusal after fencing,
// and check that the caller can retry the same epoch once the cause of the
// refusal is gone.

// refusalCase sets up a job so that a guest call passes fencing and is then
// refused, and can later clear the cause so the same call is accepted.
type refusalCase struct {
	code   codes.Code
	reason pb.ErrorReason
	// setup puts the job in the refusing situation.
	setup func(t *testing.T, sm *statemachine.StateManager)
	// call issues the guest call; refusing selects the refused form.
	call func(t *testing.T, sm *statemachine.StateManager, epoch int64, refusing bool) (string, error)
	// fix clears the cause of the refusal.
	fix func(t *testing.T, sm *statemachine.StateManager)
	// accepted is the outcome of the accepted retry.
	accepted pb.Outcome
}

func refusalCases() map[string]func() refusalCase {
	return map[string]func() refusalCase{
		// A Resume of an IDLE job is refused for its state.
		"State": func() refusalCase {
			return refusalCase{
				code:   codes.FailedPrecondition,
				reason: pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
				setup: func(t *testing.T, sm *statemachine.StateManager) {
					t.Helper()
					setJob(t, sm, pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_UNSPECIFIED)
				},
				call: func(_ *testing.T, sm *statemachine.StateManager, epoch int64, _ bool) (string, error) {
					return sm.StartGuestOp(guestJob, statemachine.OpTypeResume, epoch, future(),
						instant(statemachine.GuestResult{}, nil))
				},
				fix: func(t *testing.T, sm *statemachine.StateManager) {
					t.Helper()
					setJob(t, sm, pb.JobState_JOB_STATE_SUSPENDED, pb.Outcome_OUTCOME_SUSPENDED)
				},
				accepted: pb.Outcome_OUTCOME_RESUMED,
			}
		},
		// A Suspend whose deadline has passed is DEADLINE_INFEASIBLE.
		"Deadline": func() refusalCase {
			return refusalCase{
				code:   codes.FailedPrecondition,
				reason: pb.ErrorReason_DEADLINE_INFEASIBLE,
				setup: func(t *testing.T, sm *statemachine.StateManager) {
					t.Helper()
					setJob(t, sm, pb.JobState_JOB_STATE_RUNNING, pb.Outcome_OUTCOME_UNSPECIFIED)
				},
				call: func(_ *testing.T, sm *statemachine.StateManager, epoch int64, refusing bool) (string, error) {
					deadline := future()
					if refusing {
						deadline = time.Now().Add(-time.Second)
					}
					return sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, epoch, deadline,
						instant(suspendedResult, nil))
				},
				fix:      func(*testing.T, *statemachine.StateManager) {},
				accepted: pb.Outcome_OUTCOME_SUSPENDED,
			}
		},
		// A Suspend while a Kill runs is Aborted; after the Kill it is
		// answered RELEASED at once.
		"Aborted": func() refusalCase {
			stub := newGuestStub(statemachine.GuestResult{})
			var killID string
			return refusalCase{
				code:   codes.Aborted,
				reason: pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
				setup: func(t *testing.T, sm *statemachine.StateManager) {
					t.Helper()
					setJob(t, sm, pb.JobState_JOB_STATE_RUNNING, pb.Outcome_OUTCOME_UNSPECIFIED)
					killID = startKill(t, sm, stub.kill)
					waitStarted(t, stub)
				},
				call: func(_ *testing.T, sm *statemachine.StateManager, epoch int64, _ bool) (string, error) {
					return sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, epoch, future(),
						instant(suspendedResult, nil))
				},
				fix: func(t *testing.T, sm *statemachine.StateManager) {
					t.Helper()
					close(stub.release)
					checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
				},
				accepted: pb.Outcome_OUTCOME_RELEASED,
			}
		},
	}
}

// runRefusalCase: the job's epoch is seeded to 1; a call with epoch 5 passes
// fencing and is refused, which leaves epoch 1; the same call with epoch 3
// is refused for the same cause, not fenced; the cause is cleared and the
// call with epoch 5 is retried and accepted, which stores epoch 5.
func runRefusalCase(t *testing.T, kind string) {
	t.Helper()
	rc := refusalCases()[kind]()
	sm := statemachine.NewStateManager()
	rc.setup(t, sm)
	sm.SeedEpoch(guestJob, 1)

	_, err := rc.call(t, sm, 5, true)
	requireRefusal(t, err, rc.code, rc.reason)
	if got := jobStatus(t, sm).GetEpoch(); got != 1 {
		t.Errorf("after a refused call with epoch 5: epoch %d, want 1", got)
	}

	// A lower call after the refusal passes fencing and is refused for the
	// same cause.
	_, err = rc.call(t, sm, 3, true)
	requireRefusal(t, err, rc.code, rc.reason)
	if got := jobStatus(t, sm).GetEpoch(); got != 1 {
		t.Errorf("after the lower call: epoch %d, want 1", got)
	}

	// The caller retries the same epoch once the cause is gone.
	rc.fix(t, sm)
	opID, err := rc.call(t, sm, 5, false)
	if err != nil {
		t.Fatalf("retry with epoch 5 after the refusal: %v", err)
	}
	checkComplete(t, waitForOperation(t, sm, opID), rc.accepted)
	if got := jobStatus(t, sm).GetEpoch(); got != 5 {
		t.Errorf("after the accepted retry: epoch %d, want 5", got)
	}
	_, err = rc.call(t, sm, 3, false)
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
}

func TestGuestOp_RefusalKeepsEpoch_State(t *testing.T) {
	runRefusalCase(t, "State")
}

func TestGuestOp_RefusalKeepsEpoch_Deadline(t *testing.T) {
	runRefusalCase(t, "Deadline")
}

func TestGuestOp_RefusalKeepsEpoch_Aborted(t *testing.T) {
	runRefusalCase(t, "Aborted")
}

// TestGuestOp_EpochAcceptedStaleAndSeed: accepted calls (an immediate
// answer, a started worker, a preemption) raise the epoch; STALE_EPOCH
// refusals never move it; SeedEpoch raises it.
func TestGuestOp_EpochAcceptedStaleAndSeed(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	checkEpoch := func(want int64) {
		t.Helper()
		if got := jobStatus(t, sm).GetEpoch(); got != want {
			t.Errorf("epoch %d, want %d", got, want)
		}
	}

	// Immediate answer: Resume of a RUNNING job.
	opID := startGuest(t, sm, statemachine.OpTypeResume, 2, mustNotRun(t))
	checkComplete(t, getOp(t, sm, opID), pb.Outcome_OUTCOME_RESUMED)
	checkEpoch(2)

	// Started worker, then preempted by a higher epoch.
	stub := newGuestStub(suspendedResult)
	startGuest(t, sm, statemachine.OpTypeSuspend, 3, stub.run)
	waitStarted(t, stub)
	checkEpoch(3)
	opID = startGuest(t, sm, statemachine.OpTypeSuspend, 4, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	checkEpoch(4)
	close(stub.release)

	// STALE_EPOCH: lower, and same epoch with a different call.
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 1, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 4, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	checkEpoch(4)

	// The watcher seed raises the epoch.
	sm.SeedEpoch(guestJob, 9)
	checkEpoch(9)
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 8, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
}
