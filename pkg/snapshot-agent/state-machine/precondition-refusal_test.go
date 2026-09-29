package statemachine_test

import (
	"errors"
	"strings"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// Tests for the job state after a Suspend or Resume fails a precondition:
// the job is FAULTED, as for any other failure, and only Kill clears it.

var preconditionReasons = []pb.ErrorReason{
	pb.ErrorReason_PRECONDITION_READINESS,
	pb.ErrorReason_PRECONDITION_PROBES,
	pb.ErrorReason_PRECONDITION_MEMORY,
	pb.ErrorReason_PRECONDITION_NODE,
}

// controlReasons are the other failure reasons; they leave the job FAULTED
// too.
var controlReasons = []pb.ErrorReason{
	pb.ErrorReason_BACKEND_ERROR,
	pb.ErrorReason_VERIFY_FAILED,
	pb.ErrorReason_DEADLINE_EXCEEDED,
}

// preconditionStart is a job state and the guest call made from it.
type preconditionStart struct {
	state   pb.JobState
	outcome pb.Outcome
	intent  statemachine.OpType
}

var preconditionStarts = []preconditionStart{
	{pb.JobState_JOB_STATE_RUNNING, pb.Outcome_OUTCOME_RESUMED, statemachine.OpTypeSuspend},
	{pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_UNSPECIFIED, statemachine.OpTypeSuspend},
	{pb.JobState_JOB_STATE_SAVED, pb.Outcome_OUTCOME_UNSPECIFIED, statemachine.OpTypeSuspend},
	{pb.JobState_JOB_STATE_SAVED, pb.Outcome_OUTCOME_UNSPECIFIED, statemachine.OpTypeResume},
	{pb.JobState_JOB_STATE_SUSPENDED, pb.Outcome_OUTCOME_SUSPENDED, statemachine.OpTypeResume},
	{pb.JobState_JOB_STATE_SUSPENDED, pb.Outcome_OUTCOME_SUSPENDED, statemachine.OpTypeSuspend},
}

func (s preconditionStart) String() string {
	return strings.TrimPrefix(s.state.String(), "JOB_STATE_") + "/" + string(s.intent)
}

// newPreconditionSM returns a StateManager with guestJob in start, holding
// known byte counts.
func newPreconditionSM(t *testing.T, start preconditionStart) *statemachine.StateManager {
	t.Helper()
	sm := statemachine.NewStateManager()
	setJob(t, sm, start.state, start.outcome)
	sm.InternalMu().Lock()
	job := sm.InternalJobs()[guestJob]
	job.DeviceBytes = 7
	job.HostBytesPinned = 11
	sm.InternalMu().Unlock()
	return sm
}

// failWith returns a worker that fails with reason before touching the
// guest, the way the pipelines report a failed check.
func failWith(reason pb.ErrorReason) statemachine.GuestWorker {
	return instant(statemachine.GuestResult{}, statemachine.NewOpError(reason, errors.New("check failed")))
}

// runRefusal fails a guest call from start with reason and checks that the
// operation failed with it and the job is FAULTED with its other fields
// unchanged. It returns the operation ID.
func runRefusal(t *testing.T, sm *statemachine.StateManager, start preconditionStart, reason pb.ErrorReason) string {
	t.Helper()
	opID := startGuest(t, sm, start.intent, 1, failWith(reason))
	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, reason)

	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_FAULTED {
		t.Errorf("state after %s: %s, want FAULTED", reason, st.GetState())
	}
	if st.GetLastOutcome() != start.outcome || st.GetDeviceBytes() != 7 || st.GetHostBytesPinned() != 11 {
		t.Errorf("a failed operation changed the job: outcome %s device %d host %d",
			st.GetLastOutcome(), st.GetDeviceBytes(), st.GetHostBytesPinned())
	}
	sm.InternalMu().RLock()
	pids := sm.InternalJobs()[guestJob].PIDs
	sm.InternalMu().RUnlock()
	if len(pids) != 1 || pids[0] != 42 {
		t.Errorf("a failed operation changed the PIDs: %v", pids)
	}
	return opID
}

// Every precondition reason from every start state leaves the job FAULTED.
func TestPreconditionRefusal_LeavesFaulted(t *testing.T) {
	for _, start := range preconditionStarts {
		for _, reason := range preconditionReasons {
			t.Run(start.String()+"/"+reason.String(), func(t *testing.T) {
				runRefusal(t, newPreconditionSM(t, start), start, reason)
			})
		}
	}
}

// Controls: the other failure reasons leave the job FAULTED the same way.
func TestPreconditionRefusal_ControlsFaulted(t *testing.T) {
	for _, start := range preconditionStarts {
		for _, reason := range controlReasons {
			t.Run(start.String()+"/"+reason.String(), func(t *testing.T) {
				runRefusal(t, newPreconditionSM(t, start), start, reason)
			})
		}
	}
}

// Kill after a precondition failure ends IDLE with OUTCOME_KILLED.
func TestPreconditionRefusal_KillClears(t *testing.T) {
	for _, start := range preconditionStarts {
		t.Run(start.String(), func(t *testing.T) {
			sm := newPreconditionSM(t, start)
			runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_READINESS)
			stub := newGuestStub(statemachine.GuestResult{})
			close(stub.release)
			opID := startKill(t, sm, stub.kill)
			checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_KILLED)
			checkKilled(t, sm)
		})
	}
}

// A retry is refused for the FAULTED state; the same epoch still returns
// the failed operation.
func TestPreconditionRefusal_RetryRefused(t *testing.T) {
	start := preconditionStarts[0]
	sm := newPreconditionSM(t, start)
	opID := runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_READINESS)

	sameID, err := sm.StartGuestOp(guestJob, start.intent, 1, future(), mustNotRun(t))
	if err != nil || sameID != opID {
		t.Errorf("same-epoch retry: got %q, %v; want the failed operation %q", sameID, err, opID)
	}
	_, err = sm.StartGuestOp(guestJob, start.intent, 2, future(), mustNotRun(t))
	if err == nil {
		t.Error("a retry of a FAULTED job must be refused")
	}
}
