package statemachine_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// Tests for PENDING LEAD DECISION D-AGENT-9 (--precondition-refusal). The
// collapse keeps the tests of the chosen option and deletes the others.

var preconditionReasons = []pb.ErrorReason{
	pb.ErrorReason_PRECONDITION_READINESS,
	pb.ErrorReason_PRECONDITION_PROBES,
	pb.ErrorReason_PRECONDITION_MEMORY,
	pb.ErrorReason_PRECONDITION_NODE,
}

// controlReasons leave the job FAULTED in both modes.
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

// newPreconditionSM returns a StateManager in mode with guestJob in start,
// holding known byte counts.
func newPreconditionSM(t *testing.T, mode string, start preconditionStart) *statemachine.StateManager {
	t.Helper()
	sm := statemachine.NewStateManager(statemachine.WithPreconditionRefusal(mode))
	setJob(t, sm, start.state, start.outcome)
	sm.InternalMu().Lock()
	job := sm.InternalJobs()[guestJob]
	job.DeviceBytes = 7
	job.HostBytesPinned = 11
	sm.InternalMu().Unlock()
	return sm
}

// failWith returns a worker that fails with reason before touching the
// guest, the way the AGENT-A2 pipelines report a failed check.
func failWith(reason pb.ErrorReason) statemachine.GuestWorker {
	return instant(statemachine.GuestResult{}, statemachine.NewOpError(reason, errors.New("check failed")))
}

// runRefusal fails a guest call from start with reason and checks the
// operation and the job's state afterwards. It returns the operation ID.
func runRefusal(
	t *testing.T, sm *statemachine.StateManager, start preconditionStart, reason pb.ErrorReason, wantState pb.JobState,
) string {
	t.Helper()
	opID := startGuest(t, sm, start.intent, 1, failWith(reason))
	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, reason)

	st := jobStatus(t, sm)
	if st.GetState() != wantState {
		t.Errorf("state after %s: %s, want %s", reason, st.GetState(), wantState)
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

func TestPreconditionRefusal_Values(t *testing.T) {
	for _, mode := range []string{statemachine.PreconditionRefusalFaulted, statemachine.PreconditionRefusalUnchanged} {
		if !statemachine.ValidPreconditionRefusal(mode) {
			t.Errorf("ValidPreconditionRefusal(%q) = false", mode)
		}
	}
	for _, mode := range []string{"", "Faulted", "keep", "unchanged "} {
		if statemachine.ValidPreconditionRefusal(mode) {
			t.Errorf("ValidPreconditionRefusal(%q) = true", mode)
		}
	}
}

// TestPreconditionRefusal_Faulted_LeavesFaulted: every precondition reason
// from every start state leaves the job FAULTED, with the default, an
// explicit "faulted" and an unknown value.
func TestPreconditionRefusal_Faulted_LeavesFaulted(t *testing.T) {
	for _, mode := range []string{statemachine.PreconditionRefusalFaulted, "bogus"} {
		for _, start := range preconditionStarts {
			for _, reason := range preconditionReasons {
				t.Run(mode+"/"+start.String()+"/"+reason.String(), func(t *testing.T) {
					sm := newPreconditionSM(t, mode, start)
					runRefusal(t, sm, start, reason, pb.JobState_JOB_STATE_FAULTED)
				})
			}
		}
	}
	t.Run("default", func(t *testing.T) {
		start := preconditionStarts[0]
		sm := newGuestSM(t, start.state)
		opID := startGuest(t, sm, start.intent, 1, failWith(pb.ErrorReason_PRECONDITION_NODE))
		checkFailed(t, waitForOperation(t, sm, opID), pb.ErrorReason_PRECONDITION_NODE)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_FAULTED)
	})
}

// TestPreconditionRefusal_Unchanged_KeepsState: every precondition reason
// returns the job to its start state; the operation still fails with it.
func TestPreconditionRefusal_Unchanged_KeepsState(t *testing.T) {
	for _, start := range preconditionStarts {
		for _, reason := range preconditionReasons {
			t.Run(start.String()+"/"+reason.String(), func(t *testing.T) {
				sm := newPreconditionSM(t, statemachine.PreconditionRefusalUnchanged, start)
				runRefusal(t, sm, start, reason, start.state)
			})
		}
	}
}

func testControlsFaulted(t *testing.T, mode string) {
	t.Helper()
	for _, start := range preconditionStarts {
		for _, reason := range controlReasons {
			t.Run(start.String()+"/"+reason.String(), func(t *testing.T) {
				sm := newPreconditionSM(t, mode, start)
				runRefusal(t, sm, start, reason, pb.JobState_JOB_STATE_FAULTED)
			})
		}
	}
}

// Controls: other failure reasons leave the job FAULTED in both modes.
func TestPreconditionRefusal_Faulted_ControlsFaulted(t *testing.T) {
	testControlsFaulted(t, statemachine.PreconditionRefusalFaulted)
}

func TestPreconditionRefusal_Unchanged_ControlsFaulted(t *testing.T) {
	testControlsFaulted(t, statemachine.PreconditionRefusalUnchanged)
}

// Kill after a precondition failure ends IDLE with OUTCOME_KILLED.
func testKillAfterRefusal(t *testing.T, mode string, wantState func(preconditionStart) pb.JobState) {
	t.Helper()
	for _, start := range preconditionStarts {
		t.Run(start.String(), func(t *testing.T) {
			sm := newPreconditionSM(t, mode, start)
			runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_READINESS, wantState(start))
			stub := newGuestStub(statemachine.GuestResult{})
			close(stub.release)
			opID := startKill(t, sm, stub.kill)
			checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_KILLED)
			checkKilled(t, sm)
		})
	}
}

func TestPreconditionRefusal_Faulted_KillClears(t *testing.T) {
	testKillAfterRefusal(t, statemachine.PreconditionRefusalFaulted,
		func(preconditionStart) pb.JobState { return pb.JobState_JOB_STATE_FAULTED })
}

func TestPreconditionRefusal_Unchanged_KillClears(t *testing.T) {
	testKillAfterRefusal(t, statemachine.PreconditionRefusalUnchanged,
		func(s preconditionStart) pb.JobState { return s.state })
}

// Under "faulted", a retry is refused for the FAULTED state; the same
// epoch still returns the failed operation.
func TestPreconditionRefusal_Faulted_RetryRefused(t *testing.T) {
	start := preconditionStarts[0]
	sm := newPreconditionSM(t, statemachine.PreconditionRefusalFaulted, start)
	opID := runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_READINESS, pb.JobState_JOB_STATE_FAULTED)

	sameID, err := sm.StartGuestOp(guestJob, start.intent, 1, future(), mustNotRun(t))
	if err != nil || sameID != opID {
		t.Errorf("same-epoch retry: got %q, %v; want the failed operation %q", sameID, err, opID)
	}
	_, err = sm.StartGuestOp(guestJob, start.intent, 2, future(), mustNotRun(t))
	if err == nil {
		t.Error("a retry of a FAULTED job must be refused")
	}
}

// Under "unchanged", the same epoch returns the failed operation, and a
// retry with the next epoch runs and completes.
func TestPreconditionRefusal_Unchanged_RetryNextEpoch(t *testing.T) {
	for _, start := range preconditionStarts {
		t.Run(start.String(), func(t *testing.T) {
			sm := newPreconditionSM(t, statemachine.PreconditionRefusalUnchanged, start)
			opID := runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_READINESS, start.state)

			sameID, err := sm.StartGuestOp(guestJob, start.intent, 1, future(), mustNotRun(t))
			if err != nil || sameID != opID {
				t.Errorf("same-epoch retry: got %q, %v; want the failed operation %q", sameID, err, opID)
			}

			want := pb.Outcome_OUTCOME_RESUMED
			worker := instant(statemachine.GuestResult{}, nil)
			if start.intent == statemachine.OpTypeSuspend {
				want = pb.Outcome_OUTCOME_SUSPENDED
				worker = instant(suspendedResult, nil)
			}
			retryID := startGuest(t, sm, start.intent, 2, worker)
			checkComplete(t, waitForOperation(t, sm, retryID), want)
		})
	}
}

// Under "unchanged", a precondition failure of a call that preempted a
// running guest operation still leaves FAULTED: the aborted operation may
// have changed the guest.
func TestPreconditionRefusal_Unchanged_PreemptedLeavesFaulted(t *testing.T) {
	start := preconditionStarts[0]
	sm := newPreconditionSM(t, statemachine.PreconditionRefusalUnchanged, start)
	stub := newGuestStub(suspendedResult)
	startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
	waitStarted(t, stub)

	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 2, failWith(pb.ErrorReason_PRECONDITION_MEMORY))
	checkFailed(t, waitForOperation(t, sm, opID), pb.ErrorReason_PRECONDITION_MEMORY)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_FAULTED)
	close(stub.release)
}

// preconditionLogBuffer is a bytes.Buffer safe for concurrent use.
type preconditionLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *preconditionLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *preconditionLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testRefusalLog(t *testing.T, mode string, wantAfter pb.JobState) {
	t.Helper()
	logs := &preconditionLogBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	start := preconditionStarts[4] // SUSPENDED/Resume
	sm := newPreconditionSM(t, mode, start)
	runRefusal(t, sm, start, pb.ErrorReason_PRECONDITION_NODE, wantAfter)

	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "Guest operation failed") {
			continue
		}
		fields := map[string]any{}
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		for key, want := range map[string]any{
			"reason":              float64(pb.ErrorReason_PRECONDITION_NODE),
			"stateBefore":         float64(start.state),
			"stateAfter":          float64(wantAfter),
			"preconditionRefusal": mode,
		} {
			if fields[key] != want {
				t.Errorf("log field %s = %v, want %v", key, fields[key], want)
			}
		}
		return
	}
	t.Fatalf("no failure log line in %q", logs.String())
}

func TestPreconditionRefusal_Faulted_LogFields(t *testing.T) {
	testRefusalLog(t, statemachine.PreconditionRefusalFaulted, pb.JobState_JOB_STATE_FAULTED)
}

func TestPreconditionRefusal_Unchanged_LogFields(t *testing.T) {
	testRefusalLog(t, statemachine.PreconditionRefusalUnchanged, pb.JobState_JOB_STATE_SUSPENDED)
}
