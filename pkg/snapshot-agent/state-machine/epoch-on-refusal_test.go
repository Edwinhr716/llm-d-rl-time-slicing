package statemachine_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

// Tests for PENDING LEAD DECISION D-AGENT-6 (--epoch-on-refusal). The
// collapse keeps the tests of the chosen option and deletes the others.

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
// fencing and is refused; the same refused call is repeated with epoch 3;
// the cause is cleared and the call with epoch 5 is retried.
func runRefusalCase(t *testing.T, mode, kind string) {
	t.Helper()
	rc := refusalCases()[kind]()
	sm := statemachine.NewStateManager(statemachine.WithEpochOnRefusal(mode))
	rc.setup(t, sm)
	sm.SeedEpoch(guestJob, 1)

	_, err := rc.call(t, sm, 5, true)
	requireRefusal(t, err, rc.code, rc.reason)

	raise := mode == statemachine.EpochOnRefusalRaise
	wantEpoch := int64(1)
	if raise {
		wantEpoch = 5
	}
	if got := jobStatus(t, sm).GetEpoch(); got != wantEpoch {
		t.Errorf("after a refused call with epoch 5: epoch %d, want %d", got, wantEpoch)
	}

	// A lower call after the refusal: fenced only if the refusal raised the
	// epoch; otherwise it is refused for the same cause.
	_, err = rc.call(t, sm, 3, true)
	if raise {
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	} else {
		requireRefusal(t, err, rc.code, rc.reason)
		wantEpoch = 1
	}
	if got := jobStatus(t, sm).GetEpoch(); got != wantEpoch {
		t.Errorf("after the lower call: epoch %d, want %d", got, wantEpoch)
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

func TestEpochOnRefusal_Values(t *testing.T) {
	for _, mode := range []string{statemachine.EpochOnRefusalRaise, statemachine.EpochOnRefusalAcceptedOnly} {
		if !statemachine.ValidEpochOnRefusal(mode) {
			t.Errorf("ValidEpochOnRefusal(%q) = false", mode)
		}
	}
	for _, mode := range []string{"", "Raise", "accepted_only", "accepted"} {
		if statemachine.ValidEpochOnRefusal(mode) {
			t.Errorf("ValidEpochOnRefusal(%q) = true", mode)
		}
	}
}

func TestEpochOnRefusal_Raise_State(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalRaise, "State")
}

func TestEpochOnRefusal_Raise_Deadline(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalRaise, "Deadline")
}

func TestEpochOnRefusal_Raise_Aborted(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalRaise, "Aborted")
}

func TestEpochOnRefusal_AcceptedOnly_State(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalAcceptedOnly, "State")
}

func TestEpochOnRefusal_AcceptedOnly_Deadline(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalAcceptedOnly, "Deadline")
}

func TestEpochOnRefusal_AcceptedOnly_Aborted(t *testing.T) {
	runRefusalCase(t, statemachine.EpochOnRefusalAcceptedOnly, "Aborted")
}

// TestEpochOnRefusal_Raise_IsDefault: without the option, and with an
// unknown value, a refused call raises the epoch.
func TestEpochOnRefusal_Raise_IsDefault(t *testing.T) {
	for name, opts := range map[string][]statemachine.Option{
		"default": nil,
		"unknown": {statemachine.WithEpochOnRefusal("bogus")},
	} {
		t.Run(name, func(t *testing.T) {
			sm := newGuestSM(t, pb.JobState_JOB_STATE_IDLE, opts...)
			_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 4, future(), mustNotRun(t))
			requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
			if got := jobStatus(t, sm).GetEpoch(); got != 4 {
				t.Errorf("epoch %d, want 4", got)
			}
		})
	}
}

// Accepted calls (a started worker, an immediate answer, a preemption)
// raise the epoch; STALE_EPOCH refusals never move it; SeedEpoch raises it.
func testEpochAcceptedStaleSeed(t *testing.T, mode string) {
	t.Helper()
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING, statemachine.WithEpochOnRefusal(mode))
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

	// The watcher seed raises the epoch in both modes.
	sm.SeedEpoch(guestJob, 9)
	checkEpoch(9)
	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 8, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
}

func TestEpochOnRefusal_Raise_AcceptedStaleAndSeed(t *testing.T) {
	testEpochAcceptedStaleSeed(t, statemachine.EpochOnRefusalRaise)
}

func TestEpochOnRefusal_AcceptedOnly_AcceptedStaleAndSeed(t *testing.T) {
	testEpochAcceptedStaleSeed(t, statemachine.EpochOnRefusalAcceptedOnly)
}

// syncBuffer is a bytes.Buffer safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// refusalLog returns the fields of the first refusal log line.
func refusalLog(t *testing.T, logs string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "refused after epoch fencing") {
			continue
		}
		fields := map[string]any{}
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		return fields
	}
	t.Fatalf("no refusal log line in %q", logs)
	return nil
}

func testRefusalLogFields(t *testing.T, mode string, wantLast float64, wantRaised bool) {
	t.Helper()
	logs := &syncBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	sm := newGuestSM(t, pb.JobState_JOB_STATE_IDLE, statemachine.WithEpochOnRefusal(mode))
	sm.SeedEpoch(guestJob, 1)
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 5, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	fields := refusalLog(t, logs.String())
	if fields["epoch"] != float64(5) || fields["lastEpoch"] != wantLast || fields["epochRaised"] != wantRaised {
		t.Errorf("refusal log: epoch=%v lastEpoch=%v epochRaised=%v, want 5 %v %v",
			fields["epoch"], fields["lastEpoch"], fields["epochRaised"], wantLast, wantRaised)
	}
	if fields["epochOnRefusal"] != mode {
		t.Errorf("refusal log: epochOnRefusal=%v, want %s", fields["epochOnRefusal"], mode)
	}
}

func TestEpochOnRefusal_Raise_LogFields(t *testing.T) {
	testRefusalLogFields(t, statemachine.EpochOnRefusalRaise, 5, true)
}

func TestEpochOnRefusal_AcceptedOnly_LogFields(t *testing.T) {
	testRefusalLogFields(t, statemachine.EpochOnRefusalAcceptedOnly, 1, false)
}
