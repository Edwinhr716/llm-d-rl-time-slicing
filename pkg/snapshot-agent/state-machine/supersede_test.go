package statemachine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// wantKillSupersedeReason is the ErrorReason of an operation superseded by
// a Kill.
const wantKillSupersedeReason = pb.ErrorReason_ERROR_REASON_UNSPECIFIED

const supersedeLogMsg = "Kill superseded the running operation"

// lockedBuffer is an io.Writer safe for concurrent use.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs sends the default slog logger to a buffer for the rest of
// the test.
func captureLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	logs := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// checkSupersedeLog checks that the Kill logged one supersede line for
// opID with the operation's type and reason.
func checkSupersedeLog(t *testing.T, logs *lockedBuffer, opID string, opType statemachine.OpType) {
	t.Helper()
	found := 0
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		if rec["msg"] != supersedeLogMsg || rec["supersededOp"] != opID {
			continue
		}
		found++
		if rec["supersededType"] != string(opType) {
			t.Errorf("supersededType = %v, want %s", rec["supersededType"], opType)
		}
		if rec["supersededReason"] != wantKillSupersedeReason.String() {
			t.Errorf("supersededReason = %v, want %s", rec["supersededReason"], wantKillSupersedeReason)
		}
	}
	if found != 1 {
		t.Errorf("found %d supersede log lines for %s, want 1; logs:\n%s", found, opID, logs.String())
	}
}

// checkSuperseded checks the superseded operation: FAILED with the Kill
// supersede reason, and an error text that keeps its prefix.
func checkSuperseded(t *testing.T, sm *statemachine.StateManager, opID string) {
	t.Helper()
	op := getOp(t, sm, opID)
	checkFailed(t, op, wantKillSupersedeReason)
	if op.Error != "superseded by Kill: T reached" {
		t.Errorf("unexpected supersede message %q", op.Error)
	}
}

// supersedeGuest runs a guest operation from state, supersedes it with a
// Kill and checks the result.
func supersedeGuest(t *testing.T, state pb.JobState, intent statemachine.OpType, res statemachine.GuestResult) {
	t.Helper()
	logs := captureLogs(t)
	sm := newGuestSM(t, state)
	opStub := newGuestStub(res)
	killStub := newGuestStub(statemachine.GuestResult{})

	opID := startGuest(t, sm, intent, 1, opStub.run)
	opCtx := waitStarted(t, opStub)
	killID := startKill(t, sm, killStub.kill)
	waitStarted(t, killStub)

	checkSuperseded(t, sm, opID)
	if !errors.Is(opCtx.Err(), context.Canceled) {
		t.Errorf("superseded operation's context: expected Canceled, got %v", opCtx.Err())
	}
	checkSupersedeLog(t, logs, opID, intent)

	// The superseded worker returns late and writes nothing.
	close(opStub.release)
	waitReturned(t, opStub)
	checkSuperseded(t, sm, opID)

	close(killStub.release)
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	checkKilled(t, sm)
}

// supersedeForeground runs a foreground Snapshot or Restore from state,
// supersedes it with a Kill and checks the result.
func supersedeForeground(
	t *testing.T, state pb.JobState, opType statemachine.OpType,
	start func(sm *statemachine.StateManager, worker func() error) (string, error),
) {
	t.Helper()
	logs := captureLogs(t)
	sm := newGuestSM(t, state)
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	opID, err := start(sm, func() error {
		close(started)
		<-release
		close(returned)
		return nil
	})
	if err != nil {
		t.Fatalf("%s: %v", opType, err)
	}
	<-started
	killID := startKill(t, sm, func(context.Context) error { return nil })
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	checkSuperseded(t, sm, opID)
	checkSupersedeLog(t, logs, opID, opType)

	// The superseded worker returns late and writes nothing.
	close(release)
	<-returned
	waitForOperation(t, sm, opID)
	checkSuperseded(t, sm, opID)
	checkKilled(t, sm)
}

func TestSupersede_Suspend(t *testing.T) {
	supersedeGuest(t, pb.JobState_JOB_STATE_RUNNING, statemachine.OpTypeSuspend, suspendedResult)
}

func TestSupersede_Resume(t *testing.T) {
	supersedeGuest(t, pb.JobState_JOB_STATE_SUSPENDED, statemachine.OpTypeResume, statemachine.GuestResult{})
}

func TestSupersede_Snapshot(t *testing.T) {
	supersedeForeground(t, pb.JobState_JOB_STATE_RUNNING, statemachine.OpTypeSnapshot,
		func(sm *statemachine.StateManager, worker func() error) (string, error) {
			return sm.StartSnapshot(guestJob, guestGroup, worker)
		})
}

func TestSupersede_Restore(t *testing.T) {
	supersedeForeground(t, pb.JobState_JOB_STATE_SAVED, statemachine.OpTypeRestore,
		func(sm *statemachine.StateManager, worker func() error) (string, error) {
			return sm.StartRestore(guestJob, guestGroup, worker)
		})
}

// TestSupersede_HigherEpochKeepsStaleEpoch pins the control case: a guest
// call with a higher epoch aborts the running operation with STALE_EPOCH,
// not with the Kill supersede reason, and logs no Kill supersede line.
func TestSupersede_HigherEpochKeepsStaleEpoch(t *testing.T) {
	logs := captureLogs(t)
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	suspendStub := newGuestStub(suspendedResult)
	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, suspendStub.run)
	waitStarted(t, suspendStub)

	resumeStub := newGuestStub(statemachine.GuestResult{})
	startGuest(t, sm, statemachine.OpTypeResume, 2, resumeStub.run)
	waitStarted(t, resumeStub)
	checkFailed(t, getOp(t, sm, suspendID), pb.ErrorReason_STALE_EPOCH)
	if strings.Contains(logs.String(), supersedeLogMsg) {
		t.Errorf("a higher-epoch abort logged a Kill supersede line:\n%s", logs.String())
	}
	close(suspendStub.release)
	close(resumeStub.release)
	waitReturned(t, suspendStub)
	waitReturned(t, resumeStub)
}
