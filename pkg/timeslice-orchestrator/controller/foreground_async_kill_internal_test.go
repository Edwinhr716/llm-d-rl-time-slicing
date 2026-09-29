package controller

import (
	"context"
	"testing"
	"time"
)

// The kill path (ORCH-A4, kill.go) with foreground wait option B
// (async-requeue, D-ORCH-1): kills run ahead of the node pass, so a foreground
// operation in flight neither delays nor repeats them, and a kill does not
// drop the in-flight record.
func TestForegroundWait_Async_KillHungHostAtTWithOpInFlight(t *testing.T) {
	window, budget := 400*time.Millisecond, 100*time.Millisecond
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, window, budget)
	fx.ctrl.ForegroundWait = ForegroundWaitAsyncRequeue
	if err := fx.ctrl.startForegroundOp(context.Background(), killGroup, "trainer", killNode, "op-r", "restore"); err == nil {
		t.Fatal("startForegroundOp returned nil, want errForegroundPending")
	}

	cleared := fx.passUntilClear(t, 2*time.Second)
	if cleared.IsZero() {
		t.Fatal("host never cleared")
	}
	calls := fx.killCalls()
	if len(calls) != 1 || calls[0].reason != killReasonDeadline || calls[0].job != killGuest {
		t.Fatalf("Kill calls = %+v, want one deadline kill of %s", calls, killGuest)
	}
	if calls[0].at.Before(noticeAt.Add(window - budget)) {
		t.Errorf("Kill sent at %v, before T", calls[0].at.Sub(noticeAt))
	}
	if cleared.After(noticeAt.Add(window + 200*time.Millisecond)) {
		t.Errorf("host cleared at notice + %v, want by N (%v)", cleared.Sub(noticeAt), window)
	}
	if op := fx.ctrl.getForegroundOp(killGroup, killNode); op == nil || op.operationID != "op-r" {
		t.Errorf("in-flight record = %+v after the kill, want op-r kept", op)
	}
	if !fx.ctrl.foregroundOpsInFlight(killGroup) {
		t.Error("foregroundOpsInFlight = false with a record")
	}
}
