package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

func pendingOperation(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
	return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
}

// TestWaitForOperation_ForegroundOpTimeout bounds the whole wait for one
// snapshot or restore, even when every poll answers PENDING.
func TestWaitForOperation_ForegroundOpTimeout(t *testing.T) {
	ctrl := NewController(nil, nil, nil, nil, &MockSnapshotAgentStore{OperationFunc: pendingOperation})
	ctrl.ForegroundOpTimeout = 300 * time.Millisecond

	start := time.Now()
	err := ctrl.waitForOperation(context.Background(), "g", "job-1", "node-1", "op-1", "restore")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("waitForOperation returned nil for an operation that never finishes")
	}
	if !strings.Contains(err.Error(), "foreground operation timeout 300ms") {
		t.Errorf("error = %v, want it to name the foreground operation timeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed < 300*time.Millisecond || elapsed > 900*time.Millisecond {
		t.Errorf("returned after %v, want about 300ms", elapsed)
	}
}

// TestWaitForOperation_CallerCancelIsNotAForegroundTimeout keeps the two
// failures apart in the error text.
func TestWaitForOperation_CallerCancelIsNotAForegroundTimeout(t *testing.T) {
	ctrl := NewController(nil, nil, nil, nil, &MockSnapshotAgentStore{OperationFunc: pendingOperation})
	ctrl.ForegroundOpTimeout = time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := ctrl.waitForOperation(ctx, "g", "job-1", "node-1", "op-1", "restore")
	if err == nil {
		t.Fatal("waitForOperation returned nil after the caller's context ended")
	}
	if strings.Contains(err.Error(), "foreground operation timeout") {
		t.Errorf("error = %v, want the caller's deadline, not the foreground timeout", err)
	}
}

// operationDoneAfter reports PENDING until d has passed, then COMPLETE.
func operationDoneAfter(d time.Duration) func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
	start := time.Now()
	return func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		if time.Since(start) < d {
			return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
		}
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
	}
}

// TestWaitForKillOperation_PollsAtKillInterval: a kill that finishes at 250 ms
// is seen within one 100 ms poll when KillPollInterval is set to 100 ms, where
// the 1 s snapshot poll would take 1 s.
func TestWaitForKillOperation_PollsAtKillInterval(t *testing.T) {
	ctrl := NewController(nil, nil, nil, nil, &MockSnapshotAgentStore{OperationFunc: operationDoneAfter(250 * time.Millisecond)})
	if ctrl.KillPollInterval != time.Second {
		t.Fatalf("default KillPollInterval = %v, want 1s", ctrl.KillPollInterval)
	}
	ctrl.KillPollInterval = 100 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := ctrl.WaitForKillOperation(ctx, "g", "guest", "node-1", "op-kill"); err != nil {
		t.Fatalf("WaitForKillOperation: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond || elapsed >= 600*time.Millisecond {
		t.Errorf("WaitForKillOperation returned after %v, want within one 100ms poll of 250ms", elapsed)
	}

	// Control: the snapshot/restore wait polls every second.
	ctrl = NewController(nil, nil, nil, nil, &MockSnapshotAgentStore{OperationFunc: operationDoneAfter(250 * time.Millisecond)})
	start = time.Now()
	if err := ctrl.waitForOperation(ctx, "g", "guest", "node-1", "op-snap", "snapshot"); err != nil {
		t.Fatalf("waitForOperation: %v", err)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("waitForOperation returned after %v, want at least the 1s poll", elapsed)
	}
}
