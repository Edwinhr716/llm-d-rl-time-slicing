package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

func TestValidateForegroundWait(t *testing.T) {
	tests := []struct {
		mode    string
		wantErr bool
	}{
		{mode: ForegroundWaitBlocking},
		{mode: ForegroundWaitAsync, wantErr: true},
		{mode: "", wantErr: true},
		{mode: "other", wantErr: true},
	}
	for _, tc := range tests {
		if err := ValidateForegroundWait(tc.mode); (err != nil) != tc.wantErr {
			t.Errorf("ValidateForegroundWait(%q) = %v, want error %v", tc.mode, err, tc.wantErr)
		}
	}
}

func TestController_WaitForOperation_ForegroundOpTimeout(t *testing.T) {
	pending := func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
	}

	t.Run("bounded", func(t *testing.T) {
		c := &Controller{
			agentStore:          &MockSnapshotAgentStore{OperationFunc: pending},
			ForegroundOpTimeout: 300 * time.Millisecond,
		}
		start := time.Now()
		err := c.waitForOperation(context.Background(), "test-group", "test-job", "node-1", "op-1", "restore")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitForOperation() = %v, want an error wrapping context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("waitForOperation took %v, want it bounded near 300ms", elapsed)
		}
	})

	t.Run("zero keeps the caller's context", func(t *testing.T) {
		c := &Controller{agentStore: &MockSnapshotAgentStore{OperationFunc: pending}}
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := c.waitForOperation(ctx, "test-group", "test-job", "node-1", "op-1", "restore")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitForOperation() = %v, want an error wrapping context.DeadlineExceeded", err)
		}
		// With no bound it polls until the caller's deadline, past the first
		// 1 s poll.
		if elapsed := time.Since(start); elapsed < time.Second {
			t.Errorf("waitForOperation returned after %v, want it to run until the caller's deadline", elapsed)
		}
	})
}
