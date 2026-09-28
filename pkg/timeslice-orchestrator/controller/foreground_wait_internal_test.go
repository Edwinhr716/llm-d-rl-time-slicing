package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
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
		{mode: ForegroundWaitAsyncPoll},
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

// captureLogs points the default slog logger at a JSON buffer for the rest of
// the test and returns a function that decodes the records written so far.
func captureLogs(t *testing.T) func() []map[string]any {
	t.Helper()
	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &buf}, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		var records []map[string]any
		dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
		for dec.More() {
			var rec map[string]any
			if err := dec.Decode(&rec); err != nil {
				t.Fatalf("decode log record: %v", err)
			}
			records = append(records, rec)
		}
		return records
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func findRecord(records []map[string]any, msg string) map[string]any {
	for _, rec := range records {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

func TestForegroundWait_Blocking_LogLines(t *testing.T) {
	status := func(s agentpb.OperationStatus) func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		return func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
			return &agentpb.GetOperationResponse{Status: s}, nil
		}
	}

	tests := []struct {
		name        string
		op          func(context.Context, string, string) (*agentpb.GetOperationResponse, error)
		timeout     time.Duration
		cancelAfter time.Duration
		opType      string
		wantOutcome string
	}{
		{
			name:        "complete",
			op:          status(agentpb.OperationStatus_OPERATION_STATUS_COMPLETE),
			opType:      "restore",
			wantOutcome: "complete",
		},
		{
			name:        "failed",
			op:          status(agentpb.OperationStatus_OPERATION_STATUS_FAILED),
			opType:      "snapshot",
			wantOutcome: "failed",
		},
		{
			name:        "timeout",
			op:          status(agentpb.OperationStatus_OPERATION_STATUS_PENDING),
			timeout:     300 * time.Millisecond,
			opType:      "restore",
			wantOutcome: "timeout",
		},
		{
			name:        "cancelled",
			op:          status(agentpb.OperationStatus_OPERATION_STATUS_PENDING),
			cancelAfter: 300 * time.Millisecond,
			opType:      "snapshot",
			wantOutcome: "cancelled",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			records := captureLogs(t)
			c := &Controller{
				agentStore:          &MockSnapshotAgentStore{OperationFunc: tc.op},
				ForegroundOpTimeout: tc.timeout,
			}
			ctx := context.Background()
			if tc.cancelAfter > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				timer := time.AfterFunc(tc.cancelAfter, cancel)
				defer timer.Stop()
				defer cancel()
			}
			err := c.waitForOperation(ctx, "g-1", "job-1", "node-1", "op-1", tc.opType)
			if (err == nil) != (tc.wantOutcome == "complete") {
				t.Fatalf("waitForOperation() = %v, want outcome %s", err, tc.wantOutcome)
			}

			got := records()
			for _, msg := range []string{"Foreground operation started", "Foreground operation finished"} {
				rec := findRecord(got, msg)
				if rec == nil {
					t.Fatalf("no %q record in %v", msg, got)
				}
				if rec["level"] != "INFO" {
					t.Errorf("%q level = %v, want INFO", msg, rec["level"])
				}
				want := map[string]any{"group": "g-1", "job": "job-1", "operation_id": "op-1", "type": tc.opType}
				for k, v := range want {
					if rec[k] != v {
						t.Errorf("%q %s = %v, want %v", msg, k, rec[k], v)
					}
				}
			}
			if rec := findRecord(got, "Foreground operation started"); rec["outcome"] != nil {
				t.Errorf("started record has outcome %v, want none", rec["outcome"])
			}
			if rec := findRecord(got, "Foreground operation finished"); rec["outcome"] != tc.wantOutcome {
				t.Errorf("finished outcome = %v, want %s", rec["outcome"], tc.wantOutcome)
			}
		})
	}
}
