package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/accelerator-orchestrator/api/v1alpha1"
)

func TestGroup_GettersAndSetters(t *testing.T) {
	tests := []struct {
		name       string
		groupID    string
		nodes      []string
		lockingJob string
		activeJob  string
		state      pb.GroupStatus_State
	}{
		{
			name:       "empty/nil values",
			groupID:    "",
			nodes:      nil,
			lockingJob: "",
			activeJob:  "",
			state:      pb.GroupStatus_STATE_UNSPECIFIED,
		},
		{
			name:       "populated values",
			groupID:    "group-123",
			nodes:      []string{"node-a", "node-b"},
			lockingJob: "job-a",
			activeJob:  "job-b",
			state:      pb.GroupStatus_STATE_IDLE,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGroup(tc.groupID)
			g.SetNodes(tc.nodes)

			if g.ID() != tc.groupID {
				t.Errorf("ID() = %q, want %q", g.ID(), tc.groupID)
			}

			if !reflect.DeepEqual(g.Nodes(), tc.nodes) {
				t.Errorf("Nodes() = %+v, want %+v", g.Nodes(), tc.nodes)
			}

			// Verify deep copy/mutation protection for Nodes
			if len(tc.nodes) > 0 {
				mutatedNodes := g.Nodes()
				mutatedNodes[0] = "mutated"
				if reflect.DeepEqual(g.Nodes(), mutatedNodes) {
					t.Errorf("mutating returned Nodes slice modified the internal state")
				}
			}

			g.SetLockingJob(tc.lockingJob)
			if g.LockingJob() != tc.lockingJob {
				t.Errorf("LockingJob() = %q, want %q", g.LockingJob(), tc.lockingJob)
			}

			g.SetActiveJob(tc.activeJob)
			if g.ActiveJob() != tc.activeJob {
				t.Errorf("ActiveJob() = %q, want %q", g.ActiveJob(), tc.activeJob)
			}

			beforeSet := time.Now()
			g.SetState(tc.state)
			gotState, gotTimestamp := g.State()

			if gotState != tc.state {
				t.Errorf("State() state = %v, want %v", gotState, tc.state)
			}
			if gotTimestamp.Before(beforeSet) || gotTimestamp.After(time.Now()) {
				t.Errorf("State() timestamp %v is not close to set time", gotTimestamp)
			}
		})
	}
}

func TestGroup_LockAndUnlock(t *testing.T) {
	ctx := context.Background()

	type step struct {
		op          string // "lock" or "unlock"
		jobID       string
		expectedErr error
		wantLocked  string
	}

	tests := []struct {
		name      string
		lockStore LockStore
		steps     []step
	}{
		{
			name:      "lock/unlock sequence without lockStore",
			lockStore: nil,
			steps: []step{
				{op: "lock", jobID: "job-a", expectedErr: nil, wantLocked: "job-a"},
				// Without lockStore, memory locks are overwritten directly in Lock()
				// since it doesn't check local LockingJob.
				{op: "lock", jobID: "job-b", expectedErr: nil, wantLocked: "job-b"},
				{op: "unlock", jobID: "job-b", expectedErr: nil, wantLocked: ""},
			},
		},
		{
			name:      "lock/unlock sequence with MemLockStore",
			lockStore: NewMemLockStore(),
			steps: []step{
				{op: "lock", jobID: "job-a", expectedErr: nil, wantLocked: "job-a"},
				{op: "lock", jobID: "job-b", expectedErr: ErrAlreadyLocked, wantLocked: "job-a"},
				{op: "lock", jobID: "job-a", expectedErr: nil, wantLocked: "job-a"}, // idempotent
				{op: "unlock", jobID: "job-b", expectedErr: ErrNotLockHolder, wantLocked: "job-a"},
				{op: "unlock", jobID: "job-a", expectedErr: nil, wantLocked: ""},
				{op: "unlock", jobID: "job-a", expectedErr: nil, wantLocked: ""}, // idempotent unlock
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGroup("group-1")
			g.lockStore = tc.lockStore

			for i, s := range tc.steps {
				var err error
				switch s.op {
				case "lock":
					err = g.Lock(ctx, s.jobID)
				case "unlock":
					err = g.Unlock(ctx, s.jobID)
				}

				if !errors.Is(err, s.expectedErr) {
					t.Fatalf("step %d: %s with %q returned error %v, want %v", i, s.op, s.jobID, err, s.expectedErr)
				}

				if g.LockingJob() != s.wantLocked {
					t.Errorf("step %d: after %s, LockingJob() = %q, want %q", i, s.op, g.LockingJob(), s.wantLocked)
				}
			}
		})
	}
}
