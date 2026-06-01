package store

import (
	"reflect"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/accelerator-orchestrator/api/v1alpha1"
)

func TestJob_GettersAndSetters(t *testing.T) {
	tests := []struct {
		name         string
		jobID        string
		groupID      string
		pods         []string
		contextState map[string]pb.SnapshotAgentJobState_State
	}{
		{
			name:         "empty/nil values",
			jobID:        "",
			groupID:      "",
			pods:         nil,
			contextState: nil,
		},
		{
			name:    "populated values",
			jobID:   "job-123",
			groupID: "group-abc",
			pods:    []string{"pod-a", "pod-b"},
			contextState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_RUNNING,
				"node-2": pb.SnapshotAgentJobState_STATE_SAVED,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			j := NewJob(tc.jobID, tc.groupID)

			if j.JobID() != tc.jobID {
				t.Errorf("JobID() = %q, want %q", j.JobID(), tc.jobID)
			}
			if j.GroupID() != tc.groupID {
				t.Errorf("GroupID() = %q, want %q", j.GroupID(), tc.groupID)
			}

			j.SetPods(tc.pods)
			if !reflect.DeepEqual(j.Pods(), tc.pods) {
				t.Errorf("Pods() = %+v, want %+v", j.Pods(), tc.pods)
			}

			j.SetContextState(tc.contextState)
			if !reflect.DeepEqual(j.ContextState(), tc.contextState) {
				t.Errorf("ContextState() = %+v, want %+v", j.ContextState(), tc.contextState)
			}

			// Verify deep copy/mutation protection for Pods
			if len(tc.pods) > 0 {
				mutatedPods := j.Pods()
				mutatedPods[0] = "mutated"
				if reflect.DeepEqual(j.Pods(), mutatedPods) {
					t.Errorf("mutating returned Pods slice modified the internal state")
				}
			}

			// Verify deep copy/mutation protection for ContextState
			if len(tc.contextState) > 0 {
				mutatedState := j.ContextState()
				for k := range mutatedState {
					mutatedState[k] = pb.SnapshotAgentJobState_STATE_UNSPECIFIED
				}
				if reflect.DeepEqual(j.ContextState(), mutatedState) {
					t.Errorf("mutating returned ContextState map modified the internal state")
				}
			}
		})
	}
}

func TestJob_UpdateContextState(t *testing.T) {
	tests := []struct {
		name          string
		initialState  map[string]pb.SnapshotAgentJobState_State
		updateNode    string
		updateVal     pb.SnapshotAgentJobState_State
		expectedState map[string]pb.SnapshotAgentJobState_State
	}{
		{
			name:         "update nil state",
			initialState: nil,
			updateNode:   "node-1",
			updateVal:    pb.SnapshotAgentJobState_STATE_RUNNING,
			expectedState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_RUNNING,
			},
		},
		{
			name: "add new node to existing state",
			initialState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_RUNNING,
			},
			updateNode: "node-2",
			updateVal:  pb.SnapshotAgentJobState_STATE_SAVED,
			expectedState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_RUNNING,
				"node-2": pb.SnapshotAgentJobState_STATE_SAVED,
			},
		},
		{
			name: "overwrite existing node state",
			initialState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_RUNNING,
			},
			updateNode: "node-1",
			updateVal:  pb.SnapshotAgentJobState_STATE_SAVED,
			expectedState: map[string]pb.SnapshotAgentJobState_State{
				"node-1": pb.SnapshotAgentJobState_STATE_SAVED,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			j := NewJob("job-1", "group-1")
			j.SetContextState(tc.initialState)

			j.UpdateContextState(tc.updateNode, tc.updateVal)

			if !reflect.DeepEqual(j.ContextState(), tc.expectedState) {
				t.Errorf("ContextState() after UpdateContextState = %+v, want %+v", j.ContextState(), tc.expectedState)
			}
		})
	}
}
