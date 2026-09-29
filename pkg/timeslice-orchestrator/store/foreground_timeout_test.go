package store_test

import (
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// TestForegroundTimeoutFault_JobMark: the mark reads as FAULTED on its
// node only, wins over later agent reports, and is cleared when the job's pods
// change but not when the same pods are observed again.
func TestForegroundTimeoutFault_JobMark(t *testing.T) {
	job := store.NewJob("group-1", "trainer")
	job.SetPods([]string{"uid-a", "uid-b"})
	job.UpdateContextState("node-1", pb.SnapshotAgentJobState_STATE_TRANSITIONING)
	job.UpdateContextState("node-2", pb.SnapshotAgentJobState_STATE_SAVED)

	job.MarkForegroundTimeoutFault("node-1", "op-1")
	if op, ok := job.ForegroundTimeoutFault("node-1"); !ok || op != "op-1" {
		t.Errorf("ForegroundTimeoutFault(node-1) = %q, %v, want op-1, true", op, ok)
	}
	if _, ok := job.ForegroundTimeoutFault("node-2"); ok {
		t.Error("ForegroundTimeoutFault(node-2) = true, want false")
	}

	// The agent finishing the operation does not clear the mark.
	job.UpdateContextState("node-1", pb.SnapshotAgentJobState_STATE_SAVED)
	cs := job.ContextState()
	if cs["node-1"] != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Errorf("node-1 = %v, want FAULTED", cs["node-1"])
	}
	if cs["node-2"] != pb.SnapshotAgentJobState_STATE_SAVED {
		t.Errorf("node-2 = %v, want SAVED", cs["node-2"])
	}

	// The same pods in another order: still FAULTED.
	job.SetPods([]string{"uid-b", "uid-a"})
	if got := job.ContextState()["node-1"]; got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Errorf("after the same pods were observed again, node-1 = %v, want FAULTED", got)
	}

	// A replaced pod clears it.
	job.SetPods([]string{"uid-a", "uid-c"})
	if got := job.ContextState()["node-1"]; got != pb.SnapshotAgentJobState_STATE_SAVED {
		t.Errorf("after a pod was replaced, node-1 = %v, want the agent's SAVED", got)
	}
	if _, ok := job.ForegroundTimeoutFault("node-1"); ok {
		t.Error("ForegroundTimeoutFault(node-1) still set after a pod was replaced")
	}
}
