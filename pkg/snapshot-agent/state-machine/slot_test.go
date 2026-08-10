package statemachine_test

import (
	"errors"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"gotest.tools/v3/assert"
)

func waitForState(t *testing.T, mgr *sm.StateManager, jobID string, want pb.JobState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range mgr.GetJobStatus() {
			if s.JobId == jobID && s.State == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for job %s to reach %s", jobID, want)
}

func newRunningJob(t *testing.T, jobID string) *sm.StateManager {
	t.Helper()
	mgr := sm.NewStateManager()
	mgr.RegisterJob(jobID, "")
	assert.NilError(t, mgr.TransitionToRunning(jobID, []int{123}))
	return mgr
}

// snapshotToSlot drives a job through a successful snapshot into a slot.
func snapshotToSlot(t *testing.T, mgr *sm.StateManager, jobID, slot string) {
	t.Helper()
	_, err := mgr.StartSnapshotSlot(jobID, "", slot, func() error { return nil })
	assert.NilError(t, err)
	waitForState(t, mgr, jobID, pb.JobState_JOB_STATE_SAVED)
}

// restoreSlot drives a job through a successful restore from a slot.
func restoreSlot(t *testing.T, mgr *sm.StateManager, jobID, slot string) {
	t.Helper()
	opID, err := mgr.StartRestoreSlot(jobID, "", slot, func() error { return nil })
	assert.NilError(t, err)
	assert.Assert(t, opID != "already-running", "restore of slot %q short-circuited", slot)
	waitForState(t, mgr, jobID, pb.JobState_JOB_STATE_RUNNING)
}

// TestSlotSwapWhileRunning covers the memory-regions core case: a RUNNING
// job may restore a *different* slot (live slot swap), while restoring the
// already-loaded slot short-circuits.
func TestSlotSwapWhileRunning(t *testing.T) {
	mgr := newRunningJob(t, "job-1")

	// Snapshot into slot-a, restore it back: RUNNING with slot-a loaded.
	snapshotToSlot(t, mgr, "job-1", "slot-a")
	restoreSlot(t, mgr, "job-1", "slot-a")

	// Snapshot into slot-b (allowed: job is RUNNING), restore slot-a.
	snapshotToSlot(t, mgr, "job-1", "slot-b")
	restoreSlot(t, mgr, "job-1", "slot-a")

	// Live swap: RUNNING with slot-a loaded, restore slot-b must proceed.
	restoreSlot(t, mgr, "job-1", "slot-b")

	// Redundant restore of the loaded slot short-circuits.
	opID, err := mgr.StartRestoreSlot("job-1", "", "slot-b", func() error {
		t.Error("worker must not run for an already-loaded slot")
		return nil
	})
	assert.NilError(t, err)
	assert.Equal(t, opID, "already-running")
}

// TestSlotlessBehaviorUnchanged pins the upstream semantics for backends
// without slot naming (CUDA/app): restore while RUNNING always
// short-circuits, and FAULTED jobs stay rejected.
func TestSlotlessBehaviorUnchanged(t *testing.T) {
	t.Run("restore while running short-circuits", func(t *testing.T) {
		mgr := newRunningJob(t, "job-1")
		opID, err := mgr.StartRestore("job-1", "", func() error { return nil })
		assert.NilError(t, err)
		assert.Equal(t, opID, "already-running")
	})

	t.Run("snapshot of faulted job rejected", func(t *testing.T) {
		mgr := newRunningJob(t, "job-1")
		_, err := mgr.StartSnapshot("job-1", "", func() error { return errFailed })
		assert.NilError(t, err)
		waitForState(t, mgr, "job-1", pb.JobState_JOB_STATE_FAULTED)

		_, err = mgr.StartSnapshot("job-1", "", func() error { return nil })
		assert.ErrorContains(t, err, "must be RUNNING")
	})

	t.Run("restore of faulted job rejected", func(t *testing.T) {
		mgr := newRunningJob(t, "job-1")
		_, err := mgr.StartSnapshot("job-1", "", func() error { return errFailed })
		assert.NilError(t, err)
		waitForState(t, mgr, "job-1", pb.JobState_JOB_STATE_FAULTED)

		_, err = mgr.StartRestore("job-1", "", func() error { return nil })
		assert.ErrorContains(t, err, "must be SAVED")
	})
}

// TestSlotFaultRecovery: with a named slot, snapshot and restore may reset a
// FAULTED job (faults are typically transient: dead PID, cr_client timeout).
func TestSlotFaultRecovery(t *testing.T) {
	fail := func() error { return errFailed }

	t.Run("snapshot resets faulted job", func(t *testing.T) {
		mgr := newRunningJob(t, "job-1")
		_, err := mgr.StartSnapshotSlot("job-1", "", "slot-a", fail)
		assert.NilError(t, err)
		waitForState(t, mgr, "job-1", pb.JobState_JOB_STATE_FAULTED)

		snapshotToSlot(t, mgr, "job-1", "slot-a")
	})

	t.Run("restore resets faulted job", func(t *testing.T) {
		mgr := newRunningJob(t, "job-1")
		snapshotToSlot(t, mgr, "job-1", "slot-a")
		restoreSlot(t, mgr, "job-1", "slot-a")
		snapshotToSlot(t, mgr, "job-1", "slot-b")

		// Fail a restore: job FAULTED.
		_, err := mgr.StartRestoreSlot("job-1", "", "slot-a", fail)
		assert.NilError(t, err)
		waitForState(t, mgr, "job-1", pb.JobState_JOB_STATE_FAULTED)

		// A fresh restore resets it.
		restoreSlot(t, mgr, "job-1", "slot-a")
	})
}

// TestSlotSnapshotStillRequiresRunning: slot-aware snapshots keep the
// RUNNING requirement for non-faulted jobs (IDLE and SAVED are rejected).
func TestSlotSnapshotStillRequiresRunning(t *testing.T) {
	mgr := sm.NewStateManager()
	mgr.RegisterJob("job-1", "")

	_, err := mgr.StartSnapshotSlot("job-1", "", "slot-a", func() error { return nil })
	assert.ErrorContains(t, err, "must be RUNNING")

	assert.NilError(t, mgr.TransitionToRunning("job-1", []int{123}))
	snapshotToSlot(t, mgr, "job-1", "slot-a")

	// SAVED: still rejected.
	_, err = mgr.StartSnapshotSlot("job-1", "", "slot-b", func() error { return nil })
	assert.ErrorContains(t, err, "must be RUNNING")
}

var errFailed = errors.New("backend operation failed")
