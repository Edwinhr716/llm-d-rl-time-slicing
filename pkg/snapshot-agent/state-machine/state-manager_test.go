package statemachine

import (
	"errors"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

func TestStateManager_Transitions(t *testing.T) {
	sm := NewStateManager()
	jobID := "test-job"
	group := "default"

	// Initial state check
	sm.mu.Lock()
	job := sm.getOrCreateJob(jobID, group)
	if job.State != pb.JobState_JOB_STATE_IDLE {
		t.Errorf("Expected initial state IDLE, got %v", job.State)
	}
	sm.mu.Unlock()

	// 1. Successful Snapshot
	opID, err := sm.StartSnapshot(jobID, group, func() (int64, int64, error) {
		return 100, 200, nil
	})
	if err != nil {
		t.Fatalf("StartSnapshot failed: %v", err)
	}

	// Verify transitioning state
	if job.State != pb.JobState_JOB_STATE_TRANSITIONING {
		t.Errorf("Expected state TRANSITIONING, got %v", job.State)
	}

	// Wait for background op
	time.Sleep(100 * time.Millisecond)

	if job.State != pb.JobState_JOB_STATE_SAVED {
		t.Errorf("Expected state SAVED, got %v", job.State)
	}

	op, ok := sm.GetOperation(opID)
	if !ok || op.Status != pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		t.Errorf("Expected operation COMPLETE, got %v", op.Status)
	}

	// 2. Concurrency Guard: Try another snapshot while one is "theoretically" running
	// (we'll manually set state to TRANSITIONING for this test)
	job.mu.Lock()
	job.State = pb.JobState_JOB_STATE_TRANSITIONING
	job.mu.Unlock()

	_, err = sm.StartSnapshot(jobID, group, func() (int64, int64, error) { return 0, 0, nil })
	if err == nil {
		t.Error("Expected error for concurrent transition, got nil")
	}

	// 3. Fault Protection
	job.mu.Lock()
	job.State = pb.JobState_JOB_STATE_FAULTED
	job.mu.Unlock()

	_, err = sm.StartSnapshot(jobID, group, func() (int64, int64, error) { return 0, 0, nil })
	if err == nil {
		t.Error("Expected error for FAULTED job, got nil")
	}

	// 4. Successful Restore
	job.mu.Lock()
	job.State = pb.JobState_JOB_STATE_SAVED
	job.mu.Unlock()

	opID, err = sm.StartRestore(jobID, group, func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("StartRestore failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if job.State != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("Expected state RUNNING, got %v", job.State)
	}

	// 5. Redundancy Optimization
	opID2, err := sm.StartRestore(jobID, group, func() error { return nil })
	if err != nil {
		t.Fatalf("StartRestore failed: %v", err)
	}
	if opID2 != "already-running" {
		t.Errorf("Expected already-running, got %s", opID2)
	}

	// 6. Error handling
	job.mu.Lock()
	job.State = pb.JobState_JOB_STATE_SAVED
	job.mu.Unlock()

	_, err = sm.StartRestore(jobID, group, func() error {
		return errors.New("restore failed")
	})
	if err != nil {
		t.Fatalf("StartRestore failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if job.State != pb.JobState_JOB_STATE_FAULTED {
		t.Errorf("Expected state FAULTED after error, got %v", job.State)
	}
}
