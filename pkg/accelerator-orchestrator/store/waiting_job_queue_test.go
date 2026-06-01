package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWaitingJobQueue_BasicAndDuplicates(t *testing.T) {
	type op struct {
		action   string // "enqueue", "dequeue", "exists", "len"
		val      string // input for action
		wantBool bool   // expected result for enqueue/dequeue/exists
		wantInt  int    // expected result for len
		wantVal  string // expected output for dequeue
	}

	tests := []struct {
		name  string
		steps []op
	}{
		{
			name: "basic enqueue and dequeue FIFO",
			steps: []op{
				{action: "len", wantInt: 0},
				{action: "enqueue", val: "job-a", wantBool: true},
				{action: "enqueue", val: "job-b", wantBool: true},
				{action: "len", wantInt: 2},
				{action: "exists", val: "job-a", wantBool: true},
				{action: "exists", val: "job-c", wantBool: false},
				{action: "dequeue", wantBool: true, wantVal: "job-a"},
				{action: "len", wantInt: 1},
				{action: "dequeue", wantBool: true, wantVal: "job-b"},
				{action: "len", wantInt: 0},
				{action: "dequeue", wantBool: false, wantVal: ""},
			},
		},
		{
			name: "prevent duplicates",
			steps: []op{
				{action: "enqueue", val: "job-a", wantBool: true},
				{action: "enqueue", val: "job-a", wantBool: false}, // duplicate
				{action: "len", wantInt: 1},
				{action: "dequeue", wantBool: true, wantVal: "job-a"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewWaitingJobQueue()
			for i, step := range tc.steps {
				switch step.action {
				case "enqueue":
					got := q.Enqueue(step.val)
					if got != step.wantBool {
						t.Fatalf("step %d: Enqueue(%q) = %t, want %t", i, step.val, got, step.wantBool)
					}
				case "dequeue":
					gotVal, ok := q.Dequeue()
					if ok != step.wantBool || gotVal != step.wantVal {
						t.Fatalf("step %d: Dequeue() = (%q, %t), want (%q, %t)", i, gotVal, ok, step.wantVal, step.wantBool)
					}
				case "exists":
					got := q.Exists(step.val)
					if got != step.wantBool {
						t.Fatalf("step %d: Exists(%q) = %t, want %t", i, step.val, got, step.wantBool)
					}
				case "len":
					got := q.Len()
					if got != step.wantInt {
						t.Fatalf("step %d: Len() = %d, want %d", i, got, step.wantInt)
					}
				}
			}
		})
	}
}

func TestWaitingJobQueue_Reset(t *testing.T) {
	tests := []struct {
		name        string
		initial     []string
		resetInput  []WaitingJob
		expectedIDs []string
	}{
		{
			name:        "reset empty queue to empty",
			initial:     nil,
			resetInput:  nil,
			expectedIDs: nil,
		},
		{
			name:    "reset empty queue with populated values",
			initial: nil,
			resetInput: []WaitingJob{
				{JobID: "job-1", QueuedSince: time.Now()},
				{JobID: "job-2", QueuedSince: time.Now()},
			},
			expectedIDs: []string{"job-1", "job-2"},
		},
		{
			name:    "reset and deduplicate",
			initial: []string{"old-job"},
			resetInput: []WaitingJob{
				{JobID: "job-1", QueuedSince: time.Now()},
				{JobID: "job-2", QueuedSince: time.Now()},
				{JobID: "job-1", QueuedSince: time.Now().Add(time.Second)}, // duplicate
			},
			expectedIDs: []string{"job-1", "job-2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewWaitingJobQueue()
			for _, id := range tc.initial {
				_ = q.Enqueue(id)
			}

			q.Reset(tc.resetInput)

			if q.Len() != len(tc.expectedIDs) {
				t.Fatalf("after Reset, Len() = %d, want %d", q.Len(), len(tc.expectedIDs))
			}

			// Check order and existence
			list := q.List()
			if len(list) != len(tc.expectedIDs) {
				t.Fatalf("after Reset, List() returned %d items, want %d", len(list), len(tc.expectedIDs))
			}

			for i, got := range list {
				if got.JobID != tc.expectedIDs[i] {
					t.Errorf("at index %d, got job ID %q, want %q", i, got.JobID, tc.expectedIDs[i])
				}
			}

			// Ensure old jobs are removed
			for _, old := range tc.initial {
				if q.Exists(old) {
					// Unless it was explicitly part of reset
					stillExpected := false
					for _, exp := range tc.expectedIDs {
						if exp == old {
							stillExpected = true
							break
						}
					}
					if !stillExpected {
						t.Errorf("old job %q still exists in queue after Reset", old)
					}
				}
			}
		})
	}
}

func TestWaitingJobQueue_Concurrency(t *testing.T) {
	q := NewWaitingJobQueue()
	var wg sync.WaitGroup

	numWorkers := 100
	jobsPerWorker := 10

	// 1. Concurrent enqueue of unique jobs
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < jobsPerWorker; j++ {
				jobID := fmt.Sprintf("worker-%d-job-%d", workerID, j)
				q.Enqueue(jobID)
			}
		}(i)
	}
	wg.Wait()

	expectedLen := numWorkers * jobsPerWorker
	if q.Len() != expectedLen {
		t.Errorf("Concurrent unique enqueue got len %d, want %d", q.Len(), expectedLen)
	}

	// 2. Concurrent enqueue of the SAME job (only 1 should succeed)
	q = NewWaitingJobQueue()
	wg.Add(numWorkers)
	successCount := 0
	var countMu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			if q.Enqueue("single-job") {
				countMu.Lock()
				successCount++
				countMu.Unlock()
			}
		}()
	}
	wg.Wait()

	if q.Len() != 1 {
		t.Errorf("Concurrent duplicate enqueue got len %d, want 1", q.Len())
	}
	if successCount != 1 {
		t.Errorf("Concurrent duplicate enqueue reported %d successes, want 1", successCount)
	}
}
