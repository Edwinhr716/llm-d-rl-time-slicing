package store

import (
	"context"
	"errors"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/accelerator-orchestrator/api/v1alpha1"
)

func TestJobStore_Get(t *testing.T) {
	tests := []struct {
		name    string
		initial []*Job
		jobID   string
		groupID string
		wantJob *Job
		wantErr error
	}{
		{
			name:    "empty store",
			initial: nil,
			jobID:   "job-a",
			groupID: "group-1",
			wantErr: ErrNotFound,
		},
		{
			name: "job exists",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			jobID:   "job-a",
			groupID: "group-1",
			wantJob: NewJob("job-a", "group-1"),
			wantErr: nil,
		},
		{
			name: "different group, same jobID",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			jobID:   "job-a",
			groupID: "group-2",
			wantErr: ErrNotFound,
		},
		{
			name: "different jobID, same group",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			jobID:   "job-b",
			groupID: "group-1",
			wantErr: ErrNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewJobStore()
			for _, j := range tc.initial {
				if err := s.Put(ctx, j); err != nil {
					t.Fatalf("failed to put initial job: %v", err)
				}
			}

			got, err := s.Get(ctx, tc.jobID, tc.groupID)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Get() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				if got.JobID() != tc.wantJob.JobID() || got.GroupID() != tc.wantJob.GroupID() {
					t.Errorf("Get() got job with ID %q and GroupID %q, want ID %q and GroupID %q",
						got.JobID(), got.GroupID(), tc.wantJob.JobID(), tc.wantJob.GroupID())
				}
			}
		})
	}
}

func TestJobStore_Put(t *testing.T) {
	tests := []struct {
		name        string
		initial     []*Job
		putJob      *Job
		expectedLen int
	}{
		{
			name:        "put into empty store",
			initial:     nil,
			putJob:      NewJob("job-a", "group-1"),
			expectedLen: 1,
		},
		{
			name: "overwrite existing job",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			putJob:      NewJob("job-a", "group-1"),
			expectedLen: 1,
		},
		{
			name: "put another job",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			putJob:      NewJob("job-b", "group-1"),
			expectedLen: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewJobStore()
			for _, j := range tc.initial {
				if err := s.Put(ctx, j); err != nil {
					t.Fatalf("failed to put initial job: %v", err)
				}
			}

			err := s.Put(ctx, tc.putJob)
			if err != nil {
				t.Fatalf("Put() returned error: %v", err)
			}

			// Verify internal length via ListByGroup
			var count int
			// We know we only use group-1 for tests here
			list1, err := s.ListByGroup(ctx, "group-1")
			if err != nil {
				t.Fatalf("ListByGroup() failed: %v", err)
			}
			count += len(list1)

			if count != tc.expectedLen {
				t.Errorf("expected store size %d, got %d", tc.expectedLen, count)
			}

			// Retrieve and verify
			got, err := s.Get(ctx, tc.putJob.JobID(), tc.putJob.GroupID())
			if err != nil {
				t.Fatalf("failed to retrieve put job: %v", err)
			}
			if got.JobID() != tc.putJob.JobID() || got.GroupID() != tc.putJob.GroupID() {
				t.Errorf("retrieved job does not match put job")
			}
		})
	}
}

func TestJobStore_ListByGroup(t *testing.T) {
	tests := []struct {
		name        string
		initial     []*Job
		listGroup   string
		expectedIDs []string
	}{
		{
			name:        "list empty store",
			initial:     nil,
			listGroup:   "group-1",
			expectedIDs: nil,
		},
		{
			name: "list group with single job",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			listGroup:   "group-1",
			expectedIDs: []string{"job-a"},
		},
		{
			name: "list group with multiple jobs",
			initial: []*Job{
				NewJob("job-a", "group-1"),
				NewJob("job-b", "group-1"),
				NewJob("job-c", "group-2"),
			},
			listGroup:   "group-1",
			expectedIDs: []string{"job-a", "job-b"},
		},
		{
			name: "list group with no matching jobs",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			listGroup:   "group-2",
			expectedIDs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewJobStore()
			for _, j := range tc.initial {
				if err := s.Put(ctx, j); err != nil {
					t.Fatalf("failed to put initial job: %v", err)
				}
			}

			list, err := s.ListByGroup(ctx, tc.listGroup)
			if err != nil {
				t.Fatalf("ListByGroup() returned error: %v", err)
			}

			gotIDs := make([]string, 0, len(list))
			for _, j := range list {
				gotIDs = append(gotIDs, j.JobID())
			}

			// Order check is optional but let's ensure elements match
			if len(gotIDs) != len(tc.expectedIDs) {
				t.Fatalf("ListByGroup() returned %d items, want %d", len(gotIDs), len(tc.expectedIDs))
			}

			// Simple subset validation (since order in map iteration is randomized in Go)
			for _, id := range tc.expectedIDs {
				found := false
				for _, gotID := range gotIDs {
					if gotID == id {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("ListByGroup() did not contain expected job ID %q", id)
				}
			}
		})
	}
}

func TestJobStore_Delete(t *testing.T) {
	tests := []struct {
		name          string
		initial       []*Job
		deleteJobID   string
		deleteGroupID string
		expectedErr   error
	}{
		{
			name:          "delete from empty store",
			initial:       nil,
			deleteJobID:   "job-a",
			deleteGroupID: "group-1",
			expectedErr:   nil, // Store Delete of non-existent is generally a no-op/nil err
		},
		{
			name: "delete existing job",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			deleteJobID:   "job-a",
			deleteGroupID: "group-1",
			expectedErr:   nil,
		},
		{
			name: "delete with wrong groupID",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			deleteJobID:   "job-a",
			deleteGroupID: "group-2",
			expectedErr:   nil, // will be no-op
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewJobStore()
			for _, j := range tc.initial {
				if err := s.Put(ctx, j); err != nil {
					t.Fatalf("failed to put initial job: %v", err)
				}
			}

			err := s.Delete(ctx, tc.deleteJobID, tc.deleteGroupID)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("Delete() error = %v, want %v", err, tc.expectedErr)
			}

			// If we deleted a valid job, make sure it's gone
			if len(tc.initial) > 0 && tc.deleteGroupID == "group-1" && tc.deleteJobID == "job-a" {
				_, err := s.Get(ctx, tc.deleteJobID, tc.deleteGroupID)
				if !errors.Is(err, ErrNotFound) {
					t.Errorf("Get() after Delete returned error %v, want ErrNotFound", err)
				}
			}
		})
	}
}

func TestJobStore_UpdateContextState(t *testing.T) {
	tests := []struct {
		name          string
		initial       []*Job
		updateJobID   string
		updateGroupID string
		updateNode    string
		updateVal     pb.SnapshotAgentJobState_State
		expectedErr   error
	}{
		{
			name:          "update non-existent job",
			initial:       nil,
			updateJobID:   "job-a",
			updateGroupID: "group-1",
			updateNode:    "node-1",
			updateVal:     pb.SnapshotAgentJobState_STATE_RUNNING,
			expectedErr:   ErrNotFound,
		},
		{
			name: "update existing job",
			initial: []*Job{
				NewJob("job-a", "group-1"),
			},
			updateJobID:   "job-a",
			updateGroupID: "group-1",
			updateNode:    "node-1",
			updateVal:     pb.SnapshotAgentJobState_STATE_RUNNING,
			expectedErr:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := NewJobStore()
			for _, j := range tc.initial {
				if err := s.Put(ctx, j); err != nil {
					t.Fatalf("failed to put initial job: %v", err)
				}
			}

			err := s.UpdateContextState(ctx, tc.updateJobID, tc.updateGroupID, tc.updateNode, tc.updateVal)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("UpdateContextState() error = %v, want %v", err, tc.expectedErr)
			}

			if tc.expectedErr == nil {
				got, err := s.Get(ctx, tc.updateJobID, tc.updateGroupID)
				if err != nil {
					t.Fatalf("failed to get job after update: %v", err)
				}
				if got.ContextState()[tc.updateNode] != tc.updateVal {
					t.Errorf("ContextState for node %q = %v, want %v", tc.updateNode, got.ContextState()[tc.updateNode], tc.updateVal)
				}
			}
		})
	}
}
