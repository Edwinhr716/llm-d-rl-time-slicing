package statemachine

import (
	"fmt"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Recovered is what restart recovery observed on the node for one job.
type Recovered struct {
	// State is IDLE, RUNNING, SAVED, SUSPENDED or FAULTED.
	State pb.JobState
	// PIDs are the job's CUDA processes: the checkpointed ones for SAVED,
	// the running ones for RUNNING, the guest's processes for SUSPENDED.
	PIDs []int
	// HostBytesPinned is the host memory a SUSPENDED guest pins.
	HostBytesPinned int64
}

// RecoverJob sets a registered job's state from what restart recovery
// observed on the node. The agent keeps no state across restarts: the node
// is the record, and recovery runs once at startup, before any RPC is
// served. A SUSPENDED job gets last outcome SUSPENDED, a RUNNING or SAVED
// one gets none, and every other field keeps its value (the epoch comes
// from the mirror's guest-epoch annotation). It returns false and changes
// nothing for an unknown job or a job with a running operation.
func (sm *StateManager) RecoverJob(jobID string, rec Recovered) (bool, error) {
	switch rec.State {
	case pb.JobState_JOB_STATE_IDLE, pb.JobState_JOB_STATE_RUNNING, pb.JobState_JOB_STATE_SAVED,
		pb.JobState_JOB_STATE_SUSPENDED, pb.JobState_JOB_STATE_FAULTED:
	default:
		return false, fmt.Errorf("job %s: cannot recover to state %s", jobID, rec.State)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	job, ok := sm.jobs[jobID]
	if !ok {
		return false, nil
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.current != nil {
		return false, nil
	}

	job.State = rec.State
	job.PIDs = append([]int(nil), rec.PIDs...)
	switch rec.State {
	case pb.JobState_JOB_STATE_SUSPENDED:
		job.LastOutcome = pb.Outcome_OUTCOME_SUSPENDED
		job.HostBytesPinned = rec.HostBytesPinned
	case pb.JobState_JOB_STATE_RUNNING, pb.JobState_JOB_STATE_SAVED:
		// Readers count KILLED or RELEASED as vacated.
		job.LastOutcome = pb.Outcome_OUTCOME_UNSPECIFIED
		job.HostBytesPinned = 0
	case pb.JobState_JOB_STATE_IDLE:
		job.PIDs = nil
		job.DeviceBytes = 0
		job.HostBytesPinned = 0
	}
	return true, nil
}
