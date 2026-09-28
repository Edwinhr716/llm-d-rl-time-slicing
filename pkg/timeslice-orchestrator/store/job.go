package store

import (
	"maps"
	"sync"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// Job represents the state of a job within a group.
type Job struct {
	mu           sync.RWMutex
	jobID        string
	groupID      string
	pods         []string // pod UUIDs
	contextState map[string]pb.SnapshotAgentJobState_State

	// background is true for a guest's mirror job (pod label
	// timeslice.io/role=background). Background jobs are driven by their
	// node's background participant (the VK), never by the foreground
	// snapshot/restore loop, and never make the group FAULTED.
	background bool
	// podNodes holds the nodes on which the job has a mirror pod that is not
	// in a terminal phase. Only tracked for background jobs.
	podNodes []string
	// killed holds the nodes on which the agent reports the job's last
	// outcome as OUTCOME_KILLED.
	killed map[string]bool
}

// NewJob creates a new Job with default values.
func NewJob(groupID, jobID string) *Job {
	return &Job{
		jobID:        jobID,
		groupID:      groupID,
		contextState: make(map[string]pb.SnapshotAgentJobState_State),
	}
}

func (j *Job) JobID() string {
	return j.jobID
}

func (j *Job) GroupID() string {
	return j.groupID
}

func (j *Job) Pods() []string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return append([]string(nil), j.pods...)
}

func (j *Job) SetPods(pods []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.pods = append([]string(nil), pods...)
}

func (j *Job) ContextState() map[string]pb.SnapshotAgentJobState_State {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return maps.Clone(j.contextState)
}

func (j *Job) SetContextState(cs map[string]pb.SnapshotAgentJobState_State) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(cs) == 0 {
		// ensure we never set j.contextState to nil
		j.contextState = make(map[string]pb.SnapshotAgentJobState_State)
		return
	}
	j.contextState = maps.Clone(cs)
}

func (j *Job) UpdateContextState(nodeName string, state pb.SnapshotAgentJobState_State) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.contextState[nodeName] = state
}

// Background reports whether the job is a background (guest mirror) job.
func (j *Job) Background() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.background
}

// SetBackground records whether the job is a background (guest mirror) job.
func (j *Job) SetBackground(background bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.background = background
}

// PodNodes returns the nodes on which the job has a non-terminal pod.
func (j *Job) PodNodes() []string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return append([]string(nil), j.podNodes...)
}

// SetPodNodes records the nodes on which the job has a non-terminal pod.
func (j *Job) SetPodNodes(nodes []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.podNodes = append([]string(nil), nodes...)
}

// Killed reports whether the agent on nodeName reported the job killed.
func (j *Job) Killed(nodeName string) bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.killed[nodeName]
}

// SetKilled records whether the agent on nodeName reports the job killed.
func (j *Job) SetKilled(nodeName string, killed bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.killed == nil {
		j.killed = make(map[string]bool)
	}
	j.killed[nodeName] = killed
}
