package store

import (
	"maps"
	"sync"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// Role says whether a job owns its hosts or borrows them.
type Role int

const (
	// RoleForeground is a job that owns its hosts, such as an RL trainer. It is
	// the default: a job is foreground unless it is known to be a guest.
	RoleForeground Role = iota
	// RoleBackground is a guest that borrows a host while the foreground job
	// is idle. Its failures belong to it and never fault the group.
	RoleBackground
)

// String returns the role name used in logs.
func (r Role) String() string {
	if r == RoleBackground {
		return "background"
	}
	return "foreground"
}

// Job represents the state of a job within a group.
type Job struct {
	mu           sync.RWMutex
	jobID        string
	groupID      string
	role         Role
	pods         []string // pod UUIDs
	contextState map[string]pb.SnapshotAgentJobState_State
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

// Role returns the job's role. It is RoleForeground unless SetRole said otherwise.
func (j *Job) Role() Role {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.role
}

// SetRole records the job's role, as read from its pods.
func (j *Job) SetRole(role Role) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.role = role
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
