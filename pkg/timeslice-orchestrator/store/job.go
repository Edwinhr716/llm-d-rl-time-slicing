package store

import (
	"maps"
	"sync"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

// Job represents the state of a job within a group.
type Job struct {
	mu           sync.RWMutex
	jobID        string
	groupID      string
	pods         []string // pod UUIDs
	contextState map[string]pb.SnapshotAgentJobState_State
	// backendConfig is the snapshot backend last supplied by the client on Yield. It is forwarded on
	// every Snapshot/Restore for the job; nil means the agent default. In-memory only: lost on restart.
	backendConfig *agentpb.BackendConfig
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

// BackendConfig returns the snapshot backend config stored for the job, or nil when the client
// never supplied one (the agent's default backend applies). The returned message must not be mutated.
func (j *Job) BackendConfig() *agentpb.BackendConfig {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.backendConfig
}

// SetBackendConfig stores the snapshot backend config for the job. A nil cfg clears it.
func (j *Job) SetBackendConfig(cfg *agentpb.BackendConfig) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.backendConfig = cfg
}
