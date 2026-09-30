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

	// timeoutFaults maps a node to the ID of a foreground operation that
	// passed --foreground-op-timeout there. The orchestrator, not the agent,
	// marked the job FAULTED on those nodes. See MarkForegroundTimeoutFault.
	timeoutFaults map[string]string
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
	if len(j.timeoutFaults) > 0 && !samePods(j.pods, pods) {
		// A pod was deleted or replaced: the fault belonged to the old pods,
		// as for a FAULTED job the agent reports.
		j.timeoutFaults = nil
	}
	j.pods = append([]string(nil), pods...)
}

// ContextState returns the job's state per node as the agents last reported
// it, except that a node where MarkForegroundTimeoutFault marked the job reads
// STATE_FAULTED, so every FAULTED check (the group fault in Acquire, the
// reconcile loop, GetGroupStatus) sees it.
func (j *Job) ContextState() map[string]pb.SnapshotAgentJobState_State {
	j.mu.RLock()
	defer j.mu.RUnlock()
	cs := maps.Clone(j.contextState)
	if cs == nil && len(j.timeoutFaults) > 0 {
		cs = make(map[string]pb.SnapshotAgentJobState_State, len(j.timeoutFaults))
	}
	for node := range j.timeoutFaults {
		cs[node] = pb.SnapshotAgentJobState_STATE_FAULTED
	}
	return cs
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

// MarkForegroundTimeoutFault marks the job FAULTED on node because its
// foreground snapshot or restore operationID did not finish within
// --foreground-op-timeout. The agent may still report the job TRANSITIONING;
// the mark wins on read (ContextState) until the job's pods change.
func (j *Job) MarkForegroundTimeoutFault(node, operationID string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.timeoutFaults == nil {
		j.timeoutFaults = make(map[string]string)
	}
	j.timeoutFaults[node] = operationID
}

// ForegroundTimeoutFault returns the ID of the timed-out operation that marked
// the job FAULTED on node, and whether there is one.
func (j *Job) ForegroundTimeoutFault(node string) (string, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	op, ok := j.timeoutFaults[node]
	return op, ok
}

// samePods reports whether a and b hold the same pod UIDs, in any order.
func samePods(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, uid := range a {
		seen[uid]++
	}
	for _, uid := range b {
		if seen[uid] == 0 {
			return false
		}
		seen[uid]--
	}
	return true
}
