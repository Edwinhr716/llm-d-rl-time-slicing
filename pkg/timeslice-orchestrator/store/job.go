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
	// podNodes holds the nodes on which the job has a mirror pod that is not
	// in a terminal phase. Only tracked for background jobs.
	podNodes []string
	// killed holds the nodes on which the agent reports the job's last
	// outcome as OUTCOME_KILLED, or on which the orchestrator saw its own Kill
	// of the job confirmed. Job IDs are unique per incarnation, so the
	// controller only ever sets it.
	killed map[string]bool
	// unconfirmedKill holds the nodes on which a Kill of the job ran but was
	// not confirmed within the kill budget, and the notice window then ran
	// out, so the node was handed back anyway (decision D-NS-6, "keep").
	unconfirmedKill map[string]bool

	// timeoutFaults maps a node to the ID of a foreground operation that
	// passed --foreground-op-timeout there under the faulted or bounded
	// action. The orchestrator, not the agent, marked the job FAULTED on
	// those nodes. See MarkForegroundTimeoutFault.
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

// Background reports whether the job is a background (guest mirror) job.
// Background jobs are driven by their node's background participant (the VK)
// and the kill path, never by the foreground snapshot/restore loop, and never
// make the group FAULTED.
func (j *Job) Background() bool {
	return j.Role() == RoleBackground
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

// UnconfirmedKill reports whether the node was handed back to the foreground
// after a Kill of the job on nodeName that was never confirmed.
func (j *Job) UnconfirmedKill(nodeName string) bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.unconfirmedKill[nodeName]
}

// SetUnconfirmedKill records whether the node was handed back after an
// unconfirmed Kill of the job on nodeName.
func (j *Job) SetUnconfirmedKill(nodeName string, unconfirmed bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.unconfirmedKill == nil {
		j.unconfirmedKill = make(map[string]bool)
	}
	j.unconfirmedKill[nodeName] = unconfirmed
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
