package simulate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"k8s.io/client-go/util/workqueue"
)

// trackQueue wraps a rate limiting queue and tracks Done() and AddRateLimited() calls.
type TrackQueue struct {
	workqueue.TypedRateLimitingInterface[string]
	mu                  sync.Mutex
	doneCount           int
	addRateLimitedCount int
}

func (t *TrackQueue) Done(item string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.doneCount++
	t.TypedRateLimitingInterface.Done(item)
}

func (t *TrackQueue) AddRateLimited(item string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addRateLimitedCount++
	t.TypedRateLimitingInterface.AddRateLimited(item)
}

func (t *TrackQueue) getDoneCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.doneCount
}

func (t *TrackQueue) getAddRateLimitedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addRateLimitedCount
}

type pendingOp struct {
	node        string
	job         string
	opType      string // "snapshot", "restore" or "kill"
	targetState agentpb.JobState
}

// FakeSnapshotAgentStore simulates the behavior of snapshot agents on nodes.
type FakeSnapshotAgentStore struct {
	mu                sync.Mutex
	jobStates         map[string]map[string]agentpb.JobState // node -> job -> state
	lastOutcomes      map[string]map[string]agentpb.Outcome  // node -> job -> last outcome
	unreachable       map[string]bool                        // node -> every call fails
	pendingOperations map[string]pendingOp
	opCounter         int

	// Optional hooks for tests to observe events
	OnSnapshot func(node, jobID string)
	OnRestore  func(node, jobID string)
	// OnKill runs (synchronously, without the store lock) for every Kill
	// call, also one that fails because the node is unreachable.
	OnKill func(node, jobID, reason string, deadline time.Time, reached bool)
	// OnOperationComplete runs (synchronously, without the store lock) when
	// GetOperation completes an operation: opType is "snapshot", "restore"
	// or "kill".
	OnOperationComplete func(node, jobID, opType string)
}

func NewFakeSnapshotAgentStore() *FakeSnapshotAgentStore {
	return &FakeSnapshotAgentStore{
		jobStates:         make(map[string]map[string]agentpb.JobState),
		lastOutcomes:      make(map[string]map[string]agentpb.Outcome),
		unreachable:       make(map[string]bool),
		pendingOperations: make(map[string]pendingOp),
	}
}

// errUnreachable is what every call to an unreachable node returns.
var errUnreachable = errors.New("snapshot agent unreachable")

// SetUnreachable makes every call to the node's agent fail (true) or work
// again (false).
func (f *FakeSnapshotAgentStore) SetUnreachable(node string, unreachable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable[node] = unreachable
}

// SetJobState allows setting the initial state of a job on a node.
func (f *FakeSnapshotAgentStore) SetJobState(node, job string, state agentpb.JobState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.jobStates[node]; !ok {
		f.jobStates[node] = make(map[string]agentpb.JobState)
	}
	f.jobStates[node][job] = state
}

// GetJobState allows reading the state of a job on a node.
func (f *FakeSnapshotAgentStore) GetJobState(node, job string) agentpb.JobState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if nodes, ok := f.jobStates[node]; ok {
		return nodes[job]
	}
	return agentpb.JobState_JOB_STATE_UNSPECIFIED
}

// GetLastOutcome reads the last outcome the agent reports for a job on a node.
func (f *FakeSnapshotAgentStore) GetLastOutcome(node, job string) agentpb.Outcome {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastOutcomes[node][job]
}

func (f *FakeSnapshotAgentStore) GetStatus(ctx context.Context, nodeName string) (*agentpb.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreachable[nodeName] {
		return nil, errUnreachable
	}

	var statuses []*agentpb.JobStatus
	if jobs, ok := f.jobStates[nodeName]; ok {
		for jobID, state := range jobs {
			statuses = append(statuses, &agentpb.JobStatus{
				JobId:       jobID,
				State:       state,
				LastOutcome: f.lastOutcomes[nodeName][jobID],
			})
		}
	}
	return &agentpb.StatusResponse{JobStatuses: statuses}, nil
}

func (f *FakeSnapshotAgentStore) CloseClient(nodeName string) error {
	return nil
}

func (f *FakeSnapshotAgentStore) Snapshot(
	ctx context.Context,
	nodeName,
	jobID,
	groupID string,
) (*agentpb.SnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreachable[nodeName] {
		return nil, errUnreachable
	}

	f.opCounter++
	opID := fmt.Sprintf("op-snap-%d", f.opCounter)
	f.pendingOperations[opID] = pendingOp{
		node:        nodeName,
		job:         jobID,
		opType:      "snapshot",
		targetState: agentpb.JobState_JOB_STATE_SAVED,
	}

	// Transition to transitioning
	if _, ok := f.jobStates[nodeName]; !ok {
		f.jobStates[nodeName] = make(map[string]agentpb.JobState)
	}
	f.jobStates[nodeName][jobID] = agentpb.JobState_JOB_STATE_TRANSITIONING

	if f.OnSnapshot != nil {
		go f.OnSnapshot(nodeName, jobID)
	}

	return &agentpb.SnapshotResponse{OperationId: opID}, nil
}

func (f *FakeSnapshotAgentStore) Restore(ctx context.Context, nodeName, jobID, groupID string) (*agentpb.RestoreResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreachable[nodeName] {
		return nil, errUnreachable
	}

	f.opCounter++
	opID := fmt.Sprintf("op-restore-%d", f.opCounter)
	f.pendingOperations[opID] = pendingOp{
		node:        nodeName,
		job:         jobID,
		opType:      "restore",
		targetState: agentpb.JobState_JOB_STATE_RUNNING,
	}

	// Transition to transitioning
	if _, ok := f.jobStates[nodeName]; !ok {
		f.jobStates[nodeName] = make(map[string]agentpb.JobState)
	}
	f.jobStates[nodeName][jobID] = agentpb.JobState_JOB_STATE_TRANSITIONING

	if f.OnRestore != nil {
		go f.OnRestore(nodeName, jobID)
	}

	return &agentpb.RestoreResponse{OperationId: opID}, nil
}

// Kill starts a kill operation. It completes on the next GetOperation: the
// job goes IDLE and its last outcome becomes OUTCOME_KILLED.
func (f *FakeSnapshotAgentStore) Kill(
	ctx context.Context, nodeName, jobID, reason string, deadline time.Time,
) (*agentpb.KillResponse, error) {
	f.mu.Lock()
	reached := !f.unreachable[nodeName]
	opID := ""
	if reached {
		f.opCounter++
		opID = fmt.Sprintf("op-kill-%d", f.opCounter)
		f.pendingOperations[opID] = pendingOp{
			node:        nodeName,
			job:         jobID,
			opType:      "kill",
			targetState: agentpb.JobState_JOB_STATE_IDLE,
		}
	}
	onKill := f.OnKill
	f.mu.Unlock()

	if onKill != nil {
		onKill(nodeName, jobID, reason, deadline, reached)
	}
	if !reached {
		return nil, errUnreachable
	}
	return &agentpb.KillResponse{OperationId: opID}, nil
}

func (f *FakeSnapshotAgentStore) GetOperation(
	ctx context.Context,
	nodeName,
	operationID string,
) (*agentpb.GetOperationResponse, error) {
	f.mu.Lock()
	if f.unreachable[nodeName] {
		f.mu.Unlock()
		return nil, errUnreachable
	}

	op, ok := f.pendingOperations[operationID]
	if !ok {
		f.mu.Unlock()
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED}, nil
	}

	// Apply the transition
	if _, ok := f.jobStates[op.node]; !ok {
		f.jobStates[op.node] = make(map[string]agentpb.JobState)
	}
	f.jobStates[op.node][op.job] = op.targetState
	outcome := agentpb.Outcome_OUTCOME_UNSPECIFIED
	if op.opType == "kill" {
		outcome = agentpb.Outcome_OUTCOME_KILLED
		if _, ok := f.lastOutcomes[op.node]; !ok {
			f.lastOutcomes[op.node] = make(map[string]agentpb.Outcome)
		}
		f.lastOutcomes[op.node][op.job] = outcome
	}
	delete(f.pendingOperations, operationID)
	onComplete := f.OnOperationComplete
	f.mu.Unlock()

	if onComplete != nil {
		onComplete(op.node, op.job, op.opType)
	}
	return &agentpb.GetOperationResponse{
		Status:    agentpb.OperationStatus_OPERATION_STATUS_COMPLETE,
		ElapsedMs: 10,
		Outcome:   outcome,
	}, nil
}
