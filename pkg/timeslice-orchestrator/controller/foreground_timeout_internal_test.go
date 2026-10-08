package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/util/workqueue"
)

const (
	timeoutGroup = "group-1"
	timeoutNode  = "node-1"
)

// testOpTimeout is shorter than the 1 s operation poll, so a wait times out
// without a single poll.
const testOpTimeout = 300 * time.Millisecond

// timeoutAgent is a scripted snapshot agent for one node. Snapshot and Restore
// start an operation that stays PENDING until finish is called. The job
// states it reports are set by the test and do not change on their own, so a
// stale SAVED or RUNNING while an operation is pending is what a controller
// without the duplicate guard would act on again.
type timeoutAgent struct {
	mu        sync.Mutex
	states    map[string]agentpb.JobState
	ops       map[string]agentpb.OperationStatus
	lost      map[string]bool
	snapshots int
	restores  int
	nextOp    int
}

func newTimeoutAgent(states map[string]agentpb.JobState) *timeoutAgent {
	return &timeoutAgent{states: states, ops: map[string]agentpb.OperationStatus{}, lost: map[string]bool{}}
}

func (a *timeoutAgent) start(kind string) string {
	a.nextOp++
	id := fmt.Sprintf("%s-%d", kind, a.nextOp)
	a.ops[id] = agentpb.OperationStatus_OPERATION_STATUS_PENDING
	return id
}

// finish completes operation id and sets the job states the agent reports.
func (a *timeoutAgent) finish(id string, states map[string]agentpb.JobState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ops[id] = agentpb.OperationStatus_OPERATION_STATUS_COMPLETE
	for job, s := range states {
		a.states[job] = s
	}
}

// forget makes the agent answer NotFound for operation id, as after an agent
// restart.
func (a *timeoutAgent) forget(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lost[id] = true
}

// agentCalls counts the Snapshot and Restore calls the agent received.
type agentCalls struct {
	snapshots int
	restores  int
}

func (a *timeoutAgent) counts() agentCalls {
	a.mu.Lock()
	defer a.mu.Unlock()
	return agentCalls{snapshots: a.snapshots, restores: a.restores}
}

func (a *timeoutAgent) store() *MockSnapshotAgentStore {
	return &MockSnapshotAgentStore{
		GetStatusFunc: func(context.Context, string) (*agentpb.StatusResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			resp := &agentpb.StatusResponse{}
			for job, s := range a.states {
				resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: s})
			}
			return resp, nil
		},
		SnapshotFunc: func(context.Context, string, string, string) (*agentpb.SnapshotResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.snapshots++
			return &agentpb.SnapshotResponse{OperationId: a.start("snap")}, nil
		},
		RestoreFunc: func(context.Context, string, string, string) (*agentpb.RestoreResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.restores++
			return &agentpb.RestoreResponse{OperationId: a.start("restore")}, nil
		},
		OperationFunc: func(_ context.Context, _, id string) (*agentpb.GetOperationResponse, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.lost[id] {
				return nil, status.Errorf(codes.NotFound, "operation %s not found", id)
			}
			s, ok := a.ops[id]
			if !ok {
				return nil, status.Errorf(codes.NotFound, "operation %s not found", id)
			}
			return &agentpb.GetOperationResponse{Status: s}, nil
		},
	}
}

// timeoutNodes is the infrastructure observer: the group has one node.
type timeoutNodes struct{ groups *store.GroupStore }

func (timeoutNodes) Init(context.Context) error { return nil }

func (o timeoutNodes) ObserveGroupState(ctx context.Context, groupID string) error {
	group, _, err := o.groups.GetOrCreate(ctx, groupID)
	if err != nil {
		return err
	}
	group.Status().SetNodes([]string{timeoutNode})
	return nil
}

type timeoutFixture struct {
	ctrl   *Controller
	agent  *timeoutAgent
	groups *store.GroupStore
	jobs   *store.JobStore
	queue  workqueue.TypedRateLimitingInterface[string]
}

// newTimeoutFixture holds the group lock for "trainer". The agent reports the
// given job states; every job in states has pods.
func newTimeoutFixture(t *testing.T, action string, states map[string]agentpb.JobState) *timeoutFixture {
	t.Helper()
	ctx := context.Background()
	lockStore := store.NewMemLockStore()
	if err := lockStore.Lock(ctx, timeoutGroup, "trainer"); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}
	groups := store.NewGroupStore(lockStore)
	jobs := store.NewJobStore()
	for jobID := range states {
		job := store.NewJob(timeoutGroup, jobID)
		job.SetPods([]string{jobID + "-pod-1"})
		if err := jobs.Put(ctx, job); err != nil {
			t.Fatalf("failed to put job: %v", err)
		}
	}
	agent := newTimeoutAgent(states)
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		NewRateLimiter(time.Second, 30*time.Second),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "timeout-" + t.Name()},
	)
	t.Cleanup(queue.ShutDown)
	ctrl := NewController(groups, jobs, queue, timeoutNodes{groups: groups}, agent.store())
	ctrl.ForegroundOpTimeout = testOpTimeout
	ctrl.ForegroundOpTimeoutAction = action
	return &timeoutFixture{ctrl: ctrl, agent: agent, groups: groups, jobs: jobs, queue: queue}
}

func (f *timeoutFixture) reconcile(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.ctrl.reconcileGroup(ctx, timeoutGroup)
}

func (f *timeoutFixture) state(t *testing.T, jobID string) pb.SnapshotAgentJobState_State {
	t.Helper()
	job, err := f.jobs.Get(context.Background(), timeoutGroup, jobID)
	if err != nil {
		t.Fatalf("failed to get job %s: %v", jobID, err)
	}
	return job.ContextState()[timeoutNode]
}

func (f *timeoutFixture) loaded(t *testing.T) string {
	t.Helper()
	group, err := f.groups.Get(context.Background(), timeoutGroup)
	if err != nil {
		t.Fatalf("failed to get group: %v", err)
	}
	return group.Status().LoadedJob()
}

// captureLogs sends slog output to a buffer for the rest of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// trainerSaved is the restore scenario: the trainer holds the lock and its
// context is SAVED, so reconcile restores it.
func trainerSaved() map[string]agentpb.JobState {
	return map[string]agentpb.JobState{"trainer": agentpb.JobState_JOB_STATE_SAVED}
}

// otherRunning is the snapshot scenario: another job is RUNNING and must be
// snapshotted before the trainer is restored.
func otherRunning() map[string]agentpb.JobState {
	return map[string]agentpb.JobState{
		"other":   agentpb.JobState_JOB_STATE_RUNNING,
		"trainer": agentpb.JobState_JOB_STATE_SAVED,
	}
}

func TestForegroundOpTimeout_ValidateAction(t *testing.T) {
	for _, action := range []string{
		ForegroundOpTimeoutActionRetry, ForegroundOpTimeoutActionFaulted, ForegroundOpTimeoutActionBounded,
	} {
		if err := ValidateForegroundOpTimeoutAction(action); err != nil {
			t.Errorf("ValidateForegroundOpTimeoutAction(%q) = %v, want nil", action, err)
		}
	}
	for _, action := range []string{"", "fault", "RETRY"} {
		if err := ValidateForegroundOpTimeoutAction(action); err == nil {
			t.Errorf("ValidateForegroundOpTimeoutAction(%q) = nil, want an error", action)
		}
	}
}

// TestForegroundOpTimeout_Retry_IsDefault keeps today's behaviour when the
// flag is not set.
func TestForegroundOpTimeout_Retry_IsDefault(t *testing.T) {
	ctrl := NewController(nil, nil, nil, nil, nil)
	if ctrl.ForegroundOpTimeoutAction != ForegroundOpTimeoutActionRetry {
		t.Errorf("default action = %q, want %q", ctrl.ForegroundOpTimeoutAction, ForegroundOpTimeoutActionRetry)
	}
	if ctrl.ForegroundOpTimeoutRetries != DefaultForegroundOpTimeoutRetries {
		t.Errorf("default retries = %d, want %d", ctrl.ForegroundOpTimeoutRetries, DefaultForegroundOpTimeoutRetries)
	}
	if got := (&Controller{}).foregroundOpTimeoutAction(); got != ForegroundOpTimeoutActionRetry {
		t.Errorf("empty action = %q, want %q", got, ForegroundOpTimeoutActionRetry)
	}
}

// TestForegroundOpTimeout_Retry_RestoreNoDuplicate: a restore passes the
// timeout while the agent keeps reporting SAVED. Every pass returns an error
// (the group is retried), none issues a second Restore, and the job is never
// FAULTED. When the operation finishes, the next pass loads the trainer.
func TestForegroundOpTimeout_Retry_RestoreNoDuplicate(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionRetry, trainerSaved())
	logs := captureLogs(t)

	for pass := 1; pass <= 3; pass++ {
		err := fx.reconcile(t)
		if !errors.Is(err, ErrForegroundOpTimeout) {
			t.Fatalf("pass %d: reconcile = %v, want a foreground operation timeout", pass, err)
		}
		if restores := fx.agent.counts().restores; restores != 1 {
			t.Fatalf("pass %d: Restore calls = %d, want 1 (no duplicate while the first is pending)", pass, restores)
		}
		if got := fx.state(t, "trainer"); got == pb.SnapshotAgentJobState_STATE_FAULTED {
			t.Fatalf("pass %d: trainer is FAULTED under the retry action", pass)
		}
	}
	for _, want := range []string{
		"Foreground operation timed out", "action=retry", "type=restore",
		"job=trainer", "group=group-1", "operation_id=restore-1", "outcome=retry",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %q", want)
		}
	}

	fx.agent.finish("restore-1", map[string]agentpb.JobState{"trainer": agentpb.JobState_JOB_STATE_RUNNING})
	if err := fx.reconcile(t); err != nil {
		t.Fatalf("reconcile after the operation finished: %v", err)
	}
	if got := fx.loaded(t); got != "trainer" {
		t.Errorf("LoadedJob = %q, want trainer", got)
	}
	if restores := fx.agent.counts().restores; restores != 1 {
		t.Errorf("Restore calls = %d, want 1", restores)
	}
}

// TestForegroundOpTimeout_Retry_SnapshotNoDuplicate is the snapshot side: the
// outgoing job's snapshot hangs while the agent still says RUNNING. No second
// Snapshot, and the trainer is never restored over the outgoing context.
func TestForegroundOpTimeout_Retry_SnapshotNoDuplicate(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionRetry, otherRunning())

	for pass := 1; pass <= 3; pass++ {
		if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
			t.Fatalf("pass %d: reconcile = %v, want a foreground operation timeout", pass, err)
		}
		calls := fx.agent.counts()
		snapshots, restores := calls.snapshots, calls.restores
		if snapshots != 1 || restores != 0 {
			t.Fatalf("pass %d: Snapshot, Restore calls = %d, %d, want 1, 0", pass, snapshots, restores)
		}
	}

	fx.agent.finish("snap-1", map[string]agentpb.JobState{"other": agentpb.JobState_JOB_STATE_SAVED})
	// The snapshot is done, so this pass restores the trainer, which hangs in
	// turn: that is one new operation, not a duplicate.
	if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
		t.Fatalf("reconcile after the snapshot = %v, want the restore to time out", err)
	}
	if calls := fx.agent.counts(); calls.snapshots != 1 || calls.restores != 1 {
		t.Errorf("Snapshot, Restore calls = %d, %d, want 1, 1", calls.snapshots, calls.restores)
	}
}

// TestForegroundOpTimeout_Retry_UsesRateLimiter: a timed-out pass goes back
// on the queue through the rate limiter (the Q13 1 s / 30 s backoff).
func TestForegroundOpTimeout_Retry_UsesRateLimiter(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionRetry, trainerSaved())
	queue := fx.queue
	queue.Add(timeoutGroup)
	fx.ctrl.processNextWorkItem(context.Background())
	if got := queue.NumRequeues(timeoutGroup); got != 1 {
		t.Errorf("NumRequeues after a timed-out pass = %d, want 1", got)
	}
}

// TestForegroundOpTimeout_Retry_LostOperationIsForgotten: if the agent no
// longer knows the timed-out operation (it restarted), the guard lets go and
// the next pass acts on the agent's state again.
func TestForegroundOpTimeout_Retry_LostOperationIsForgotten(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionRetry, trainerSaved())
	if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
		t.Fatalf("reconcile = %v, want a foreground operation timeout", err)
	}
	fx.agent.forget("restore-1")
	if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
		t.Fatalf("second reconcile = %v, want the new restore to time out", err)
	}
	if restores := fx.agent.counts().restores; restores != 2 {
		t.Errorf("Restore calls = %d, want 2 (a new restore once the old one is unknown)", restores)
	}
}

// TestForegroundOpTimeout_Faulted_Restore: the first timeout marks the trainer
// FAULTED on the node. Later passes issue nothing, and the fault stays after
// the agent finishes the operation, until the job's pods are replaced.
func TestForegroundOpTimeout_Faulted_Restore(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionFaulted, trainerSaved())
	logs := captureLogs(t)

	err := fx.reconcile(t)
	if !errors.Is(err, ErrForegroundOpTimeout) || !strings.Contains(err.Error(), "marked FAULTED") {
		t.Fatalf("reconcile = %v, want a timeout that marks the job FAULTED", err)
	}
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Fatalf("trainer state = %v, want FAULTED", got)
	}
	job, err := fx.jobs.Get(context.Background(), timeoutGroup, "trainer")
	if err != nil {
		t.Fatalf("failed to get trainer: %v", err)
	}
	if op, ok := job.ForegroundTimeoutFault(timeoutNode); !ok || op != "restore-1" {
		t.Errorf("ForegroundTimeoutFault = %q, %v, want restore-1, true", op, ok)
	}
	for _, want := range []string{"level=ERROR", "Foreground operation timed out", "action=faulted", "outcome=faulted"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %q", want)
		}
	}

	fx.agent.finish("restore-1", map[string]agentpb.JobState{"trainer": agentpb.JobState_JOB_STATE_SAVED})
	for pass := 2; pass <= 3; pass++ {
		if err := fx.reconcile(t); err == nil || !strings.Contains(err.Error(), "FAULTED") {
			t.Fatalf("pass %d: reconcile = %v, want the FAULTED error", pass, err)
		}
	}
	if restores := fx.agent.counts().restores; restores != 1 {
		t.Errorf("Restore calls = %d, want 1", restores)
	}
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Errorf("trainer state after the agent finished = %v, want still FAULTED", got)
	}

	// A pod delete (new pod UID) clears the fault.
	job.SetPods([]string{"trainer-pod-2"})
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_SAVED {
		t.Errorf("trainer state after its pod was replaced = %v, want the agent's SAVED", got)
	}
}

// TestForegroundOpTimeout_Faulted_SnapshotFailsClosed: the outgoing job's
// snapshot times out and it is marked FAULTED. Its context may still be on the
// accelerator, so the trainer is never restored.
func TestForegroundOpTimeout_Faulted_SnapshotFailsClosed(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionFaulted, otherRunning())

	if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
		t.Fatalf("reconcile = %v, want a foreground operation timeout", err)
	}
	if got := fx.state(t, "other"); got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Fatalf("other state = %v, want FAULTED", got)
	}
	// Even once the agent reports the snapshot saved, the mark holds.
	fx.agent.finish("snap-1", map[string]agentpb.JobState{"other": agentpb.JobState_JOB_STATE_SAVED})
	for pass := 2; pass <= 3; pass++ {
		if err := fx.reconcile(t); err == nil || !strings.Contains(err.Error(), "FAULTED") {
			t.Fatalf("pass %d: reconcile = %v, want the FAULTED error", pass, err)
		}
	}
	if calls := fx.agent.counts(); calls.snapshots != 1 || calls.restores != 0 {
		t.Errorf("Snapshot, Restore calls = %d, %d, want 1, 0", calls.snapshots, calls.restores)
	}
}

// TestForegroundOpTimeout_Bounded_RetriesThenFaults: with 2 retries, the
// first two timeouts retry the same operation and the third marks the job
// FAULTED. Only one Restore is ever issued.
func TestForegroundOpTimeout_Bounded_RetriesThenFaults(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionBounded, trainerSaved())
	fx.ctrl.ForegroundOpTimeoutRetries = 2
	logs := captureLogs(t)

	for pass := 1; pass <= 2; pass++ {
		err := fx.reconcile(t)
		if !errors.Is(err, ErrForegroundOpTimeout) || strings.Contains(err.Error(), "FAULTED") {
			t.Fatalf("pass %d: reconcile = %v, want a timeout that retries", pass, err)
		}
		if got := fx.state(t, "trainer"); got == pb.SnapshotAgentJobState_STATE_FAULTED {
			t.Fatalf("pass %d: trainer FAULTED before the bound", pass)
		}
	}
	err := fx.reconcile(t)
	if !errors.Is(err, ErrForegroundOpTimeout) || !strings.Contains(err.Error(), "marked FAULTED") {
		t.Fatalf("pass 3: reconcile = %v, want the job marked FAULTED", err)
	}
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Errorf("trainer state = %v, want FAULTED", got)
	}
	if restores := fx.agent.counts().restores; restores != 1 {
		t.Errorf("Restore calls = %d, want 1", restores)
	}
	for _, want := range []string{"action=bounded", "timeouts=3", "outcome=faulted"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %q", want)
		}
	}
}

// TestForegroundOpTimeout_Bounded_RecoversWithinBound: a slow restore that
// finishes after the first timeout loads the trainer with no fault.
func TestForegroundOpTimeout_Bounded_RecoversWithinBound(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionBounded, trainerSaved())
	fx.ctrl.ForegroundOpTimeoutRetries = 2

	if err := fx.reconcile(t); !errors.Is(err, ErrForegroundOpTimeout) {
		t.Fatalf("reconcile = %v, want a foreground operation timeout", err)
	}
	fx.agent.finish("restore-1", map[string]agentpb.JobState{"trainer": agentpb.JobState_JOB_STATE_RUNNING})
	if err := fx.reconcile(t); err != nil {
		t.Fatalf("reconcile after the restore finished: %v", err)
	}
	if got := fx.loaded(t); got != "trainer" {
		t.Errorf("LoadedJob = %q, want trainer", got)
	}
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_RUNNING {
		t.Errorf("trainer state = %v, want RUNNING", got)
	}
	if restores := fx.agent.counts().restores; restores != 1 {
		t.Errorf("Restore calls = %d, want 1", restores)
	}
}

// TestForegroundOpTimeout_Bounded_ZeroRetriesFaultsAtOnce matches faulted.
func TestForegroundOpTimeout_Bounded_ZeroRetriesFaultsAtOnce(t *testing.T) {
	fx := newTimeoutFixture(t, ForegroundOpTimeoutActionBounded, trainerSaved())
	fx.ctrl.ForegroundOpTimeoutRetries = 0

	if err := fx.reconcile(t); err == nil || !strings.Contains(err.Error(), "marked FAULTED") {
		t.Fatalf("reconcile = %v, want the job marked FAULTED at the first timeout", err)
	}
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_FAULTED {
		t.Errorf("trainer state = %v, want FAULTED", got)
	}
}
