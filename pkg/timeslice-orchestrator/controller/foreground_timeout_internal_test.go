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
// that ignored the FAULTED mark would act on.
type timeoutAgent struct {
	mu        sync.Mutex
	states    map[string]agentpb.JobState
	ops       map[string]agentpb.OperationStatus
	snapshots int
	restores  int
	nextOp    int
}

func newTimeoutAgent(states map[string]agentpb.JobState) *timeoutAgent {
	return &timeoutAgent{states: states, ops: map[string]agentpb.OperationStatus{}}
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
}

// newTimeoutFixture holds the group lock for "trainer". The agent reports the
// given job states; every job in states has pods.
func newTimeoutFixture(t *testing.T, states map[string]agentpb.JobState) *timeoutFixture {
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
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "timeout-" + t.Name()},
	)
	t.Cleanup(queue.ShutDown)
	ctrl := NewController(groups, jobs, queue, timeoutNodes{groups: groups}, agent.store())
	ctrl.ForegroundOpTimeout = testOpTimeout
	return &timeoutFixture{ctrl: ctrl, agent: agent, groups: groups, jobs: jobs}
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

// TestForegroundOpTimeout_Restore: the first timeout marks the trainer
// FAULTED on the node. Later passes issue nothing, and the fault stays after
// the agent finishes the operation, until the job's pods are replaced.
func TestForegroundOpTimeout_Restore(t *testing.T) {
	fx := newTimeoutFixture(t, trainerSaved())
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
	for _, want := range []string{"level=ERROR", "Foreground operation timed out", "outcome=faulted"} {
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
	if got := fx.loaded(t); got == "trainer" {
		t.Error("trainer reported loaded while FAULTED")
	}

	// A pod delete (new pod UID) clears the fault.
	job.SetPods([]string{"trainer-pod-2"})
	if got := fx.state(t, "trainer"); got != pb.SnapshotAgentJobState_STATE_SAVED {
		t.Errorf("trainer state after its pod was replaced = %v, want the agent's SAVED", got)
	}
}

// TestForegroundOpTimeout_SnapshotFailsClosed: the outgoing job's
// snapshot times out and it is marked FAULTED. Its context may still be on the
// accelerator, so the trainer is never restored.
func TestForegroundOpTimeout_SnapshotFailsClosed(t *testing.T) {
	fx := newTimeoutFixture(t, otherRunning())

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
