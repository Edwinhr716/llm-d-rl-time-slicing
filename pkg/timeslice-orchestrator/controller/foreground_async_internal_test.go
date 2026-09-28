package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/util/workqueue"
)

// Tests for foreground wait option B (async-requeue, D-ORCH-1). They are all
// named TestForegroundWait_Async_* so the collapse agent can find them.

// asyncFakeAgent is a scripted snapshot agent. Snapshot and Restore put the job
// in TRANSITIONING and leave the operation PENDING until the test completes,
// fails or keeps it pending.
type asyncFakeAgent struct {
	store.SnapshotAgentStore

	mu        sync.Mutex
	states    map[string]map[string]agentpb.JobState // node -> job -> state
	ops       map[string]*asyncFakeOp
	counter   int
	snapshots int
	restores  int
	getOps    int
	getOpErr  error
}

type asyncFakeOp struct {
	node, job string
	target    agentpb.JobState
	status    agentpb.OperationStatus
}

func newAsyncFakeAgent() *asyncFakeAgent {
	return &asyncFakeAgent{
		states: make(map[string]map[string]agentpb.JobState),
		ops:    make(map[string]*asyncFakeOp),
	}
}

func (f *asyncFakeAgent) setState(node, job string, state agentpb.JobState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states[node] == nil {
		f.states[node] = make(map[string]agentpb.JobState)
	}
	f.states[node][job] = state
}

func (f *asyncFakeAgent) GetStatus(_ context.Context, node string) (*agentpb.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for job, state := range f.states[node] {
		resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: state})
	}
	return resp, nil
}

func (f *asyncFakeAgent) start(node, job, kind string, target agentpb.JobState) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counter++
	id := fmt.Sprintf("op-%s-%d", kind, f.counter)
	f.ops[id] = &asyncFakeOp{node: node, job: job, target: target, status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}
	if f.states[node] == nil {
		f.states[node] = make(map[string]agentpb.JobState)
	}
	f.states[node][job] = agentpb.JobState_JOB_STATE_TRANSITIONING
	return id
}

func (f *asyncFakeAgent) Snapshot(_ context.Context, node, job, _ string) (*agentpb.SnapshotResponse, error) {
	id := f.start(node, job, "snapshot", agentpb.JobState_JOB_STATE_SAVED)
	f.mu.Lock()
	f.snapshots++
	f.mu.Unlock()
	return &agentpb.SnapshotResponse{OperationId: id}, nil
}

func (f *asyncFakeAgent) Restore(_ context.Context, node, job, _ string) (*agentpb.RestoreResponse, error) {
	id := f.start(node, job, "restore", agentpb.JobState_JOB_STATE_RUNNING)
	f.mu.Lock()
	f.restores++
	f.mu.Unlock()
	return &agentpb.RestoreResponse{OperationId: id}, nil
}

func (f *asyncFakeAgent) GetOperation(_ context.Context, _, id string) (*agentpb.GetOperationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getOps++
	if f.getOpErr != nil {
		return nil, f.getOpErr
	}
	op, ok := f.ops[id]
	if !ok {
		return nil, fmt.Errorf("operation %s not found", id)
	}
	resp := &agentpb.GetOperationResponse{Status: op.status}
	if op.status == agentpb.OperationStatus_OPERATION_STATUS_FAILED {
		msg := "boom"
		resp.Error = &msg
	}
	return resp, nil
}

// finishAll completes (or fails) every pending operation.
func (f *asyncFakeAgent) finishAll(status agentpb.OperationStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.ops {
		if op.status != agentpb.OperationStatus_OPERATION_STATUS_PENDING {
			continue
		}
		op.status = status
		if status == agentpb.OperationStatus_OPERATION_STATUS_COMPLETE {
			f.states[op.node][op.job] = op.target
		} else {
			f.states[op.node][op.job] = agentpb.JobState_JOB_STATE_FAULTED
		}
	}
}

// agentCalls counts the operation RPCs the controller made.
type agentCalls struct{ snapshots, restores, getOps int }

func (f *asyncFakeAgent) counts() agentCalls {
	f.mu.Lock()
	defer f.mu.Unlock()
	return agentCalls{snapshots: f.snapshots, restores: f.restores, getOps: f.getOps}
}

type noopInfra struct{}

func (noopInfra) Init(context.Context) error                      { return nil }
func (noopInfra) ObserveGroupState(context.Context, string) error { return nil }

// recordingQueue records how the controller requeues a group.
type recordingQueue struct {
	workqueue.TypedRateLimitingInterface[string]
	mu          sync.Mutex
	rateLimited int
	forgets     int
	addAfters   []time.Duration
}

func newRecordingQueue() *recordingQueue {
	return &recordingQueue{TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "async-test"},
	)}
}

func (q *recordingQueue) AddRateLimited(item string) {
	q.mu.Lock()
	q.rateLimited++
	q.mu.Unlock()
	q.TypedRateLimitingInterface.AddRateLimited(item)
}

func (q *recordingQueue) Forget(item string) {
	q.mu.Lock()
	q.forgets++
	q.mu.Unlock()
	q.TypedRateLimitingInterface.Forget(item)
}

func (q *recordingQueue) AddAfter(item string, d time.Duration) {
	q.mu.Lock()
	q.addAfters = append(q.addAfters, d)
	q.mu.Unlock()
	q.TypedRateLimitingInterface.AddAfter(item, d)
}

// logCapture records slog messages with their attributes.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // slog.Handler.Handle signature requires passing Record by value
func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})
	l.mu.Lock()
	l.lines = append(l.lines, b.String())
	l.mu.Unlock()
	return nil
}
func (l *logCapture) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logCapture) WithGroup(string) slog.Handler      { return l }

// has reports whether a line contains the message and every fragment.
func (l *logCapture) has(msg string, fragments ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if !strings.HasPrefix(line, msg) {
			continue
		}
		ok := true
		for _, f := range fragments {
			if !strings.Contains(line, f) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func captureLogs(t *testing.T) *logCapture {
	t.Helper()
	capture := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return capture
}

type asyncFixture struct {
	ctrl   *Controller
	agent  *asyncFakeAgent
	groups *store.GroupStore
	jobs   *store.JobStore
	queue  *recordingQueue
}

// newAsyncFixture builds an async-mode controller. Each job is registered in
// the job store on every node with the agent state given.
func newAsyncFixture(t *testing.T, nodes []string, jobStates map[string]agentpb.JobState) *asyncFixture {
	t.Helper()
	groups := store.NewGroupStore(store.NewMemLockStore())
	jobs := store.NewJobStore()
	agent := newAsyncFakeAgent()
	queue := newRecordingQueue()
	t.Cleanup(queue.ShutDown)
	ctx := context.Background()
	for jobID, state := range jobStates {
		for _, node := range nodes {
			agent.setState(node, jobID, state)
		}
		if err := jobs.Put(ctx, store.NewJob("g1", jobID)); err != nil {
			t.Fatalf("put job: %v", err)
		}
	}
	group, _, err := groups.GetOrCreate(ctx, "g1")
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	group.Status().SetNodes(nodes)
	ctrl := NewController(groups, jobs, queue, noopInfra{}, agent)
	ctrl.ForegroundWait = ForegroundWaitAsyncRequeue
	return &asyncFixture{ctrl: ctrl, agent: agent, groups: groups, jobs: jobs, queue: queue}
}

func (f *asyncFixture) group(t *testing.T) *store.Group {
	t.Helper()
	g, err := f.groups.Get(context.Background(), "g1")
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	return g
}

func TestForegroundWait_Async_RestoreDoesNotBlockWorker(t *testing.T) {
	logs := captureLogs(t)
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()

	start := time.Now()
	err := fx.ctrl.reconcileGroup(ctx, "g1")
	if !errors.Is(err, errForegroundPending) {
		t.Fatalf("first reconcile = %v, want errForegroundPending", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("reconcile took %v with the restore pending; the worker must not block", elapsed)
	}
	if op := fx.ctrl.getForegroundOp("g1", "n1"); op == nil || op.opType != "restore" || op.operationID == "" {
		t.Fatalf("in-flight record = %+v, want a restore with its operation ID", op)
	}
	if !logs.has("Foreground operation started", "group=g1", "job=job-a", "type=restore", "operation_id=op-restore-") {
		t.Errorf("missing started log line, got %v", logs.lines)
	}

	// Still pending: one GetOperation, no second Restore.
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("second reconcile = %v, want errForegroundPending", err)
	}
	if n := fx.agent.counts(); n.restores != 1 || n.getOps != 1 {
		t.Errorf("restores=%d getOps=%d, want 1 and 1", n.restores, n.getOps)
	}
	if fx.group(t).Status().LoadedJob() == "job-a" {
		t.Error("job-a reported loaded while its restore is pending")
	}

	fx.agent.finishAll(agentpb.OperationStatus_OPERATION_STATUS_COMPLETE)
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); err != nil {
		t.Fatalf("reconcile after completion = %v, want nil", err)
	}
	if got := fx.group(t).Status().LoadedJob(); got != "job-a" {
		t.Errorf("LoadedJob = %q, want job-a (Acquire returns on this)", got)
	}
	if fx.ctrl.getForegroundOp("g1", "n1") != nil {
		t.Error("record kept after completion")
	}
	if !logs.has("Foreground operation finished", "job=job-a", "type=restore", "outcome=complete") {
		t.Errorf("missing finished log line, got %v", logs.lines)
	}
}

func TestForegroundWait_Async_SnapshotThenRestore(t *testing.T) {
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{
		"job-a": agentpb.JobState_JOB_STATE_SAVED,
		"job-b": agentpb.JobState_JOB_STATE_RUNNING,
	})
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()

	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 1 = %v, want pending snapshot", err)
	}
	if op := fx.ctrl.getForegroundOp("g1", "n1"); op == nil || op.opType != "snapshot" || op.jobID != "job-b" {
		t.Fatalf("record = %+v, want snapshot of job-b", op)
	}
	fx.agent.finishAll(agentpb.OperationStatus_OPERATION_STATUS_COMPLETE)

	// The snapshot completes and the restore starts in the same pass.
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 2 = %v, want pending restore", err)
	}
	if op := fx.ctrl.getForegroundOp("g1", "n1"); op == nil || op.opType != "restore" || op.jobID != "job-a" {
		t.Fatalf("record = %+v, want restore of job-a", op)
	}
	fx.agent.finishAll(agentpb.OperationStatus_OPERATION_STATUS_COMPLETE)

	if err := fx.ctrl.reconcileGroup(ctx, "g1"); err != nil {
		t.Fatalf("pass 3 = %v, want nil", err)
	}
	if got := fx.group(t).Status().LoadedJob(); got != "job-a" {
		t.Errorf("LoadedJob = %q, want job-a", got)
	}
	if n := fx.agent.counts(); n.snapshots != 1 || n.restores != 1 {
		t.Errorf("snapshots=%d restores=%d, want 1 and 1", n.snapshots, n.restores)
	}
}

func TestForegroundWait_Async_OperationFailed(t *testing.T) {
	logs := captureLogs(t)
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()

	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 1 = %v, want pending", err)
	}
	fx.agent.finishAll(agentpb.OperationStatus_OPERATION_STATUS_FAILED)
	err := fx.ctrl.reconcileGroup(ctx, "g1")
	if err == nil || errors.Is(err, errForegroundPending) || !strings.Contains(err.Error(), "failed: boom") {
		t.Fatalf("pass 2 = %v, want the operation failure", err)
	}
	if fx.ctrl.getForegroundOp("g1", "n1") != nil {
		t.Error("record kept after failure")
	}
	if !logs.has("Foreground operation finished", "outcome=failed") {
		t.Errorf("missing failed log line, got %v", logs.lines)
	}
}

func TestForegroundWait_Async_Timeout(t *testing.T) {
	logs := captureLogs(t)
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.ctrl.ForegroundOpTimeout = 50 * time.Millisecond
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()

	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 1 = %v, want pending", err)
	}
	time.Sleep(100 * time.Millisecond)
	err := fx.ctrl.reconcileGroup(ctx, "g1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pass 2 = %v, want an error wrapping context.DeadlineExceeded", err)
	}
	if !logs.has("Foreground operation finished", "outcome=timeout", "type=restore") {
		t.Errorf("missing timeout log line, got %v", logs.lines)
	}

	// After the timeout the base path applies: TRANSITIONING is an error and
	// the operation is not polled or re-issued.
	getOpsBefore := fx.agent.counts().getOps
	err = fx.ctrl.reconcileGroup(ctx, "g1")
	if err == nil || errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 3 = %v, want the base TRANSITIONING error", err)
	}
	if n := fx.agent.counts(); n.restores != 1 || n.getOps != getOpsBefore {
		t.Errorf("restores=%d getOps=%d (before %d): a timed-out operation must not be re-issued or polled",
			n.restores, n.getOps, getOpsBefore)
	}

	// The agent finishes late: the record goes and the job is loaded.
	fx.agent.finishAll(agentpb.OperationStatus_OPERATION_STATUS_COMPLETE)
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); err != nil {
		t.Fatalf("pass 4 = %v, want nil", err)
	}
	if fx.ctrl.getForegroundOp("g1", "n1") != nil {
		t.Error("timed-out record kept after the job left TRANSITIONING")
	}
	if got := fx.group(t).Status().LoadedJob(); got != "job-a" {
		t.Errorf("LoadedJob = %q, want job-a", got)
	}
}

// TestForegroundWait_Async_LostAfterRestart: a new process finds the job
// TRANSITIONING with no record. It logs the loss, tracks it by agent state and
// never issues a second operation.
func TestForegroundWait_Async_LostAfterRestart(t *testing.T) {
	logs := captureLogs(t)
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_TRANSITIONING})
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()

	for i := range 3 {
		if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
			t.Fatalf("pass %d = %v, want pending", i, err)
		}
	}
	if !logs.has("Foreground operation lost", "group=g1", "job=job-a", "node=n1") {
		t.Errorf("missing lost log line, got %v", logs.lines)
	}
	if n := fx.agent.counts(); n != (agentCalls{}) {
		t.Errorf("snapshots=%d restores=%d getOps=%d, want no agent operation calls", n.snapshots, n.restores, n.getOps)
	}

	fx.agent.setState("n1", "job-a", agentpb.JobState_JOB_STATE_RUNNING)
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); err != nil {
		t.Fatalf("reconcile after the agent finished = %v, want nil", err)
	}
	if !logs.has("Foreground operation finished", "job=job-a", "outcome=complete", "type=unknown") {
		t.Errorf("missing finished log line for the lost operation, got %v", logs.lines)
	}
	if got := fx.group(t).Status().LoadedJob(); got != "job-a" {
		t.Errorf("LoadedJob = %q, want job-a", got)
	}
}

func TestForegroundWait_Async_GetOperationErrorKeepsWaiting(t *testing.T) {
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.group(t).Spec().RequestLock("job-a")
	ctx := context.Background()
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 1 = %v, want pending", err)
	}
	fx.agent.mu.Lock()
	fx.agent.getOpErr = errors.New("unavailable")
	fx.agent.mu.Unlock()
	if err := fx.ctrl.reconcileGroup(ctx, "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("pass 2 = %v, want pending on a GetOperation error", err)
	}
	if fx.ctrl.getForegroundOp("g1", "n1") == nil {
		t.Error("record dropped on a transient GetOperation error")
	}
}

func TestForegroundWait_Async_OtherNodesStillReconciled(t *testing.T) {
	fx := newAsyncFixture(t, []string{"n1", "n2"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.group(t).Spec().RequestLock("job-a")
	if err := fx.ctrl.reconcileGroup(context.Background(), "g1"); !errors.Is(err, errForegroundPending) {
		t.Fatalf("reconcile = %v, want pending", err)
	}
	if n := fx.agent.counts(); n.restores != 2 {
		t.Errorf("restores = %d, want one per node started in the same pass", n.restores)
	}
}

func TestForegroundWait_Async_PendingRequeuesWithoutRateLimit(t *testing.T) {
	fx := newAsyncFixture(t, []string{"n1"}, map[string]agentpb.JobState{"job-a": agentpb.JobState_JOB_STATE_SAVED})
	fx.group(t).Spec().RequestLock("job-a")
	fx.queue.Add("g1")
	if !fx.ctrl.processNextWorkItem(context.Background()) {
		t.Fatal("processNextWorkItem reported shutdown")
	}
	fx.queue.mu.Lock()
	defer fx.queue.mu.Unlock()
	if fx.queue.rateLimited != 0 {
		t.Errorf("AddRateLimited called %d times, want 0 for an in-flight operation", fx.queue.rateLimited)
	}
	if fx.queue.forgets != 1 || len(fx.queue.addAfters) != 1 || fx.queue.addAfters[0] != foregroundCheckInterval {
		t.Errorf("forgets=%d addAfters=%v, want 1 and [%v]", fx.queue.forgets, fx.queue.addAfters, foregroundCheckInterval)
	}
}

// TestForegroundWait_Async_HungGroupDoesNotBlockOthers runs the controller
// with one worker. Group g1's restore never finishes; group g2's finishes at
// once. g2 must be loaded without waiting for g1's --foreground-op-timeout.
func TestForegroundWait_Async_HungGroupDoesNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groups := store.NewGroupStore(store.NewMemLockStore())
	jobs := store.NewJobStore()
	hung := newAsyncFakeAgent()
	hung.setState("n1", "job-1", agentpb.JobState_JOB_STATE_SAVED)
	hung.setState("n2", "job-2", agentpb.JobState_JOB_STATE_SAVED)
	agent := &splitAgent{hungNode: "n1", ok: hung}
	queue := newRecordingQueue()
	defer queue.ShutDown()

	for _, g := range []struct{ id, node, job string }{{"g1", "n1", "job-1"}, {"g2", "n2", "job-2"}} {
		group, _, err := groups.GetOrCreate(ctx, g.id)
		if err != nil {
			t.Fatalf("create group: %v", err)
		}
		group.Status().SetNodes([]string{g.node})
		if err := jobs.Put(ctx, store.NewJob(g.id, g.job)); err != nil {
			t.Fatalf("put job: %v", err)
		}
		group.Spec().RequestLock(g.job)
	}

	ctrl := NewController(groups, jobs, queue, noopInfra{}, agent)
	ctrl.ForegroundWait = ForegroundWaitAsyncRequeue
	ctrl.ForegroundOpTimeout = time.Minute
	ctrl.ResyncPeriod = time.Hour
	go func() { _ = ctrl.Run(ctx, 1) }() //nolint:errcheck // Run only fails on Init, which cannot fail here
	ctrl.EnqueueWork("g1")
	ctrl.EnqueueWork("g2")

	deadline := time.Now().Add(5 * time.Second)
	for {
		g2, err := groups.Get(ctx, "g2")
		if err == nil && g2.Status().LoadedJob() == "job-2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("g2 was not loaded within 5 s while g1's restore hangs")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if op := ctrl.getForegroundOp("g1", "n1"); op == nil {
		t.Error("g1's hung restore is no longer tracked")
	}
}

// splitAgent completes operations at once except on hungNode, where they stay
// pending forever.
type splitAgent struct {
	store.SnapshotAgentStore
	hungNode string
	ok       *asyncFakeAgent
}

func (s *splitAgent) GetStatus(ctx context.Context, node string) (*agentpb.StatusResponse, error) {
	return s.ok.GetStatus(ctx, node)
}

func (s *splitAgent) Restore(ctx context.Context, node, job, group string) (*agentpb.RestoreResponse, error) {
	return s.ok.Restore(ctx, node, job, group)
}

func (s *splitAgent) Snapshot(ctx context.Context, node, job, group string) (*agentpb.SnapshotResponse, error) {
	return s.ok.Snapshot(ctx, node, job, group)
}

func (s *splitAgent) GetOperation(ctx context.Context, node, id string) (*agentpb.GetOperationResponse, error) {
	if node == s.hungNode {
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
	}
	s.ok.mu.Lock()
	op := s.ok.ops[id]
	op.status = agentpb.OperationStatus_OPERATION_STATUS_COMPLETE
	s.ok.states[op.node][op.job] = op.target
	s.ok.mu.Unlock()
	return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
}
