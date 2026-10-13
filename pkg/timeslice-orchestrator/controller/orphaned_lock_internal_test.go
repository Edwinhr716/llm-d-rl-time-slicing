package controller

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	orphanGroup = "group-1"
	orphanGrace = 200 * time.Millisecond
)

// afterQueue records the delays passed to AddAfter. reconcileGroup is called
// directly, so nothing reads from it.
type afterQueue struct {
	mu    sync.Mutex
	after []time.Duration
}

func (q *afterQueue) Add(string)            {}
func (q *afterQueue) AddRateLimited(string) {}
func (q *afterQueue) Forget(string)         {}
func (q *afterQueue) Done(string)           {}
func (q *afterQueue) Get() (string, bool)   { return "", true }
func (q *afterQueue) ShutDown()             {}
func (q *afterQueue) AddAfter(_ string, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.after = append(q.after, d)
}

func (q *afterQueue) delays() []time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]time.Duration(nil), q.after...)
}

// orphanFixture is a group locked by "trainer" with "next" waiting. The agent
// reports state for the trainer while the trainer is in the job store.
type orphanFixture struct {
	ctrl   *Controller
	locks  *store.MemLockStore
	groups *store.GroupStore
	jobs   *store.JobStore
	queue  *afterQueue
	state  atomic.Int32
}

func newOrphanFixture(t *testing.T, trainerHasPods bool) *orphanFixture {
	t.Helper()
	ctx := context.Background()
	fx := &orphanFixture{locks: store.NewMemLockStore(), jobs: store.NewJobStore(), queue: &afterQueue{}}
	fx.state.Store(int32(agentpb.JobState_JOB_STATE_RUNNING))
	if err := fx.locks.Lock(ctx, orphanGroup, "trainer"); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}
	fx.groups = store.NewGroupStore(fx.locks)
	group, _, err := fx.groups.GetOrCreate(ctx, orphanGroup)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	group.Spec().RequestLock("next")
	if trainerHasPods {
		fx.addTrainer(t)
	}
	agents := &MockSnapshotAgentStore{
		GetStatusFunc: func(ctx context.Context, _ string) (*agentpb.StatusResponse, error) {
			jobs, err := fx.jobs.ListByGroup(ctx, orphanGroup)
			if err != nil {
				return nil, err
			}
			resp := &agentpb.StatusResponse{}
			for _, job := range jobs {
				resp.JobStatuses = append(resp.JobStatuses,
					&agentpb.JobStatus{JobId: job.JobID(), State: agentpb.JobState(fx.state.Load())})
			}
			return resp, nil
		},
	}
	fx.ctrl = NewController(fx.groups, fx.jobs, fx.queue, timeoutNodes{groups: fx.groups}, agents)
	fx.ctrl.OrphanedLockGrace = orphanGrace
	return fx
}

func (fx *orphanFixture) addTrainer(t *testing.T) {
	t.Helper()
	job := store.NewJob(orphanGroup, "trainer")
	job.SetPods([]string{"trainer-pod-1"})
	if err := fx.jobs.Put(context.Background(), job); err != nil {
		t.Fatalf("failed to put trainer: %v", err)
	}
}

// removeTrainer is what the infrastructure observer does when the trainer's
// pods are gone.
func (fx *orphanFixture) removeTrainer(t *testing.T) {
	t.Helper()
	if err := fx.jobs.Delete(context.Background(), orphanGroup, "trainer"); err != nil {
		t.Fatalf("failed to delete trainer: %v", err)
	}
}

func (fx *orphanFixture) reconcile(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return fx.ctrl.reconcileGroup(ctx, orphanGroup)
}

func (fx *orphanFixture) mustReconcile(t *testing.T) {
	t.Helper()
	if err := fx.reconcile(t); err != nil {
		t.Fatalf("reconcileGroup: %v", err)
	}
}

// wantHolder checks the lock holder in the group and in the lock store.
func (fx *orphanFixture) wantHolder(t *testing.T, want string) {
	t.Helper()
	ctx := context.Background()
	group, err := fx.groups.Get(ctx, orphanGroup)
	if err != nil {
		t.Fatalf("failed to get group: %v", err)
	}
	stored, err := fx.locks.GetLock(ctx, orphanGroup)
	if err != nil {
		t.Fatalf("failed to read lock store: %v", err)
	}
	if inGroup := group.Spec().LockingJob(); inGroup != want || stored != want {
		t.Errorf("lock holder = %q (lock store %q), want %q", inGroup, stored, want)
	}
}

// TestOrphanedLock_ReleasedAfterGrace is bug 6: the lock holder's pods were
// deleted while it held the lock, and nothing released it, so the group kept
// the lock (and its node) until a manual Yield. After the grace the controller
// yields on the holder's behalf and the waiter is promoted in the same pass.
func TestOrphanedLock_ReleasedAfterGrace(t *testing.T) {
	logs := captureLogs(t)
	fx := newOrphanFixture(t, true)
	before := testutil.ToFloat64(metrics.OrphanedLockReleasesTotal)

	fx.mustReconcile(t)
	fx.removeTrainer(t)
	fx.mustReconcile(t)
	fx.wantHolder(t, "trainer")
	delays := fx.queue.delays()
	if len(delays) == 0 || delays[len(delays)-1] <= 0 || delays[len(delays)-1] > orphanGrace {
		t.Errorf("AddAfter delays = %v, want a requeue within the grace %v", delays, orphanGrace)
	}

	time.Sleep(orphanGrace + 100*time.Millisecond)
	fx.mustReconcile(t)
	fx.wantHolder(t, "next")
	if got := testutil.ToFloat64(metrics.OrphanedLockReleasesTotal) - before; got != 1 {
		t.Errorf("orphaned lock releases = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), "Released the group lock of a holder whose pods are gone") {
		t.Errorf("no release log line; logs:\n%s", logs.String())
	}
}

// TestOrphanedLock_NeverSeenHolderKept: a job may take the lock before its
// pods are deployed. A holder never seen with pods is not released.
func TestOrphanedLock_NeverSeenHolderKept(t *testing.T) {
	fx := newOrphanFixture(t, false)
	fx.mustReconcile(t)
	time.Sleep(orphanGrace + 100*time.Millisecond)
	fx.mustReconcile(t)
	fx.wantHolder(t, "trainer")
	if delays := fx.queue.delays(); len(delays) != 0 {
		t.Errorf("AddAfter delays = %v, want none", delays)
	}
}

// TestOrphanedLock_PodsBackWithinGraceKept: pods that reappear within the
// grace cancel the release, and a later absence starts a new grace.
func TestOrphanedLock_PodsBackWithinGraceKept(t *testing.T) {
	fx := newOrphanFixture(t, true)
	fx.mustReconcile(t)
	fx.removeTrainer(t)
	fx.mustReconcile(t)
	fx.addTrainer(t)
	fx.mustReconcile(t)
	time.Sleep(orphanGrace + 100*time.Millisecond)
	fx.mustReconcile(t)
	fx.wantHolder(t, "trainer")

	fx.removeTrainer(t)
	fx.mustReconcile(t)
	fx.wantHolder(t, "trainer")
}

// TestOrphanedLock_FaultedWithPodsFailsClosed: a FAULTED holder that still has
// pods keeps the lock, as before.
func TestOrphanedLock_FaultedWithPodsFailsClosed(t *testing.T) {
	fx := newOrphanFixture(t, true)
	fx.state.Store(int32(agentpb.JobState_JOB_STATE_FAULTED))
	if err := fx.reconcile(t); err == nil {
		t.Fatal("reconcileGroup succeeded with the holder FAULTED, want the fail-closed error")
	}
	time.Sleep(orphanGrace + 100*time.Millisecond)
	if err := fx.reconcile(t); err == nil {
		t.Fatal("reconcileGroup succeeded with the holder FAULTED, want the fail-closed error")
	}
	fx.wantHolder(t, "trainer")
}

// TestOrphanedLock_FaultedThenPodsGoneReleased is the observed sequence: the
// holder FAULTED (fail closed), then its pods were deleted.
func TestOrphanedLock_FaultedThenPodsGoneReleased(t *testing.T) {
	fx := newOrphanFixture(t, true)
	fx.state.Store(int32(agentpb.JobState_JOB_STATE_FAULTED))
	if err := fx.reconcile(t); err == nil {
		t.Fatal("reconcileGroup succeeded with the holder FAULTED, want the fail-closed error")
	}
	fx.removeTrainer(t)
	fx.mustReconcile(t)
	time.Sleep(orphanGrace + 100*time.Millisecond)
	fx.mustReconcile(t)
	fx.wantHolder(t, "next")
}

// TestOrphanedLock_ZeroGraceDisables: a grace of zero never releases.
func TestOrphanedLock_ZeroGraceDisables(t *testing.T) {
	fx := newOrphanFixture(t, true)
	fx.ctrl.OrphanedLockGrace = 0
	fx.mustReconcile(t)
	fx.removeTrainer(t)
	fx.mustReconcile(t)
	time.Sleep(orphanGrace + 100*time.Millisecond)
	fx.mustReconcile(t)
	fx.wantHolder(t, "trainer")
}
