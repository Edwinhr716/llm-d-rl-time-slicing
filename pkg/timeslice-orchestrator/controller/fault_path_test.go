package controller_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/util/workqueue"
)

// TestNewRateLimiter is Q13 scenario S1b: client-go's default limiter starts at
// 5 ms, which retried a failing group nine times in 0.7 s, and caps at 1000 s.
// The proposed limiter starts at 1 s and caps at 30 s.
func TestNewRateLimiter(t *testing.T) {
	limiter := controller.NewRateLimiter(time.Second, 30*time.Second)
	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, wantDelay := range want {
		if got := limiter.When("g"); got != wantDelay {
			t.Errorf("failure %d: When() = %v, want %v", i+1, got, wantDelay)
		}
	}
	// Per-group: another group starts from the base delay.
	if got := limiter.When("other"); got != time.Second {
		t.Errorf("other group's first When() = %v, want 1s", got)
	}
	limiter.Forget("g")
	if got := limiter.NumRequeues("g"); got != 0 {
		t.Errorf("NumRequeues after Forget = %d, want 0", got)
	}
	if got := limiter.When("g"); got != time.Second {
		t.Errorf("When() after Forget = %v, want 1s", got)
	}
}

func TestDefaultWorkers(t *testing.T) {
	if controller.DefaultWorkers != 4 {
		t.Errorf("DefaultWorkers = %d, want 4", controller.DefaultWorkers)
	}
}

// slowRetryQueue makes every rate-limited retry wait an hour, so only
// AddAfter or Add can bring a group back within a test.
func slowRetryQueue(name string) workqueue.TypedRateLimitingInterface[string] {
	return workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.NewRateLimiter(time.Hour, time.Hour),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: name},
	)
}

// holderWaitFixture is a group whose lock is held by "trainer" while the agent
// reports the trainer TRANSITIONING until running is set.
type holderWaitFixture struct {
	groups  *store.GroupStore
	ctrl    *controller.Controller
	queue   workqueue.TypedRateLimitingInterface[string]
	running atomic.Bool
}

func newHolderWaitFixture(t *testing.T) *holderWaitFixture {
	t.Helper()
	ctx := context.Background()
	lockStore := store.NewMemLockStore()
	if err := lockStore.Lock(ctx, "group-1", "trainer"); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}
	groups := store.NewGroupStore(lockStore)
	jobs := store.NewJobStore()
	if err := jobs.Put(ctx, store.NewJob("group-1", "trainer")); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}

	fx := &holderWaitFixture{groups: groups, queue: slowRetryQueue("holder-wait")}
	orch := &mockInfrastructureOrchestrator{
		observeFunc: func(ctx context.Context, groupID string) error {
			group, _, err := groups.GetOrCreate(ctx, groupID)
			if err != nil {
				return err
			}
			group.Status().SetNodes([]string{"node-1"})
			return nil
		},
	}
	agents := &controller.MockSnapshotAgentStore{
		GetStatusFunc: func(context.Context, string) (*agentpb.StatusResponse, error) {
			state := agentpb.JobState_JOB_STATE_TRANSITIONING
			if fx.running.Load() {
				state = agentpb.JobState_JOB_STATE_RUNNING
			}
			return &agentpb.StatusResponse{JobStatuses: []*agentpb.JobStatus{{JobId: "trainer", State: state}}}, nil
		},
	}
	fx.ctrl = controller.NewController(groups, jobs, fx.queue, orch, agents)
	return fx
}

func (fx *holderWaitFixture) loadedJob() string {
	group, err := fx.groups.Get(context.Background(), "group-1")
	if err != nil {
		return ""
	}
	return group.Status().LoadedJob()
}

// holderWaitWindow is how long after the agent reports RUNNING the holder must
// be loaded: one 1 s requeue plus scheduling slack.
const holderWaitWindow = 2500 * time.Millisecond

// runHolderWait starts the controller, lets the first pass see TRANSITIONING,
// flips the agent to RUNNING, and reports whether the holder is loaded within
// holderWaitWindow.
func runHolderWait(t *testing.T, requeue time.Duration) bool {
	t.Helper()
	fx := newHolderWaitFixture(t)
	fx.ctrl.HolderWaitRequeue = requeue

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := fx.ctrl.Run(ctx, 1); err != nil {
			t.Errorf("Controller Run failed: %v", err)
		}
	}()
	fx.queue.Add("group-1")

	// Let the first pass fail on TRANSITIONING and schedule its retry.
	time.Sleep(300 * time.Millisecond)
	if got := fx.loadedJob(); got != "" {
		t.Fatalf("LoadedJob() = %q while the agent reports TRANSITIONING, want empty", got)
	}
	fx.running.Store(true)

	return waitWithTimeout(func() bool { return fx.loadedJob() == "trainer" }, holderWaitWindow) == nil
}

// TestHolderWaitRequeue is Q13 §3: an agent state change was not seen until the
// resync, 9.1 s later, so the holder's Acquire waited on nothing. With the
// requeue the holder is loaded within about a second of the agent reporting
// RUNNING, even though the failed pass's own retry is an hour away.
func TestHolderWaitRequeue(t *testing.T) {
	if !runHolderWait(t, time.Second) {
		t.Error("holder not loaded within 2.5s of the agent reporting RUNNING with a 1s holder-wait requeue")
	}
}

// TestHolderWaitRequeue_DisabledControl shows the requeue is what makes the
// difference: without it nothing reconciles the group again in the window.
func TestHolderWaitRequeue_DisabledControl(t *testing.T) {
	if runHolderWait(t, 0) {
		t.Error("holder loaded without a holder-wait requeue; the test no longer isolates the requeue")
	}
}

// runHungGroup starts the controller with the given workers while group-a's
// agent status call hangs, then reports whether group-b is reconciled within
// the window.
func runHungGroup(t *testing.T, workers int, window time.Duration) bool {
	t.Helper()
	groups := store.NewGroupStore(store.NewMemLockStore())
	queue := slowRetryQueue("hung-group")

	var bObserved atomic.Bool
	orch := &mockInfrastructureOrchestrator{
		observeFunc: func(ctx context.Context, groupID string) error {
			group, _, err := groups.GetOrCreate(ctx, groupID)
			if err != nil {
				return err
			}
			if groupID == "group-b" {
				bObserved.Store(true)
				group.Status().SetNodes([]string{"node-b"})
				return nil
			}
			group.Status().SetNodes([]string{"node-a"})
			return nil
		},
	}
	hung := make(chan struct{}, 1)
	agents := &controller.MockSnapshotAgentStore{
		GetStatusFunc: func(ctx context.Context, node string) (*agentpb.StatusResponse, error) {
			if node == "node-a" {
				select {
				case hung <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &agentpb.StatusResponse{}, nil
		},
	}
	ctrl := controller.NewController(groups, store.NewJobStore(), queue, orch, agents)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := ctrl.Run(ctx, workers); err != nil {
			t.Errorf("Controller Run failed: %v", err)
		}
	}()

	queue.Add("group-a")
	select {
	case <-hung:
	case <-time.After(2 * time.Second):
		t.Fatal("group-a never reached its agent call")
	}
	queue.Add("group-b")
	return waitWithTimeout(bObserved.Load, window) == nil
}

// TestWorkers_HungGroupDoesNotBlockOthers is Q13 scenario S5b: with one
// worker, one hung agent call stopped every other group and the resync.
func TestWorkers_HungGroupDoesNotBlockOthers(t *testing.T) {
	if !runHungGroup(t, controller.DefaultWorkers, 2*time.Second) {
		t.Error("group-b not reconciled within 2s while group-a hangs, with DefaultWorkers")
	}
}

// TestWorkers_SingleWorkerControl reproduces the S5b blast radius with the old
// single worker.
func TestWorkers_SingleWorkerControl(t *testing.T) {
	if runHungGroup(t, 1, 1500*time.Millisecond) {
		t.Error("group-b reconciled while group-a hangs with one worker; the test no longer isolates workers")
	}
}
