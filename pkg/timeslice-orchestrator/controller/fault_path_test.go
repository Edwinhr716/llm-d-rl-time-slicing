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

func TestDefaultWorkers(t *testing.T) {
	if controller.DefaultWorkers != 4 {
		t.Errorf("DefaultWorkers = %d, want 4", controller.DefaultWorkers)
	}
}

// slowRetryQueue makes every rate-limited retry wait an hour, so only
// Add can bring a group back within a test.
func slowRetryQueue(name string) workqueue.TypedRateLimitingInterface[string] {
	return workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.NewRateLimiter(time.Hour, time.Hour),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: name},
	)
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
