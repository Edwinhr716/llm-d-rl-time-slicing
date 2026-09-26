package store_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

func queuedIDs(q *store.WaitingJobQueue) []string {
	jobs := q.List()
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.JobID)
	}
	return ids
}

func TestWaitingJobQueue_Remove(t *testing.T) {
	tests := []struct {
		name     string
		enqueue  []string
		remove   string
		wantOK   bool
		wantJobs []string
	}{
		{name: "front", enqueue: []string{"a", "b", "c"}, remove: "a", wantOK: true, wantJobs: []string{"b", "c"}},
		{name: "middle keeps FIFO order", enqueue: []string{"a", "b", "c"}, remove: "b", wantOK: true, wantJobs: []string{"a", "c"}},
		{name: "back", enqueue: []string{"a", "b", "c"}, remove: "c", wantOK: true, wantJobs: []string{"a", "b"}},
		{name: "absent", enqueue: []string{"a"}, remove: "z", wantOK: false, wantJobs: []string{"a"}},
		{name: "empty queue", enqueue: nil, remove: "a", wantOK: false, wantJobs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := store.NewWaitingJobQueue()
			for _, id := range tt.enqueue {
				q.Enqueue(id)
			}
			if got := q.Remove(tt.remove); got != tt.wantOK {
				t.Errorf("Remove(%q) = %v, want %v", tt.remove, got, tt.wantOK)
			}
			if got := queuedIDs(q); !reflect.DeepEqual(got, tt.wantJobs) {
				t.Errorf("queue = %v, want %v", got, tt.wantJobs)
			}
			if q.Exists(tt.remove) {
				t.Errorf("Exists(%q) = true after Remove", tt.remove)
			}
			if q.Len() != len(tt.wantJobs) {
				t.Errorf("Len() = %d, want %d", q.Len(), len(tt.wantJobs))
			}
		})
	}
}

func TestWaitingJobQueue_RemoveThenEnqueueGoesToBack(t *testing.T) {
	q := store.NewWaitingJobQueue()
	q.Enqueue("a")
	q.Enqueue("b")
	if !q.Remove("a") {
		t.Fatal("Remove(a) = false, want true")
	}
	if !q.Enqueue("a") {
		t.Fatal("Enqueue(a) after Remove = false, want true (not a duplicate any more)")
	}
	if got, want := queuedIDs(q), []string{"b", "a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("queue = %v, want %v", got, want)
	}
}

func TestGroupSpec_CancelRequest(t *testing.T) {
	ctx := context.Background()
	group, err := store.NewGroup(ctx, "g", store.NewGroupLockStoreWrapper(store.NewMemLockStore(), "g"))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	spec := group.Spec()

	spec.RequestLock("a")
	spec.RequestLock("b")
	if !spec.CancelRequest("b") {
		t.Error("CancelRequest(b) = false for a queued job, want true")
	}
	if spec.CancelRequest("b") {
		t.Error("second CancelRequest(b) = true, want false")
	}
	if promoted, err := spec.TryPromote(ctx); err != nil || !promoted {
		t.Fatalf("TryPromote = %v, %v; want true, nil", promoted, err)
	}
	// A granted lock is not withdrawn by a cancel; only Yield releases it.
	if spec.CancelRequest("a") {
		t.Error("CancelRequest(a) = true for the lock holder, want false")
	}
	if got := spec.LockingJob(); got != "a" {
		t.Errorf("LockingJob() = %q, want a", got)
	}
}

// TestGroupSpec_CancelledWaiterNotPromotedAfterYield is Q13 scenario S3: a
// waiter whose Acquire had already failed was promoted 0.021 s after the holder
// yielded, and the controller then snapshotted the running trainer for nobody.
// Once the failed Acquire withdraws its request, a Yield promotes no one.
func TestGroupSpec_CancelledWaiterNotPromotedAfterYield(t *testing.T) {
	ctx := context.Background()
	group, err := store.NewGroup(ctx, "g", store.NewGroupLockStoreWrapper(store.NewMemLockStore(), "g"))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	spec := group.Spec()

	spec.RequestLock("trainer")
	if _, err := spec.TryPromote(ctx); err != nil {
		t.Fatalf("TryPromote: %v", err)
	}
	spec.RequestLock("zombie")
	spec.CancelRequest("zombie")

	if err := spec.Yield(ctx, "trainer"); err != nil {
		t.Fatalf("Yield: %v", err)
	}
	promoted, err := spec.TryPromote(ctx)
	if err != nil {
		t.Fatalf("TryPromote: %v", err)
	}
	if promoted || spec.LockingJob() != "" {
		t.Errorf("TryPromote after Yield promoted %q, want no one", spec.LockingJob())
	}
	if got := spec.ActiveJob(); got != "trainer" {
		t.Errorf("ActiveJob() = %q, want trainer (its context stays loaded)", got)
	}
}
