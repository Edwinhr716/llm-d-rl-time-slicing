package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

func TestGroupSnapshot_ServingSince(t *testing.T) {
	ctx := context.Background()

	newLockedGroup := func(t *testing.T) *store.Group {
		t.Helper()
		g, err := store.NewGroup(ctx, "group-1", store.NewGroupLockStoreWrapper(store.NewMemLockStore(), "group-1"))
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}
		g.Spec().RequestLock("job-1")
		if _, err := g.Spec().TryPromote(ctx); err != nil {
			t.Fatalf("failed to promote: %v", err)
		}
		return g
	}

	t.Run("zero when unlocked", func(t *testing.T) {
		g, err := store.NewGroup(ctx, "group-1", nil)
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}
		if got := g.Snapshot().ServingSince(); !got.IsZero() {
			t.Errorf("ServingSince() = %v, want zero", got)
		}
	})

	t.Run("zero while the context is still loading", func(t *testing.T) {
		g := newLockedGroup(t)
		snap := g.Snapshot()
		if snap.LockedAt.IsZero() {
			t.Error("LockedAt should be stamped by the grant")
		}
		if got := snap.ServingSince(); !got.IsZero() {
			t.Errorf("ServingSince() = %v, want zero before the context is loaded", got)
		}
	})

	t.Run("zero when a different job is loaded", func(t *testing.T) {
		g := newLockedGroup(t)
		g.Status().SetLoadedJob("job-2")
		if got := g.Snapshot().ServingSince(); !got.IsZero() {
			t.Errorf("ServingSince() = %v, want zero when the loaded job is not the holder", got)
		}
	})

	t.Run("stamped when the holder's context is loaded", func(t *testing.T) {
		g := newLockedGroup(t)
		before := time.Now()
		g.Status().SetLoadedJob("job-1")
		after := time.Now()

		snap := g.Snapshot()
		got := snap.ServingSince()
		if got.Before(before) || got.After(after) {
			t.Errorf("ServingSince() = %v, want between %v and %v", got, before, after)
		}
		// The load completes after the grant, so the load is what starts the
		// quantum: the interval between them is handoff, not serving.
		if !got.Equal(snap.LoadedAt) {
			t.Errorf("ServingSince() = %v, want LoadedAt %v", got, snap.LoadedAt)
		}
	})

	t.Run("zero again after the holder yields", func(t *testing.T) {
		g := newLockedGroup(t)
		g.Status().SetLoadedJob("job-1")
		if err := g.Spec().Yield(ctx, "job-1"); err != nil {
			t.Fatalf("Yield failed: %v", err)
		}
		snap := g.Snapshot()
		if !snap.LockedAt.IsZero() {
			t.Errorf("LockedAt = %v, want zero after yield", snap.LockedAt)
		}
		if got := snap.ServingSince(); !got.IsZero() {
			t.Errorf("ServingSince() = %v, want zero after yield", got)
		}
	})

	t.Run("zero for a lock recovered from the lock store", func(t *testing.T) {
		lockStore := store.NewMemLockStore()
		if err := lockStore.Lock(ctx, "group-1", "job-1"); err != nil {
			t.Fatalf("failed to seed lock: %v", err)
		}
		g, err := store.NewGroup(ctx, "group-1", store.NewGroupLockStoreWrapper(lockStore, "group-1"))
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}
		g.Status().SetLoadedJob("job-1")

		snap := g.Snapshot()
		if snap.LockingJob != "job-1" {
			t.Fatalf("LockingJob = %q, want job-1", snap.LockingJob)
		}
		// The lock store persists only the holder's identity. Restarting the
		// orchestrator must fail open rather than hand the recovered holder a
		// fresh quantum it did not earn.
		if got := snap.ServingSince(); !got.IsZero() {
			t.Errorf("ServingSince() = %v, want zero for a recovered lock", got)
		}
	})
}

func TestGroupStatus_SetLoadedJob_StampsOnChangeOnly(t *testing.T) {
	ctx := context.Background()
	g, err := store.NewGroup(ctx, "group-1", nil)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	g.Status().SetLoadedJob("job-1")
	first := g.Snapshot().LoadedAt
	if first.IsZero() {
		t.Fatal("LoadedAt should be stamped on the first load")
	}

	// The controller re-asserts the loaded job on every reconcile; a repeated
	// assertion of the same job must not restart the quantum.
	time.Sleep(2 * time.Millisecond)
	g.Status().SetLoadedJob("job-1")
	if got := g.Snapshot().LoadedAt; !got.Equal(first) {
		t.Errorf("LoadedAt = %v, want unchanged %v", got, first)
	}

	g.Status().SetLoadedJob("")
	if got := g.Snapshot().LoadedAt; !got.IsZero() {
		t.Errorf("LoadedAt = %v, want zero when nothing is loaded", got)
	}
}
