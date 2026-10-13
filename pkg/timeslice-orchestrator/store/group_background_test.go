package store_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

func newBackgroundGroup(t *testing.T) *store.Group {
	t.Helper()
	group, err := store.NewGroup(context.Background(), "group-1", nil)
	if err != nil {
		t.Fatalf("NewGroup failed: %v", err)
	}
	group.Status().SetNodes([]string{"node-a"})
	group.Status().SetState(pb.GroupStatus_STATE_IDLE_YIELDED)
	return group
}

func TestGroupSpec_ParticipantLifecycle(t *testing.T) {
	group := newBackgroundGroup(t)
	spec := group.Spec()
	now := time.Now()

	spec.RegisterParticipant("node-a", "vk/node-a", now)
	if spec.BackgroundHeld() {
		t.Fatal("a blocked participant without a grant must not count as held")
	}
	if spec.Granted("node-a") {
		t.Fatal("Granted() = true before Grant")
	}
	if !spec.Grant("node-a") {
		t.Fatal("Grant() = false for a registered participant")
	}
	if !spec.Granted("node-a") || !spec.BackgroundHeld() {
		t.Fatal("after Grant the node must be granted and held")
	}

	// Leaving Acquire keeps a participant that holds a grant (fail closed).
	spec.UnregisterParticipant("node-a")
	if !spec.Granted("node-a") {
		t.Fatal("UnregisterParticipant dropped a participant holding a grant")
	}

	if !spec.ClearGrant("node-a") {
		t.Fatal("ClearGrant() = false, want true for a held grant")
	}
	if spec.BackgroundHeld() {
		t.Fatal("BackgroundHeld() = true after ClearGrant")
	}
	// Idempotent: a second clear reports that nothing was held.
	if spec.ClearGrant("node-a") {
		t.Fatal("second ClearGrant() = true, want false")
	}
	if spec.Grant("node-a") {
		t.Fatal("Grant() = true for a forgotten participant")
	}
}

func TestGroupSpec_UnregisterDropsParticipantWithoutHold(t *testing.T) {
	spec := newBackgroundGroup(t).Spec()
	spec.RegisterParticipant("node-a", "vk/node-a", time.Now())
	spec.UnregisterParticipant("node-a")
	if spec.Grant("node-a") {
		t.Fatal("participant without a hold survived UnregisterParticipant")
	}
}

func TestGroupSpec_ClearGrantKeepsBlockedParticipant(t *testing.T) {
	spec := newBackgroundGroup(t).Spec()
	spec.RegisterParticipant("node-a", "vk/node-a", time.Now())
	spec.Grant("node-a")
	spec.ClearGrant("node-a")
	if spec.Granted("node-a") {
		t.Fatal("Granted() = true after ClearGrant")
	}
	// Still blocked in Acquire, so it can be granted again.
	if !spec.Grant("node-a") {
		t.Fatal("ClearGrant forgot a participant still blocked in Acquire")
	}
}

func TestGroupSpec_TouchClaimsUnknownParticipant(t *testing.T) {
	spec := newBackgroundGroup(t).Spec()
	now := time.Now()
	if !spec.Touch("node-a", "vk/node-a", now) {
		t.Fatal("Touch() = false for an unknown participant")
	}
	if !spec.BackgroundHeld() {
		t.Fatal("a claim must count as held (fail closed)")
	}
	if spec.Granted("node-a") {
		t.Fatal("a claim must not count as a grant")
	}
	if spec.Touch("node-a", "vk/node-a", now.Add(time.Second)) {
		t.Fatal("Touch() = true for a known participant")
	}
	// A claim survives leaving Acquire and is cleared only by ClearGrant.
	spec.UnregisterParticipant("node-a")
	if !spec.BackgroundHeld() {
		t.Fatal("UnregisterParticipant dropped a claim")
	}
	if !spec.ClearGrant("node-a") {
		t.Fatal("ClearGrant() = false for a claim")
	}
	if spec.BackgroundHeld() {
		t.Fatal("BackgroundHeld() = true after clearing the claim")
	}
}

func TestGroupSpec_RequestLockClearsLend(t *testing.T) {
	spec := newBackgroundGroup(t).Spec()
	spec.SetLend(true)
	if !spec.Lend() {
		t.Fatal("Lend() = false after SetLend(true)")
	}
	spec.RequestLock("job-1")
	if spec.Lend() {
		t.Fatal("RequestLock did not clear the lend hint")
	}
}

func TestGroupSpec_EnsureNoticeKeepsStart(t *testing.T) {
	spec := newBackgroundGroup(t).Spec()
	first := time.Now()
	if got := spec.EnsureNotice(first); !got.Equal(first) {
		t.Fatalf("EnsureNotice() = %v, want %v", got, first)
	}
	if got := spec.EnsureNotice(first.Add(time.Second)); !got.Equal(first) {
		t.Fatalf("second EnsureNotice() = %v, want the first start %v", got, first)
	}
	spec.ClearNotice()
	if !spec.NoticeAt().IsZero() {
		t.Fatal("NoticeAt() not zero after ClearNotice")
	}
}

func TestGroupSnapshot_EffectiveState(t *testing.T) {
	tests := []struct {
		name  string
		setup func(spec *store.GroupSpec)
		want  pb.GroupStatus_State
	}{
		{
			name:  "no participant reports the recorded state",
			setup: func(*store.GroupSpec) {},
			want:  pb.GroupStatus_STATE_IDLE_YIELDED,
		},
		{
			name: "blocked participant without a grant reports the recorded state",
			setup: func(spec *store.GroupSpec) {
				spec.RegisterParticipant("node-a", "vk/node-a", time.Now())
			},
			want: pb.GroupStatus_STATE_IDLE_YIELDED,
		},
		{
			name: "grant reports BACKGROUND",
			setup: func(spec *store.GroupSpec) {
				spec.RegisterParticipant("node-a", "vk/node-a", time.Now())
				spec.Grant("node-a")
			},
			want: pb.GroupStatus_STATE_BACKGROUND,
		},
		{
			name: "claim reports BACKGROUND",
			setup: func(spec *store.GroupSpec) {
				spec.Touch("node-a", "vk/node-a", time.Now())
			},
			want: pb.GroupStatus_STATE_BACKGROUND,
		},
		{
			name: "notice reports VACATING",
			setup: func(spec *store.GroupSpec) {
				spec.RegisterParticipant("node-a", "vk/node-a", time.Now())
				spec.Grant("node-a")
				spec.EnsureNotice(time.Now())
			},
			want: pb.GroupStatus_STATE_VACATING,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			group := newBackgroundGroup(t)
			tc.setup(group.Spec())
			snap := group.Snapshot()
			if got := snap.EffectiveState(); got != tc.want {
				t.Errorf("EffectiveState() = %v, want %v", got, tc.want)
			}
			// The recorded state is never changed by the overlay.
			if snap.State != pb.GroupStatus_STATE_IDLE_YIELDED {
				t.Errorf("recorded State = %v, want STATE_IDLE_YIELDED", snap.State)
			}
		})
	}
}
