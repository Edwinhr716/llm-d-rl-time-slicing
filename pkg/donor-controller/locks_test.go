package donorcontroller_test

import (
	"testing"
	"time"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

func status(state v1alpha1.GroupStatus_State, locking, active string) *v1alpha1.GroupStatus {
	return &v1alpha1.GroupStatus{GroupId: "g", GroupState: state, LockingJob: locking, ActiveJob: active}
}

func TestLastLockActivity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var (
		idle       = v1alpha1.GroupStatus_STATE_IDLE
		yielded    = v1alpha1.GroupStatus_STATE_IDLE_YIELDED
		locked     = v1alpha1.GroupStatus_STATE_LOCKED
		switching  = v1alpha1.GroupStatus_STATE_SWITCHING
		vacating   = v1alpha1.GroupStatus_STATE_VACATING
		background = v1alpha1.GroupStatus_STATE_BACKGROUND
		vk         = donorcontroller.BackgroundJobPrefix + "w1"
	)
	cases := []struct {
		name      string
		prev, cur *v1alpha1.GroupStatus
		want      bool
	}{
		{"unknown group", status(locked, "j1", "j1"), nil, false},
		{"first sight idle", nil, status(idle, "", ""), false},
		{"first sight locked", nil, status(locked, "j1", "j1"), true},
		{"locked stays locked", status(locked, "j1", "j1"), status(locked, "j1", "j1"), true},
		{"switching", status(idle, "", ""), status(switching, "j2", "j1"), true},
		{"vacating", status(idle, "", ""), status(vacating, "j1", "j1"), true},
		{"locked to yielded", status(locked, "j1", "j1"), status(yielded, "", "j1"), true},
		{"idle unchanged", status(idle, "", "j1"), status(idle, "", "j1"), false},
		{"foreground active job change", status(yielded, "", "j1"), status(yielded, "", "j2"), true},
		{"idle to background", status(idle, "", ""), status(background, vk, vk), false},
		{"yielded to background", status(yielded, "", "j1"), status(background, vk, vk), false},
		{"background heartbeat", status(background, vk, vk), status(background, vk, vk), false},
		{"background to idle", status(background, vk, vk), status(idle, "", ""), false},
		{"locked to background", status(locked, "j1", "j1"), status(background, vk, vk), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := donorcontroller.LastLockActivity(tc.prev, tc.cur, now)
			if got.Equal(now) != tc.want || (!tc.want && !got.IsZero()) {
				t.Fatalf("LastLockActivity = %v, want activity=%v", got, tc.want)
			}
		})
	}
}
