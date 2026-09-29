package era

import (
	"context"
	"strings"
	"time"
)

// GroupState mirrors the orchestrator's GroupStatus.State enum, including the two computed
// states the background protocol adds (BACKGROUND = 6, VACATING = 7).
//
// This build has no copy of the orchestrator protos yet (they arrive with the VK-A6 loop).
// GroupStatus below carries only the fields the era reads, with the proto's names and
// numbers, so swapping it for the generated type is a type change, not a logic change.
type GroupState int32

// Group states, numbered as in the orchestrator proto.
const (
	StateUnspecified GroupState = 0
	StateUnknown     GroupState = 1
	StateIdle        GroupState = 2
	StateIdleYielded GroupState = 3
	StateLocked      GroupState = 4
	StateSwitching   GroupState = 5
	StateBackground  GroupState = 6
	StateVacating    GroupState = 7
)

// GroupStatus is the part of the orchestrator's GroupStatus the era reads.
type GroupStatus struct {
	GroupID    string
	GroupState GroupState
	LockingJob string
	ActiveJob  string
}

// LockSource returns the group's current status. In the VK it is the VK-A6 loop's last
// GetGroupStatus poll result (that loop already polls every 0.5 s as the VK heartbeat), so the
// era adds no second poller. A nil status or an error means the group is unknown: no activity.
type LockSource interface {
	GroupStatus(ctx context.Context, group string) (*GroupStatus, error)
}

// BackgroundJobPrefix starts the job_id of every background participant (vk/<real node>).
const BackgroundJobPrefix = "vk/"

func isBackground(job string) bool { return strings.HasPrefix(job, BackgroundJobPrefix) }

// lockHeld reports whether a foreground job holds or is taking the group lock right now.
func lockHeld(s *GroupStatus) bool {
	if s == nil {
		return false
	}
	switch s.GroupState {
	case StateLocked, StateSwitching, StateVacating:
		return !isBackground(s.LockingJob)
	}
	return false
}

// LastLockActivity returns now if the step from prev to cur shows foreground lock activity,
// and the zero time otherwise. The caller keeps the latest non-zero result.
//
// Activity is a change of (group_state, locking_job, active_job), or now while the state is
// LOCKED, SWITCHING or VACATING. The VK's own traffic never counts, or the era would never end
// while a guest serves: the computed BACKGROUND state is not compared, and a change in which
// the old or new locking_job or active_job is a background participant (vk/...) is ignored.
// A group unknown to the orchestrator (nil) is no activity; so is the first observation.
func LastLockActivity(prev, cur *GroupStatus, now time.Time) time.Time {
	if cur == nil {
		return time.Time{}
	}
	if lockHeld(cur) {
		return now
	}
	if prev == nil {
		return time.Time{}
	}
	for _, pair := range [][2]string{{prev.LockingJob, cur.LockingJob}, {prev.ActiveJob, cur.ActiveJob}} {
		if pair[0] != pair[1] && (isBackground(pair[0]) || isBackground(pair[1])) {
			return time.Time{}
		}
	}
	stateChanged := prev.GroupState != cur.GroupState &&
		prev.GroupState != StateBackground && cur.GroupState != StateBackground
	if stateChanged || prev.LockingJob != cur.LockingJob || prev.ActiveJob != cur.ActiveJob {
		return now
	}
	return time.Time{}
}
