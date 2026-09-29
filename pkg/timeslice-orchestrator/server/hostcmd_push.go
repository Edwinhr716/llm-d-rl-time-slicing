package server

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// HostCommander is the orchestrator's view of the per-host command endpoints:
// the orchestrator commands every host
// of a group to vacate and grants the foreground only once every host acked.
type HostCommander interface {
	// AllClear reports whether every host of the group acked a vacate and
	// none was lent since.
	AllClear(group string) bool
	// Lent reports whether any host of the group was commanded to resume.
	Lent(group string) bool
	// StartVacate starts the vacate barrier for the notice that started at
	// noticeAt, unless one runs already.
	StartVacate(group string, noticeAt time.Time)
}

// WithHostCommander makes a foreground Acquire command every host of the
// group to vacate and wait for every ack before it can succeed. Nil (the
// default) keeps the behaviour without host commands.
func WithHostCommander(hc HostCommander) Option {
	return func(s *Server) {
		s.hosts = hc
	}
}

// startNoticeIfHostsNotClear starts a notice and the vacate barrier when some
// host of the group is not clear, and reports whether one is not. A notice
// already running keeps its start time. Fail closed: the caller must not grant
// while this returns true.
func (s *Server) startNoticeIfHostsNotClear(ctx context.Context, group *store.Group) bool {
	if s.hosts == nil || s.hosts.AllClear(group.ID()) {
		return false
	}
	now := time.Now()
	noticeAt := group.Spec().EnsureNotice(now)
	if noticeAt.Equal(now) {
		slog.InfoContext(ctx, "Foreground Acquire started a notice to the hosts",
			"vacateWithin", s.vacateWithin(noticeAt, now))
	}
	s.hosts.StartVacate(group.ID(), noticeAt)
	return true
}

// hostsEffectiveState reports STATE_BACKGROUND while hosts are lent and no
// notice runs.
func (s *Server) hostsEffectiveState(groupID string, state pb.GroupStatus_State) pb.GroupStatus_State {
	if s.hosts == nil || state == pb.GroupStatus_STATE_VACATING || !s.hosts.Lent(groupID) {
		return state
	}
	return pb.GroupStatus_STATE_BACKGROUND
}
