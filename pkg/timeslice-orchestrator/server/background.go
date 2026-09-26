package server

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// effectiveRole maps the request role to the role the server acts on. An unset
// role is foreground, so every client written before roles existed keeps its
// behaviour. Values this server does not know are refused.
func effectiveRole(r pb.Role) (pb.Role, error) {
	switch r {
	case pb.Role_ROLE_UNSPECIFIED, pb.Role_ROLE_FOREGROUND:
		return pb.Role_ROLE_FOREGROUND, nil
	case pb.Role_ROLE_BACKGROUND:
		return pb.Role_ROLE_BACKGROUND, nil
	default:
		return pb.Role_ROLE_UNSPECIFIED, status.Errorf(codes.InvalidArgument, "unknown role %d", int32(r))
	}
}

// participantNode returns the node named by a background participant ID of
// the form "vk/<node name>".
func participantNode(participantID string) (string, error) {
	node, ok := strings.CutPrefix(participantID, backgroundParticipantPrefix)
	if !ok || node == "" || strings.Contains(node, "/") {
		return "", status.Errorf(codes.InvalidArgument,
			"background participant ID %q must have the form %s<node name>", participantID, backgroundParticipantPrefix)
	}
	return node, nil
}

func (s *Server) requireBackgroundRole() error {
	if !s.backgroundRole {
		return status.Error(codes.FailedPrecondition,
			"the background role is disabled on this server (background_protocol = 0)")
	}
	return nil
}

// requireGroupNode checks that node is one of the group's nodes.
func requireGroupNode(group *store.Group, node string) error {
	if !slices.Contains(group.Status().Nodes(), node) {
		return status.Errorf(codes.FailedPrecondition, "node %s is not a node of group %s", node, group.ID())
	}
	return nil
}

// acquireBackground implements Acquire(ROLE_BACKGROUND). The participant never
// enters the foreground queue. It blocks until its node is granted to it and
// no notice runs. A participant that already holds a grant returns at once. A
// claim is not a grant. If the call ends without a grant, the participant is
// unregistered unless it holds a grant or a claim.
func (s *Server) acquireBackground(
	ctx context.Context,
	group *store.Group,
	jobID, node string,
	startTime time.Time,
) (*pb.AcquireResponse, error) {
	if err := s.requireBackgroundRole(); err != nil {
		return nil, err
	}
	if node == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required for ROLE_BACKGROUND")
	}
	if want := backgroundParticipantPrefix + node; jobID != want {
		return nil, status.Errorf(codes.InvalidArgument,
			"job_id for ROLE_BACKGROUND on node %s must be %q, got %q", node, want, jobID)
	}
	if err := requireGroupNode(group, node); err != nil {
		return nil, err
	}

	spec := group.Spec()
	spec.RegisterParticipant(node, jobID, time.Now())
	if s.ctrl != nil {
		s.ctrl.EnqueueWork(group.ID())
	}

	granted := func() bool {
		return spec.Granted(node) && spec.NoticeAt().IsZero()
	}
	success := func() *pb.AcquireResponse {
		slog.InfoContext(ctx, "Background Acquire succeeded, node granted", "node", node)
		return &pb.AcquireResponse{Success: true, WaitedMs: time.Since(startTime).Milliseconds()}
	}
	if granted() {
		return success(), nil
	}

	ticker := time.NewTicker(s.acquirePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			spec.UnregisterParticipant(node)
			slog.InfoContext(ctx, "Background Acquire context cancelled", "node", node, "error", ctx.Err())
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
			current, err := s.groupStore.Get(ctx, group.ID())
			if err != nil || current != group {
				spec.UnregisterParticipant(node)
				if err == nil || errors.Is(err, store.ErrNotFound) {
					return nil, status.Errorf(codes.NotFound, "group %s no longer exists", group.ID())
				}
				return nil, status.Errorf(codes.Internal, "failed to get group: %v", err)
			}
			if granted() {
				return success(), nil
			}
		}
	}
}

// yieldBackground implements Yield(ROLE_BACKGROUND): it hands the node's grant
// or claim back and asks the reconcile loop to check the node now. It is
// idempotent and succeeds even if nothing was held.
func (s *Server) yieldBackground(ctx context.Context, group *store.Group, jobID string) (*pb.YieldResponse, error) {
	if err := s.requireBackgroundRole(); err != nil {
		return nil, err
	}
	node, err := participantNode(jobID)
	if err != nil {
		return nil, err
	}
	held := group.Spec().ClearGrant(node)
	slog.InfoContext(ctx, "Background Yield", "node", node, "held", held)
	if s.ctrl != nil {
		s.ctrl.EnqueueWork(group.ID())
	}
	return &pb.YieldResponse{Success: true}, nil
}

// touchParticipant records a background participant's status poll, its
// heartbeat. An unknown participant is registered with a claim (fail closed).
// A poll never grants. With the background role disabled the field is ignored,
// exactly as a server without it would.
func (s *Server) touchParticipant(ctx context.Context, group *store.Group, participantID string) error {
	if !s.backgroundRole {
		return nil
	}
	node, err := participantNode(participantID)
	if err != nil {
		return err
	}
	if err := requireGroupNode(group, node); err != nil {
		return err
	}
	if group.Spec().Touch(node, participantID, time.Now()) {
		slog.InfoContext(ctx, "Unknown background participant registered with a claim", "node", node)
		if s.ctrl != nil {
			s.ctrl.EnqueueWork(group.ID())
		}
	}
	return nil
}

// startNoticeIfBackgroundHeld starts a notice when a background participant
// holds a grant or a claim, and reports whether one does. A notice already
// running keeps its start time.
func (s *Server) startNoticeIfBackgroundHeld(ctx context.Context, group *store.Group) bool {
	spec := group.Spec()
	if !spec.BackgroundHeld() {
		return false
	}
	now := time.Now()
	if noticeAt := spec.EnsureNotice(now); noticeAt.Equal(now) {
		slog.InfoContext(ctx, "Foreground Acquire started a notice to the background",
			"vacateWithin", s.vacateWithin(noticeAt, now))
		if s.ctrl != nil {
			s.ctrl.EnqueueWork(group.ID())
		}
	}
	return true
}

// vacateWithin returns T - now, where T = noticeAt + N - K, never negative.
func (s *Server) vacateWithin(noticeAt, now time.Time) time.Duration {
	d := noticeAt.Add(s.noticeWindow - s.killBudget).Sub(now)
	if d < 0 {
		return 0
	}
	return d
}
