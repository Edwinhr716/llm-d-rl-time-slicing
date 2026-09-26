package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GroupStore defines the interface for group store operations needed by the server.
type GroupStore interface {
	Get(ctx context.Context, id string) (*store.Group, error)
	List(ctx context.Context) ([]*store.Group, error)
}

// JobStore defines the interface for job store operations needed by the server.
type JobStore interface {
	Get(ctx context.Context, groupID, jobID string) (*store.Job, error)
	ListByGroup(ctx context.Context, groupID string) ([]*store.Job, error)
}

// checkAcquireFunc defines the signature for the Acquire check hook.
type checkAcquireFunc func(ctx context.Context, groupID, jobID string, startTime time.Time) (*pb.AcquireResponse, error, bool)

// Server implements the TimeSliceOrchestratorService gRPC server.
type Server struct {
	pb.UnimplementedTimeSliceOrchestratorServiceServer
	ctrl                *controller.Controller
	groupStore          GroupStore
	jobStore            JobStore
	acquirePollInterval time.Duration
	checkAcquire        checkAcquireFunc
	servingQuantum      time.Duration
	budget              *budget.Publisher

	// Background participant protocol (roles). See WithBackgroundRole.
	backgroundRole bool
	minBubble      time.Duration
	noticeWindow   time.Duration
	killBudget     time.Duration
}

// BackgroundProtocolVersion is the value of GroupStatus.background_protocol
// on a server that speaks the background participant protocol.
const BackgroundProtocolVersion = 1

// backgroundParticipantPrefix prefixes a background participant's job_id:
// "vk/<node name>".
const backgroundParticipantPrefix = "vk/"

// Defaults for the notice timing. Both are PENDING LEAD DECISION values; the
// command line sets them through --notice-window and --kill-budget.
const (
	DefaultNoticeWindow = 30 * time.Second
	DefaultKillBudget   = 3 * time.Second
)

// budgetPublishInterval is how often the dispatch budget is republished when
// nothing is polling GetGroupStatus. It also bounds how long an evicted key
// stays absent, which matters because the consuming gate fails open on one.
const budgetPublishInterval = 1 * time.Second

// heldBudgetPublishInterval replaces it when a rising-edge open delay is
// configured. The hold-down can only be honoured to the resolution of the
// loop that evaluates it, and the delay being calibrated against is on the
// order of a second, so a 1 s loop would double it in the worst case.
const heldBudgetPublishInterval = 250 * time.Millisecond

// Option configures a Server.
type Option func(*Server)

// WithServingQuantum sets the minimum serving quantum: the shortest time a job
// that has just been granted the lock and had its context restored is allowed
// to run before the orchestrator advertises pre-emption pressure to it. Zero
// (the default) disables the quantum and preserves the previous behaviour.
func WithServingQuantum(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.servingQuantum = d
		}
	}
}

// WithDispatchBudgetPublisher makes the server publish the batch tenant's
// dispatch budget through p. Nil (the default) disables publishing entirely and
// preserves the previous behaviour.
func WithDispatchBudgetPublisher(p *budget.Publisher) Option {
	return func(s *Server) {
		s.budget = p
	}
}

// WithBackgroundRole enables the background participant protocol: Acquire and
// Yield accept ROLE_BACKGROUND, GetGroupStatus accepts participant_id and
// reports background_protocol = 1. Disabled (the default) the server reports
// background_protocol = 0, so a background participant never calls Acquire,
// and refuses ROLE_BACKGROUND with FailedPrecondition. Foreground callers are
// unaffected either way.
func WithBackgroundRole(enabled bool) Option {
	return func(s *Server) {
		s.backgroundRole = enabled
	}
}

// WithMinBubble sets the minimum expected_idle for which a foreground Yield
// records a lend hint. Zero (the default) never records one, so the
// accelerator is never lent and the group goes IDLE_YIELDED as before.
func WithMinBubble(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.minBubble = d
		}
	}
}

// WithNoticeTiming sets the notice window N and the kill budget K used to
// compute vacate_within = notice + N - K - now. Non-positive values keep the
// defaults.
func WithNoticeTiming(noticeWindow, killBudget time.Duration) Option {
	return func(s *Server) {
		if noticeWindow > 0 {
			s.noticeWindow = noticeWindow
		}
		if killBudget > 0 {
			s.killBudget = killBudget
		}
	}
}

// NewServer creates a new Server instance.
func NewServer(ctrl *controller.Controller, groupStore GroupStore, jobStore JobStore, opts ...Option) *Server {
	s := &Server{
		ctrl:                ctrl,
		groupStore:          groupStore,
		jobStore:            jobStore,
		acquirePollInterval: 1 * time.Second,
		noticeWindow:        DefaultNoticeWindow,
		killBudget:          DefaultKillBudget,
	}
	s.checkAcquire = s.defaultCheckAcquire
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Acquire implements TimeSliceOrchestratorService.Acquire.
func (s *Server) Acquire(ctx context.Context, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	ctx = logging.WithServerMethod(ctx, "Acquire")
	ctx = logging.WithJobID(ctx, req.GetJobId())
	ctx = logging.WithGroupID(ctx, req.GetGroupId())
	slog.InfoContext(ctx, "Acquire called")

	groupID := req.GetGroupId()
	jobID := req.GetJobId()
	startTime := time.Now()

	role, err := effectiveRole(req.GetRole())
	if err != nil {
		return nil, err
	}

	// 1. Get Group
	group, err := s.groupStore.Get(ctx, groupID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "group %s not found", groupID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get group: %v", err)
	}

	if role == pb.Role_ROLE_BACKGROUND {
		return s.acquireBackground(ctx, group, jobID, req.GetNodeName(), startTime)
	}

	// 2. Request Lock
	group.Spec().RequestLock(jobID)
	// Foreground wait, option A: a foreground Acquire while background guests
	// hold the accelerator starts the notice and keeps blocking below until
	// they are gone (see defaultCheckAcquire).
	s.startNoticeIfBackgroundHeld(ctx, group)
	if s.ctrl != nil {
		s.ctrl.EnqueueWork(groupID)
	}

	// 3. Wait Loop
	ticker := time.NewTicker(s.acquirePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Acquire context cancelled", "error", ctx.Err())
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
			resp, err, done := s.checkAcquire(ctx, groupID, jobID, startTime)
			if done {
				return resp, err
			}
		}
	}
}

func (s *Server) defaultCheckAcquire(
	ctx context.Context,
	groupID, jobID string,
	startTime time.Time,
) (*pb.AcquireResponse, error, bool) {
	// Re-read group to get the latest status and spec from the store (fixes stale group bug)
	group, err := s.groupStore.Get(ctx, groupID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get group: %v", err), true
	}

	// Check if group is faulted
	faulted, err := s.isGroupFaulted(ctx, groupID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if group is faulted: %v", err), true
	}
	if faulted {
		return nil, status.Errorf(codes.Unavailable, "group %s is faulted", groupID), true
	}

	// Fail closed: never hand the accelerator to a foreground job while a
	// background participant holds a grant or a claim. Keep the notice
	// running and wait (foreground wait option A).
	if s.startNoticeIfBackgroundHeld(ctx, group) {
		return nil, nil, false //nolint:nilnil // returning nil, nil is intended when done is false
	}

	// Check if we are the lock holder AND the context is loaded
	// (fixes premature success bug)
	if group.Spec().LockingJob() == jobID && group.Status().LoadedJob() == jobID {
		// The accelerator is back with the foreground: any notice is over.
		group.Spec().ClearNotice()
		slog.InfoContext(ctx, "Acquire succeeded, job loaded and lock held")
		metrics.AcquireWaitDuration.WithLabelValues(groupID).Observe(time.Since(startTime).Seconds())
		return &pb.AcquireResponse{
			Success:         true,
			ContextRestored: true, // Default to true, as we don't have enough info to determine if it was zero-overhead
			WaitedMs:        time.Since(startTime).Milliseconds(),
		}, nil, true
	}

	return nil, nil, false //nolint:nilnil // returning nil, nil is intended when done is false
}

func (s *Server) isGroupFaulted(ctx context.Context, groupID string) (bool, error) {
	jobs, err := s.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		return false, err
	}
	for _, job := range jobs {
		for _, state := range job.ContextState() {
			if state == pb.SnapshotAgentJobState_STATE_FAULTED {
				return true, nil
			}
		}
	}
	return false, nil
}

// Yield implements TimeSliceOrchestratorService.Yield.
func (s *Server) Yield(ctx context.Context, req *pb.YieldRequest) (*pb.YieldResponse, error) {
	ctx = logging.WithServerMethod(ctx, "Yield")
	ctx = logging.WithJobID(ctx, req.GetJobId())
	ctx = logging.WithGroupID(ctx, req.GetGroupId())
	slog.InfoContext(ctx, "Yield called")

	groupID := req.GetGroupId()
	jobID := req.GetJobId()

	role, err := effectiveRole(req.GetRole())
	if err != nil {
		return nil, err
	}

	// Validate the hint before anything changes, so a bad request is refused
	// without yielding.
	lendHint := false
	if req.ExpectedIdle != nil {
		if err := req.GetExpectedIdle().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid expected_idle: %v", err)
		}
		idle := req.GetExpectedIdle().AsDuration()
		if idle < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "expected_idle must not be negative, got %v", idle)
		}
		lendHint = s.minBubble > 0 && idle >= s.minBubble
	}

	// 1. Get Group
	group, err := s.groupStore.Get(ctx, groupID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "group %s not found", groupID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get group: %v", err)
	}

	if role == pb.Role_ROLE_BACKGROUND {
		return s.yieldBackground(ctx, group, jobID)
	}

	// 2. Take Snapshot BEFORE Yield
	snap := group.Snapshot()

	// 3. Call Yield
	err = group.Spec().Yield(ctx, jobID)
	if err != nil {
		if errors.Is(err, store.ErrNotLockHolder) {
			return nil, status.Errorf(codes.PermissionDenied, "job %s does not hold lock for group %s", jobID, groupID)
		}
		return nil, status.Errorf(codes.Internal, "failed to yield: %v", err)
	}

	// Record the lend hint only. The handler can run before the informers
	// have synced, so it decides nothing; the reconcile loop does.
	group.Spec().SetLend(lendHint)

	if s.ctrl != nil {
		s.ctrl.EnqueueWork(groupID)
	}

	// 4. Construct Response from Snapshot
	numWaiters := snap.WaiterQueueDepth
	if numWaiters == 0 {
		metrics.DeferredSnapshotsTotal.WithLabelValues(groupID).Inc()
	}
	return &pb.YieldResponse{
		Success:          true,
		PendingWaiters:   int64(numWaiters),
		SnapshotDeferred: numWaiters == 0,
	}, nil
}

// ListGroups implements TimeSliceOrchestratorService.ListGroups.
func (s *Server) ListGroups(ctx context.Context, req *pb.ListGroupsRequest) (*pb.ListGroupsResponse, error) {
	ctx = logging.WithServerMethod(ctx, "ListGroups")
	slog.InfoContext(ctx, "ListGroups called")
	groups, err := s.groupStore.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list groups: %v", err)
	}
	var ids []string
	for _, g := range groups {
		ids = append(ids, g.ID())
	}
	return &pb.ListGroupsResponse{GroupIds: ids}, nil
}

// GetGroupStatus implements TimeSliceOrchestratorService.GetGroupStatus.
func (s *Server) GetGroupStatus(ctx context.Context, req *pb.GetGroupStatusRequest) (*pb.GetGroupStatusResponse, error) {
	ctx = logging.WithServerMethod(ctx, "GetGroupStatus")
	ctx = logging.WithGroupID(ctx, req.GetGroupId())
	slog.InfoContext(ctx, "GetGroupStatus called")

	group, err := s.groupStore.Get(ctx, req.GetGroupId())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "group %s not found", req.GetGroupId())
		}
		return nil, status.Errorf(codes.Internal, "failed to get group: %v", err)
	}

	if participantID := req.GetParticipantId(); participantID != "" {
		if err := s.touchParticipant(ctx, group, participantID); err != nil {
			return nil, err
		}
	}

	snap := group.Snapshot()

	if snap.State == pb.GroupStatus_STATE_UNKNOWN {
		return nil, status.Errorf(codes.Unavailable, "group %s state is unknown", req.GetGroupId())
	}

	groupStatus := &pb.GroupStatus{
		GroupId:          snap.ID,
		GroupState:       snap.EffectiveState(),
		StateTimestamp:   timestamppb.New(snap.StateTimestamp),
		LockingJob:       snap.LockingJob,
		ActiveJob:        snap.ActiveJob,
		WaiterQueueDepth: int64(s.advertisedWaiterDepth(ctx, snap)),
		LoadedJob:        snap.LoadedJob,
	}
	if s.backgroundRole {
		groupStatus.BackgroundProtocol = BackgroundProtocolVersion
	}
	if !snap.NoticeAt.IsZero() {
		groupStatus.VacateWithin = durationpb.New(s.vacateWithin(snap.NoticeAt, time.Now()))
	}

	// Write the dispatch budget through before returning. This response is what
	// makes a cooperative tenant drain and yield the accelerator, so publishing
	// here — rather than reacting to the outage afterwards — is what puts the
	// signal ahead of the outage instead of behind it.
	s.publishDispatchBudget(ctx)

	jobs, err := s.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list jobs for group %s: %v", group.ID(), err)
	}

	var agentJobStates []*pb.SnapshotAgentJobState
	for _, job := range jobs {
		for agent, jobState := range job.ContextState() {
			agentJobStates = append(agentJobStates, &pb.SnapshotAgentJobState{
				Agent:    agent,
				JobState: jobState,
				JobId:    job.JobID(),
			})
		}
	}

	return &pb.GetGroupStatusResponse{
		Group:          groupStatus,
		AgentJobStates: agentJobStates,
	}, nil
}

// advertisedWaiterDepth returns the waiter queue depth to report in
// GetGroupStatus. Cooperative tenants poll this field and yield the lock as
// soon as it is non-zero, so reporting it truthfully the instant a waiter
// queues lets a tenant be pre-empted before it has served anything: the lock is
// handed back and forth with nothing but cuda-checkpoint traffic in between.
//
// The minimum serving quantum fixes that by withholding only the advertisement
// of pre-emption pressure. Waiters stay enqueued and are promoted by the
// controller exactly as before, so nothing is starved and no client has to
// cooperate: a holder that releases early, crashes, or is deleted still frees
// the lock immediately, and the withholding is bounded by the quantum measured
// from the moment the holder could first actually serve.
func (s *Server) advertisedWaiterDepth(ctx context.Context, snap *store.GroupSnapshot) int {
	served, withheld := s.quantumWithholding(snap)
	if !withheld {
		return snap.WaiterQueueDepth
	}

	slog.InfoContext(ctx, "Withholding pre-emption pressure for minimum serving quantum",
		"holder", snap.LockingJob,
		"served", served,
		"quantum", s.servingQuantum,
		"suppressedWaiters", snap.WaiterQueueDepth,
	)
	metrics.QuantumSuppressedPollsTotal.WithLabelValues(snap.ID).Inc()
	return 0
}

// quantumWithholding reports whether the minimum serving quantum is currently
// withholding pre-emption pressure from the holder, and for how long the holder
// has been able to serve. It is the side-effect-free half of
// advertisedWaiterDepth, so callers that are not answering a poll (the dispatch
// budget publisher) do not inflate the suppressed-poll counter or the log.
func (s *Server) quantumWithholding(snap *store.GroupSnapshot) (time.Duration, bool) {
	if s.servingQuantum <= 0 || snap.WaiterQueueDepth == 0 {
		return 0, false
	}

	servingSince := snap.ServingSince()
	if servingSince.IsZero() {
		// Not serving (no holder, or a grant whose context is still being
		// restored, or a holder recovered from the lock store). Fail open.
		return 0, false
	}

	served := time.Since(servingSince)
	if served >= s.servingQuantum {
		return served, false
	}
	return served, true
}

// quietAdvertisedWaiterDepth is advertisedWaiterDepth without the logging and
// the counter.
func (s *Server) quietAdvertisedWaiterDepth(snap *store.GroupSnapshot) int {
	if _, withheld := s.quantumWithholding(snap); withheld {
		return 0
	}
	return snap.WaiterQueueDepth
}

// publishDispatchBudget evaluates every group and publishes whether the batch
// tenant can currently serve.
//
// The batch tenant is a single job and can hold at most one group's lock, so
// the aggregate across groups is an OR: the budget is Available if any group
// has it serving. Every failure path publishes Blocked rather than skipping the
// write, because the consuming gate reads a missing or stale-open key as full
// capacity — the safe direction for this signal is always "do not dispatch".
func (s *Server) publishDispatchBudget(ctx context.Context) {
	if s.budget == nil {
		return
	}

	value := budget.Blocked
	groups, err := s.groupStore.List(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to list groups for dispatch budget; publishing blocked", "error", err)
	}
	for _, g := range groups {
		snap := g.Snapshot()
		if budget.For(snap, s.budget.BatchJob(), s.quietAdvertisedWaiterDepth(snap)) == budget.Available {
			value = budget.Available
			break
		}
	}

	if err := s.budget.Publish(ctx, value); err != nil {
		slog.ErrorContext(ctx, "Failed to publish dispatch budget", "error", err)
	}
}

// runBudgetPublisher republishes the dispatch budget on a fixed interval until
// ctx is done. GetGroupStatus already writes through on every poll, which is
// what gets the signal out ahead of a yield; this loop exists for the edges no
// poll covers — process start (the key must exist before the gate first reads
// it), a tenant that stops polling, and recreating a key that was evicted.
func (s *Server) runBudgetPublisher(ctx context.Context) {
	interval := budgetPublishInterval
	if s.budget.OpenDelay() > 0 {
		interval = heldBudgetPublishInterval
	}
	slog.InfoContext(ctx, "Starting dispatch budget publisher",
		"key", s.budget.Key(),
		"batchJob", s.budget.BatchJob(),
		"interval", interval,
		"openDelay", s.budget.OpenDelay(),
		"externalRisingEdge", s.budget.ExternalRisingEdge(),
	)
	s.publishDispatchBudget(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Stopping dispatch budget publisher")
			return
		case <-ticker.C:
			s.publishDispatchBudget(ctx)
		}
	}
}

// StartServer starts the gRPC server on the specified port and handles graceful shutdown when the context is canceled.
// It also starts the controller in the background.
func StartServer(
	ctx context.Context,
	port int,
	metricsPort int,
	ctrl *controller.Controller,
	groupStore GroupStore,
	jobStore JobStore,
	workers int,
	opts ...Option,
) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Start HTTP metrics server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", metricsPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		slog.InfoContext(ctx, "Starting HTTP metrics server", "port", metricsPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "HTTP metrics server failed", "error", err)
		}
	}()

	// Start controller in background
	go func() {
		slog.InfoContext(ctx, "Starting controller from server", "workers", workers)
		if err := ctrl.Run(ctx, workers); err != nil {
			slog.ErrorContext(ctx, "Error running controller", "error", err)
		}
	}()

	s := grpc.NewServer()
	orchServer := NewServer(ctrl, groupStore, jobStore, opts...)
	if orchServer.budget != nil {
		go orchServer.runBudgetPublisher(ctx)
	}
	pb.RegisterTimeSliceOrchestratorServiceServer(s, orchServer)

	errChan := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "Starting gRPC server", "port", port)
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errChan <- fmt.Errorf("failed to serve: %w", err)
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		slog.InfoContext(ctx, "Context canceled, shutting down servers gracefully")
		s.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "HTTP metrics server shutdown error", "error", err)
		}
		cancel()
		<-errChan
		slog.InfoContext(ctx, "Server stopped")
	}

	return nil
}
