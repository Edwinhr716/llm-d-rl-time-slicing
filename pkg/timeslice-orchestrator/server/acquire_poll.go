package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"google.golang.org/grpc/metadata"
)

// Foreground wait option async-poll (PENDING LEAD DECISION D-ORCH-1).
//
// A client opts in per call by sending the request metadata
// AcquireModeMetadataKey = AcquireModePoll. On a server started with
// --foreground-wait=async-poll such an Acquire checks once and returns at once:
// success=true when the job holds the lock with its context loaded, otherwise
// success=false with waited_ms counted from the job's first poll, and the
// client calls Acquire again. The request and response messages are unchanged.
//
// Callers that do not send the metadata (clients built before this option)
// keep the blocking Acquire, and a server in blocking mode ignores the
// metadata and blocks, so either client works against either server.
//
// Only a foreground Acquire can poll. A ROLE_BACKGROUND Acquire (the VK's
// guest participant) ignores the metadata and keeps its own blocking wait.
//
// The blocking Acquire withdraws the caller from the waiter queue on every
// return other than a grant. A poll that is not granted must keep the job
// queued, so here the job is withdrawn when a poll ends in an error, and when
// the client stops polling for pollStaleAfter.

// AcquireModeMetadataKey is the request metadata key a polling client sends.
const AcquireModeMetadataKey = "x-timeslice-acquire-mode"

// AcquireModePoll is the AcquireModeMetadataKey value that asks for an Acquire
// that returns at once.
const AcquireModePoll = "poll"

// pollStaleAfter is how long a job may go without polling before its next poll
// counts as a new wait (for waited_ms and the wait metric) and enqueues the
// group again. A job that has not polled for this long is also withdrawn from
// the waiter queue.
const pollStaleAfter = 30 * time.Second

// pollStaleMargin is added to the stale timer, so that it fires only after the
// wait is stale.
const pollStaleMargin = 100 * time.Millisecond

// pollWait is the first and the latest poll of one job's pending Acquire.
type pollWait struct {
	first time.Time
	last  time.Time
}

// pollTracker remembers when each polling job started waiting. It is memory
// only: after a restart a job's next poll starts a new wait.
type pollTracker struct {
	mu     sync.Mutex
	waits  map[string]pollWait
	timers map[string]*time.Timer
	// stale overrides pollStaleAfter (tests). Zero means pollStaleAfter.
	stale time.Duration
}

func (p *pollTracker) staleAfter() time.Duration {
	if p.stale > 0 {
		return p.stale
	}
	return pollStaleAfter
}

func pollKey(groupID, jobID string) string {
	return groupID + "/" + jobID
}

// mark records a poll at now and returns when the wait started, and whether
// this poll started it.
func (p *pollTracker) mark(groupID, jobID string, now time.Time) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waits == nil {
		p.waits = make(map[string]pollWait)
	}
	key := pollKey(groupID, jobID)
	w, ok := p.waits[key]
	isNew := !ok || now.Sub(w.last) > p.staleAfter()
	if isNew {
		w.first = now
	}
	w.last = now
	p.waits[key] = w
	return w.first, isNew
}

// clear forgets a job's wait and stops its stale timer.
func (p *pollTracker) clear(groupID, jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearLocked(pollKey(groupID, jobID))
}

func (p *pollTracker) clearLocked(key string) {
	delete(p.waits, key)
	if tm, ok := p.timers[key]; ok {
		tm.Stop()
		delete(p.timers, key)
	}
}

// arm (re)starts the job's stale timer: fire runs once the job has not polled
// for staleAfter.
func (p *pollTracker) arm(groupID, jobID string, fire func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.timers == nil {
		p.timers = make(map[string]*time.Timer)
	}
	key := pollKey(groupID, jobID)
	if tm, ok := p.timers[key]; ok {
		tm.Stop()
	}
	p.timers[key] = time.AfterFunc(p.staleAfter()+pollStaleMargin, fire)
}

// expire forgets the job's wait if its latest poll is at least staleAfter
// before now, and reports whether it did.
func (p *pollTracker) expire(groupID, jobID string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := pollKey(groupID, jobID)
	w, ok := p.waits[key]
	if !ok || now.Sub(w.last) < p.staleAfter() {
		return false
	}
	p.clearLocked(key)
	return true
}

// WithForegroundWait sets the --foreground-wait mode. Only
// controller.ForegroundWaitAsyncPoll changes the server: it lets clients that
// send the poll metadata get an Acquire that returns at once. Any other value
// keeps every Acquire blocking.
func WithForegroundWait(mode string) Option {
	return func(s *Server) {
		s.acquirePoll = mode == controller.ForegroundWaitAsyncPoll
	}
}

// wantsPoll reports whether the caller sent the poll metadata.
func wantsPoll(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, v := range md.Get(AcquireModeMetadataKey) {
		if v == AcquireModePoll {
			return true
		}
	}
	return false
}

// acquirePollOnce answers one poll of a foreground Acquire. The caller has
// already requested the lock and started any notice.
func (s *Server) acquirePollOnce(ctx context.Context, groupID, jobID string, now time.Time) (*pb.AcquireResponse, error) {
	startTime, isNew := s.polls.mark(groupID, jobID, now)
	if isNew && s.ctrl != nil {
		// Once per wait, as the blocking Acquire does, so polling does not
		// add a reconcile (and its agent Status calls) per poll.
		s.ctrl.EnqueueWork(groupID)
	}

	resp, err, done := s.checkAcquire(ctx, groupID, jobID, startTime)
	if done {
		s.polls.clear(groupID, jobID)
		if err != nil {
			// As the blocking Acquire does on every return other than a grant.
			s.withdrawPoller(ctx, groupID, jobID)
		}
		return resp, err
	}
	// The timer outlives this RPC, so it must not inherit its cancellation.
	bg := context.WithoutCancel(ctx)
	s.polls.arm(groupID, jobID, func() { s.dropStalePoller(bg, groupID, jobID) })

	waited := time.Since(startTime)
	slog.InfoContext(ctx, "Acquire not granted yet, returning to the polling client",
		"waitedMs", waited.Milliseconds(), "firstPoll", isNew)
	return &pb.AcquireResponse{
		Success:  false,
		WaitedMs: waited.Milliseconds(),
	}, nil
}

// dropStalePoller withdraws a job whose client stopped polling, so that it is
// not promoted on the next Yield for a caller that has gone.
func (s *Server) dropStalePoller(ctx context.Context, groupID, jobID string) {
	if !s.polls.expire(groupID, jobID, time.Now()) {
		return
	}
	ctx = logging.WithJobID(logging.WithGroupID(ctx, groupID), jobID)
	slog.InfoContext(ctx, "Polling client stopped polling, withdrawing its Acquire",
		"after", s.polls.staleAfter())
	s.withdrawPoller(ctx, groupID, jobID)
}

// withdrawPoller removes the job from the group's waiter queue.
func (s *Server) withdrawPoller(ctx context.Context, groupID, jobID string) {
	group, err := s.groupStore.Get(ctx, groupID)
	if err != nil {
		slog.WarnContext(ctx, "Cannot withdraw the polling job: group lookup failed", "error", err)
		return
	}
	s.dropWaiter(ctx, group, jobID)
}
