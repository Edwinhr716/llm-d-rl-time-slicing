//go:build evalwire

// Package host is the reference host for decision D-NS-4, option "keep": the
// protocol half of the VK only, as the frozen contract (§3) describes it. The
// VK stays a lock participant. Per group of its node it
//
//  1. polls GetGroupStatus with participant_id every 0.5 s (the heartbeat),
//  2. waits in Acquire(ROLE_BACKGROUND, node_name) until the node is lent,
//  3. on the grant resumes the guests it suspended (Resume, then Ready) and
//     marks new guests Ready,
//  4. on vacate_within marks each guest NotReady, then Suspends it by the
//     deadline, then hands the grant back with Yield(ROLE_BACKGROUND),
//
// and goes back to 2. The guest-side work (readiness, the agent calls) is done
// by an Executor, which the evaluator scripts identically for every option.
//
// It is compiled only with the evalwire build tag and is never part of the
// product binary.
package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Executor does the guest-side work on one host. The interface is fixed by
// the D-NS-4 evaluation plan; do not change it.
type Executor interface {
	Guests() []string                                    // guests on this host now
	SetNotReady(ctx context.Context, guest string) error // returns once confirmed
	Suspend(ctx context.Context, guest string, deadline time.Time) error
	Resume(ctx context.Context, guest string, deadline time.Time) error
	SetReady(ctx context.Context, guest string) error
}

// Config configures one reference host.
type Config struct {
	// Node is the real node name; the participant ID is "vk/<Node>".
	Node string
	// OrchAddr is the orchestrator's gRPC address (host:port).
	OrchAddr string
	// ListenAddr is unused by "keep": the orchestrator never calls the host.
	ListenAddr string
	// AgentAddr is unused by the reference host: the Executor makes the
	// agent calls.
	AgentAddr string
	// Exec does the guest-side work.
	Exec Executor
}

// Timing of the VK loop. The poll interval and L come from the contract.
const (
	pollInterval = 500 * time.Millisecond
	// liveness is L: a grant is trusted only while a status poll succeeded
	// within L.
	liveness = 3 * time.Second
	// deadlineMargin is taken off vacate_within to cover the poll's latency.
	deadlineMargin = 250 * time.Millisecond
	// participantGrace delays participant_id on polls until the first
	// Acquire has registered, so the host's own first poll does not create a
	// claim on the orchestrator.
	participantGrace = 200 * time.Millisecond
	// resumeBudget bounds each guest Resume.
	resumeBudget = 30 * time.Second
	// discoverInterval is how often the host lists groups.
	discoverInterval = time.Second
	// retryInterval spaces retries of failed RPCs and readiness writes.
	retryInterval = 200 * time.Millisecond
	// notMemberRecheck is how long a group the node is not part of is left
	// alone before it is tried again.
	notMemberRecheck = 10 * time.Second
	rpcTimeout       = 5 * time.Second
)

var errNotMember = errors.New("node is not a member of the group")

// Host is a running reference host.
type Host struct {
	cfg    Config
	conn   *grpc.ClientConn
	client pb.TimeSliceOrchestratorServiceClient
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// guestMu guards suspended and ready. Guest work is serialized per host.
	guestMu   sync.Mutex
	suspended map[string]bool
	ready     map[string]bool
}

// Start starts the reference host. It returns at once; the loops run until
// Stop or until ctx ends.
func Start(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.Node == "" || cfg.OrchAddr == "" || cfg.Exec == nil {
		return nil, errors.New("host: Node, OrchAddr and Exec are required")
	}
	conn, err := grpc.NewClient(cfg.OrchAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("host: dial orchestrator: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	h := &Host{
		cfg:       cfg,
		conn:      conn,
		client:    pb.NewTimeSliceOrchestratorServiceClient(conn),
		cancel:    cancel,
		suspended: make(map[string]bool),
		ready:     make(map[string]bool),
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.manage(ctx)
	}()
	return h, nil
}

// Stop stops the host and waits for its loops to end. It hands nothing back:
// like a VK that dies, it leaves any grant to the orchestrator.
func (h *Host) Stop() {
	h.cancel()
	h.wg.Wait()
	if err := h.conn.Close(); err != nil {
		slog.Warn("host: closing the orchestrator connection failed", "node", h.cfg.Node, "error", err)
	}
}

func (h *Host) participantID() string { return "vk/" + h.cfg.Node }

// manage lists the groups every second and runs one participant loop per
// group this node belongs to.
func (h *Host) manage(ctx context.Context) {
	var mu sync.Mutex
	running := make(map[string]bool)
	skipUntil := make(map[string]time.Time)
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()
	for {
		groups, err := h.listGroups(ctx)
		if err == nil {
			for _, group := range groups {
				mu.Lock()
				start := !running[group] && time.Now().After(skipUntil[group])
				if start {
					running[group] = true
				}
				mu.Unlock()
				if !start {
					continue
				}
				h.wg.Add(1)
				go func() {
					defer h.wg.Done()
					err := newParticipant(h, group).run(ctx)
					mu.Lock()
					defer mu.Unlock()
					delete(running, group)
					if errors.Is(err, errNotMember) {
						skipUntil[group] = time.Now().Add(notMemberRecheck)
					}
				}()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Host) listGroups(ctx context.Context) ([]string, error) {
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := h.client.ListGroups(rctx, &pb.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetGroupIds(), nil
}

// participant is the VK loop for one group.
type participant struct {
	h     *Host
	group string

	mu sync.Mutex
	// acquireSentAt is when the first background Acquire was sent; zero
	// before. Polls carry participant_id from acquireSentAt + grace on.
	acquireSentAt time.Time
	// lastOK is when the last successful poll was sent.
	lastOK time.Time
	// protocol is GroupStatus.background_protocol from the last poll.
	protocol int32
	// deadline is the vacate deadline from the last poll, zero when no
	// notice runs.
	deadline time.Time
	// gone is set when the group no longer exists.
	gone bool
	// changed is signalled after every poll.
	changed chan struct{}
}

func newParticipant(h *Host, group string) *participant {
	return &participant{h: h, group: group, changed: make(chan struct{}, 1)}
}

// run is the contract §3 VK loop. It returns when ctx ends, the group is
// gone, or the node is not one of its nodes.
func (p *participant) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.poll(ctx)

	// 1. Wait until the server speaks the background protocol.
	for {
		if p.snapshot().protocol == 1 {
			break
		}
		if err := p.wait(ctx); err != nil {
			return err
		}
	}

	vacated := false
	for {
		// 2. Wait in Acquire(ROLE_BACKGROUND); vacate on a notice meanwhile.
		if err := p.acquire(ctx, vacated); err != nil {
			return err
		}
		// 3. Granted: resume, then hold until a notice or the lease is lost.
		p.onGrant(ctx)
		var err error
		if vacated, err = p.holdGrant(ctx); err != nil {
			return err
		}
	}
}

// pollState is a copy of what the poller last saw.
type pollState struct {
	lastOK   time.Time
	protocol int32
	deadline time.Time
	gone     bool
}

func (p *participant) snapshot() pollState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pollState{lastOK: p.lastOK, protocol: p.protocol, deadline: p.deadline, gone: p.gone}
}

// wait blocks until the next poll or ctx ends.
func (p *participant) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.changed:
	}
	if p.snapshot().gone {
		return errors.New("group is gone")
	}
	return nil
}

// poll sends GetGroupStatus every 0.5 s.
func (p *participant) poll(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		p.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *participant) pollOnce(ctx context.Context) {
	req := &pb.GetGroupStatusRequest{GroupId: p.group}
	p.mu.Lock()
	if !p.acquireSentAt.IsZero() && time.Since(p.acquireSentAt) >= participantGrace {
		req.ParticipantId = p.h.participantID()
	}
	p.mu.Unlock()

	sent := time.Now()
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := p.h.client.GetGroupStatus(rctx, req)
	cancel()

	p.mu.Lock()
	switch {
	case err == nil:
		gs := resp.GetGroup()
		p.lastOK = sent
		p.protocol = gs.GetBackgroundProtocol()
		p.deadline = time.Time{}
		if gs.VacateWithin != nil {
			p.deadline = sent.Add(gs.GetVacateWithin().AsDuration() - deadlineMargin)
		}
	case status.Code(err) == codes.NotFound:
		p.gone = true
	default:
		slog.DebugContext(ctx, "host: status poll failed", "node", p.h.cfg.Node, "group", p.group, "error", err)
	}
	p.mu.Unlock()

	select {
	case p.changed <- struct{}{}:
	default:
	}
}

// acquireResult is the outcome of one background Acquire.
type acquireResult struct{ err error }

// acquire waits in Acquire(ROLE_BACKGROUND) until the node is lent. While it
// waits, a notice makes it vacate any guest that is still up and hand back
// any grant or claim the orchestrator may hold for it.
func (p *participant) acquire(ctx context.Context, vacated bool) error {
	for {
		actx, cancel := context.WithCancel(ctx)
		done := make(chan acquireResult, 1)
		p.mu.Lock()
		if p.acquireSentAt.IsZero() {
			p.acquireSentAt = time.Now()
		}
		p.mu.Unlock()
		go func() {
			_, err := p.h.client.Acquire(actx, &pb.AcquireRequest{
				JobId:    p.h.participantID(),
				GroupId:  p.group,
				Role:     pb.Role_ROLE_BACKGROUND,
				NodeName: p.h.cfg.Node,
			})
			done <- acquireResult{err: err}
		}()

		var res acquireResult
	waiting:
		for {
			select {
			case <-ctx.Done():
				cancel()
				return ctx.Err()
			case res = <-done:
				break waiting
			case <-p.changed:
				st := p.snapshot()
				if st.gone {
					cancel()
					return errors.New("group is gone")
				}
				if st.deadline.IsZero() {
					vacated = false
					continue
				}
				if !vacated {
					vacated = true
					p.vacate(ctx, st.deadline)
				}
			}
		}
		cancel()

		if res.err == nil {
			return nil
		}
		switch status.Code(res.err) {
		case codes.FailedPrecondition:
			if strings.Contains(status.Convert(res.err).Message(), "is not a node of group") {
				return errNotMember
			}
		case codes.NotFound:
			return errors.New("group is gone")
		default:
		}
		slog.DebugContext(ctx, "host: background Acquire failed, retrying",
			"node", p.h.cfg.Node, "group", p.group, "error", res.err)
		if err := sleep(ctx, retryInterval); err != nil {
			return err
		}
	}
}

// onGrant resumes the guests this host suspended (Resume, then Ready) and
// marks guests it has never seen Ready. Between guests it checks the lease:
// a notice, or no successful poll within L, stops it.
func (p *participant) onGrant(ctx context.Context) {
	h := p.h
	slog.InfoContext(ctx, "host: node granted", "node", h.cfg.Node, "group", p.group)
	h.guestMu.Lock()
	defer h.guestMu.Unlock()
	for _, guest := range h.cfg.Exec.Guests() {
		if !p.leaseValid() {
			slog.InfoContext(ctx, "host: lease lost while resuming", "node", h.cfg.Node, "group", p.group)
			return
		}
		if h.ready[guest] {
			continue
		}
		if h.suspended[guest] {
			rctx, cancel := context.WithTimeout(ctx, resumeBudget)
			err := h.cfg.Exec.Resume(rctx, guest, time.Now().Add(resumeBudget))
			cancel()
			if err != nil {
				// Never Ready before a completed Resume.
				slog.WarnContext(ctx, "host: resume failed", "node", h.cfg.Node, "guest", guest, "error", err)
				continue
			}
			delete(h.suspended, guest)
		}
		if err := h.cfg.Exec.SetReady(ctx, guest); err != nil {
			slog.WarnContext(ctx, "host: set ready failed", "node", h.cfg.Node, "guest", guest, "error", err)
			continue
		}
		h.ready[guest] = true
	}
}

// leaseValid reports whether no notice runs and a poll succeeded within L.
func (p *participant) leaseValid() bool {
	st := p.snapshot()
	return st.deadline.IsZero() && time.Since(st.lastOK) < liveness
}

// holdGrant waits while the node is lent. A notice makes it vacate and
// return; so does losing the lease (no successful poll within L), after which
// the loop re-enters Acquire and the orchestrator decides again. It reports
// whether it vacated for the current notice.
func (p *participant) holdGrant(ctx context.Context) (bool, error) {
	for {
		st := p.snapshot()
		if !st.deadline.IsZero() {
			p.vacate(ctx, st.deadline)
			return true, nil
		}
		if time.Since(st.lastOK) >= liveness {
			slog.WarnContext(ctx, "host: lease lost, re-entering Acquire", "node", p.h.cfg.Node, "group", p.group)
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-p.changed:
			if p.snapshot().gone {
				return false, errors.New("group is gone")
			}
		case <-time.After(pollInterval):
		}
	}
}

// vacate marks every guest that is not suspended NotReady, then suspends it
// by deadline, one guest at a time, then hands the grant back with
// Yield(ROLE_BACKGROUND). A guest whose NotReady is never confirmed is not
// suspended (NotReady must come first). The Yield is sent even when a guest
// failed to suspend: the orchestrator decides "vacated" from the agent's
// report, not from the Yield alone.
func (p *participant) vacate(ctx context.Context, deadline time.Time) {
	h := p.h
	slog.InfoContext(ctx, "host: vacating", "node", h.cfg.Node, "group", p.group, "deadline", deadline)
	h.guestMu.Lock()
	for _, guest := range h.cfg.Exec.Guests() {
		if h.suspended[guest] {
			continue
		}
		delete(h.ready, guest)
		if !notReadyBy(ctx, h.cfg.Exec, guest, deadline) {
			slog.WarnContext(ctx, "host: NotReady not confirmed by the deadline, not suspending",
				"node", h.cfg.Node, "guest", guest)
			continue
		}
		sctx, cancel := context.WithDeadline(ctx, deadline)
		err := h.cfg.Exec.Suspend(sctx, guest, deadline)
		cancel()
		if err != nil {
			slog.WarnContext(ctx, "host: suspend failed", "node", h.cfg.Node, "guest", guest, "error", err)
			continue
		}
		h.suspended[guest] = true
	}
	h.guestMu.Unlock()
	p.yield(ctx, deadline)
}

// notReadyBy retries SetNotReady until it is confirmed or deadline passes.
func notReadyBy(ctx context.Context, exec Executor, guest string, deadline time.Time) bool {
	for {
		nctx, cancel := context.WithDeadline(ctx, deadline)
		err := exec.SetNotReady(nctx, guest)
		cancel()
		if err == nil {
			return true
		}
		if ctx.Err() != nil || !time.Now().Add(retryInterval).Before(deadline) {
			return false
		}
		if sleep(ctx, retryInterval) != nil {
			return false
		}
	}
}

// yield sends Yield(ROLE_BACKGROUND), retrying until it succeeds or the
// deadline passes (at least one attempt).
func (p *participant) yield(ctx context.Context, deadline time.Time) {
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		_, err := p.h.client.Yield(rctx, &pb.YieldRequest{
			JobId:   p.h.participantID(),
			GroupId: p.group,
			Role:    pb.Role_ROLE_BACKGROUND,
		})
		cancel()
		if err == nil {
			slog.InfoContext(ctx, "host: background Yield sent", "node", p.h.cfg.Node, "group", p.group)
			return
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			slog.WarnContext(ctx, "host: background Yield failed", "node", p.h.cfg.Node, "group", p.group, "error", err)
			return
		}
		if sleep(ctx, retryInterval) != nil {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
