//go:build evalwire

// Package host is the reference host for D-NS-4 option ns-push-agent. It is
// compiled only with the evalwire build tag and is never part of the product
// binary.
//
// It stands in for the snapshot agent's HostCommandService on one node: it
// serves SuspendAll and ResumeAll on Config.ListenAddr and drives the guests
// of the node through Config.Exec. Per guest, SuspendAll runs SetNotReady and
// then Suspend; ResumeAll runs Resume and then SetReady. Guests are handled
// one at a time. The epoch rules are the ones documented on
// HostCommandService in pkg/snapshot-agent/api/v1alpha1/host_command.proto.
//
// It is not the real agent pipeline: there is no pod discovery, no
// checkpoint, and state is in memory only.
package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc"
)

// Executor drives the guests of one host. The interface is fixed by the
// D-NS-4 evaluation plan.
type Executor interface {
	Guests() []string                                    // guests on this host now
	SetNotReady(ctx context.Context, guest string) error // returns once confirmed
	Suspend(ctx context.Context, guest string, deadline time.Time) error
	Resume(ctx context.Context, guest string, deadline time.Time) error
	SetReady(ctx context.Context, guest string) error
}

// Config configures a Host.
type Config struct {
	// Node is the name of the node the host runs on (logging only).
	Node string
	// OrchAddr is unused: in ns-push-agent the orchestrator calls the host.
	OrchAddr string
	// ListenAddr is where HostCommandService is served, for example
	// 127.0.0.2:9101.
	ListenAddr string
	// AgentAddr is unused: this host is the agent's host endpoint.
	AgentAddr string
	// Exec drives the guests.
	Exec Executor
}

const (
	cmdSuspend = "SuspendAll"
	cmdResume  = "ResumeAll"
)

// command is one host command. Its epoch and deadline change when a caller
// with a higher epoch adopts it.
type command struct {
	kind     string
	epoch    int64
	deadline time.Time
	prev     *command // runs first
	started  bool
	dropped  bool // replaced before it started
	done     chan struct{}
	ack      *agentpb.HostCommandAck
}

// Host is a running reference host.
type Host struct {
	cfg    Config
	lis    net.Listener
	srv    *grpc.Server
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	seen      bool
	maxEpoch  int64
	cur       *command
	suspended map[string]bool
}

// Start serves HostCommandService on cfg.ListenAddr until ctx is done or Stop
// is called.
func Start(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.Exec == nil {
		return nil, errors.New("host: Exec is required")
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("host %s: listen on %s: %w", cfg.Node, cfg.ListenAddr, err)
	}
	hctx, cancel := context.WithCancel(ctx)
	h := &Host{
		cfg:       cfg,
		lis:       lis,
		srv:       grpc.NewServer(),
		ctx:       hctx,
		cancel:    cancel,
		suspended: make(map[string]bool),
	}
	agentpb.RegisterHostCommandServiceServer(h.srv, &service{h: h})
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = h.srv.Serve(lis)
	}()
	go func() {
		<-hctx.Done()
		h.srv.Stop()
	}()
	return h, nil
}

// Addr returns the address the host listens on.
func (h *Host) Addr() string { return h.lis.Addr().String() }

// Stop stops serving and waits for the server to exit. A command still
// running is abandoned where it is.
func (h *Host) Stop() {
	h.cancel()
	h.wg.Wait()
}

type service struct {
	agentpb.UnimplementedHostCommandServiceServer
	h *Host
}

func (s *service) SuspendAll(ctx context.Context, req *agentpb.SuspendAllRequest) (*agentpb.HostCommandAck, error) {
	return s.h.handle(ctx, cmdSuspend, req.GetGroup(), req.GetEpoch(), req.GetDeadline().AsTime())
}

func (s *service) ResumeAll(ctx context.Context, req *agentpb.ResumeAllRequest) (*agentpb.HostCommandAck, error) {
	return s.h.handle(ctx, cmdResume, req.GetGroup(), req.GetEpoch(), req.GetDeadline().AsTime())
}

func stale(epoch int64, why string) *agentpb.HostCommandAck {
	return &agentpb.HostCommandAck{
		Epoch:   epoch,
		Outcome: agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_STALE_EPOCH,
		Error:   why,
	}
}

// handle applies the epoch rules, then waits for the command's ack or for
// the caller to give up. The command keeps running if the caller gives up.
func (h *Host) handle(
	ctx context.Context, kind, group string, epoch int64, deadline time.Time,
) (*agentpb.HostCommandAck, error) {
	h.mu.Lock()
	var cmd *command
	switch {
	case h.seen && epoch < h.maxEpoch:
		h.mu.Unlock()
		return stale(epoch, fmt.Sprintf("epoch %d below %d", epoch, h.maxEpoch)), nil
	case h.seen && epoch == h.maxEpoch:
		if h.cur == nil || h.cur.epoch != epoch || h.cur.kind != kind {
			h.mu.Unlock()
			return stale(epoch, fmt.Sprintf("epoch %d already used for another command", epoch)), nil
		}
		cmd = h.cur // join or replay
	default:
		h.seen = true
		h.maxEpoch = epoch
		cmd = h.newCommandLocked(kind, epoch, deadline)
	}
	h.mu.Unlock()
	slog.DebugContext(ctx, "Host command received", "node", h.cfg.Node, "group", group, "command", kind,
		"epoch", epoch, "deadline", deadline)

	select {
	case <-cmd.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.ctx.Done():
		return nil, h.ctx.Err()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneAck(cmd.ack), nil
}

// newCommandLocked adopts, queues or starts a command for a new highest
// epoch. Assumes h.mu is held.
func (h *Host) newCommandLocked(kind string, epoch int64, deadline time.Time) *command {
	cur := h.cur
	running := cur != nil && !isDone(cur)
	if running && cur.kind == kind {
		// Adopt: the running (or queued) command now answers this epoch.
		cur.epoch = epoch
		cur.deadline = deadline
		return cur
	}
	cmd := &command{kind: kind, epoch: epoch, deadline: deadline, done: make(chan struct{})}
	if running {
		if !cur.started {
			// Replaced before it started: it never runs.
			cur.dropped = true
			cur.ack = stale(cur.epoch, fmt.Sprintf("replaced by epoch %d", epoch))
			close(cur.done)
			cmd.prev = cur.prev
		} else {
			cmd.prev = cur
		}
	}
	h.cur = cmd
	go h.run(cmd)
	return cmd
}

func isDone(cmd *command) bool {
	select {
	case <-cmd.done:
		return true
	default:
		return false
	}
}

func cloneAck(ack *agentpb.HostCommandAck) *agentpb.HostCommandAck {
	out := &agentpb.HostCommandAck{Epoch: ack.GetEpoch(), Outcome: ack.GetOutcome(), Error: ack.GetError()}
	for _, t := range ack.GetTargets() {
		out.Targets = append(out.Targets, &agentpb.TargetResult{Target: t.GetTarget(), Error: t.GetError()})
	}
	return out
}

// run waits for the previous command, then executes cmd.
func (h *Host) run(cmd *command) {
	if cmd.prev != nil {
		select {
		case <-cmd.prev.done:
		case <-h.ctx.Done():
			return
		}
	}
	h.mu.Lock()
	if cmd.dropped {
		h.mu.Unlock()
		return
	}
	cmd.started = true
	h.mu.Unlock()

	var targets []*agentpb.TargetResult
	failed := false
	if cmd.kind == cmdSuspend {
		targets, failed = h.suspendAll(cmd)
	} else {
		targets, failed = h.resumeAll(cmd)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	ack := &agentpb.HostCommandAck{Epoch: cmd.epoch, Targets: targets}
	switch {
	case failed:
		ack.Outcome = agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_FAILED
		ack.Error = "at least one target failed"
	case cmd.kind == cmdSuspend:
		ack.Outcome = agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR
	default:
		ack.Outcome = agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_RESUMED
	}
	cmd.ack = ack
	close(cmd.done)
}

func (h *Host) deadline(cmd *command) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cmd.deadline
}

// suspendAll runs SetNotReady then Suspend on every guest not yet suspended.
func (h *Host) suspendAll(cmd *command) ([]*agentpb.TargetResult, bool) {
	var targets []*agentpb.TargetResult
	failed := false
	for _, guest := range h.cfg.Exec.Guests() {
		h.mu.Lock()
		done := h.suspended[guest]
		h.mu.Unlock()
		if done {
			continue
		}
		res := &agentpb.TargetResult{Target: guest}
		targets = append(targets, res)
		if err := h.cfg.Exec.SetNotReady(h.ctx, guest); err != nil {
			res.Error = fmt.Sprintf("set not ready: %v", err)
			failed = true
			continue
		}
		if err := h.cfg.Exec.Suspend(h.ctx, guest, h.deadline(cmd)); err != nil {
			res.Error = fmt.Sprintf("suspend: %v", err)
			failed = true
			continue
		}
		h.mu.Lock()
		h.suspended[guest] = true
		h.mu.Unlock()
	}
	return targets, failed
}

// resumeAll runs Resume then SetReady on every guest this host suspended.
func (h *Host) resumeAll(cmd *command) ([]*agentpb.TargetResult, bool) {
	h.mu.Lock()
	guests := make([]string, 0, len(h.suspended))
	for g := range h.suspended {
		guests = append(guests, g)
	}
	h.mu.Unlock()
	slices.Sort(guests)

	var targets []*agentpb.TargetResult
	failed := false
	for _, guest := range guests {
		res := &agentpb.TargetResult{Target: guest}
		targets = append(targets, res)
		if err := h.cfg.Exec.Resume(h.ctx, guest, h.deadline(cmd)); err != nil {
			res.Error = fmt.Sprintf("resume: %v", err)
			failed = true
			continue
		}
		h.mu.Lock()
		delete(h.suspended, guest)
		h.mu.Unlock()
		if err := h.cfg.Exec.SetReady(h.ctx, guest); err != nil {
			res.Error = fmt.Sprintf("set ready: %v", err)
			failed = true
		}
	}
	return targets, failed
}
