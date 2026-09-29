// Package hostcmd is the VK side of the host command protocol: the VK serves
// HostCommandService on its real node and the orchestrator commands it. The VK
// never reads the orchestrator's lock.
//
//   - Resume: the node is lent. Create the missing mirrors, resume the suspended
//     ones, and release each guest's Ready once its engine serves. Ack RESUMED.
//     While lent, a guest that arrives later gets its mirror too.
//   - Vacate: for each guest, hold it NotReady and confirm that, then suspend it
//     by the deadline (or delete the mirror and wait until it is gone). A guest
//     that cannot be suspended is killed. Ack VACATED only when every guest is
//     suspended or gone.
//
// The fencing follows the contract: epochs increase strictly; a lower epoch, or
// the same epoch with the other command, is answered STALE_EPOCH without acting;
// a higher epoch of the other command aborts the running one; a higher epoch of
// the same command joins it. Command work is detached from the RPC, so a caller
// that gives up can retry and join it. After a start the VK assumes nothing: no
// mirror is created before the first Resume (fail closed).
package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	hcpb "github.com/edwinhr716/guest-kubelet/api/hostcommand/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Host is what the server needs from the mirror backend. *mirror.Backend implements it.
type Host interface {
	Guests() ([]mirror.Guest, error)
	Create(ctx context.Context, guest *corev1.Pod) error
	HoldNotReady(guest *corev1.Pod, reason string)
	ReleaseReady(guest *corev1.Pod)
	ConfirmNotReady(ctx context.Context, guest *corev1.Pod) error
	MirrorNow(ctx context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error)
	VacateMirror(ctx context.Context, guest, m *corev1.Pod) error
	KillMirror(ctx context.Context, m *corev1.Pod) error
	WaitMirrorGone(ctx context.Context, m *corev1.Pod) error
	BumpEpoch(ctx context.Context, m *corev1.Pod) (*corev1.Pod, int64, error)
	AnnotateMirror(ctx context.Context, m *corev1.Pod, key, value string) error
}

// Config configures the server. Durations left zero get the defaults.
type Config struct {
	// Node is the real node. A command for another node is refused.
	Node string
	// Group returns the real node's group (D-VK-3), or false when there is none. A Resume
	// for another group is refused; a Vacate is always carried out.
	Group func() (string, bool)
	Host  Host
	// Agent, if set, suspends and resumes all of the node's guests in one call per command
	// (D-NS-5 ns-host) and kills per guest. Freezer is then not used.
	Agent HostAgent
	// Freezer suspends and resumes mirrors one by one (the per-guest fallback). With neither
	// Agent nor Freezer, a vacate deletes the mirror.
	Freezer Freezer
	// IsGuest selects the pods on the virtual node that get a mirror.
	IsGuest func(*corev1.Pod) bool
	// EngineReady reports whether a running mirror really serves. The default is the mirror's
	// ContainersReady; the VK readiness prober (M2) plugs in here.
	EngineReady func(guest, m *corev1.Pod) bool

	VacateMargin  time.Duration // taken off the vacate deadline for the ack's way back (250 ms)
	ResumeBudget  time.Duration // bound on each guest's Freezer.Resume (30 s)
	KillTimeout   time.Duration // bound on the kill sequence of one guest (30 s)
	ServeInterval time.Duration // while lent, how often guests are looked at again (1 s)
	PollInterval  time.Duration // how often a Resume looks for an engine that serves (200 ms)
	ReplyMargin   time.Duration // an unfinished command is answered this long before the caller's RPC deadline (250 ms)
}

func (c *Config) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.VacateMargin, 250*time.Millisecond)
	set(&c.ResumeBudget, 30*time.Second)
	set(&c.KillTimeout, 30*time.Second)
	set(&c.ServeInterval, time.Second)
	set(&c.PollInterval, 200*time.Millisecond)
	set(&c.ReplyMargin, 250*time.Millisecond)
	if c.EngineReady == nil {
		c.EngineReady = MirrorContainersReady
	}
	if c.IsGuest == nil {
		c.IsGuest = func(*corev1.Pod) bool { return true }
	}
}

// MirrorContainersReady is the default engine check: the mirror runs and its containers are
// ready. The mirror has no probes, so this only says the processes started.
func MirrorContainersReady(_, m *corev1.Pod) bool {
	if m.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range m.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// operation is one running command.
type operation struct {
	command hcpb.Command
	epoch   int64
	cancel  context.CancelFunc
	done    chan struct{}
	// Set before done is closed.
	outcome hcpb.Outcome
	guests  []*hcpb.GuestResult
	err     string
}

// Server serves HostCommandService for one real node.
type Server struct {
	hcpb.UnimplementedHostCommandServiceServer

	cfg   Config
	ctx   context.Context // lifetime of detached work
	nudge chan struct{}
	wg    sync.WaitGroup

	mu       sync.Mutex
	epoch    int64        // highest epoch seen
	epochCmd hcpb.Command // command of the highest epoch seen
	op       *operation
	lent     context.CancelFunc // non-nil while the node is lent; cancels the serve loop

	// guestMu serializes guest work between commands and the serve loop.
	guestMu sync.Mutex

	stateMu   sync.Mutex
	suspended map[types.UID]bool // suspended by this process
	released  map[types.UID]bool // Ready released by this process
}

// New returns a server whose detached work lives until ctx ends.
func New(ctx context.Context, config *Config) (*Server, error) {
	cfg := *config
	if cfg.Node == "" || cfg.Group == nil || cfg.Host == nil {
		return nil, errors.New("hostcmd: Node, Group and Host are required")
	}
	cfg.defaults()
	return &Server{
		cfg: cfg, ctx: ctx, nudge: make(chan struct{}, 1),
		suspended: map[types.UID]bool{}, released: map[types.UID]bool{},
	}, nil
}

// Wait blocks until all detached work has stopped. Cancel the ctx given to New first.
func (s *Server) Wait() { s.wg.Wait() }

// GuestWaiting implements provider.CreateOwner: a guest without a mirror arrived. While the
// node is lent the serve loop creates its mirror at once; otherwise the next Resume does.
func (s *Server) GuestWaiting(*corev1.Pod) {
	select {
	case s.nudge <- struct{}{}:
	default:
	}
}

// Lent reports whether the last command carried out was a Resume.
func (s *Server) Lent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lent != nil
}

// Vacate implements HostCommandService.Vacate.
func (s *Server) Vacate(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
	if err := s.checkNode(req.GetNodeName()); err != nil {
		return nil, err
	}
	if g, ok := s.cfg.Group(); !ok || g != req.GetGroupId() {
		// Vacating is always safe: carry it out, but say so.
		log.G(ctx).WithField("group", req.GetGroupId()).WithField("nodeGroup", g).
			Warn("host command: vacate for a group this node is not in; vacating anyway")
	}
	return s.command(ctx, hcpb.Command_COMMAND_VACATE, req.GetEpoch(), req.GetDeadline().AsTime())
}

// Resume implements HostCommandService.Resume.
func (s *Server) Resume(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
	if err := s.checkNode(req.GetNodeName()); err != nil {
		return nil, err
	}
	if g, ok := s.cfg.Group(); !ok || g != req.GetGroupId() {
		// Fail closed: lend only to the node's own group. The epoch is not recorded.
		msg := fmt.Sprintf("node %s is in group %q (resolved: %t), not %q", s.cfg.Node, g, ok, req.GetGroupId())
		log.G(ctx).WithField("epoch", req.GetEpoch()).Warn("host command: resume refused: " + msg)
		return s.ack(req.GetEpoch(), hcpb.Command_COMMAND_RESUME, hcpb.Outcome_OUTCOME_FAILED, nil, msg), nil
	}
	return s.command(ctx, hcpb.Command_COMMAND_RESUME, req.GetEpoch(), req.GetDeadline().AsTime())
}

func (s *Server) checkNode(node string) error {
	if node != s.cfg.Node {
		return status.Errorf(codes.FailedPrecondition, "this host is node %q, not %q", s.cfg.Node, node)
	}
	return nil
}

func (s *Server) ack(
	epoch int64, cmd hcpb.Command, outcome hcpb.Outcome, guests []*hcpb.GuestResult, errMsg string,
) *hcpb.HostAck {
	s.mu.Lock()
	current := s.epoch
	s.mu.Unlock()
	return &hcpb.HostAck{
		NodeName: s.cfg.Node, Epoch: epoch, Command: cmd, Outcome: outcome,
		CurrentEpoch: current, Guests: guests, Error: errMsg,
	}
}

// command runs or joins a command and returns its ack. It never blocks past the caller's RPC
// deadline: a command still running ReplyMargin before it is answered OUTCOME_UNSPECIFIED
// ("not finished"), so the caller sees the host reachable and calls again with the same epoch
// to join. A guest resume can outlast one Resume attempt; timing out would count against the
// host's liveness and could get healthy guests killed.
func (s *Server) command(ctx context.Context, cmd hcpb.Command, epoch int64, deadline time.Time) (*hcpb.HostAck, error) {
	op, stale := s.startOrJoin(cmd, epoch, deadline) //nolint:contextcheck // detached on purpose
	if stale != nil {
		return stale, nil
	}
	var early <-chan time.Time
	if d, ok := ctx.Deadline(); ok {
		t := time.NewTimer(time.Until(d) - s.cfg.ReplyMargin)
		defer t.Stop()
		early = t.C
	}
	select {
	case <-op.done:
	case <-early:
		select {
		case <-op.done:
		default:
			return s.ack(epoch, cmd, hcpb.Outcome_OUTCOME_UNSPECIFIED, nil,
				"still running: call again with the same epoch to join"), nil
		}
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return s.ack(epoch, cmd, op.outcome, op.guests, op.err), nil
}

// startOrJoin fences the epoch and returns the operation to wait for, or a STALE_EPOCH ack.
func (s *Server) startOrJoin(cmd hcpb.Command, epoch int64, deadline time.Time) (*operation, *hcpb.HostAck) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if epoch < s.epoch || (epoch == s.epoch && s.epochCmd != cmd) {
			return nil, &hcpb.HostAck{
				NodeName: s.cfg.Node, Epoch: epoch, Command: cmd,
				Outcome: hcpb.Outcome_OUTCOME_STALE_EPOCH, CurrentEpoch: s.epoch,
			}
		}
		s.epoch, s.epochCmd = epoch, cmd
		running := s.op
		if running == nil || isDone(running) {
			break
		}
		if running.command == cmd {
			return running, nil // same kind: join
		}
		// The other kind: abort it and start ours once it stopped.
		running.cancel()
		s.mu.Unlock()
		<-running.done
		s.mu.Lock()
	}
	opCtx, cancel := context.WithCancel(s.ctx)
	op := &operation{command: cmd, epoch: epoch, cancel: cancel, done: make(chan struct{})}
	s.op = op
	if cmd == hcpb.Command_COMMAND_VACATE {
		// Stop lending before the vacate starts, so the serve loop creates nothing more.
		s.stopLendingLocked()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		s.run(opCtx, op, deadline)
	}()
	return op, nil
}

func isDone(op *operation) bool {
	select {
	case <-op.done:
		return true
	default:
		return false
	}
}

func (s *Server) stopLendingLocked() {
	if s.lent != nil {
		s.lent()
		s.lent = nil
	}
}

// startLending starts the serve loop unless it runs, and unless op was superseded.
func (s *Server) startLending(op *operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op != op || s.lent != nil {
		return
	}
	lctx, cancel := context.WithCancel(s.ctx)
	s.lent = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveLoop(lctx)
	}()
}

// run carries out one command on every guest in parallel and records the outcome.
func (s *Server) run(ctx context.Context, op *operation, deadline time.Time) {
	defer close(op.done)
	logger := log.G(ctx).WithField("command", op.command.String()).WithField("epoch", op.epoch)
	start := time.Now()
	if op.command == hcpb.Command_COMMAND_RESUME {
		// Lend once the command's own pass is over (before done is closed, so a vacate that
		// supersedes it stops the serve loop), whatever the outcome: the orchestrator treats
		// the node as lent from the Resume on.
		defer s.startLending(op) //nolint:contextcheck // the serve loop outlives the command; the server context bounds it
	}
	s.guestMu.Lock()
	defer s.guestMu.Unlock()
	guests, err := s.cfg.Host.Guests()
	if err != nil {
		op.outcome, op.err = hcpb.Outcome_OUTCOME_FAILED, err.Error()
		logger.WithError(err).Warn("host command: cannot list guests")
		return
	}
	var results []*hcpb.GuestResult
	if s.cfg.Agent != nil {
		results = s.runAgent(ctx, op, guests, deadline)
	} else {
		results = s.runPerGuest(ctx, op, guests, deadline)
	}

	op.guests = results
	op.outcome = doneOutcome(op.command)
	for _, res := range results {
		switch res.GetOutcome() {
		case hcpb.Outcome_OUTCOME_ABORTED:
			op.outcome, op.err = hcpb.Outcome_OUTCOME_ABORTED, "aborted by a newer command"
		case hcpb.Outcome_OUTCOME_FAILED:
			if op.outcome != hcpb.Outcome_OUTCOME_ABORTED {
				op.outcome = hcpb.Outcome_OUTCOME_FAILED
				op.err = fmt.Sprintf("guest %s: %s", res.GetGuest(), res.GetError())
			}
		default:
		}
	}
	logger.WithField("outcome", op.outcome.String()).WithField("guests", len(results)).
		WithField("took", time.Since(start).String()).Info("host command done")
}

// runPerGuest carries out the command guest by guest through the Freezer, in parallel.
func (s *Server) runPerGuest(ctx context.Context, op *operation, guests []mirror.Guest, deadline time.Time) []*hcpb.GuestResult {
	var results []*hcpb.GuestResult
	var resMu sync.Mutex
	var wg sync.WaitGroup
	for _, g := range guests {
		if !s.cfg.IsGuest(g.Pod) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var gerr error
			if op.command == hcpb.Command_COMMAND_VACATE {
				gerr = s.vacateGuest(ctx, g.Pod, deadline)
			} else {
				gerr = s.resumeGuest(ctx, g, deadline)
			}
			res := guestResult(ctx, op.command, g.Pod, gerr)
			resMu.Lock()
			results = append(results, res)
			resMu.Unlock()
		}()
	}
	wg.Wait()
	return results
}

// guestResult is the ack entry of one guest: done, ABORTED when ctx was cancelled by a newer
// command, FAILED otherwise.
func guestResult(ctx context.Context, cmd hcpb.Command, pod *corev1.Pod, err error) *hcpb.GuestResult {
	res := &hcpb.GuestResult{Guest: pod.Namespace + "/" + pod.Name, Outcome: doneOutcome(cmd)}
	switch {
	case err == nil:
	case ctx.Err() != nil:
		res.Outcome, res.Error = hcpb.Outcome_OUTCOME_ABORTED, err.Error()
	default:
		res.Outcome, res.Error = hcpb.Outcome_OUTCOME_FAILED, err.Error()
	}
	return res
}

func doneOutcome(cmd hcpb.Command) hcpb.Outcome {
	if cmd == hcpb.Command_COMMAND_RESUME {
		return hcpb.Outcome_OUTCOME_RESUMED
	}
	return hcpb.Outcome_OUTCOME_VACATED
}
