//go:build evalwire

// Package host is the reference host of decision D-NS-4, option ns-push-vk:
// the protocol half of the virtual kubelet only. It serves the per-host
// command endpoint (HostCommandService) and drives the guests of its node
// through the Executor interface:
//
//   - Vacate: SetNotReady then Suspend, per guest, then ack VACATED.
//   - Resume: Resume then SetReady, per guest, then ack RESUMED.
//
// It is compiled only with the evalwire build tag and is never part of the
// product binary.
package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"google.golang.org/grpc"
)

// Executor acts on the guests of one host. The interface is fixed by the
// evaluation plan; every option's reference host calls it.
type Executor interface {
	// Guests returns the guests on this host now.
	Guests() []string
	// SetNotReady marks a guest NotReady and returns once confirmed.
	SetNotReady(ctx context.Context, guest string) error
	// Suspend suspends a guest; it must finish by deadline.
	Suspend(ctx context.Context, guest string, deadline time.Time) error
	// Resume resumes a suspended guest.
	Resume(ctx context.Context, guest string, deadline time.Time) error
	// SetReady marks a guest Ready.
	SetReady(ctx context.Context, guest string) error
}

// Config configures a reference host.
type Config struct {
	// Node is the node name of this host.
	Node string
	// OrchAddr is the orchestrator address. Unused: in this option the
	// orchestrator calls the host, never the other way round.
	OrchAddr string
	// ListenAddr is where the command endpoint listens, for example
	// 127.0.0.2:9101.
	ListenAddr string
	// AgentAddr is the snapshot agent address. Unused: the Executor talks to
	// the agent.
	AgentAddr string
	// Exec acts on the guests.
	Exec Executor
}

// stopGrace bounds the graceful stop of the command server.
const stopGrace = 5 * time.Second

// guestState is what the host knows about one guest. It starts unknown after
// every host start, so a vacate suspends and a resume resumes it (fail
// closed: the host never assumes a guest is suspended).
type guestState int

const (
	guestUnknown guestState = iota
	guestSuspended
	guestRunning
)

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

// Host is a running reference host.
type Host struct {
	hcpb.UnimplementedHostCommandServiceServer

	cfg  Config
	log  *slog.Logger
	ctx  context.Context // lifetime of detached command work
	stop context.CancelFunc
	srv  *grpc.Server
	lis  net.Listener
	wg   sync.WaitGroup

	mu       sync.Mutex
	epoch    int64        // highest epoch seen
	epochCmd hcpb.Command // command of the highest epoch seen
	op       *operation
	guests   map[string]guestState
}

// Start listens on cfg.ListenAddr and serves the command endpoint until ctx
// is done or Stop is called.
func Start(ctx context.Context, cfg Config) (*Host, error) { //nolint:gocritic // the evaluation plan fixes this signature
	if cfg.Exec == nil {
		return nil, errors.New("host: Config.Exec is required")
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("host %s: listen on %s: %w", cfg.Node, cfg.ListenAddr, err)
	}
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	hst := &Host{
		cfg:    cfg,
		log:    slog.Default().With("host", cfg.Node),
		ctx:    hctx,
		stop:   cancel,
		srv:    grpc.NewServer(),
		lis:    lis,
		guests: make(map[string]guestState),
	}
	hcpb.RegisterHostCommandServiceServer(hst.srv, hst)
	hst.wg.Go(func() {
		if err := hst.srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			hst.log.Error("Host command server failed", "error", err)
		}
	})
	hst.wg.Go(func() {
		select {
		case <-ctx.Done():
			hst.Stop()
		case <-hctx.Done():
		}
	})
	return hst, nil
}

// Addr returns the address the command endpoint listens on.
func (h *Host) Addr() string {
	return h.lis.Addr().String()
}

// Stop stops the command server and aborts the running command. Restarting
// a host is Stop then Start: the new host knows no epoch and no guest state.
func (h *Host) Stop() {
	h.stop()
	done := make(chan struct{})
	go func() {
		h.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopGrace):
		h.srv.Stop()
		<-done
	}
}

// Wait blocks until the host stopped.
func (h *Host) Wait() {
	h.wg.Wait()
}

// Vacate implements HostCommandService.Vacate.
func (h *Host) Vacate(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
	return h.command(ctx, hcpb.Command_COMMAND_VACATE, req.GetEpoch(), req.GetDeadline().AsTime())
}

// Resume implements HostCommandService.Resume.
func (h *Host) Resume(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
	return h.command(ctx, hcpb.Command_COMMAND_RESUME, req.GetEpoch(), req.GetDeadline().AsTime())
}

// command runs or joins a command and returns its ack. The work runs detached
// from the RPC, so a caller that gives up can retry and join it.
func (h *Host) command(ctx context.Context, cmd hcpb.Command, epoch int64, deadline time.Time) (*hcpb.HostAck, error) {
	// The command work is detached from the RPC on purpose: a caller that
	// gives up can retry and join it.
	op, stale := h.startOrJoin(cmd, epoch, deadline) //nolint:contextcheck // detached on purpose, see above
	if stale != nil {
		return stale, nil
	}
	select {
	case <-op.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	h.mu.Lock()
	current := h.epoch
	h.mu.Unlock()
	return &hcpb.HostAck{
		NodeName:     h.cfg.Node,
		Epoch:        epoch,
		Command:      cmd,
		Outcome:      op.outcome,
		CurrentEpoch: current,
		Guests:       op.guests,
		Error:        op.err,
	}, nil
}

// startOrJoin fences the epoch and returns the operation to wait for, or a
// STALE_EPOCH ack.
func (h *Host) startOrJoin(cmd hcpb.Command, epoch int64, deadline time.Time) (*operation, *hcpb.HostAck) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		if epoch < h.epoch || (epoch == h.epoch && h.epochCmd != cmd) {
			return nil, &hcpb.HostAck{
				NodeName:     h.cfg.Node,
				Epoch:        epoch,
				Command:      cmd,
				Outcome:      hcpb.Outcome_OUTCOME_STALE_EPOCH,
				CurrentEpoch: h.epoch,
			}
		}
		h.epoch = epoch
		h.epochCmd = cmd
		running := h.op
		if running == nil || isDone(running) {
			break
		}
		if running.command == cmd {
			// Same kind: join the running command.
			return running, nil
		}
		// The other kind: abort it and start ours once it stopped.
		running.cancel()
		h.mu.Unlock()
		<-running.done
		h.mu.Lock()
	}
	opCtx, cancel := context.WithCancel(h.ctx)
	op := &operation{command: cmd, epoch: epoch, cancel: cancel, done: make(chan struct{})}
	h.op = op
	h.wg.Go(func() {
		defer cancel()
		h.run(opCtx, op, deadline)
	})
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

// run executes a command on every guest in parallel and records the outcome.
func (h *Host) run(ctx context.Context, op *operation, deadline time.Time) {
	defer close(op.done)
	guests := h.cfg.Exec.Guests()
	results := make([]*hcpb.GuestResult, len(guests))
	var wg sync.WaitGroup
	for i, guest := range guests {
		wg.Go(func() {
			results[i] = h.runGuest(ctx, op.command, guest, deadline)
		})
	}
	wg.Wait()

	op.guests = results
	op.outcome = doneOutcome(op.command)
	for _, res := range results {
		switch res.GetOutcome() {
		case hcpb.Outcome_OUTCOME_ABORTED:
			op.outcome = hcpb.Outcome_OUTCOME_ABORTED
			op.err = "aborted by a newer command"
		case hcpb.Outcome_OUTCOME_FAILED:
			if op.outcome != hcpb.Outcome_OUTCOME_ABORTED {
				op.outcome = hcpb.Outcome_OUTCOME_FAILED
				op.err = fmt.Sprintf("guest %s: %s", res.GetGuest(), res.GetError())
			}
		default:
		}
	}
	h.log.Info("Host command done", "command", op.command.String(), "epoch", op.epoch,
		"outcome", op.outcome.String(), "guests", len(guests))
}

func doneOutcome(cmd hcpb.Command) hcpb.Outcome {
	if cmd == hcpb.Command_COMMAND_RESUME {
		return hcpb.Outcome_OUTCOME_RESUMED
	}
	return hcpb.Outcome_OUTCOME_VACATED
}

// runGuest runs NotReady then Suspend (vacate) or Resume then Ready (resume)
// on one guest, skipping a guest already known to be in the wanted state.
func (h *Host) runGuest(ctx context.Context, cmd hcpb.Command, guest string, deadline time.Time) *hcpb.GuestResult {
	res := &hcpb.GuestResult{Guest: guest, Outcome: doneOutcome(cmd)}
	want := guestSuspended
	if cmd == hcpb.Command_COMMAND_RESUME {
		want = guestRunning
	}
	h.mu.Lock()
	known := h.guests[guest]
	h.mu.Unlock()
	if known == want {
		return res
	}
	h.setGuest(guest, guestUnknown)

	var err error
	if cmd == hcpb.Command_COMMAND_RESUME {
		err = h.resumeGuest(ctx, guest, deadline)
	} else {
		err = h.vacateGuest(ctx, guest, deadline)
	}
	switch {
	case err == nil:
		h.setGuest(guest, want)
	case ctx.Err() != nil:
		res.Outcome = hcpb.Outcome_OUTCOME_ABORTED
		res.Error = err.Error()
	default:
		res.Outcome = hcpb.Outcome_OUTCOME_FAILED
		res.Error = err.Error()
	}
	return res
}

func (h *Host) vacateGuest(ctx context.Context, guest string, deadline time.Time) error {
	if err := h.cfg.Exec.SetNotReady(ctx, guest); err != nil {
		return fmt.Errorf("set not ready: %w", err)
	}
	if err := h.cfg.Exec.Suspend(ctx, guest, deadline); err != nil {
		return fmt.Errorf("suspend: %w", err)
	}
	return nil
}

func (h *Host) resumeGuest(ctx context.Context, guest string, deadline time.Time) error {
	if err := h.cfg.Exec.Resume(ctx, guest, deadline); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if err := h.cfg.Exec.SetReady(ctx, guest); err != nil {
		return fmt.Errorf("set ready: %w", err)
	}
	return nil
}

func (h *Host) setGuest(guest string, state guestState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guests[guest] = state
}
