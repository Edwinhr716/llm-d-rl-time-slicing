// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build evalwire

// Package host is the reference host for D-NS-4 option hybrid: the protocol
// half of the virtual kubelet only. The guest work (readiness, suspend,
// resume) is behind Executor, which the evaluation driver implements.
//
// It keeps the background lock wiring: it is the node's ROLE_BACKGROUND
// participant (job "vk/<node>"), keeps one background Acquire outstanding and
// heartbeats with GetGroupStatus(participant_id) every 0.5 s. On top of it, it
// serves the host command endpoint:
//
//   - Vacate(epoch, deadline): for each guest, SetNotReady (confirmed) then
//     Suspend, serially; then background Yield, re-Acquire, and ack
//     OUTCOME_VACATED. The ack is the RPC response.
//   - Resume(epoch, deadline): for each suspended guest, Resume then
//     SetReady; ack OUTCOME_RESUMED. Guests start only on this command, never
//     because the background Acquire returned.
//
// Fallback: if a heartbeat shows vacate_within and no Vacate command has
// started within 1 s, the host vacates on its own (the pull path). The
// orchestrator still waits for the ack, which the next Vacate gets at once.
//
// A guest the host has not suspended or resumed itself (for example after a
// host restart) is unknown: a Vacate suspends it (it may be live, so fail
// closed) and a Resume resumes it. Guest work runs detached from the command
// RPC, so an orchestrator that disconnects mid-command does not abort it.
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
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Executor does the guest work on one host. Its shape is fixed by the D-NS-4
// evaluation plan; do not change it.
type Executor interface {
	Guests() []string                                    // guests on this host now
	SetNotReady(ctx context.Context, guest string) error // returns once confirmed
	Suspend(ctx context.Context, guest string, deadline time.Time) error
	Resume(ctx context.Context, guest string, deadline time.Time) error
	SetReady(ctx context.Context, guest string) error
}

// Config configures a host.
type Config struct {
	// Node is the real node name.
	Node string
	// OrchAddr is the orchestrator's gRPC address.
	OrchAddr string
	// ListenAddr is where the host command endpoint listens,
	// <node InternalIP>:<host command port>.
	ListenAddr string
	// AgentAddr is the node's snapshot agent. This host does not call it:
	// the Executor does the guest work.
	AgentAddr string
	// Exec does the guest work.
	Exec Executor
}

const (
	heartbeatInterval = 500 * time.Millisecond
	retryInterval     = 500 * time.Millisecond
	fallbackGrace     = 1 * time.Second
	probeTimeout      = 300 * time.Millisecond
)

// Host is a running reference host.
type Host struct {
	hcpb.UnimplementedHostCommandServiceServer

	cfg     Config
	orch    pb.TimeSliceOrchestratorServiceClient
	conn    *grpc.ClientConn
	srv     *grpc.Server
	lis     net.Listener
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	kick    chan struct{}
	stopped sync.Once

	// opMu serializes guest work: one command (or fallback) at a time.
	opMu sync.Mutex

	mu        sync.Mutex
	group     string
	highEpoch int64
	// serving is false after a successful vacate and true otherwise (a
	// host that starts may have live guests). It drives the pull fallback.
	serving bool
	// suspended records guests known suspended (true) or resumed (false).
	// A guest not in the map is unknown.
	suspended map[string]bool
	// vacating is true while guest work for a vacate runs.
	vacating bool
	// noticeSeen is when a heartbeat first showed vacate_within for the
	// current notice; zero when none runs.
	noticeSeen time.Time
	// lastVacate is when the last vacate started.
	lastVacate time.Time
}

// Start starts the host: the command endpoint on ListenAddr and the lock
// loop against OrchAddr.
func Start(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.Node == "" || cfg.OrchAddr == "" || cfg.ListenAddr == "" || cfg.Exec == nil {
		return nil, errors.New("host: Node, OrchAddr, ListenAddr and Exec are required")
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("host %s: listen %s: %w", cfg.Node, cfg.ListenAddr, err)
	}
	conn, err := grpc.NewClient(cfg.OrchAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("host %s: dial orchestrator: %w", cfg.Node, err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	h := &Host{
		cfg:       cfg,
		orch:      pb.NewTimeSliceOrchestratorServiceClient(conn),
		conn:      conn,
		srv:       grpc.NewServer(),
		lis:       lis,
		cancel:    cancel,
		kick:      make(chan struct{}, 1),
		serving:   true,
		suspended: make(map[string]bool),
	}
	hcpb.RegisterHostCommandServiceServer(h.srv, h)
	h.wg.Add(2)
	go func() {
		defer h.wg.Done()
		_ = h.srv.Serve(lis)
	}()
	go func() {
		defer h.wg.Done()
		h.run(runCtx)
	}()
	return h, nil
}

// Addr returns the command endpoint's address.
func (h *Host) Addr() string { return h.lis.Addr().String() }

// Group returns the group the host found, or "" before it found one.
func (h *Host) Group() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.group
}

// Stop stops the host and waits for its goroutines.
func (h *Host) Stop() {
	h.stopped.Do(func() {
		h.cancel()
		h.srv.Stop()
		_ = h.conn.Close()
		h.wg.Wait()
	})
}

func (h *Host) participantID() string { return "vk/" + h.cfg.Node }

// run finds the group, then keeps one background Acquire outstanding and
// heartbeats.
func (h *Host) run(ctx context.Context) {
	group := h.discoverGroup(ctx)
	if group == "" {
		return
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.acquireLoop(ctx, group)
	}()
	h.kickAcquire()
	// Let the first Acquire register the participant before the first
	// heartbeat, so the heartbeat is not taken as a claim.
	sleep(ctx, 200*time.Millisecond)
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		h.heartbeat(ctx, group)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// discoverGroup finds the group whose nodes include this node: a background
// Acquire in any other group fails at once, in this group it blocks.
func (h *Host) discoverGroup(ctx context.Context) string {
	for ctx.Err() == nil {
		resp, err := h.orch.ListGroups(ctx, &pb.ListGroupsRequest{})
		if err == nil {
			for _, g := range resp.GetGroupIds() {
				pctx, cancel := context.WithTimeout(ctx, probeTimeout)
				_, err := h.orch.Acquire(pctx, h.acquireRequest(g))
				cancel()
				if err == nil || status.Code(err) == codes.DeadlineExceeded {
					h.mu.Lock()
					h.group = g
					h.mu.Unlock()
					slog.InfoContext(ctx, "Reference host found its group", "node", h.cfg.Node, "group", g)
					return g
				}
			}
		}
		sleep(ctx, retryInterval)
	}
	return ""
}

func (h *Host) acquireRequest(group string) *pb.AcquireRequest {
	return &pb.AcquireRequest{
		JobId:    h.participantID(),
		GroupId:  group,
		Role:     pb.Role_ROLE_BACKGROUND,
		NodeName: h.cfg.Node,
	}
}

func (h *Host) kickAcquire() {
	select {
	case h.kick <- struct{}{}:
	default:
	}
}

// acquireLoop issues one background Acquire per kick and retries it across
// errors (for example an orchestrator restart). The Acquire returning means
// the node is lent; guests still start only on the Resume command.
func (h *Host) acquireLoop(ctx context.Context, group string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.kick:
		}
		for ctx.Err() == nil {
			if _, err := h.orch.Acquire(ctx, h.acquireRequest(group)); err != nil {
				sleep(ctx, retryInterval)
				continue
			}
			break
		}
	}
}

func (h *Host) heartbeat(ctx context.Context, group string) {
	cctx, cancel := context.WithTimeout(ctx, heartbeatInterval)
	defer cancel()
	resp, err := h.orch.GetGroupStatus(cctx, &pb.GetGroupStatusRequest{GroupId: group, ParticipantId: h.participantID()})
	if err != nil {
		return
	}
	h.checkFallback(ctx, resp.GetGroup().GetVacateWithin())
}

// checkFallback starts a vacate by pull when a notice runs and no Vacate
// command has started within fallbackGrace of the host first seeing it.
func (h *Host) checkFallback(ctx context.Context, within *durationpb.Duration) {
	now := time.Now()
	h.mu.Lock()
	if within == nil {
		h.noticeSeen = time.Time{}
		h.mu.Unlock()
		return
	}
	if h.noticeSeen.IsZero() {
		h.noticeSeen = now
	}
	due := h.serving && !h.vacating && now.Sub(h.noticeSeen) >= fallbackGrace && h.lastVacate.Before(h.noticeSeen)
	h.mu.Unlock()
	if !due {
		return
	}
	deadline := now.Add(within.AsDuration())
	slog.InfoContext(ctx, "Reference host vacating by pull (no command received)", "node", h.cfg.Node, "deadline", deadline)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.opMu.Lock()
		defer h.opMu.Unlock()
		if _, ok := h.vacate(ctx, deadline); ok {
			h.afterVacate(ctx)
		}
	}()
}

// Vacate implements HostCommandService.Vacate.
func (h *Host) Vacate(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
	ack := &hcpb.HostAck{NodeName: h.cfg.Node, Epoch: req.GetEpoch(), Command: hcpb.Command_COMMAND_VACATE}
	if stale := h.admit(req.GetNodeName(), req.GetGroupId(), req.GetEpoch(), ack); stale {
		return ack, nil
	}
	h.opMu.Lock()
	defer h.opMu.Unlock()
	deadline := req.GetDeadline().AsTime()
	results, ok := h.vacate(context.WithoutCancel(ctx), deadline)
	ack.Guests = results
	ack.CurrentEpoch = h.currentEpoch()
	if !ok {
		ack.Outcome = hcpb.Outcome_OUTCOME_FAILED
		ack.Error = "not every guest suspended"
		return ack, nil
	}
	h.afterVacate(context.WithoutCancel(ctx))
	ack.Outcome = hcpb.Outcome_OUTCOME_VACATED
	return ack, nil
}

// Resume implements HostCommandService.Resume.
func (h *Host) Resume(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
	ack := &hcpb.HostAck{NodeName: h.cfg.Node, Epoch: req.GetEpoch(), Command: hcpb.Command_COMMAND_RESUME}
	if stale := h.admit(req.GetNodeName(), req.GetGroupId(), req.GetEpoch(), ack); stale {
		return ack, nil
	}
	h.opMu.Lock()
	defer h.opMu.Unlock()
	if cur := h.currentEpoch(); cur > req.GetEpoch() {
		// A newer command (a Vacate) arrived while this one waited: never
		// resume after it.
		ack.Outcome = hcpb.Outcome_OUTCOME_STALE_EPOCH
		ack.CurrentEpoch = cur
		return ack, nil
	}
	deadline := req.GetDeadline().AsTime()
	ectx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	ok := true
	for _, g := range h.cfg.Exec.Guests() {
		if suspended, known := h.guestState(g); known && !suspended {
			continue
		}
		r := &hcpb.GuestResult{Guest: g, Outcome: hcpb.Outcome_OUTCOME_RESUMED}
		if err := h.cfg.Exec.Resume(ectx, g, deadline); err != nil {
			r.Outcome, r.Error, ok = hcpb.Outcome_OUTCOME_FAILED, "resume: "+err.Error(), false
		} else {
			h.setSuspended(g, false)
			if err := h.cfg.Exec.SetReady(ectx, g); err != nil {
				r.Outcome, r.Error, ok = hcpb.Outcome_OUTCOME_FAILED, "set ready: "+err.Error(), false
			}
		}
		ack.Guests = append(ack.Guests, r)
	}
	h.mu.Lock()
	h.serving = true
	h.mu.Unlock()
	ack.CurrentEpoch = h.currentEpoch()
	if !ok {
		ack.Outcome = hcpb.Outcome_OUTCOME_FAILED
		ack.Error = "not every guest resumed"
		return ack, nil
	}
	ack.Outcome = hcpb.Outcome_OUTCOME_RESUMED
	return ack, nil
}

// admit checks the node and the epoch of a command and records the epoch. It
// fills ack and reports true when the command must not run.
func (h *Host) admit(node, group string, epoch int64, ack *hcpb.HostAck) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if node != h.cfg.Node {
		ack.Outcome = hcpb.Outcome_OUTCOME_FAILED
		ack.Error = fmt.Sprintf("command for node %q sent to node %q", node, h.cfg.Node)
		ack.CurrentEpoch = h.highEpoch
		return true
	}
	if epoch < h.highEpoch {
		ack.Outcome = hcpb.Outcome_OUTCOME_STALE_EPOCH
		ack.CurrentEpoch = h.highEpoch
		return true
	}
	h.highEpoch = epoch
	if h.group == "" {
		h.group = group
	}
	return false
}

// vacate takes every guest not already suspended through SetNotReady then
// Suspend, one guest at a time. The caller holds opMu.
func (h *Host) vacate(ctx context.Context, deadline time.Time) ([]*hcpb.GuestResult, bool) {
	h.mu.Lock()
	h.vacating = true
	h.lastVacate = time.Now()
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.vacating = false
		h.mu.Unlock()
	}()
	ectx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var results []*hcpb.GuestResult
	ok := true
	for _, g := range h.cfg.Exec.Guests() {
		r := &hcpb.GuestResult{Guest: g, Outcome: hcpb.Outcome_OUTCOME_VACATED}
		results = append(results, r)
		if suspended, known := h.guestState(g); known && suspended {
			continue
		}
		if err := h.cfg.Exec.SetNotReady(ectx, g); err != nil {
			r.Outcome, r.Error, ok = hcpb.Outcome_OUTCOME_FAILED, "set not ready: "+err.Error(), false
			continue
		}
		if err := h.cfg.Exec.Suspend(ectx, g, deadline); err != nil {
			r.Outcome, r.Error, ok = hcpb.Outcome_OUTCOME_FAILED, "suspend: "+err.Error(), false
			continue
		}
		h.setSuspended(g, true)
	}
	if ok {
		h.mu.Lock()
		h.serving = false
		h.mu.Unlock()
	}
	return results, ok
}

// afterVacate hands the grant back (background Yield, idempotent) and puts a
// new background Acquire in line for the next lend.
func (h *Host) afterVacate(ctx context.Context) {
	group := h.Group()
	if group == "" {
		return
	}
	yctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := h.orch.Yield(yctx, &pb.YieldRequest{
		JobId: h.participantID(), GroupId: group, Role: pb.Role_ROLE_BACKGROUND,
	}); err != nil {
		slog.InfoContext(ctx, "Reference host background Yield failed", "node", h.cfg.Node, "error", err)
	}
	h.kickAcquire()
}

func (h *Host) currentEpoch() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.highEpoch
}

// guestState reports whether the host suspended (true) or resumed (false) the
// guest last, and whether it knows at all.
func (h *Host) guestState(guest string) (bool, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	suspended, known := h.suspended[guest]
	return suspended, known
}

func (h *Host) setSuspended(guest string, suspended bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.suspended[guest] = suspended
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
