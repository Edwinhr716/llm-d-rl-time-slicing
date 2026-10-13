package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/group"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// Values of --freezer.
const (
	freezerAgent  = "agent"
	freezerDelete = "delete"
	freezerFake   = "fake"
)

// hostCommandWiring builds and runs the host command server (VK-A6, D-NS-4 ns-push-vk) when
// --host-command-port is set. Every method is a no-op when it is not.
type hostCommandWiring struct {
	o     *options
	allow []netip.Prefix
	agent *hostcmd.AgentClient
	srv   *hostcmd.Server
	// journal keeps the last command on the virtual Node (M5); nil when host commands are off.
	journal hostcmd.Journal
	// readyCh is closed by ready() once the guest informer has synced.
	readyCh   chan struct{}
	readyOnce sync.Once
}

// faults, when not nil, plays armed /debug/fault faults on the freezer=agent client.
func newHostCommandWiring(opts *options, faults *hostcmd.FaultInjector) (*hostCommandWiring, error) {
	wiring := &hostCommandWiring{o: opts, readyCh: make(chan struct{})}
	if opts.hostCommandPort == 0 {
		return wiring, nil
	}
	if opts.hostCommandPort < 0 || opts.hostCommandPort > 65535 {
		return nil, fmt.Errorf("--host-command-port=%d: out of range", opts.hostCommandPort)
	}
	allow, err := parseAllow(opts.hostCommandAllow)
	if err != nil {
		return nil, err
	}
	wiring.allow = allow
	switch opts.freezer {
	case freezerDelete, freezerFake:
	case freezerAgent:
		addr := opts.hcAgentAddr
		if addr == "" {
			addr = net.JoinHostPort(opts.hostIP, strconv.Itoa(opts.hcAgentPort))
		}
		var dialOpts []grpc.DialOption
		if faults != nil {
			dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(faults.Interceptor()))
		}
		if wiring.agent, err = hostcmd.DialAgent(addr, dialOpts...); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("--freezer=%q: want %q, %q or %q", opts.freezer, freezerAgent, freezerDelete, freezerFake)
	}
	return wiring, nil
}

// parseAllow reads --host-command-allow: IPs or CIDRs, comma-separated.
func parseAllow(v string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, f := range strings.Split(v, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if p, err := netip.ParsePrefix(f); err == nil {
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(f)
		if err != nil {
			return nil, fmt.Errorf("--host-command-allow: %q is not an IP or CIDR", f)
		}
		out = append(out, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
	}
	return out, nil
}

// build creates the server on top of the mirror backend. It returns nil when host commands
// are off. ctx bounds the server's detached work.
func (w *hostCommandWiring) build(
	ctx context.Context, backend *mirror.Backend, resolver *group.Resolver, nodes corev1client.NodeInterface,
	hostUID types.UID,
) (provider.CreateOwner, error) {
	if w.o.hostCommandPort == 0 {
		return nil, nil //nolint:nilnil // nil owner means host commands are off
	}
	// The journal is bound to this host Node object: a record from before a host recreate
	// (same name, new UID) is refused at restore (fail closed).
	w.journal = &provider.NodeJournal{Nodes: nodes, Name: w.o.nodeName, HostUID: hostUID}
	cfg := &hostcmd.Config{
		Node:              w.o.hostNode,
		Group:             func() (string, bool) { return resolver.Current().Group() },
		Host:              backend,
		IsGuest:           provider.MatchesGuest,
		EngineReady:       backend.EngineReady,
		VacateMargin:      w.o.vacateMargin,
		ResumeBudget:      w.o.resumeBudget,
		EngineStartBudget: w.o.engineStart,
		KillTimeout:       w.o.killTimeout,
		Journal:           w.journal,
		Ready:             w.readyCh,
	}
	switch w.o.freezer {
	case freezerAgent:
		cfg.Agent = w.agent
	case freezerFake:
		cfg.Freezer = &hostcmd.FakeFreezer{
			SuspendDelay: w.o.fakeSuspendDelay, ResumeDelay: w.o.fakeResumeDelay, Annotate: backend.AnnotateMirror,
		}
	}
	srv, err := hostcmd.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	w.srv = srv
	return srv, nil
}

// start listens on --host-ip:--host-command-port and serves until ctx ends. Only the leader
// gets here, so only one replica holds the port.
func (w *hostCommandWiring) start(ctx context.Context) error {
	if w.srv == nil {
		return nil
	}
	addr := net.JoinHostPort(w.o.hostIP, strconv.Itoa(w.o.hostCommandPort))
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("host command server: listen on %s: %w", addr, err)
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(w.checkPeer))
	hcpb.RegisterHostCommandServiceServer(gs, w.srv)
	log.G(ctx).WithField("addr", addr).WithField("freezer", w.o.freezer).
		WithField("allow", w.o.hostCommandAllow).Info("host command server starting")
	go func() {
		if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.G(ctx).WithError(err).Error("host command server stopped")
		}
	}()
	go func() {
		<-ctx.Done()
		gs.Stop()
	}()
	return nil
}

// checkPeer refuses callers outside --host-command-allow.
func (w *hostCommandWiring) checkPeer(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	if len(w.allow) > 0 && !w.allowed(ctx) {
		p, _ := peer.FromContext(ctx)
		addr := "unknown"
		if p != nil {
			addr = p.Addr.String()
		}
		log.G(ctx).WithField("peer", addr).WithField("method", info.FullMethod).Warn("host command refused: caller not allowed")
		return nil, status.Error(codes.PermissionDenied, "caller not allowed")
	}
	return handler(ctx, req)
}

func (w *hostCommandWiring) allowed(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return false
	}
	ap, err := netip.ParseAddrPort(p.Addr.String())
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	for _, pre := range w.allow {
		if pre.Contains(ip) {
			return true
		}
	}
	return false
}

// server is the host command server for Recover, or nil when host commands are off.
func (w *hostCommandWiring) server() provider.RecoverServer {
	if w.srv == nil {
		return nil
	}
	return w.srv
}

// ready lets host commands act: every guest is listed (the pod informer has synced).
func (w *hostCommandWiring) ready() {
	w.readyOnce.Do(func() { close(w.readyCh) })
}

func (w *hostCommandWiring) close(ctx context.Context) {
	if w.agent == nil {
		return
	}
	if err := w.agent.Close(); err != nil {
		log.G(ctx).WithError(err).Warn("closing the snapshot-agent connection failed")
	}
}

// killer is the agent Kill that relist repeats for a kill sequence a restart interrupted (M5),
// or nil when there is no agent (--freezer other than agent, or host commands off).
func (w *hostCommandWiring) killer() mirror.KillFunc {
	if w.agent == nil {
		return nil
	}
	return w.agent.Kill
}
