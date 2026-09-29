package main

import (
	"context"
	"fmt"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	pb "github.com/edwinhr716/guest-kubelet/api/timeslice_orchestrator/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/orchestrator"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// Values of --group-source (decision O3) and --freezer.
const (
	groupSourceNodeLabel = "node-label"
	freezerDelete        = "delete"
	freezerFake          = "fake"
	freezerAgent         = "agent"
)

// orchestratorWiring builds and runs the orchestrator loop (VK-A6) when --orchestrator-addr
// is set. Every method is a no-op when it is not.
type orchestratorWiring struct {
	client kubernetes.Interface
	o      *options
	conn   *grpc.ClientConn
	loop   *orchestrator.Loop

	// isGuest selects the guests the loop gives a mirror (default provider.IsActiveGuest). main sets it to
	// also require admission (VK-A7), so the loop never creates a mirror for a refused guest.
	isGuest func(*corev1.Pod) bool
}

func newOrchestratorWiring(client kubernetes.Interface, opts *options) (*orchestratorWiring, error) {
	wiring := &orchestratorWiring{client: client, o: opts}
	if opts.orchAddr == "" {
		return wiring, nil
	}
	if opts.groupSource != groupSourceNodeLabel {
		return nil, fmt.Errorf("--group-source=%q: only %q is implemented", opts.groupSource, groupSourceNodeLabel)
	}
	switch opts.freezer {
	case freezerDelete, freezerFake:
	case freezerAgent:
		if opts.agentAddr == "" {
			return nil, fmt.Errorf("--freezer=%s needs --agent-addr", freezerAgent)
		}
	default:
		return nil, fmt.Errorf("--freezer=%q: want %q, %q or %q", opts.freezer, freezerDelete, freezerFake, freezerAgent)
	}
	conn, err := grpc.NewClient(opts.orchAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial orchestrator %s: %w", opts.orchAddr, err)
	}
	wiring.conn = conn
	return wiring, nil
}

// build creates the loop on top of the mirror backend. It returns nil when the loop is off.
func (w *orchestratorWiring) build(backend *mirror.Backend) (*orchestrator.Loop, error) {
	if w.conn == nil {
		return nil, nil //nolint:nilnil // nil loop means the loop is off
	}
	var freezer orchestrator.Freezer
	switch w.o.freezer {
	case freezerFake:
		freezer = &orchestrator.FakeFreezer{
			SuspendDelay: w.o.fakeSuspendDelay, ResumeDelay: w.o.fakeResumeDelay, Annotate: backend.AnnotateMirror,
		}
	case freezerAgent:
		lf, err := backend.LoopFreezer()
		if err != nil {
			return nil, fmt.Errorf("--freezer=%s: %w", freezerAgent, err)
		}
		freezer = lf
	}
	loop, err := orchestrator.New(&orchestrator.Config{
		Node:         w.o.hostNode,
		Client:       pb.NewTimeSliceOrchestratorServiceClient(w.conn),
		Group:        orchestrator.NodeLabelGroup(w.client, w.o.hostNode),
		Host:         backend,
		Freezer:      freezer,
		IsGuest:      w.guestFilter(),
		EngineReady:  backend.EngineReady,
		PollInterval: w.o.orchPoll,
		Liveness:     w.o.liveness,
		VacateMargin: w.o.vacateMargin,
		ResumeBudget: w.o.resumeBudget,
		RPCTimeout:   w.o.orchRPCTimeout,
	})
	if err != nil {
		return nil, err
	}
	w.loop = loop
	return loop, nil
}

// guestFilter is isGuest, or provider.IsActiveGuest when unset.
func (w *orchestratorWiring) guestFilter() func(*corev1.Pod) bool {
	if w.isGuest != nil {
		return w.isGuest
	}
	return provider.IsActiveGuest
}

// start runs the loop in the background until ctx ends. Only the leader gets here.
func (w *orchestratorWiring) start(ctx context.Context) {
	if w.loop == nil {
		return
	}
	log.G(ctx).WithField("addr", w.o.orchAddr).WithField("freezer", w.o.freezer).Info("orchestrator loop starting")
	go func() {
		if err := w.loop.Run(ctx); err != nil && ctx.Err() == nil {
			log.G(ctx).WithError(err).Error("orchestrator loop stopped")
		}
	}()
}

func (w *orchestratorWiring) close(ctx context.Context) {
	if w.conn == nil {
		return
	}
	if err := w.conn.Close(); err != nil {
		log.G(ctx).WithError(err).Warn("closing the orchestrator connection failed")
	}
}

// relist is M5: relist this node's mirrors and rebuild what a restart lost, before the pod
// controller and the loop start. Only the leader gets here. An error stops the guest kubelet
// (fail closed): a guest adopted without its suspended state would be served frozen.
func (w *orchestratorWiring) relist(ctx context.Context, backend *mirror.Backend) error {
	opts := provider.RecoverOptions{}
	switch {
	case w.conn != nil && w.o.freezer == freezerFake:
		opts.Frozen = func(m *corev1.Pod) (bool, error) {
			return m.Annotations[orchestrator.AnnotationFakeFreezer] == orchestrator.FakeSuspended, nil
		}
	case w.conn != nil && w.o.freezer == freezerDelete:
		// Mirrors are deleted to vacate, never frozen: no host fact to read.
	case w.o.agentAddr != "":
		// --freezer=agent, or M4 without the loop: the agent's Status is the host fact, and it
		// corrects the recorded suspend state. An agent that cannot be read leaves it trusted.
		opts.Frozen = backend.HostFrozen
		opts.RecordState = true
	}
	if w.loop != nil {
		opts.OnAdopt = func(a provider.Adopted) { w.loop.Adopt(a.Guest.UID, a.Suspended, a.Released) }
	}
	adopted, err := provider.Recover(ctx, backend, opts)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	log.G(ctx).WithField("adopted", len(adopted)).Info("recover: relist done")
	return nil
}
