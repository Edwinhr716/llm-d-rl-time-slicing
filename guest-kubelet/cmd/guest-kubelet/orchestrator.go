package main

import (
	"context"
	"fmt"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
)

// orchestratorWiring builds and runs the orchestrator loop (VK-A6) when --orchestrator-addr
// is set. Every method is a no-op when it is not.
type orchestratorWiring struct {
	client kubernetes.Interface
	o      *options
	conn   *grpc.ClientConn
	loop   *orchestrator.Loop
}

func newOrchestratorWiring(client kubernetes.Interface, opts *options) (*orchestratorWiring, error) {
	w := &orchestratorWiring{client: client, o: opts}
	if opts.orchAddr == "" {
		return w, nil
	}
	if opts.groupSource != groupSourceNodeLabel {
		return nil, fmt.Errorf("--group-source=%q: only %q is implemented", opts.groupSource, groupSourceNodeLabel)
	}
	if opts.freezer != freezerDelete && opts.freezer != freezerFake {
		return nil, fmt.Errorf("--freezer=%q: want %q or %q", opts.freezer, freezerDelete, freezerFake)
	}
	conn, err := grpc.NewClient(opts.orchAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial orchestrator %s: %w", opts.orchAddr, err)
	}
	w.conn = conn
	return w, nil
}

// build creates the loop on top of the mirror backend. It returns nil when the loop is off.
func (w *orchestratorWiring) build(backend *mirror.Backend) (*orchestrator.Loop, error) {
	if w.conn == nil {
		return nil, nil //nolint:nilnil // nil loop means the loop is off
	}
	var freezer orchestrator.Freezer
	if w.o.freezer == freezerFake {
		freezer = &orchestrator.FakeFreezer{
			SuspendDelay: w.o.fakeSuspendDelay, ResumeDelay: w.o.fakeResumeDelay, Annotate: backend.AnnotateMirror,
		}
	}
	loop, err := orchestrator.New(&orchestrator.Config{
		Node:         w.o.hostNode,
		Client:       pb.NewTimeSliceOrchestratorServiceClient(w.conn),
		Group:        orchestrator.NodeLabelGroup(w.client, w.o.hostNode),
		Host:         backend,
		Freezer:      freezer,
		IsGuest:      provider.IsGuest,
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
