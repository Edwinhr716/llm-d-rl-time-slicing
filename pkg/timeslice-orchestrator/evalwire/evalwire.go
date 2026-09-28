//go:build evalwire

// Package evalwire starts an in-process orchestrator for evaluation harnesses.
// It is compiled only with the evalwire build tag and is never part of the
// product binary.
//
// Start runs app.Run, the same wiring cmd/timesliceorchestrator/main.go runs,
// from the same flag strings, on a caller-supplied clientset (usually
// k8s.io/client-go/kubernetes/fake).
//
// Differences from main.go, all needed to run in a test process:
//   - The clientset comes from Config instead of a kubeconfig.
//   - --port and --metrics-port are ignored; each Start listens on free
//     loopback ports and reports them in Orch.Addr and Orch.MetricsAddr.
//   - Config.AgentPort, when non-zero, overrides --snapshot-agent-port.
//   - Config.HostPort, when non-zero, overrides --host-command-port.
//   - The default slog logger is left to the caller.
//   - Metrics are registered once per process, so Start can run again.
package evalwire

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/app"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"k8s.io/client-go/kubernetes"
)

// Config configures one in-process orchestrator.
type Config struct {
	// Clientset is the Kubernetes client the orchestrator watches and stores
	// its lock ConfigMap in. Reuse it across Start calls to model a restart.
	Clientset kubernetes.Interface
	// AgentPort, when non-zero, overrides --snapshot-agent-port.
	AgentPort int
	// HostPort, when non-zero, overrides --host-command-port: the
	// orchestrator commands each host at <node InternalIP>:HostPort.
	HostPort int
	// Args are flags as cmd/timesliceorchestrator accepts them.
	Args []string
}

// Orch is a running in-process orchestrator.
type Orch struct {
	// Addr is the gRPC address (host:port) of the orchestrator service.
	Addr string
	// MetricsAddr is the host:port serving /metrics.
	MetricsAddr string
	// Stop cancels the orchestrator and waits for it to shut down, like a
	// SIGTERM to the binary. In-flight RPCs are cut after the server stop
	// grace (5 s). After Stop, Start may run again on the same clientset.
	Stop func()
}

var registerMetrics sync.Once

// Start parses args, validates them as main.go does and runs the
// orchestrator until ctx is done or Stop is called.
func Start(ctx context.Context, cfg Config) (*Orch, error) {
	if cfg.Clientset == nil {
		return nil, errors.New("evalwire: Clientset is required")
	}
	fs := flag.NewFlagSet("evalwire", flag.ContinueOnError)
	opts := app.RegisterFlags(fs)
	if err := fs.Parse(cfg.Args); err != nil {
		return nil, fmt.Errorf("evalwire: %w", err)
	}
	if cfg.AgentPort != 0 {
		opts.SnapshotAgentPort = cfg.AgentPort
	}
	if cfg.HostPort != 0 {
		opts.HostCommandPort = cfg.HostPort
	}
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("evalwire: %w", err)
	}
	registerMetrics.Do(metrics.Register)

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("evalwire: listen: %w", err)
	}
	metricsLis, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("evalwire: listen for metrics: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := app.Run(runCtx, opts, cfg.Clientset, lis, metricsLis); err != nil {
			slog.Error("evalwire: orchestrator stopped with an error", "error", err)
		}
	}()
	var once sync.Once
	return &Orch{
		Addr:        lis.Addr().String(),
		MetricsAddr: metricsLis.Addr().String(),
		Stop: func() {
			once.Do(func() {
				cancel()
				<-done
			})
		},
	}, nil
}
