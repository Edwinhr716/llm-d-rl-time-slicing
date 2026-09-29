//go:build evalwire

// Package evalwire starts an in-process orchestrator for evaluation harnesses.
// It is compiled only with the evalwire build tag and is never part of the
// product binary.
//
// Start wires the stores, informers, work queue, rate limiter, controller,
// workers and server options the same way cmd/timesliceorchestrator/main.go
// does, from the same flag strings, on a caller-supplied clientset (usually
// k8s.io/client-go/kubernetes/fake). Keep it in step with main.go: the test in
// this package fails when the two flag sets differ.
//
// Differences from main.go, all needed to run in a test process:
//   - The clientset comes from Config instead of a kubeconfig.
//   - --port and --metrics-port are ignored; each Start picks free loopback
//     ports and reports them in Orch.Addr and Orch.MetricsAddr.
//   - Config.AgentPort, when non-zero, overrides --snapshot-agent-port.
//   - The default slog logger is left to the caller. To get the group, job,
//     node and operation IDs from the context on each record, as main.go
//     does, wrap the handler with logging.NewContextHandler.
//   - Metrics are registered once per process, so Start can run again.
package evalwire

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"
)

// Config configures one in-process orchestrator.
type Config struct {
	// Clientset is the Kubernetes client the orchestrator watches and stores
	// its lock ConfigMap in. Reuse it across Start calls to model a restart.
	Clientset kubernetes.Interface
	// AgentPort, when non-zero, overrides --snapshot-agent-port.
	AgentPort int
	// Args are flags as cmd/timesliceorchestrator accepts them, for example
	// "--node-selector=pool=demo".
	Args []string
}

// JobInfo is one job in a group's job store.
type JobInfo struct {
	// JobID is the timeslice.io/job-id of the job's pods.
	JobID string
	// Role is "background" when a pod of the job carries
	// timeslice.io/role=background, and "foreground" otherwise.
	Role string
}

// Orch is a running in-process orchestrator.
type Orch struct {
	// Addr is the gRPC address (host:port) of the orchestrator service.
	Addr string
	// MetricsAddr is the host:port serving /metrics.
	MetricsAddr string
	// Stop cancels the orchestrator and waits for it to shut down. Like a
	// SIGTERM to the binary it stops gracefully, so it returns only after
	// in-flight RPCs (a blocked Acquire, for example) end; cancel them on the
	// client side first. After Stop, Start may run again on the same
	// clientset.
	Stop func()
	// GroupJobs returns the jobs the orchestrator's job store holds for the
	// group right now, sorted by job ID.
	GroupJobs func(group string) []JobInfo
}

// flagValues holds the parsed flags, one field per flag in main.go.
type flagValues struct {
	port                     int
	metricsPort              int
	kubeconfig               string
	controllerWorkers        int
	snapshotAgentPort        int
	resyncPeriod             time.Duration
	servingQuantum           time.Duration
	budgetRedisAddr          string
	budgetKey                string
	budgetJob                string
	budgetOpenDelay          time.Duration
	budgetExternalRisingEdge bool
	lockNamespace            string
	lockConfigMap            string
	watchNamespaces          string
	nodeSelector             string
	skipUnboundPods          bool
}

// newFlagSet declares the flags of cmd/timesliceorchestrator/main.go with the
// same names, defaults and environment overrides.
func newFlagSet() (*flag.FlagSet, *flagValues) {
	fv := &flagValues{}
	fs := flag.NewFlagSet("evalwire", flag.ContinueOnError)
	fs.IntVar(&fv.port, "port", 50051, "The server port (ignored: Start picks a free port)")
	fs.IntVar(&fv.metricsPort, "metrics-port", 8080, "The metrics server port (ignored: Start picks a free port)")
	fs.StringVar(&fv.kubeconfig, "kubeconfig", "", "Ignored: the clientset comes from Config")
	fs.IntVar(&fv.controllerWorkers, "controller-workers", 1, "The number of workers for the controller")
	fs.IntVar(&fv.snapshotAgentPort, "snapshot-agent-port", 9001, "The default port for snapshot agents")
	fs.DurationVar(&fv.resyncPeriod, "resync-period", 30*time.Second, "The period for periodic resync of agent states")
	fs.DurationVar(&fv.servingQuantum, "serving-quantum", envDuration("TIMESLICE_SERVING_QUANTUM", 0),
		"Minimum run time after a restore before waiters are advertised")
	fs.StringVar(&fv.budgetRedisAddr, "dispatch-budget-redis-addr", os.Getenv("TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR"),
		"host:port of the Redis to publish the dispatch budget to")
	fs.StringVar(&fv.budgetKey, "dispatch-budget-key", envString("TIMESLICE_DISPATCH_BUDGET_KEY", budget.DefaultKey),
		"Redis key of the dispatch budget")
	fs.StringVar(&fv.budgetJob, "dispatch-budget-job", os.Getenv("TIMESLICE_DISPATCH_BUDGET_JOB"),
		"job ID of the batch tenant whose availability is published")
	fs.DurationVar(&fv.budgetOpenDelay, "dispatch-budget-open-delay",
		envDuration("TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY", 0), "Hold-down before publishing the rising edge")
	fs.BoolVar(&fv.budgetExternalRisingEdge, "dispatch-budget-external-rising-edge",
		envBool("TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE", false), "Publish only \"0\"")
	fs.StringVar(&fv.lockNamespace, "lock-namespace", store.Namespace, "Namespace of the lock ConfigMap")
	fs.StringVar(&fv.lockConfigMap, "lock-configmap", store.ConfigMapName, "Name of the lock ConfigMap")
	fs.StringVar(&fv.watchNamespaces, "watch-namespaces", "", "Comma-separated namespaces whose pods are watched")
	fs.StringVar(&fv.nodeSelector, "node-selector", "", "Label selector limiting the nodes this orchestrator sees")
	fs.BoolVar(&fv.skipUnboundPods, "skip-unbound-pods", false,
		"With --node-selector set, do not count a pod toward its group until it is bound to a node")
	return fs, fv
}

// validate applies the checks main.go runs after flag.Parse and returns the
// parsed scope.
func (fv *flagValues) validate() (infrastructure.Scope, error) {
	scope, err := infrastructure.ParseScope(fv.watchNamespaces, fv.nodeSelector)
	if err != nil {
		return infrastructure.Scope{}, err
	}
	if fv.budgetRedisAddr != "" && fv.budgetJob == "" {
		return infrastructure.Scope{}, errors.New("--dispatch-budget-job is required when --dispatch-budget-redis-addr is set")
	}
	return scope, nil
}

var registerMetrics sync.Once

// Start parses cfg.Args, wires an orchestrator as main.go does and starts it.
// It returns once the gRPC and metrics ports accept connections.
func Start(ctx context.Context, cfg Config) (*Orch, error) {
	if cfg.Clientset == nil {
		return nil, errors.New("evalwire: Config.Clientset is required")
	}
	fs, fv := newFlagSet()
	if err := fs.Parse(cfg.Args); err != nil {
		return nil, fmt.Errorf("evalwire: %w", err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("evalwire: unexpected arguments %v", fs.Args())
	}
	if cfg.AgentPort != 0 {
		fv.snapshotAgentPort = cfg.AgentPort
	}
	scope, err := fv.validate()
	if err != nil {
		return nil, fmt.Errorf("evalwire: %w", err)
	}

	grpcPort, err := freePort(ctx)
	if err != nil {
		return nil, err
	}
	metricsPort, err := freePort(ctx)
	if err != nil {
		return nil, err
	}

	registerMetrics.Do(metrics.Register)

	ctx, cancel := context.WithCancel(ctx)
	clientset := cfg.Clientset

	// From here on this mirrors run() in cmd/timesliceorchestrator/main.go.
	informerFactories := scope.NewInformerFactories(clientset, time.Minute*30)

	lockStore := store.NewConfigMapLockStore(clientset, store.WithConfigMap(fv.lockNamespace, fv.lockConfigMap))
	groupStore := store.NewGroupStore(lockStore)
	jobStore := store.NewJobStore()
	snapshotAgentStore := store.NewGRPCSnapshotAgentStore(0, fv.snapshotAgentPort)
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{
			Name: "groups",
		},
	)

	infraOpts := make([]infrastructure.Option, 0, len(informerFactories.Pods))
	for _, f := range informerFactories.Pods[1:] {
		infraOpts = append(infraOpts, infrastructure.WithPodInformers(f.Core().V1().Pods()))
	}
	if !scope.AllNodes() {
		infraOpts = append(infraOpts, infrastructure.WithNodeScopedPods())
		if fv.skipUnboundPods {
			infraOpts = append(infraOpts, infrastructure.WithSkipUnboundPods())
		}
	} else if fv.skipUnboundPods {
		slog.Warn("--skip-unbound-pods has no effect without --node-selector")
	}
	infraOrch := infrastructure.NewKubernetesOrchestrator(
		informerFactories.Nodes.Core().V1().Nodes(),
		informerFactories.Pods[0].Core().V1().Pods(),
		groupStore,
		jobStore,
		snapshotAgentStore,
		infraOpts...,
	)
	if err := infraOrch.Start(ctx, queue); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start infrastructure orchestrator: %w", err)
	}

	ctrl := controller.NewController(
		groupStore,
		jobStore,
		queue,
		infraOrch,
		snapshotAgentStore,
	)
	ctrl.ResyncPeriod = fv.resyncPeriod

	// Listers for GroupJobs, created before the factories start so their
	// informers are the ones the orchestrator already uses.
	podListers := make([]corev1listers.PodLister, 0, len(informerFactories.Pods))
	for _, f := range informerFactories.Pods {
		podListers = append(podListers, f.Core().V1().Pods().Lister())
	}

	informerFactories.Nodes.Start(ctx.Done())
	for _, f := range informerFactories.Pods {
		f.Start(ctx.Done())
	}

	opts := []server.Option{server.WithServingQuantum(fv.servingQuantum)}
	var publisher *budget.Publisher
	if fv.budgetRedisAddr != "" {
		publisher = budget.NewPublisher(budget.NewRedisWriter(fv.budgetRedisAddr), fv.budgetKey, fv.budgetJob).
			WithOpenDelay(fv.budgetOpenDelay).
			WithExternalRisingEdge(fv.budgetExternalRisingEdge)
		opts = append(opts, server.WithDispatchBudgetPublisher(publisher))
	}

	slog.InfoContext(ctx, "Starting TimeSlice Orchestrator server (evalwire)",
		"grpcPort", grpcPort,
		"metricsPort", metricsPort,
		"controllerWorkers", fv.controllerWorkers,
		"servingQuantum", fv.servingQuantum,
		"lockConfigMap", lockStore.ConfigMapRef(),
		"watchNamespaces", scope.Namespaces,
		"nodeSelector", scope.NodeSelector,
		"skipUnboundPods", fv.skipUnboundPods,
	)

	// serveErr is written before exited is closed and read only after.
	var serveErr error
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		serveErr = server.StartServer(ctx, grpcPort, metricsPort, ctrl, groupStore, jobStore, fv.controllerWorkers, opts...)
	}()

	orch := &Orch{
		Addr:        net.JoinHostPort("127.0.0.1", strconv.Itoa(grpcPort)),
		MetricsAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(metricsPort)),
	}
	orch.GroupJobs = func(group string) []JobInfo {
		return groupJobs(ctx, jobStore, podListers, group)
	}
	var stopOnce sync.Once
	orch.Stop = func() {
		stopOnce.Do(func() {
			cancel()
			<-exited
			if serveErr != nil {
				slog.Error("evalwire: orchestrator stopped with an error", "error", serveErr)
			}
			if publisher != nil {
				if err := publisher.Close(); err != nil {
					slog.Error("evalwire: failed to close dispatch budget publisher", "error", err)
				}
			}
		})
	}

	for _, addr := range []string{orch.Addr, orch.MetricsAddr} {
		if err := waitListening(ctx, addr, exited); err != nil {
			orch.Stop()
			return nil, err
		}
	}
	return orch, nil
}

// groupJobs lists the job store's jobs for group, with each job's role read
// from the timeslice.io/role label of its pods.
func groupJobs(ctx context.Context, jobStore *store.JobStore, podListers []corev1listers.PodLister, group string) []JobInfo {
	jobs, err := jobStore.ListByGroup(ctx, group)
	if err != nil {
		return nil
	}
	background := map[string]bool{}
	selector := labels.SelectorFromSet(labels.Set{infrastructure.PodLabelKey: group})
	for _, lister := range podListers {
		pods, err := lister.List(selector)
		if err != nil {
			continue
		}
		for _, pod := range pods {
			if pod.Labels[roleLabelKey] == roleBackground {
				background[pod.Labels[infrastructure.JobLabelKey]] = true
			}
		}
	}
	out := make([]JobInfo, 0, len(jobs))
	for _, job := range jobs {
		role := "foreground"
		if background[job.JobID()] {
			role = roleBackground
		}
		out = append(out, JobInfo{JobID: job.JobID(), Role: role})
	}
	slices.SortFunc(out, func(a, b JobInfo) int { return strings.Compare(a.JobID, b.JobID) })
	return out
}

// The contract's role label and its background value.
const (
	roleLabelKey   = "timeslice.io/role"
	roleBackground = "background"
)

// freePort returns a loopback TCP port that was free a moment ago.
func freePort(ctx context.Context) (int, error) {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("evalwire: pick a free port: %w", err)
	}
	addr := lis.Addr()
	if err := lis.Close(); err != nil {
		return 0, fmt.Errorf("evalwire: release port %v: %w", addr, err)
	}
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("evalwire: unexpected listener address %v", addr)
	}
	return tcpAddr.Port, nil
}

// waitListening polls addr until it accepts a TCP connection, the server
// exits or 10 s pass.
func waitListening(ctx context.Context, addr string, exited <-chan struct{}) error {
	deadline := time.Now().Add(10 * time.Second)
	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn.Close()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("evalwire: %s not listening after 10s: %w", addr, err)
		}
		select {
		case <-exited:
			return fmt.Errorf("evalwire: orchestrator exited before %s listened", addr)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// envString, envDuration and envBool match the helpers in main.go.
func envString(name, def string) string {
	if raw, ok := os.LookupEnv(name); ok && raw != "" {
		return raw
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return d
}

func envBool(name string, def bool) bool {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return b
}
