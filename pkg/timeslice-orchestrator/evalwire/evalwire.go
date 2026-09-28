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
//   - The snapshot-agent store is wrapped so a node's agent is dialled at
//     <node InternalIP>:<agent port> instead of <node name>:<agent port>:
//     fake nodes have no resolvable names.
//   - Config.HostPort is accepted for a uniform harness but unused: in the
//     "keep" option (D-NS-4) the orchestrator never calls the hosts; each
//     host polls GetGroupStatus and takes part in the lock.
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

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
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
	// HostPort is unused by the "keep" option: the orchestrator never calls
	// a host.
	HostPort int
	// Args are flags as cmd/timesliceorchestrator accepts them, for example
	// "--background-role=true".
	Args []string
}

// JobInfo is one job in a group's job store.
type JobInfo struct {
	// JobID is the timeslice.io/job-id of the job's pods.
	JobID string
	// Role is "background" when the job store holds the job as a background
	// job (its pods carry timeslice.io/role=background), and "foreground"
	// otherwise.
	Role string
}

// Orch is a running in-process orchestrator.
type Orch struct {
	// Addr is the gRPC address (host:port) of the orchestrator service.
	Addr string
	// MetricsAddr is the host:port serving /metrics.
	MetricsAddr string
	// Stop cancels the orchestrator and waits for it to shut down. Like a
	// SIGTERM to the binary it stops gracefully; in-flight RPCs (a blocked
	// Acquire, for example) are cut after the server stop grace. After Stop,
	// Start may run again on the same clientset.
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
	foregroundWait           string
	foregroundOpTimeout      time.Duration
	backgroundRole           bool
	minBubble                time.Duration
	noticeWindow             time.Duration
	killBudget               time.Duration
	backgroundLiveness       time.Duration
	agentRPCTimeout          time.Duration
	retryBaseDelay           time.Duration
	retryMaxDelay            time.Duration
	holderWaitRequeue        time.Duration
	killPollInterval         time.Duration
	lockNamespace            string
	lockConfigMap            string
	watchNamespaces          string
	nodeSelector             string

	// nodeSelectorExemptBackground is decision D-ORCH-4 (false = match,
	// true = exempt).
	nodeSelectorExemptBackground bool
}

// newFlagSet declares the flags of cmd/timesliceorchestrator/main.go with the
// same names, defaults and environment overrides.
func newFlagSet() (*flag.FlagSet, *flagValues) {
	fv := &flagValues{}
	fs := flag.NewFlagSet("evalwire", flag.ContinueOnError)
	fs.IntVar(&fv.port, "port", 50051, "The server port (ignored: Start picks a free port)")
	fs.IntVar(&fv.metricsPort, "metrics-port", 8080, "The metrics server port (ignored: Start picks a free port)")
	fs.StringVar(&fv.kubeconfig, "kubeconfig", "", "Ignored: the clientset comes from Config")
	fs.IntVar(&fv.controllerWorkers, "controller-workers", controller.DefaultWorkers, "The number of workers for the controller")
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
	fs.StringVar(&fv.foregroundWait, "foreground-wait", controller.ForegroundWaitBlocking,
		"How reconcile waits on a foreground snapshot or restore")
	fs.DurationVar(&fv.foregroundOpTimeout, "foreground-op-timeout", 10*time.Minute,
		"Upper bound on each blocking wait for a foreground operation; 0 means unbounded")
	fs.BoolVar(&fv.backgroundRole, "background-role", false, "Enable the background participant protocol")
	fs.DurationVar(&fv.minBubble, "min-bubble", 0, "Smallest Yield expected_idle that records a lend hint")
	fs.DurationVar(&fv.noticeWindow, "notice-window", server.DefaultNoticeWindow, "Notice window N")
	fs.DurationVar(&fv.killBudget, "kill-budget", server.DefaultKillBudget, "Kill budget K")
	fs.DurationVar(&fv.backgroundLiveness, "background-liveness", controller.DefaultBackgroundLiveness,
		"Background liveness L: a participant holding a grant or claim unseen this long loses it")
	fs.DurationVar(&fv.agentRPCTimeout, "agent-rpc-timeout", 5*time.Second, "Bound on every snapshot agent call")
	fs.DurationVar(&fv.retryBaseDelay, "retry-base-delay", 1*time.Second, "First retry delay after a failed reconcile")
	fs.DurationVar(&fv.retryMaxDelay, "retry-max-delay", 30*time.Second, "Cap on the retry delay")
	fs.DurationVar(&fv.holderWaitRequeue, "holder-wait-requeue", 1*time.Second,
		"Re-reconcile delay while the lock holder waits for its job to be loaded; 0 disables it")
	fs.DurationVar(&fv.killPollInterval, "kill-poll-interval", controller.DefaultKillPollInterval,
		"How often a kill operation is polled")
	fs.StringVar(&fv.lockNamespace, "lock-namespace", store.Namespace, "Namespace of the lock ConfigMap")
	fs.StringVar(&fv.lockConfigMap, "lock-configmap", store.ConfigMapName, "Name of the lock ConfigMap")
	fs.StringVar(&fv.watchNamespaces, "watch-namespaces", "", "Comma-separated namespaces whose pods are watched; empty watches all")
	fs.StringVar(&fv.nodeSelector, "node-selector", "", "Label selector limiting the nodes watched; empty watches all")
	fs.BoolVar(&fv.nodeSelectorExemptBackground, "node-selector-exempt-background", false,
		"Keep role=background pods bound to a node outside --node-selector")
	return fs, fv
}

// validate applies the checks main.go runs after flag.Parse.
func (fv *flagValues) validate() error {
	if err := controller.ValidateForegroundWait(fv.foregroundWait); err != nil {
		return fmt.Errorf("--foreground-wait: %w", err)
	}
	if fv.foregroundOpTimeout < 0 {
		return fmt.Errorf("--foreground-op-timeout must not be negative, got %v", fv.foregroundOpTimeout)
	}
	if fv.minBubble < 0 {
		return fmt.Errorf("--min-bubble must not be negative, got %v", fv.minBubble)
	}
	if fv.backgroundLiveness <= 0 {
		return fmt.Errorf("--background-liveness must be positive, got %v", fv.backgroundLiveness)
	}
	if fv.noticeWindow <= 0 || fv.killBudget <= 0 || fv.killBudget >= fv.noticeWindow {
		return fmt.Errorf("--kill-budget (%v) and --notice-window (%v) must be positive with kill budget < notice window",
			fv.killBudget, fv.noticeWindow)
	}
	if fv.budgetRedisAddr != "" && fv.budgetJob == "" {
		return errors.New("--dispatch-budget-job is required when --dispatch-budget-redis-addr is set")
	}
	return nil
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
	if err := fv.validate(); err != nil {
		return nil, fmt.Errorf("evalwire: %w", err)
	}
	scope, err := infrastructure.ParseScope(fv.watchNamespaces, fv.nodeSelector)
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
	grpcAgentStore := store.NewGRPCSnapshotAgentStore(0, fv.snapshotAgentPort).WithRPCTimeout(fv.agentRPCTimeout)
	snapshotAgentStore := &addressedAgentStore{
		inner: grpcAgentStore,
		nodes: informerFactories.Nodes.Core().V1().Nodes().Lister(),
		port:  fv.snapshotAgentPort,
	}
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.NewRateLimiter(fv.retryBaseDelay, fv.retryMaxDelay),
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
		if fv.nodeSelectorExemptBackground {
			infraOpts = append(infraOpts, infrastructure.WithNodeSelectorExemptBackground())
		}
	} else if fv.nodeSelectorExemptBackground {
		slog.Warn("--node-selector-exempt-background has no effect without --node-selector")
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
	ctrl.HolderWaitRequeue = fv.holderWaitRequeue
	ctrl.KillPollInterval = fv.killPollInterval
	ctrl.ForegroundOpTimeout = fv.foregroundOpTimeout
	ctrl.NoticeWindow = fv.noticeWindow
	ctrl.KillBudget = fv.killBudget
	ctrl.BackgroundLiveness = fv.backgroundLiveness

	informerFactories.Nodes.Start(ctx.Done())
	for _, f := range informerFactories.Pods {
		f.Start(ctx.Done())
	}

	opts := []server.Option{
		server.WithServingQuantum(fv.servingQuantum),
		server.WithBackgroundRole(fv.backgroundRole),
		server.WithMinBubble(fv.minBubble),
		server.WithNoticeTiming(fv.noticeWindow, fv.killBudget),
	}
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
		"foregroundWait", fv.foregroundWait,
		"foregroundOpTimeout", fv.foregroundOpTimeout,
		"controllerWorkers", fv.controllerWorkers,
		"backgroundRole", fv.backgroundRole,
		"minBubble", fv.minBubble,
		"noticeWindow", fv.noticeWindow,
		"killBudget", fv.killBudget,
		"backgroundLiveness", fv.backgroundLiveness,
		"agentRPCTimeout", fv.agentRPCTimeout,
		"retryBaseDelay", fv.retryBaseDelay,
		"retryMaxDelay", fv.retryMaxDelay,
		"holderWaitRequeue", fv.holderWaitRequeue,
		"killPollInterval", fv.killPollInterval,
		"lockConfigMap", lockStore.ConfigMapRef(),
		"watchNamespaces", scope.Namespaces,
		"nodeSelector", scope.NodeSelector,
		"nodeSelectorExemptBackground", fv.nodeSelectorExemptBackground,
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
		return groupJobs(ctx, jobStore, group)
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

// groupJobs lists the job store's jobs for group with their roles.
func groupJobs(ctx context.Context, jobStore *store.JobStore, group string) []JobInfo {
	jobs, err := jobStore.ListByGroup(ctx, group)
	if err != nil {
		return nil
	}
	out := make([]JobInfo, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, JobInfo{JobID: job.JobID(), Role: job.Role().String()})
	}
	slices.SortFunc(out, func(a, b JobInfo) int { return strings.Compare(a.JobID, b.JobID) })
	return out
}

// addressedAgentStore dials a node's snapshot agent at <node InternalIP>:port.
// Nodes without an InternalIP, and names that already carry a port, go to the
// wrapped store unchanged.
type addressedAgentStore struct {
	inner store.SnapshotAgentStore
	nodes corev1listers.NodeLister
	port  int
}

var _ store.SnapshotAgentStore = (*addressedAgentStore)(nil)

func (a *addressedAgentStore) address(nodeName string) string {
	if _, _, err := net.SplitHostPort(nodeName); err == nil {
		return nodeName
	}
	node, err := a.nodes.Get(nodeName)
	if err != nil {
		return nodeName
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			return net.JoinHostPort(addr.Address, strconv.Itoa(a.port))
		}
	}
	return nodeName
}

func (a *addressedAgentStore) GetStatus(ctx context.Context, nodeName string) (*agentpb.StatusResponse, error) {
	return a.inner.GetStatus(ctx, a.address(nodeName))
}

func (a *addressedAgentStore) CloseClient(nodeName string) error {
	return a.inner.CloseClient(a.address(nodeName))
}

func (a *addressedAgentStore) Snapshot(
	ctx context.Context, nodeName, jobID, groupID string,
) (*agentpb.SnapshotResponse, error) {
	return a.inner.Snapshot(ctx, a.address(nodeName), jobID, groupID)
}

func (a *addressedAgentStore) GetOperation(
	ctx context.Context, nodeName, operationID string,
) (*agentpb.GetOperationResponse, error) {
	return a.inner.GetOperation(ctx, a.address(nodeName), operationID)
}

func (a *addressedAgentStore) Restore(
	ctx context.Context, nodeName, jobID, groupID string,
) (*agentpb.RestoreResponse, error) {
	return a.inner.Restore(ctx, a.address(nodeName), jobID, groupID)
}

func (a *addressedAgentStore) Kill(
	ctx context.Context, nodeName, jobID, reason string, deadline time.Time,
) (*agentpb.KillResponse, error) {
	return a.inner.Kill(ctx, a.address(nodeName), jobID, reason, deadline)
}

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
