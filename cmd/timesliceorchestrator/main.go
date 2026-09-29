package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Failed to run server", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// Initialize slog with ContextHandler
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	ctxHandler := logging.NewContextHandler(jsonHandler)
	slog.SetDefault(slog.New(ctxHandler))

	port := flag.Int("port", 50051, "The server port")
	metricsPort := flag.Int("metrics-port", 8080, "The metrics server port")
	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	controllerWorkers := flag.Int("controller-workers", controller.DefaultWorkers,
		"The number of workers for the controller. More than one lets other groups and the resync proceed "+
			"while one group waits on a slow agent.")
	snapshotAgentPort := flag.Int("snapshot-agent-port", 9001, "The default port for snapshot agents")
	resyncPeriod := flag.Duration("resync-period", 30*time.Second, "The period for periodic resync of agent states")
	servingQuantum := flag.Duration("serving-quantum", envDuration("TIMESLICE_SERVING_QUANTUM", 0),
		"Minimum time a job that has just acquired the lock and had its context restored is allowed to run "+
			"before GetGroupStatus advertises waiting jobs to it. 0 disables the quantum. "+
			"Overridable with the TIMESLICE_SERVING_QUANTUM environment variable.")
	budgetRedisAddr := flag.String("dispatch-budget-redis-addr", os.Getenv("TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR"),
		"host:port of the Redis to publish the batch tenant's dispatch budget to. "+
			"Empty (the default) disables publishing. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR environment variable.")
	budgetKey := flag.String("dispatch-budget-key", envString("TIMESLICE_DISPATCH_BUDGET_KEY", budget.DefaultKey),
		"Redis key to publish the batch tenant's dispatch budget to: \"1\" when it can serve, \"0\" when it "+
			"cannot. Must match the consuming gate's budget_key. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_KEY environment variable.")
	budgetJob := flag.String("dispatch-budget-job", os.Getenv("TIMESLICE_DISPATCH_BUDGET_JOB"),
		"job ID of the batch tenant whose availability is published. Required when "+
			"--dispatch-budget-redis-addr is set; a group held by any other job publishes \"0\". "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_JOB environment variable.")
	budgetOpenDelay := flag.Duration("dispatch-budget-open-delay", envDuration("TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY", 0),
		"How long to hold the dispatch budget at \"0\" after the batch tenant becomes servable, to cover the "+
			"gap between its context being restored and its Service endpoint being routable again. 0 (the "+
			"default) publishes the rising edge immediately, which measurably admits traffic to an endpoint "+
			"that is not yet accepting connections. The falling edge is never delayed. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY environment variable.")
	budgetExternalRisingEdge := flag.Bool("dispatch-budget-external-rising-edge",
		envBool("TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE", false),
		"Publish only \"0\", never \"1\", leaving the rising edge to an external publisher that can observe "+
			"when the batch tenant's endpoint is actually routable — something this process cannot see, which "+
			"is why the unmitigated rising edge opens the gate 0.74-1.84 s early. \"0\" is still written on "+
			"every evaluation, so an evicted key is still recreated closed. WARNING: if nothing else writes "+
			"\"1\" to the key, the gate stays shut forever and the batch tenant never serves; watch "+
			"timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total to see the orchestrator "+
			"declining to open it. Supersedes --dispatch-budget-open-delay. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE environment variable.")
	foregroundWait := flag.String("foreground-wait", controller.ForegroundWaitBlocking,
		"How reconcile waits on a foreground snapshot or restore. \"blocking\" (option A, the default and the "+
			"only mode implemented) blocks on the agent operation, bounded by --foreground-op-timeout. "+
			"PENDING LEAD DECISION: option B (\"async\") is refused until decided.")
	foregroundOpTimeout := flag.Duration("foreground-op-timeout", 10*time.Minute,
		"Upper bound on each blocking wait for a foreground snapshot or restore operation. On expiry the "+
			"reconcile is retried. 0 means unbounded.")
	backgroundRole := flag.Bool("background-role", false,
		"Enable the background participant protocol: Acquire/Yield with ROLE_BACKGROUND, participant_id "+
			"heartbeats and GroupStatus.background_protocol = 1. Off (the default) reports "+
			"background_protocol = 0 and refuses ROLE_BACKGROUND; foreground callers are unaffected.")
	minBubble := flag.Duration("min-bubble", 0,
		"Smallest Yield expected_idle that records a lend hint. 0 (the default) never lends: the group goes "+
			"IDLE_YIELDED as before. PENDING LEAD DECISION: suggested demo value 30s.")
	noticeWindow := flag.Duration("notice-window", server.DefaultNoticeWindow,
		"Notice window N: time from a foreground Acquire to the foreground getting the accelerator back while "+
			"background guests hold it. PENDING LEAD DECISION.")
	killBudget := flag.Duration("kill-budget", server.DefaultKillBudget,
		"Kill budget K reserved at the end of the notice window; guests must vacate by T = notice + N - K. "+
			"PENDING LEAD DECISION.")
	hostCommandPort := flag.Int("host-command-port", 0,
		"Port of the per-host command endpoint (D-NS-4 ns-push-vk). When set, a foreground Acquire commands "+
			"every host of the group at <node InternalIP>:<port> to vacate by T = notice + N - K and is granted only "+
			"after every host acked; a lend (Yield with --min-bubble) commands the hosts to resume. 0 (the default) "+
			"disables host commands.")
	backgroundLiveness := flag.Duration("background-liveness", controller.DefaultBackgroundLiveness,
		"Background liveness L: with host commands on, a host whose commands have failed this long during a "+
			"vacate counts as unseen and its guests are killed through their snapshot agent.")
	// Fault-path timeouts and retries (Q13). The defaults marked PENDING LEAD DECISION
	// are proposals awaiting the lead's sign-off.
	agentRPCTimeout := flag.Duration("agent-rpc-timeout", 5*time.Second,
		"Bound on every call to a snapshot agent, including each status and operation poll. 0 disables it. "+
			"PENDING LEAD DECISION.")
	retryBaseDelay := flag.Duration("retry-base-delay", 1*time.Second,
		"First retry delay after a failed reconcile of a group; it doubles on each further failure. "+
			"PENDING LEAD DECISION.")
	retryMaxDelay := flag.Duration("retry-max-delay", 30*time.Second,
		"Cap on the retry delay after failed reconciles of a group. PENDING LEAD DECISION.")
	holderWaitRequeue := flag.Duration("holder-wait-requeue", 1*time.Second,
		"Re-reconcile a group this long after a pass that ends with the lock holder not yet loaded, so an "+
			"agent state change reaches the waiting Acquire promptly. 0 disables it. PENDING LEAD DECISION.")
	killPollInterval := flag.Duration("kill-poll-interval", controller.DefaultKillPollInterval,
		"How often a kill operation is polled. PENDING LEAD DECISION.")
	lockNamespace := flag.String("lock-namespace", store.Namespace,
		"Namespace of the ConfigMap that persists group lock holders. Give each orchestrator install in a "+
			"cluster its own lock ConfigMap; two installs sharing one fight over the same groups.")
	lockConfigMap := flag.String("lock-configmap", store.ConfigMapName,
		"Name of the ConfigMap that persists group lock holders.")
	watchNamespaces := flag.String("watch-namespaces", "",
		"Comma-separated namespaces whose pods are watched. Pods elsewhere are invisible to this "+
			"orchestrator and join no group. Empty (the default) watches all namespaces.")
	nodeSelector := flag.String("node-selector", "",
		"Label selector (kubectl syntax, e.g. pool=demo) limiting the nodes this orchestrator sees. "+
			"Nodes outside it contribute to no group, and pods bound to them are ignored, except pods labelled "+
			"timeslice.io/role=background, which stay in their group. Group membership still comes from the "+
			"group.timeslice.io/<group> node label. Empty (the default) watches all nodes.")
	flag.Parse()

	if err := controller.ValidateForegroundWait(*foregroundWait); err != nil {
		return fmt.Errorf("--foreground-wait: %w", err)
	}
	if *foregroundOpTimeout < 0 {
		return fmt.Errorf("--foreground-op-timeout must not be negative, got %v", *foregroundOpTimeout)
	}
	if *minBubble < 0 {
		return fmt.Errorf("--min-bubble must not be negative, got %v", *minBubble)
	}
	if *noticeWindow <= 0 || *killBudget <= 0 || *killBudget >= *noticeWindow {
		return fmt.Errorf("--kill-budget (%v) and --notice-window (%v) must be positive with kill budget < notice window",
			*killBudget, *noticeWindow)
	}

	if *hostCommandPort < 0 || *hostCommandPort > 65535 {
		return fmt.Errorf("--host-command-port must be 0 or a port number, got %d", *hostCommandPort)
	}
	if *backgroundLiveness <= 0 {
		return fmt.Errorf("--background-liveness must be positive, got %v", *backgroundLiveness)
	}

	scope, err := infrastructure.ParseScope(*watchNamespaces, *nodeSelector)
	if err != nil {
		return err
	}

	if *budgetRedisAddr != "" && *budgetJob == "" {
		// Defaulting here would silently publish "0" forever and stall the
		// batch tenant, so fail fast instead.
		return fmt.Errorf("--dispatch-budget-job is required when --dispatch-budget-redis-addr is set")
	}
	if *budgetExternalRisingEdge && *budgetOpenDelay > 0 {
		// Not fatal: the combination is harmless, just incoherent. The hold-down
		// only ever delays a "1", and in this mode no "1" is written at all, so
		// an operator who configured both is expecting a mitigation that will
		// never fire.
		slog.Warn("--dispatch-budget-open-delay is ignored when --dispatch-budget-external-rising-edge is set",
			"openDelay", *budgetOpenDelay)
	}

	metrics.Register()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := buildKubeConfig(*kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to load kubernetes config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	informerFactories := scope.NewInformerFactories(clientset, time.Minute*30)

	lockStore := store.NewConfigMapLockStore(clientset, store.WithConfigMap(*lockNamespace, *lockConfigMap))
	groupStore := store.NewGroupStore(lockStore)
	jobStore := store.NewJobStore()
	snapshotAgentStore := store.NewGRPCSnapshotAgentStore(0, *snapshotAgentPort).WithRPCTimeout(*agentRPCTimeout)
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		controller.NewRateLimiter(*retryBaseDelay, *retryMaxDelay),
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
		return fmt.Errorf("failed to start infrastructure orchestrator: %w", err)
	}

	ctrl := controller.NewController(
		groupStore,
		jobStore,
		queue,
		infraOrch,
		snapshotAgentStore,
	)
	ctrl.ResyncPeriod = *resyncPeriod
	ctrl.HolderWaitRequeue = *holderWaitRequeue
	ctrl.ForegroundOpTimeout = *foregroundOpTimeout
	ctrl.KillPollInterval = *killPollInterval
	ctrl.BackgroundLiveness = *backgroundLiveness

	// Start informers
	informerFactories.Nodes.Start(ctx.Done())
	for _, f := range informerFactories.Pods {
		f.Start(ctx.Done())
	}

	opts := []server.Option{
		server.WithServingQuantum(*servingQuantum),
		server.WithBackgroundRole(*backgroundRole),
		server.WithMinBubble(*minBubble),
		server.WithNoticeTiming(*noticeWindow, *killBudget),
	}
	if *hostCommandPort > 0 {
		hosts := hostcmd.New(ctx, hostcmd.Config{
			Resolve:      hostResolver(infraOrch, *hostCommandPort),
			NoticeWindow: *noticeWindow,
			KillBudget:   *killBudget,
			Enqueue:      ctrl.EnqueueWork,
		})
		defer func() {
			stop() // Close waits for the command goroutines, which stop with ctx.
			hosts.Close()
		}()
		ctrl.Hosts = hosts
		opts = append(opts, server.WithHostCommander(hosts))
	}
	if *budgetRedisAddr != "" {
		publisher := budget.NewPublisher(budget.NewRedisWriter(*budgetRedisAddr), *budgetKey, *budgetJob).
			WithOpenDelay(*budgetOpenDelay).
			WithExternalRisingEdge(*budgetExternalRisingEdge)
		defer func() {
			if err := publisher.Close(); err != nil {
				slog.Error("Failed to close dispatch budget publisher", "error", err)
			}
		}()
		opts = append(opts, server.WithDispatchBudgetPublisher(publisher))
	}

	slog.InfoContext(ctx, "Starting TimeSlice Orchestrator server",
		"servingQuantum", *servingQuantum,
		"dispatchBudgetRedisAddr", *budgetRedisAddr,
		"dispatchBudgetKey", *budgetKey,
		"dispatchBudgetJob", *budgetJob,
		"dispatchBudgetOpenDelay", *budgetOpenDelay,
		"dispatchBudgetExternalRisingEdge", *budgetExternalRisingEdge,
		"foregroundWait", *foregroundWait,
		"foregroundOpTimeout", *foregroundOpTimeout,
		"backgroundRole", *backgroundRole,
		"minBubble", *minBubble,
		"noticeWindow", *noticeWindow,
		"killBudget", *killBudget,
		"hostCommandPort", *hostCommandPort,
		"backgroundLiveness", *backgroundLiveness,
		"controllerWorkers", *controllerWorkers,
		"agentRPCTimeout", *agentRPCTimeout,
		"retryBaseDelay", *retryBaseDelay,
		"retryMaxDelay", *retryMaxDelay,
		"holderWaitRequeue", *holderWaitRequeue,
		"killPollInterval", *killPollInterval,
		"lockConfigMap", lockStore.ConfigMapRef(),
		"watchNamespaces", scope.Namespaces,
		"nodeSelector", scope.NodeSelector,
	)
	return server.StartServer(ctx, *port, *metricsPort, ctrl, groupStore, jobStore, *controllerWorkers, opts...)
}

// hostResolver returns the address of the command endpoint of a node:
// <node InternalIP>:<port>.
func hostResolver(infraOrch *infrastructure.KubernetesOrchestrator, port int) func(string) (string, error) {
	return func(node string) (string, error) {
		addr, err := infraOrch.NodeAddress(node)
		if err != nil {
			return "", err
		}
		return net.JoinHostPort(addr, strconv.Itoa(port)), nil
	}
}

// envString returns the value of the named environment variable, or def if it
// is unset or empty.
func envString(name, def string) string {
	if raw, ok := os.LookupEnv(name); ok && raw != "" {
		return raw
	}
	return def
}

// envDuration returns the duration parsed from the named environment variable,
// or def if it is unset or unparseable.
func envDuration(name string, def time.Duration) time.Duration {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		slog.Warn("Ignoring unparseable duration environment variable", "name", name, "value", raw, "error", err)
		return def
	}
	return d
}

// envBool returns the boolean parsed from the named environment variable, or
// def if it is unset or unparseable.
func envBool(name string, def bool) bool {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("Ignoring unparseable boolean environment variable", "name", name, "value", raw, "error", err)
		return def
	}
	return b
}

func buildKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}

	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	slog.Info("In-cluster config failed, trying default local kubeconfig", "error", err)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	return kubeConfig.ClientConfig()
}
