// Package app wires the timeslice orchestrator: flags, stores, informers,
// controller, host commander and servers. The binary in
// cmd/timesliceorchestrator and the evaluation wiring share it, so both run
// exactly the same process.
package app

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
)

// Options holds the orchestrator's command line options.
type Options struct {
	Port                     int
	MetricsPort              int
	Kubeconfig               string
	ControllerWorkers        int
	SnapshotAgentPort        int
	HostCommandPort          int
	ResyncPeriod             time.Duration
	ServingQuantum           time.Duration
	BudgetRedisAddr          string
	BudgetKey                string
	BudgetJob                string
	BudgetOpenDelay          time.Duration
	BudgetExternalRisingEdge bool
	ForegroundWait           string
	ForegroundOpTimeout      time.Duration
	BackgroundRole           bool
	MinBubble                time.Duration
	NoticeWindow             time.Duration
	KillBudget               time.Duration
}

// RegisterFlags registers every orchestrator flag on fs and returns the
// options they fill.
func RegisterFlags(fs *flag.FlagSet) *Options {
	opts := &Options{}
	fs.IntVar(&opts.Port, "port", 50051, "The server port")
	fs.IntVar(&opts.MetricsPort, "metrics-port", 8080, "The metrics server port")
	fs.StringVar(&opts.Kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	fs.IntVar(&opts.ControllerWorkers, "controller-workers", 1, "The number of workers for the controller")
	fs.IntVar(&opts.SnapshotAgentPort, "snapshot-agent-port", 9001, "The default port for snapshot agents")
	fs.IntVar(&opts.HostCommandPort, "host-command-port", 0,
		"Port of each host's command endpoint (the snapshot agent's HostCommandService), reached at the node's "+
			"InternalIP. 0 (the default) uses --snapshot-agent-port. Used only with --background-role.")
	fs.DurationVar(&opts.ResyncPeriod, "resync-period", 30*time.Second, "The period for periodic resync of agent states")
	fs.DurationVar(&opts.ServingQuantum, "serving-quantum", envDuration("TIMESLICE_SERVING_QUANTUM", 0),
		"Minimum time a job that has just acquired the lock and had its context restored is allowed to run "+
			"before GetGroupStatus advertises waiting jobs to it. 0 disables the quantum. "+
			"Overridable with the TIMESLICE_SERVING_QUANTUM environment variable.")
	fs.StringVar(&opts.BudgetRedisAddr, "dispatch-budget-redis-addr", os.Getenv("TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR"),
		"host:port of the Redis to publish the batch tenant's dispatch budget to. "+
			"Empty (the default) disables publishing. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR environment variable.")
	fs.StringVar(&opts.BudgetKey, "dispatch-budget-key", envString("TIMESLICE_DISPATCH_BUDGET_KEY", budget.DefaultKey),
		"Redis key to publish the batch tenant's dispatch budget to: \"1\" when it can serve, \"0\" when it "+
			"cannot. Must match the consuming gate's budget_key. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_KEY environment variable.")
	fs.StringVar(&opts.BudgetJob, "dispatch-budget-job", os.Getenv("TIMESLICE_DISPATCH_BUDGET_JOB"),
		"job ID of the batch tenant whose availability is published. Required when "+
			"--dispatch-budget-redis-addr is set; a group held by any other job publishes \"0\". "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_JOB environment variable.")
	fs.DurationVar(&opts.BudgetOpenDelay, "dispatch-budget-open-delay", envDuration("TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY", 0),
		"How long to hold the dispatch budget at \"0\" after the batch tenant becomes servable, to cover the "+
			"gap between its context being restored and its Service endpoint being routable again. 0 (the "+
			"default) publishes the rising edge immediately, which measurably admits traffic to an endpoint "+
			"that is not yet accepting connections. The falling edge is never delayed. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY environment variable.")
	fs.BoolVar(&opts.BudgetExternalRisingEdge, "dispatch-budget-external-rising-edge",
		envBool("TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE", false),
		"Publish only \"0\", never \"1\", leaving the rising edge to an external publisher that can observe "+
			"when the batch tenant's endpoint is actually routable — something this process cannot see, which "+
			"is why the unmitigated rising edge opens the gate 0.74-1.84 s early. \"0\" is still written on "+
			"every evaluation, so an evicted key is still recreated closed. WARNING: if nothing else writes "+
			"\"1\" to the key, the gate stays shut forever and the batch tenant never serves; watch "+
			"timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total to see the orchestrator "+
			"declining to open it. Supersedes --dispatch-budget-open-delay. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE environment variable.")
	fs.StringVar(&opts.ForegroundWait, "foreground-wait", controller.ForegroundWaitBlocking,
		"How reconcile waits on a foreground snapshot or restore. \"blocking\" (option A, the default and the "+
			"only mode implemented) blocks on the agent operation, bounded by --foreground-op-timeout. "+
			"PENDING LEAD DECISION: option B (\"async\") is refused until decided.")
	fs.DurationVar(&opts.ForegroundOpTimeout, "foreground-op-timeout", 10*time.Minute,
		"Upper bound on each blocking wait for a foreground snapshot or restore operation. On expiry the "+
			"reconcile is retried. 0 means unbounded.")
	fs.BoolVar(&opts.BackgroundRole, "background-role", false,
		"Enable lending to the background. The orchestrator commands each host of a group: ResumeAll after a "+
			"lending Yield, SuspendAll with the deadline T on a foreground Acquire, and grants only after every "+
			"host acked. Also accepts the ROLE_BACKGROUND Acquire/Yield and participant_id heartbeats and "+
			"reports GroupStatus.background_protocol = 1. Off (the default) reports background_protocol = 0, "+
			"refuses ROLE_BACKGROUND and sends no host command; foreground callers are unaffected.")
	fs.DurationVar(&opts.MinBubble, "min-bubble", 0,
		"Smallest Yield expected_idle that records a lend hint. 0 (the default) never lends: the group goes "+
			"IDLE_YIELDED as before. PENDING LEAD DECISION: suggested demo value 30s.")
	fs.DurationVar(&opts.NoticeWindow, "notice-window", server.DefaultNoticeWindow,
		"Notice window N: time from a foreground Acquire to the foreground getting the accelerator back while "+
			"background guests hold it. PENDING LEAD DECISION.")
	fs.DurationVar(&opts.KillBudget, "kill-budget", server.DefaultKillBudget,
		"Kill budget K reserved at the end of the notice window; guests must vacate by T = notice + N - K. "+
			"PENDING LEAD DECISION.")
	return opts
}

// Validate checks the options and logs incoherent but harmless combinations.
func (opts *Options) Validate() error {
	if err := controller.ValidateForegroundWait(opts.ForegroundWait); err != nil {
		return fmt.Errorf("--foreground-wait: %w", err)
	}
	if opts.ForegroundOpTimeout < 0 {
		return fmt.Errorf("--foreground-op-timeout must not be negative, got %v", opts.ForegroundOpTimeout)
	}
	if opts.MinBubble < 0 {
		return fmt.Errorf("--min-bubble must not be negative, got %v", opts.MinBubble)
	}
	if opts.NoticeWindow <= 0 || opts.KillBudget <= 0 || opts.KillBudget >= opts.NoticeWindow {
		return fmt.Errorf("--kill-budget (%v) and --notice-window (%v) must be positive with kill budget < notice window",
			opts.KillBudget, opts.NoticeWindow)
	}
	if opts.HostCommandPort < 0 {
		return fmt.Errorf("--host-command-port must not be negative, got %d", opts.HostCommandPort)
	}

	if opts.BudgetRedisAddr != "" && opts.BudgetJob == "" {
		// Defaulting here would silently publish "0" forever and stall the
		// batch tenant, so fail fast instead.
		return fmt.Errorf("--dispatch-budget-job is required when --dispatch-budget-redis-addr is set")
	}
	if opts.BudgetExternalRisingEdge && opts.BudgetOpenDelay > 0 {
		// Not fatal: the combination is harmless, just incoherent. The hold-down
		// only ever delays a "1", and in this mode no "1" is written at all, so
		// an operator who configured both is expecting a mitigation that will
		// never fire.
		slog.Warn("--dispatch-budget-open-delay is ignored when --dispatch-budget-external-rising-edge is set",
			"openDelay", opts.BudgetOpenDelay)
	}
	return nil
}

// Run wires the orchestrator on clientset and serves gRPC on lis and metrics
// on metricsLis until ctx is done. Metrics must already be registered.
func Run(ctx context.Context, opts *Options, clientset kubernetes.Interface, lis, metricsLis net.Listener) error {
	serving := false
	defer func() {
		if !serving {
			_ = lis.Close()
			_ = metricsLis.Close()
		}
	}()

	nodeInformerFactory := informers.NewSharedInformerFactory(clientset, time.Minute*30)
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Minute*30,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = "timeslice.io/group"
		}),
	)

	lockStore := store.NewConfigMapLockStore(clientset)
	groupStore := store.NewGroupStore(lockStore)
	jobStore := store.NewJobStore()
	snapshotAgentStore := store.NewGRPCSnapshotAgentStore(0, opts.SnapshotAgentPort)
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{
			Name: "groups",
		},
	)

	infraOrch := infrastructure.NewKubernetesOrchestrator(
		nodeInformerFactory.Core().V1().Nodes(),
		podInformerFactory.Core().V1().Pods(),
		groupStore,
		jobStore,
		snapshotAgentStore,
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
	ctrl.ResyncPeriod = opts.ResyncPeriod
	ctrl.ForegroundOpTimeout = opts.ForegroundOpTimeout

	if opts.BackgroundRole {
		hostPort := opts.HostCommandPort
		if hostPort == 0 {
			hostPort = opts.SnapshotAgentPort
		}
		hostClient := hostcmd.NewGRPCClient(infraOrch.NodeAddress, hostPort)
		defer hostClient.Close()
		ctrl.Hosts = hostcmd.NewCommander(hostcmd.Config{
			Client:       hostClient,
			NoticeWindow: opts.NoticeWindow,
			KillBudget:   opts.KillBudget,
			Enqueue:      ctrl.EnqueueWork,
		})
	}

	// Start informers
	nodeInformerFactory.Start(ctx.Done())
	podInformerFactory.Start(ctx.Done())

	serverOpts := []server.Option{
		server.WithServingQuantum(opts.ServingQuantum),
		server.WithBackgroundRole(opts.BackgroundRole),
		server.WithMinBubble(opts.MinBubble),
		server.WithNoticeTiming(opts.NoticeWindow, opts.KillBudget),
	}
	if opts.BudgetRedisAddr != "" {
		publisher := budget.NewPublisher(budget.NewRedisWriter(opts.BudgetRedisAddr), opts.BudgetKey, opts.BudgetJob).
			WithOpenDelay(opts.BudgetOpenDelay).
			WithExternalRisingEdge(opts.BudgetExternalRisingEdge)
		defer func() {
			if err := publisher.Close(); err != nil {
				slog.Error("Failed to close dispatch budget publisher", "error", err)
			}
		}()
		serverOpts = append(serverOpts, server.WithDispatchBudgetPublisher(publisher))
	}

	slog.InfoContext(ctx, "Starting TimeSlice Orchestrator server",
		"servingQuantum", opts.ServingQuantum,
		"dispatchBudgetRedisAddr", opts.BudgetRedisAddr,
		"dispatchBudgetKey", opts.BudgetKey,
		"dispatchBudgetJob", opts.BudgetJob,
		"dispatchBudgetOpenDelay", opts.BudgetOpenDelay,
		"dispatchBudgetExternalRisingEdge", opts.BudgetExternalRisingEdge,
		"foregroundWait", opts.ForegroundWait,
		"foregroundOpTimeout", opts.ForegroundOpTimeout,
		"backgroundRole", opts.BackgroundRole,
		"hostCommandPort", opts.HostCommandPort,
		"minBubble", opts.MinBubble,
		"noticeWindow", opts.NoticeWindow,
		"killBudget", opts.KillBudget,
	)
	serving = true
	return server.Serve(ctx, lis, metricsLis, ctrl, groupStore, jobStore, opts.ControllerWorkers, serverOpts...)
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
