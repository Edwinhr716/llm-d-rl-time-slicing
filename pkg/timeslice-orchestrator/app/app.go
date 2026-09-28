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

// Package app wires the timeslice orchestrator from its command line: flags,
// stores, informers, controller and gRPC server. The binary and the
// evaluation wiring both call Run, so they are wired identically.
package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// Deps are the dependencies Run would otherwise build itself.
type Deps struct {
	// Clientset, when set, is used instead of one built from --kubeconfig or
	// the in-cluster config.
	Clientset kubernetes.Interface
	// ImmediateStop stops the gRPC server without waiting for RPCs in flight
	// when ctx is done (see server.WithImmediateStop).
	ImmediateStop bool
}

var registerMetrics sync.Once

// Run parses args (the command line without the program name), wires the
// orchestrator and serves until ctx is done.
func Run(ctx context.Context, args []string, deps Deps) error {
	fs := flag.NewFlagSet("timesliceorchestrator", flag.ContinueOnError)
	port := fs.Int("port", 50051, "The server port")
	metricsPort := fs.Int("metrics-port", 8080, "The metrics server port")
	kubeconfig := fs.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	controllerWorkers := fs.Int("controller-workers", 1, "The number of workers for the controller")
	snapshotAgentPort := fs.Int("snapshot-agent-port", 9001, "The default port for snapshot agents")
	resyncPeriod := fs.Duration("resync-period", 30*time.Second, "The period for periodic resync of agent states")
	servingQuantum := fs.Duration("serving-quantum", envDuration("TIMESLICE_SERVING_QUANTUM", 0),
		"Minimum time a job that has just acquired the lock and had its context restored is allowed to run "+
			"before GetGroupStatus advertises waiting jobs to it. 0 disables the quantum. "+
			"Overridable with the TIMESLICE_SERVING_QUANTUM environment variable.")
	budgetRedisAddr := fs.String("dispatch-budget-redis-addr", os.Getenv("TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR"),
		"host:port of the Redis to publish the batch tenant's dispatch budget to. "+
			"Empty (the default) disables publishing. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_REDIS_ADDR environment variable.")
	budgetKey := fs.String("dispatch-budget-key", envString("TIMESLICE_DISPATCH_BUDGET_KEY", budget.DefaultKey),
		"Redis key to publish the batch tenant's dispatch budget to: \"1\" when it can serve, \"0\" when it "+
			"cannot. Must match the consuming gate's budget_key. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_KEY environment variable.")
	budgetJob := fs.String("dispatch-budget-job", os.Getenv("TIMESLICE_DISPATCH_BUDGET_JOB"),
		"job ID of the batch tenant whose availability is published. Required when "+
			"--dispatch-budget-redis-addr is set; a group held by any other job publishes \"0\". "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_JOB environment variable.")
	budgetOpenDelay := fs.Duration("dispatch-budget-open-delay", envDuration("TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY", 0),
		"How long to hold the dispatch budget at \"0\" after the batch tenant becomes servable, to cover the "+
			"gap between its context being restored and its Service endpoint being routable again. 0 (the "+
			"default) publishes the rising edge immediately, which measurably admits traffic to an endpoint "+
			"that is not yet accepting connections. The falling edge is never delayed. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_OPEN_DELAY environment variable.")
	budgetExternalRisingEdge := fs.Bool("dispatch-budget-external-rising-edge",
		envBool("TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE", false),
		"Publish only \"0\", never \"1\", leaving the rising edge to an external publisher that can observe "+
			"when the batch tenant's endpoint is actually routable — something this process cannot see, which "+
			"is why the unmitigated rising edge opens the gate 0.74-1.84 s early. \"0\" is still written on "+
			"every evaluation, so an evicted key is still recreated closed. WARNING: if nothing else writes "+
			"\"1\" to the key, the gate stays shut forever and the batch tenant never serves; watch "+
			"timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total to see the orchestrator "+
			"declining to open it. Supersedes --dispatch-budget-open-delay. "+
			"Overridable with the TIMESLICE_DISPATCH_BUDGET_EXTERNAL_RISING_EDGE environment variable.")
	foregroundWait := fs.String("foreground-wait", controller.ForegroundWaitBlocking,
		"How reconcile waits on a foreground snapshot or restore. \"blocking\" (option A, the default and the "+
			"only mode implemented) blocks on the agent operation, bounded by --foreground-op-timeout. "+
			"PENDING LEAD DECISION: option B (\"async\") is refused until decided.")
	foregroundOpTimeout := fs.Duration("foreground-op-timeout", 10*time.Minute,
		"Upper bound on each blocking wait for a foreground snapshot or restore operation. On expiry the "+
			"reconcile is retried. 0 means unbounded.")
	backgroundRole := fs.Bool("background-role", false,
		"Enable the background participant protocol: Acquire/Yield with ROLE_BACKGROUND, participant_id "+
			"heartbeats and GroupStatus.background_protocol = 1. Off (the default) reports "+
			"background_protocol = 0 and refuses ROLE_BACKGROUND; foreground callers are unaffected.")
	minBubble := fs.Duration("min-bubble", 0,
		"Smallest Yield expected_idle that records a lend hint. 0 (the default) never lends: the group goes "+
			"IDLE_YIELDED as before. PENDING LEAD DECISION: suggested demo value 30s.")
	noticeWindow := fs.Duration("notice-window", server.DefaultNoticeWindow,
		"Notice window N: time from a foreground Acquire to the foreground getting the accelerator back while "+
			"background guests hold it. PENDING LEAD DECISION.")
	killBudget := fs.Duration("kill-budget", server.DefaultKillBudget,
		"Kill budget K reserved at the end of the notice window; guests must vacate by T = notice + N - K. "+
			"PENDING LEAD DECISION.")
	hostCommandPort := fs.Int("host-command-port", 0,
		"Port of the host command endpoint on each shared host, dialled at <node InternalIP>:<port>. When set "+
			"(requires --background-role), the orchestrator lends on a Yield with a lend hint by granting the "+
			"waiting hosts and pushing Resume to each, pushes Vacate to every holding host when a foreground "+
			"Acquire starts a notice, and grants the foreground only after every host acked. 0 (the default) "+
			"disables the push. PENDING LEAD DECISION D-NS-4 (option hybrid).")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

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
		return fmt.Errorf("--host-command-port must be between 0 and 65535, got %d", *hostCommandPort)
	}
	if *hostCommandPort > 0 && !*backgroundRole {
		return fmt.Errorf("--host-command-port requires --background-role")
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

	registerMetrics.Do(metrics.Register)

	clientset := deps.Clientset
	if clientset == nil {
		config, err := buildKubeConfig(*kubeconfig)
		if err != nil {
			return fmt.Errorf("failed to load kubernetes config: %w", err)
		}
		cs, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}
		clientset = cs
	}

	nodeInformerFactory := informers.NewSharedInformerFactory(clientset, time.Minute*30)
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Minute*30,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = "timeslice.io/group"
		}),
	)

	lockStore := store.NewConfigMapLockStore(clientset)
	groupStore := store.NewGroupStore(lockStore)
	jobStore := store.NewJobStore()
	snapshotAgentStore := store.NewGRPCSnapshotAgentStore(0, *snapshotAgentPort)
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
	ctrl.ResyncPeriod = *resyncPeriod
	ctrl.ForegroundOpTimeout = *foregroundOpTimeout
	if *hostCommandPort > 0 {
		hostClient := hostcmd.NewClient(*hostCommandPort, infraOrch.NodeAddress)
		defer func() {
			if err := hostClient.Close(); err != nil {
				slog.Error("Failed to close host command client", "error", err)
			}
		}()
		ctrl.EnableHostPush(controller.HostPushConfig{
			Commander:    hostClient,
			NoticeWindow: *noticeWindow,
			KillBudget:   *killBudget,
		})
	}

	// Start informers
	nodeInformerFactory.Start(ctx.Done())
	podInformerFactory.Start(ctx.Done())
	defer nodeInformerFactory.Shutdown()
	defer podInformerFactory.Shutdown()

	opts := []server.Option{
		server.WithServingQuantum(*servingQuantum),
		server.WithBackgroundRole(*backgroundRole),
		server.WithMinBubble(*minBubble),
		server.WithNoticeTiming(*noticeWindow, *killBudget),
		server.WithHostAcks(*hostCommandPort > 0),
		server.WithImmediateStop(deps.ImmediateStop),
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
	)
	return server.StartServer(ctx, *port, *metricsPort, ctrl, groupStore, jobStore, *controllerWorkers, opts...)
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
