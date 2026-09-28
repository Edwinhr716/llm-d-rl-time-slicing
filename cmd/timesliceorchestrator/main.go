package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/app"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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

	opts := app.RegisterFlags(flag.CommandLine)
	flag.Parse()
	if err := opts.Validate(); err != nil {
		return err
	}

	metrics.Register()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := buildKubeConfig(opts.Kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to load kubernetes config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", opts.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	metricsLis, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", opts.MetricsPort))
	if err != nil {
		_ = lis.Close()
		return fmt.Errorf("failed to listen for metrics: %w", err)
	}
	return app.Run(ctx, opts, clientset, lis, metricsLis)
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
