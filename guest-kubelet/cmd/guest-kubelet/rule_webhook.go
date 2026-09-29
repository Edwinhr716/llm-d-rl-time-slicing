package main

// D-VK-7 option "rule": the `daemonset-rule-webhook` subcommand (internal/dsrule).

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"

	"github.com/edwinhr716/guest-kubelet/internal/dsrule"
)

func runRuleWebhook(args []string) int {
	log.L = vkslog.FromSlog(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	fs := flag.NewFlagSet("daemonset-rule-webhook", flag.ContinueOnError)
	cfg := &dsrule.Config{}
	var kubeconfig string
	fs.StringVar(&cfg.Namespace, "namespace", os.Getenv("POD_NAMESPACE"),
		"namespace of the Service and the certificate Secret (env POD_NAMESPACE)")
	fs.StringVar(&cfg.Service, "service", "daemonset-rule", "Service that fronts the webhook")
	fs.StringVar(&cfg.Secret, "secret", "daemonset-rule-tls",
		"Secret that holds the self-made serving certificate (created if missing)")
	fs.StringVar(&cfg.WebhookConfig, "webhook-config", "", "MutatingWebhookConfiguration whose caBundle this server sets")
	fs.StringVar(&cfg.Addr, "listen", ":10250", "HTTPS listen address")
	fs.StringVar(&kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if cfg.Namespace == "" || cfg.WebhookConfig == "" {
		log.G(ctx).Error("daemonset-rule-webhook needs --namespace (or POD_NAMESPACE) and --webhook-config")
		return 2
	}
	client, err := nodeutil.ClientsetFromEnv(kubeconfig)
	if err != nil {
		log.G(ctx).WithError(err).Error("kubernetes client")
		return 1
	}
	if err := dsrule.Run(ctx, client, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.G(ctx).WithError(err).Error("daemonset-rule webhook exited")
		return 1
	}
	return 0
}
