// Command tpu-shadow-plugin is the TPU shadow device plugin. It advertises
// timeslice.io/tpu-shadow, whose devices are the TPU chips (VFIO groups) the node's one
// google.com/tpu holder holds. See internal/tpushadow.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow"
	"github.com/edwinhr716/guest-kubelet/internal/tpushadow"
)

func main() {
	var cfg tpushadow.Config
	var podResources, resource, mounts string
	var poll time.Duration
	flag.StringVar(&cfg.PluginDir, "plugin-dir", "/var/lib/kubelet/device-plugins", "kubelet device-plugin directory (hostPath mount)")
	flag.StringVar(&cfg.DevRoot, "dev-root", "/host/dev", "the host's /dev as mounted in this container")
	flag.StringVar(&cfg.HostDevRoot, "host-dev-root", "/dev", "the host's /dev as the kubelet sees it")
	flag.StringVar(&mounts, "mounts", "/var/run/tpu-plugin,/tmp/tpu_logs",
		"comma-separated host directories bind-mounted at the same path into the guest")
	flag.StringVar(&cfg.Generation, "generation", "v6e", "TPU generation prefix for TPU_ACCELERATOR_TYPE")
	flag.IntVar(&cfg.MetricsBase, "metrics-base-port", 8431, "first TPU runtime metrics port")
	flag.StringVar(&podResources, "pod-resources-socket", "/var/lib/kubelet/pod-resources/kubelet.sock",
		"kubelet pod-resources socket (hostPath mount)")
	flag.StringVar(&resource, "tpu-resource", "google.com/tpu", "the normal TPU resource the donor holds")
	flag.DurationVar(&poll, "poll", tpushadow.DefaultPollInterval, "how often pod-resources is re-read")
	flag.Parse()
	cfg.Mounts = strings.FieldsFunc(mounts, func(r rune) bool { return r == ',' || r == ' ' })
	cfg.Log = slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, &cfg, podResources, resource, poll)
	cancel()
	if err != nil {
		cfg.Log.Error("tpu-shadow-plugin exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *tpushadow.Config, podResources, resource string, poll time.Duration) error {
	chips, err := tpushadow.Discover(cfg.DevRoot)
	if err != nil {
		return fmt.Errorf("TPU discovery: %w", err)
	}
	for _, c := range chips {
		cfg.Log.Info("tpu chip found", "vfio-group", c.Group)
	}
	lister, closeLister, err := gpushadow.DialPodResources(podResources)
	if err != nil {
		return fmt.Errorf("pod-resources: %w", err)
	}
	defer func() {
		if err := closeLister(); err != nil {
			cfg.Log.Warn("pod-resources close", "err", err)
		}
	}()
	return tpushadow.Run(ctx, chips, lister, resource, poll, cfg)
}
