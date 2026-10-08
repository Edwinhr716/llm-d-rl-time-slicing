// Command gpu-shadow-plugin is the GPU shadow device plugin for the guest kubelet's
// --gpu-mode=pooled (D-NS-10). It advertises timeslice.io/gpu-shadow, whose devices are the GPUs
// the node's one nvidia.com/gpu holder (the donor) holds. See internal/gpushadow.
//
// It runs as root (to use the kubelet's device-plugin and pod-resources sockets) but not
// privileged, with every capability dropped and read-only hostPath mounts except the
// device-plugin directory. It never talks to the API server.
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
)

type options struct {
	cfg          gpushadow.Config
	procDriver   string
	podResources string
	resource     string
	tools        string
	mode         string
	poll         time.Duration
	sysRoot      string
}

func main() {
	var opts options
	flag.StringVar(&opts.cfg.PluginDir, "plugin-dir", "/var/lib/kubelet/device-plugins",
		"kubelet device-plugin directory (hostPath mount)")
	flag.StringVar(&opts.cfg.DevRoot, "dev-root", "/host/dev", "the host's /dev as mounted in this container")
	flag.StringVar(&opts.cfg.HostDevRoot, "host-dev-root", "/dev", "the host's /dev as the kubelet sees it")
	flag.StringVar(&opts.procDriver, "proc-driver-root", "/host/proc/driver/nvidia",
		"the host's /proc/driver/nvidia as mounted in this container")
	flag.StringVar(&opts.cfg.HostDriverRoot, "host-driver-root", "/home/kubernetes/bin/nvidia",
		"host directory with the NVIDIA driver libraries and tools, mounted read-only into the mirror (empty: no mount)")
	flag.StringVar(&opts.cfg.ContainerDriverRoot, "container-driver-root", "/usr/local/nvidia",
		"where --host-driver-root is mounted in the mirror")
	flag.StringVar(&opts.cfg.LibraryPath, "library-path", "/usr/local/nvidia/lib64",
		"LD_LIBRARY_PATH set in the mirror so it loads the host driver libraries (empty: not set)")
	flag.StringVar(&opts.tools, "tools", "nvidia-smi",
		"comma-separated binaries in --host-driver-root/bin also mounted into the mirror's /usr/bin")
	flag.StringVar(&opts.podResources, "pod-resources-socket", "/var/lib/kubelet/pod-resources/kubelet.sock",
		"kubelet pod-resources socket (hostPath mount)")
	flag.StringVar(&opts.resource, "gpu-resource", "nvidia.com/gpu", "the normal GPU resource the donor holds")
	flag.StringVar(&opts.mode, "mode", "pooled",
		"pooled (the only mode): one resource timeslice.io/gpu-shadow whose devices are the GPUs the node's one nvidia.com/gpu holder holds")
	flag.DurationVar(&opts.poll, "poll", gpushadow.DefaultPollInterval, "how often pod-resources is re-read")
	flag.StringVar(&opts.sysRoot, "sys-root", "/host/sys", "the host's /sys as mounted in this container (GPU NUMA nodes; empty: none)")
	flag.Parse()
	opts.cfg.Tools = strings.FieldsFunc(opts.tools, func(r rune) bool { return r == ',' || r == ' ' })

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	opts.cfg.Log = logger
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, &opts)
	cancel()
	if err != nil {
		logger.Error("gpu-shadow-plugin exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts *options) error {
	logger := opts.cfg.Log
	gpus, err := gpushadow.Discover(opts.cfg.DevRoot, opts.procDriver)
	if err != nil {
		return fmt.Errorf("GPU discovery: %w", err)
	}
	for i := range gpus {
		logger.Info("gpu found", "device", gpus[i].Device, "uuid", gpus[i].UUID)
	}

	if opts.mode != "pooled" {
		return fmt.Errorf("--mode: pooled is the only mode, got %q", opts.mode)
	}
	gpushadow.AddNUMA(gpus, opts.sysRoot)
	for i := range gpus {
		logger.Info("gpu topology", "device", gpus[i].Device, "pci", gpus[i].PCI, "numa", gpus[i].NUMA)
	}

	lister, closeLister, err := gpushadow.DialPodResources(opts.podResources)
	if err != nil {
		return fmt.Errorf("pod-resources: %w", err)
	}
	defer func() {
		if err := closeLister(); err != nil {
			logger.Warn("pod-resources close", "err", err)
		}
	}()
	return gpushadow.RunPooled(ctx, gpus, lister, opts.resource, opts.poll, &opts.cfg)
}
