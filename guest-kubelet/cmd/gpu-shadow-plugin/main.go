// Command gpu-shadow-plugin is the GPU shadow device plugin for --gpu-mode=deviceplugin
// (D-NS-10 option ns-deviceplugin). See internal/gpushadow for what it does.
//
// It runs as root (to use the kubelet's device-plugin and pod-resources sockets) but not
// privileged, with every capability dropped and read-only hostPath mounts except the
// device-plugin directory. It never talks to the API server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow"
	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

type options struct {
	cfg          gpushadow.Config
	procDriver   string
	podResources string
	listen       string
	resource     string
	tools        string
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
	flag.StringVar(&opts.listen, "listen", api.DefaultHoldersAddr,
		"address of the holders endpoint (host network; keep it on loopback)")
	flag.StringVar(&opts.resource, "gpu-resource", "nvidia.com/gpu", "the normal GPU resource whose holders are reported")
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
		logger.Info("gpu found", "device", gpus[i].Device, "uuid", gpus[i].UUID, "resource", gpus[i].Resource)
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

	mux := http.NewServeMux()
	mux.Handle(api.HoldersPath, &gpushadow.HoldersHandler{Lister: lister, GPUs: gpus, Resource: opts.resource, Log: logger})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte("ok")); err != nil {
			logger.Warn("healthz write", "err", err)
		}
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", opts.listen)
	if err != nil {
		return fmt.Errorf("holders endpoint: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("holders endpoint failed", "err", err)
			cancel()
		}
	}()
	defer func() {
		if err := srv.Close(); err != nil {
			logger.Warn("holders endpoint close", "err", err)
		}
	}()

	return gpushadow.Run(ctx, gpus, &opts.cfg)
}
