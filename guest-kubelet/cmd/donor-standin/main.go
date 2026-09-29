// Command donor-standin stands in for the donor controller on host death: when the real Node
// is gone, it releases the virtual Node that ran on it (deletes it and removes the VK
// finalizer). It must run on a different node than the host it watches.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"

	"github.com/edwinhr716/guest-kubelet/internal/donorstandin"
)

func main() {
	var cfg donorstandin.Config
	var kubeconfig string
	flag.StringVar(&cfg.VirtualNode, "vk-node", "", "the virtual Node to release when the host is gone")
	flag.StringVar(&cfg.HostNode, "host-node", "", "the real Node the virtual Node runs on")
	flag.DurationVar(&cfg.Interval, "interval", 5*time.Second, "how often the host Node is checked")
	flag.StringVar(&kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	flag.Parse()

	log.L = vkslog.FromSlog(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, cfg, kubeconfig)
	cancel()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.G(ctx).WithError(err).Error("donor-standin exited")
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg donorstandin.Config, kubeconfig string) error {
	client, err := nodeutil.ClientsetFromEnv(kubeconfig)
	if err != nil {
		return err
	}
	s, err := donorstandin.New(client, cfg)
	if err != nil {
		return err
	}
	return s.Run(ctx)
}
