// Command donor-standin is a stand-in for the donor controller's host-death duty (D-VK-2
// option d): when --host-node is gone for good, it deletes --vk-node. See internal/donor.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"

	"github.com/edwinhr716/guest-kubelet/internal/donor"
)

func main() {
	os.Exit(run())
}

func run() int {
	var standin donor.Standin
	var kubeconfig string
	flag.StringVar(&standin.VKNode, "vk-node", "", "virtual Node to delete when the host is gone (required)")
	flag.StringVar(&standin.HostNode, "host-node", "", "real Node the virtual Node lives on (required)")
	flag.DurationVar(&standin.Poll, "poll", 2*time.Second, "time between polls of the host Node")
	flag.IntVar(&standin.Confirm, "confirm", 3, "polls in a row that must find the host Node absent")
	flag.StringVar(&kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	flag.Parse()

	standin.Log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	if standin.VKNode == "" || standin.HostNode == "" || standin.Poll <= 0 || standin.Confirm < 1 {
		standin.Log.Error("need --vk-node, --host-node, --poll > 0 and --confirm >= 1")
		return 2
	}
	client, err := nodeutil.ClientsetFromEnv(kubeconfig)
	if err != nil {
		standin.Log.Error("kube client", "err", err)
		return 1
	}
	standin.Client = client

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	standin.Run(ctx)
	return 0
}
