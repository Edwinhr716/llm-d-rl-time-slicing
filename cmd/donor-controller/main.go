// Command donor-controller labels a real node when a donor pod lands on it and removes the labels
// after a TTL with no donor pods on the node and no lock activity in the group. It touches only
// Node objects and reads GetGroupStatus from the orchestrator.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
)

// argsEnv holds extra flags as one space-separated string (the manifest's __ARGS__ hook). They
// are parsed before the command-line flags, so a command-line flag wins.
const argsEnv = "DONOR_CONTROLLER_ARGS"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("donor-controller exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := donorcontroller.Config{}
	var orchAddr, namespaces, kubeconfig string
	fs := flag.NewFlagSet("donor-controller", flag.ContinueOnError)
	fs.StringVar(&cfg.LabelKeys, "label-keys", donorcontroller.LabelKeysPrefix,
		"node label shape: prefix (group.timeslice.io/<group>=true) or ns (timeslice.io/donor=true + timeslice.io/group=<group>)")
	fs.DurationVar(&cfg.EraTTL, "era-ttl", donorcontroller.DefaultEraTTL,
		"remove a node's labels after this long with no donor pods on it and no lock activity in its group")
	fs.StringVar(&cfg.DonorSelector, "donor-selector", donorcontroller.DefaultDonorSelector,
		"label selector for donor pods; the group comes from their timeslice.io/group label")
	fs.StringVar(&orchAddr, "orchestrator-addr", "",
		"orchestrator gRPC host:port for GetGroupStatus; empty means donor pods alone drive the era")
	fs.StringVar(&namespaces, "watch-namespaces", "", "comma-separated namespaces to watch for donor pods; empty means all")
	fs.StringVar(&cfg.GroupFilter, "group-filter", donorcontroller.DefaultGroupFilter,
		"regular expression on the group value; other groups are ignored")
	fs.DurationVar(&cfg.VKDeregisterGrace, "vk-deregister-grace", donorcontroller.DefaultVKDeregisterGrace,
		"at era end, wait at most this long for the host's virtual Node to go before removing the labels")
	fs.BoolVar(&cfg.ReleaseDeadHosts, "release-dead-hosts", false,
		"release (delete and unfinalize) a virtual Node whose host Node is gone or was recreated")
	fs.BoolVar(&cfg.ClearStaleFences, "clear-stale-fences", false,
		"remove a host's timeslice.io/gpu-fence taint once the virtual node it names is gone for --vk-deregister-grace "+
			"and no mirror pod is left (needs nodes update)")
	fs.DurationVar(&cfg.LockPollInterval, "lock-poll-interval", donorcontroller.DefaultLockPollInterval,
		"how often GetGroupStatus is polled per labelled group")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty means in-cluster, then the default loading rules")

	args := donorcontroller.ArgsFromEnv(os.Getenv(argsEnv))
	args = append(args, os.Args[1:]...)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	cfg.WatchNamespaces = donorcontroller.SplitNamespaces(namespaces)

	restCfg, err := buildKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	var locks donorcontroller.LockSource
	if orchAddr != "" && !donorcontroller.IsPlaceholder(orchAddr) {
		src, err := donorcontroller.NewGRPCLockSource(orchAddr)
		if err != nil {
			return err
		}
		defer func() {
			if err := src.Close(); err != nil {
				slog.Warn("closing the orchestrator connection failed", "err", err)
			}
		}()
		locks = src
	} else {
		slog.Warn("no --orchestrator-addr: lock activity is not read; donor pods alone drive the era")
	}

	ctl, err := donorcontroller.New(cs, locks, nil, &cfg)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return ctl.Run(ctx)
}

func buildKubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}
