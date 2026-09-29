package main

import (
	"errors"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// probePolicy checks --guest-probe-policy (lead decision D-VK-5). Option c needs the prober:
// without it, the probes c accepts would all be ignored.
func (o *options) probePolicy() (provider.ProbePolicy, error) {
	pol, err := provider.ParseProbePolicy(o.guestProbePolicy)
	if err != nil {
		return "", fmt.Errorf("--guest-probe-policy: %w", err)
	}
	if pol == provider.ProbePolicyC && !o.readinessProbes {
		return "", errors.New("--guest-probe-policy=c needs --readiness-probes")
	}
	return pol, nil
}

// checkProbePolicy checks --guest-probe-policy at startup, before any client is built.
func (o *options) checkProbePolicy() error {
	_, err := o.probePolicy()
	return err
}

// proberOptionsC is the prober for option c: every handler (exec through pods/exec on the
// mirror, grpc health), and readiness gated on the startupProbe.
func proberOptionsC(opts *probe.Options, kubeconfig string, client kubernetes.Interface) error {
	cfg, err := restConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("rest config for exec probes: %w", err)
	}
	opts.Prober = probe.AllProber{Exec: probe.PodExecer{Config: cfg, Client: client}}
	opts.Supports = probe.SupportedAll
	opts.StartupGate = true
	return nil
}

// restConfig loads the client config the same way nodeutil.ClientsetFromEnv does.
func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		if _, err := os.Stat(kubeconfig); err == nil {
			return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}, &clientcmd.ConfigOverrides{},
			).ClientConfig()
		}
	}
	return rest.InClusterConfig()
}
