// Command guest-kubelet registers a virtual Node and acts as its kubelet.
//
// M0: the Node registers, keeps its Lease, and pods scheduled to it are reported
// Running with a fake IP. Nothing actually runs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type options struct {
	nodeName, hostIP, kubeletVersion string
	kubeletPort                      int
	cpu, memory, pods                string
	gpus                             int64
	kubeconfig                       string
	workers                          int
}

func main() {
	var o options
	flag.StringVar(&o.nodeName, "node-name", "vk-abcd", "name of the virtual Node")
	flag.StringVar(&o.hostIP, "host-ip", os.Getenv("HOST_IP"), "real host IP, advertised as the Node's InternalIP (env HOST_IP)")
	flag.IntVar(&o.kubeletPort, "kubelet-port", 10260, "port advertised in status.daemonEndpoints (the real kubelet holds 10250)")
	flag.StringVar(&o.kubeletVersion, "kubelet-version", "v1.35.8-guest-kubelet-m0", "reported in status.nodeInfo.kubeletVersion")
	flag.StringVar(&o.cpu, "cpu", "8", "advertised cpu capacity")
	flag.StringVar(&o.memory, "memory", "32Gi", "advertised memory capacity")
	flag.StringVar(&o.pods, "pods", "20", "advertised pod capacity")
	flag.Int64Var(&o.gpus, "gpus", 1, "advertised nvidia.com/gpu capacity")
	flag.StringVar(&o.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	flag.IntVar(&o.workers, "workers", 4, "pod sync workers")
	flag.Parse()

	log.L = vkslog.FromSlog(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, o); err != nil {
		log.G(ctx).WithError(err).Error("guest-kubelet exited")
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	if o.hostIP == "" {
		return fmt.Errorf("--host-ip (or env HOST_IP) is required")
	}
	cfg := provider.NodeConfig{
		Name: o.nodeName, InternalIP: o.hostIP, KubeletPort: int32(o.kubeletPort),
		KubeletVersion: o.kubeletVersion, GPUs: o.gpus,
	}
	var err error
	if cfg.CPU, err = resource.ParseQuantity(o.cpu); err != nil {
		return fmt.Errorf("--cpu: %w", err)
	}
	if cfg.Memory, err = resource.ParseQuantity(o.memory); err != nil {
		return fmt.Errorf("--memory: %w", err)
	}
	if cfg.Pods, err = resource.ParseQuantity(o.pods); err != nil {
		return fmt.Errorf("--pods: %w", err)
	}

	client, err := nodeutil.ClientsetFromEnv(o.kubeconfig)
	if err != nil {
		return err
	}
	nodeSpec := provider.NewNodeSpec(cfg)

	// NewNode wires everything like ctrl.NewManager + SetupWithManager would: informers
	// (pods filtered to spec.nodeName=<nodeName>), the node controller with a Lease, and the
	// pod controller. It calls our factory once to get the pod and node providers.
	n, err := nodeutil.NewNode(o.nodeName,
		func(nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			return provider.New(o.hostIP), provider.NodeProvider{}, nil
		},
		nodeutil.WithClient(client),
		func(c *nodeutil.NodeConfig) error {
			c.NodeSpec = nodeSpec
			c.NumWorkers = o.workers
			// The library resolves env vars (downward API, ConfigMaps) before CreatePod. That fails
			// on DaemonSet pods using status.hostIP, before our guest filter ever sees them, and
			// requeues them forever. We run nothing in M0 (and the real kubelet resolves env for the
			// mirror pod in M1), so skip it.
			c.SkipDownwardAPIResolution = true
			c.HTTPListenAddr = fmt.Sprintf(":%d", o.kubeletPort) // no TLS config, so no server starts in M0
			c.NodeStatusUpdateErrorHandler = reRegisterOnNotFound(client, &nodeSpec)
			return nil
		},
	)
	if err != nil {
		return err
	}

	go func() {
		if err := n.WaitReady(ctx, 0); err == nil {
			log.G(ctx).WithField("node", o.nodeName).Info("node registered and controllers running")
		}
	}()
	return n.Run(ctx) // blocks until ctx is cancelled or a controller fails
}

// reRegisterOnNotFound recreates the Node if someone deleted it (for example the cloud
// node lifecycle controller). The loud log line is how M0 records that it happened.
func reRegisterOnNotFound(client kubernetes.Interface, spec *corev1.Node) node.ErrorHandler {
	return func(ctx context.Context, err error) error {
		if !apierrors.IsNotFound(err) {
			return err
		}
		log.G(ctx).WithField("node", spec.Name).Warn("Node object was deleted by someone else; re-registering")
		fresh := spec.DeepCopy()
		fresh.ResourceVersion = ""
		_, err = client.CoreV1().Nodes().Create(ctx, fresh, metav1.CreateOptions{})
		return err
	}
}
