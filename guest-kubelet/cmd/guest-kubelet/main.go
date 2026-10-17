// Command guest-kubelet registers a virtual Node and acts as its kubelet.
//
// M1: every guest bound to the virtual Node runs as a mirror pod on the real host (the node
// this process runs on), and the mirror's real status is copied back to the guest.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

type options struct {
	nodeName, hostNode, hostIP, kubeletVersion string
	kubeletPort                                int
	cpu, memory, pods                          string
	gpus                                       int64
	kubeconfig                                 string
	workers                                    int

	// M1: mirror backend
	cpuHeadroom, memHeadroom string
	gpuClaim                 string
	reserveClaim             bool
	mirrorOwnerRef           bool
	orphanGrace              time.Duration

	// M1: surviving an outage
	providerIDFromHost bool
	leaderElect        bool
	leaseNamespace     string
	podName            string
}

func main() {
	var o options
	flag.StringVar(&o.hostNode, "host-node", os.Getenv("NODE_NAME"), "real node the guests run on (env NODE_NAME, downward API spec.nodeName)")
	flag.StringVar(&o.nodeName, "node-name", "", "name of the virtual Node; default vk-<last part of --host-node>")
	flag.StringVar(&o.hostIP, "host-ip", os.Getenv("HOST_IP"), "real host IP, advertised as the Node's InternalIP (env HOST_IP)")
	flag.IntVar(&o.kubeletPort, "kubelet-port", 10260, "port advertised in status.daemonEndpoints (the real kubelet holds 10250)")
	flag.StringVar(&o.kubeletVersion, "kubelet-version", "v1.35.8-guest-kubelet-m1", "reported in status.nodeInfo.kubeletVersion")
	flag.StringVar(&o.cpu, "cpu", "8", "advertised cpu capacity")
	flag.StringVar(&o.memory, "memory", "32Gi", "advertised memory capacity")
	flag.StringVar(&o.pods, "pods", "20", "advertised pod capacity")
	flag.Int64Var(&o.gpus, "gpus", 1, "advertised nvidia.com/gpu capacity")
	flag.StringVar(&o.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	flag.IntVar(&o.workers, "workers", 4, "pod sync workers")

	flag.StringVar(&o.cpuHeadroom, "mirror-cpu-headroom", "1", "cap on each mirror container's cpu request (0 = no cap)")
	flag.StringVar(&o.memHeadroom, "mirror-memory-headroom", "4Gi", "cap on each mirror container's memory request (0 = no cap)")
	flag.StringVar(&o.gpuClaim, "gpu-claim", "", "ResourceClaim (in the guest's namespace) that replaces nvidia.com/gpu on the mirror")
	// Off by default: measured in M1, kube-controller-manager's resourceclaim controller adds a
	// pod that already has spec.nodeName to the claim's reservedFor about 1 s after creation.
	flag.BoolVar(&o.reserveClaim, "reserve-claim", false, "add GPU mirrors to the claim's status.reservedFor (kube-controller-manager also does it)")
	flag.BoolVar(&o.mirrorOwnerRef, "mirror-owner-ref", true, "make the guest the mirror's owner (false: mirrors survive guest force-deletion and can be re-adopted)")
	flag.DurationVar(&o.orphanGrace, "orphan-grace", 10*time.Minute, "how long a mirror without a guest is kept for re-adoption")

	// Off by default: GKE's ValidatingAdmissionPolicy validate-node-providerid denies a Node
	// whose providerID does not end in "/<node name>", so on GKE this flag makes the Node
	// create fail. Kept for clusters without that policy.
	flag.BoolVar(&o.providerIDFromHost, "provider-id-from-host", false, "copy the host Node's spec.providerID onto the virtual Node (denied on GKE)")
	flag.BoolVar(&o.leaderElect, "leader-elect", false, "run several replicas; only the Lease holder acts as the kubelet")
	flag.StringVar(&o.leaseNamespace, "leader-elect-namespace", os.Getenv("POD_NAMESPACE"), "namespace of the leader-election Lease (env POD_NAMESPACE)")
	flag.StringVar(&o.podName, "pod-name", os.Getenv("POD_NAME"), "leader-election identity (env POD_NAME)")
	flag.Parse()

	log.L = vkslog.FromSlog(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, o); err != nil && !errors.Is(err, context.Canceled) {
		log.G(ctx).WithError(err).Error("guest-kubelet exited")
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	if o.hostIP == "" || o.hostNode == "" {
		return fmt.Errorf("--host-ip and --host-node (env HOST_IP, NODE_NAME) are required")
	}
	if o.nodeName == "" {
		o.nodeName = "vk-" + o.hostNode[strings.LastIndex(o.hostNode, "-")+1:]
	}
	client, err := nodeutil.ClientsetFromEnv(o.kubeconfig)
	if err != nil {
		return err
	}
	if !o.leaderElect {
		return runKubelet(ctx, client, o)
	}
	return runWithLeaderElection(ctx, client, o)
}

// runWithLeaderElection is LWS's (or any controller-runtime manager's) leader election, but
// the thing being protected is the kubelet role for one Node. Only the Lease holder builds the
// virtual-kubelet Node; a standby takes over within about LeaseDuration of a crash, or at once
// when the leader shuts down cleanly (ReleaseOnCancel).
func runWithLeaderElection(ctx context.Context, client kubernetes.Interface, o options) error {
	if o.leaseNamespace == "" || o.podName == "" {
		return fmt.Errorf("--leader-elect needs POD_NAMESPACE and POD_NAME")
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "guest-kubelet-" + o.nodeName, Namespace: o.leaseNamespace},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: o.podName},
	}
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   8 * time.Second,
		RenewDeadline:   5 * time.Second,
		RetryPeriod:     1 * time.Second,
		Name:            o.nodeName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.G(ctx).WithField("identity", o.podName).Info("became leader; starting the kubelet role")
				err := runKubelet(ctx, client, o)
				if ctx.Err() != nil {
					return // shutting down, or leadership lost; OnStoppedLeading decides
				}
				// The kubelet role ended on its own. The elector would keep renewing the Lease,
				// so exit: the pod restarts and the standby takes over meanwhile.
				log.G(ctx).WithError(err).Error("kubelet role stopped while leading")
				os.Exit(1)
			},
			OnStoppedLeading: func() {
				if ctx.Err() != nil {
					return // clean shutdown; the Lease was released
				}
				log.G(ctx).WithField("identity", o.podName).Error("lost leadership; exiting")
				os.Exit(1)
			},
			OnNewLeader: func(id string) {
				log.G(ctx).WithField("leader", id).Info("leader observed")
			},
		},
	})
	if err != nil {
		return err
	}
	le.Run(ctx) // returns when ctx is cancelled (or leadership is lost, handled above)
	return nil
}

// runKubelet is the M0 wiring plus the mirror backend.
func runKubelet(ctx context.Context, client kubernetes.Interface, o options) error {
	host, err := client.CoreV1().Nodes().Get(ctx, o.hostNode, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get host node %s: %w", o.hostNode, err)
	}
	cfg := provider.NodeConfig{
		Name: o.nodeName, InternalIP: o.hostIP, KubeletPort: int32(o.kubeletPort),
		KubeletVersion: o.kubeletVersion, GPUs: o.gpus,
	}
	if o.providerIDFromHost {
		cfg.ProviderID = host.Spec.ProviderID
	}
	for _, q := range []struct {
		in  string
		dst *resource.Quantity
	}{{o.cpu, &cfg.CPU}, {o.memory, &cfg.Memory}, {o.pods, &cfg.Pods}} {
		if *q.dst, err = resource.ParseQuantity(q.in); err != nil {
			return fmt.Errorf("capacity %q: %w", q.in, err)
		}
	}
	mopts := mirror.Options{
		Config: mirror.Config{
			HostNode: o.hostNode, VirtualNode: o.nodeName, GPUClaim: o.gpuClaim,
			HostTaints: host.Spec.Taints, GuestTaintKey: provider.GuestTaintKey, OwnerRef: o.mirrorOwnerRef,
		},
		ReserveClaim: o.reserveClaim, OrphanGrace: o.orphanGrace,
	}
	if mopts.CPUHeadroom, err = resource.ParseQuantity(o.cpuHeadroom); err != nil {
		return fmt.Errorf("--mirror-cpu-headroom: %w", err)
	}
	if mopts.MemoryHeadroom, err = resource.ParseQuantity(o.memHeadroom); err != nil {
		return fmt.Errorf("--mirror-memory-headroom: %w", err)
	}

	nodeSpec := provider.NewNodeSpec(cfg)
	if err := ensureProviderID(ctx, client, o.nodeName, cfg.ProviderID); err != nil {
		return err
	}

	// Our own event broadcaster, so the recorder can be wrapped: the library would otherwise
	// record ProviderCreateSuccess on DaemonSet pods that CreatePod ignored.
	eb := record.NewBroadcaster()
	eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: client.CoreV1().Events(corev1.NamespaceAll)})
	defer eb.Shutdown()
	recorder := provider.GuestOnlyRecorder{
		EventRecorder: eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: path.Join(o.nodeName, "pod-controller")}),
	}

	var backend *mirror.Backend
	n, err := nodeutil.NewNode(o.nodeName,
		func(pc nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			// pc.Pods lists the pods bound to the virtual node (the library's informer).
			backend = mirror.New(client, pc.Pods, mopts)
			return provider.New(backend), provider.NodeProvider{}, nil
		},
		nodeutil.WithClient(client),
		func(c *nodeutil.NodeConfig) error {
			c.NodeSpec = nodeSpec
			c.NumWorkers = o.workers
			c.EventRecorder = recorder
			// The library resolves env vars before CreatePod. That fails on DaemonSet pods using
			// status.hostIP before our guest filter sees them. The real kubelet resolves env for
			// the mirror, so the guest kubelet never needs it.
			c.SkipDownwardAPIResolution = true
			c.HTTPListenAddr = fmt.Sprintf(":%d", o.kubeletPort) // no TLS config, so no server starts yet
			c.NodeStatusUpdateErrorHandler = reRegisterOnNotFound(client, &nodeSpec)
			return nil
		},
	)
	if err != nil {
		return err
	}
	// Sync the mirror informer before the pod controller starts: after a restart, the first
	// GetPod for each guest must already find its mirror (re-adoption, no duplicate create).
	if err := backend.Start(ctx); err != nil {
		return err
	}
	go func() {
		if err := n.WaitReady(ctx, 0); err == nil {
			log.G(ctx).WithField("node", o.nodeName).WithField("host", o.hostNode).
				WithField("providerID", cfg.ProviderID).Info("node registered and controllers running")
		}
	}()
	return n.Run(ctx) // blocks until ctx is cancelled or a controller fails
}

// ensureProviderID sets spec.providerID on an existing Node that lacks it. The library only
// creates the Node from our spec when it does not exist, and never updates spec afterwards.
// The API allows providerID to change only from empty.
func ensureProviderID(ctx context.Context, client kubernetes.Interface, name, id string) error {
	if id == "" {
		return nil
	}
	cur, err := client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	switch cur.Spec.ProviderID {
	case id:
		return nil
	case "":
		patch := fmt.Sprintf(`{"spec":{"providerID":%q}}`, id)
		_, err = client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		return err
	default:
		log.G(ctx).WithField("have", cur.Spec.ProviderID).WithField("want", id).Warn("Node has a different providerID; leaving it")
		return nil
	}
}

// reRegisterOnNotFound recreates the Node if someone deleted it (for example the cloud
// node lifecycle controller). The loud log line is how we record that it happened.
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
