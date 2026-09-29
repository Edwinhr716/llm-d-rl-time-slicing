// Command guest-kubelet registers a virtual Node and acts as its kubelet.
//
// M1: every guest bound to the virtual Node runs as a mirror pod on the real host (the node
// this process runs on), and the mirror's real status is copied back to the guest.
//
// M2: the guest kubelet runs each guest container's readinessProbe and startupProbe itself (the
// mirror has none) and reports the guest's Ready from the results and the readinessGates.
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/keeper"
	"github.com/edwinhr716/guest-kubelet/internal/probe"
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

	// VK-A7: admission and mirror memory
	gpuAllowlist       string
	gpuMemory          string
	mirrorMemoryFactor float64

	// VK-A7: outage guard (a separate process of the same binary)
	nodeKeeper        bool
	keeperStaleAfter  time.Duration
	keeperOutageGrace time.Duration
	keeperInterval    time.Duration

	// M2: readiness
	readinessProbes bool
	debugAddr       string
}

// edgeLogSize is how many Ready edges per guest the debug endpoint keeps.
const edgeLogSize = 256

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

	flag.StringVar(&o.gpuAllowlist, "gpu-allowlist", "nvidia-l4",
		"comma-separated GPU models guests may use; a GPU guest is rejected unless the host Node's model label "+
			"(cloud.google.com/gke-accelerator or nvidia.com/gpu.product) is listed")
	flag.StringVar(&o.gpuMemory, "gpu-memory", "23034Mi",
		"device memory of one host GPU (L4: 23034Mi); the device reserve is this times --mirror-memory-factor")
	flag.Float64Var(&o.mirrorMemoryFactor, "mirror-memory-factor", 1.1,
		"a GPU mirror container's memory limit = its limit + ceil(--gpu-memory x factor), "+
			"so Suspend can hold the device memory in the cgroup")

	flag.BoolVar(&o.nodeKeeper, "node-keeper", false,
		"run as the outage guard instead of the kubelet: keep the virtual Node's Lease fresh while every guest-kubelet replica is down")
	flag.DurationVar(&o.keeperStaleAfter, "keeper-stale-after", 15*time.Second,
		"node keeper: renew the Lease once the guest kubelet has not renewed it for this long")
	flag.DurationVar(&o.keeperOutageGrace, "keeper-outage-grace", 15*time.Minute,
		"node keeper: stop renewing this long after the guest kubelet was last seen, so a dead guest kubelet still ends in NotReady")
	flag.DurationVar(&o.keeperInterval, "keeper-interval", 5*time.Second, "node keeper: time between checks")

	flag.BoolVar(&o.readinessProbes, "readiness-probes", true,
		"run the guests' readiness and startup probes (httpGet, tcpSocket, exec, grpc) and report Ready from them; "+
			"false copies the mirror's ready flags (M1)")
	// Off by default. The endpoint can force a guest Ready, so only loopback addresses are accepted.
	flag.StringVar(&o.debugAddr, "debug-addr", "",
		"loopback host:port for the M2 test hooks (/debug/readiness, /debug/ready-edges); empty disables them")
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
	if o.hostNode == "" || (o.hostIP == "" && !o.nodeKeeper) {
		return fmt.Errorf("--host-ip and --host-node (env HOST_IP, NODE_NAME) are required")
	}
	if err := o.checkDebug(); err != nil {
		return err
	}
	if o.nodeName == "" {
		o.nodeName = "vk-" + o.hostNode[strings.LastIndex(o.hostNode, "-")+1:]
	}
	client, err := nodeutil.ClientsetFromEnv(o.kubeconfig)
	if err != nil {
		return err
	}
	if o.nodeKeeper {
		return runNodeKeeper(ctx, client, o.keeperConfig())
	}
	if !o.leaderElect {
		return runKubelet(ctx, client, o)
	}
	return runWithLeaderElection(ctx, client, o)
}

// checkDebug validates --debug-addr: loopback only, and only with the prober it drives.
func (o *options) checkDebug() error {
	if o.debugAddr == "" {
		return nil
	}
	if err := probe.CheckLoopback(o.debugAddr); err != nil {
		return err
	}
	if !o.readinessProbes {
		return errors.New("--debug-addr needs --readiness-probes")
	}
	return nil
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
	gpuMem, err := resource.ParseQuantity(o.gpuMemory)
	if err != nil {
		return fmt.Errorf("--gpu-memory: %w", err)
	}
	if mopts.DeviceMemoryReserve, err = mirror.DeviceReserve(gpuMem, o.mirrorMemoryFactor); err != nil {
		return fmt.Errorf("--mirror-memory-factor: %w", err)
	}
	policy := provider.AdmissionPolicy{
		GPUAllowlist: provider.ParseGPUAllowlist(o.gpuAllowlist),
		HostGPUModel: provider.HostGPUModel(host),
	}
	log.G(ctx).WithField("hostGPUModel", policy.HostGPUModel).WithField("gpuAllowlist", policy.GPUAllowlist).
		WithField("deviceMemoryReserve", mopts.DeviceMemoryReserve.String()).Info("admission and mirror memory settings")

	nodeSpec := provider.NewNodeSpec(cfg)
	if err := ensureProviderID(ctx, client, o.nodeName, cfg.ProviderID); err != nil {
		return err
	}

	// Our own event broadcaster, so the recorder can be wrapped: the library would otherwise
	// record ProviderCreateSuccess on DaemonSet pods that CreatePod ignored, and on guests that
	// admission rejected.
	eb := record.NewBroadcaster()
	eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: client.CoreV1().Events(corev1.NamespaceAll)})
	defer eb.Shutdown()
	rejected := provider.NewRejectedSet()
	recorder := provider.GuestOnlyRecorder{
		Rejected:      rejected,
		EventRecorder: eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: path.Join(o.nodeName, "pod-controller")}),
	}

	// The prober reports verdict changes to the backend, which re-translates the guest's status.
	var backend *mirror.Backend
	var prober *probe.Manager
	if o.readinessProbes {
		// Exec probes run inside the mirror's container through the API server (pods/exec).
		restCfg, err := restConfig(o.kubeconfig)
		if err != nil {
			return fmt.Errorf("rest config for exec probes: %w", err)
		}
		prober = probe.NewManager(ctx, probe.Options{
			OnChange: func(namespace, name string) {
				if backend != nil {
					backend.Refresh(namespace, name)
				}
			},
			Recorder: recorder,
			Prober:   probe.AllProber{Exec: probe.PodExecer{Config: restCfg, Client: client}},
		})
		mopts.Prober = prober
	}
	var edges *probe.EdgeLog
	if o.debugAddr != "" {
		edges = probe.NewEdgeLog(edgeLogSize)
	}
	n, err := nodeutil.NewNode(o.nodeName,
		func(pc nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			// pc.Pods lists the pods bound to the virtual node (the library's informer).
			backend = mirror.New(client, pc.Pods, mopts)
			prov := provider.New(backend).WithAdmission(&provider.Admission{Policy: policy, Recorder: recorder, Rejected: rejected})
			if edges != nil {
				prov.SetNotifyHook(func(pod *corev1.Pod) { edges.Observe(pod) })
			}
			return prov, provider.NodeProvider{}, nil
		},
		// The library writes the guests' status; this client keeps the readiness-gate conditions
		// their owner wrote.
		nodeutil.WithClient(mirror.GateKeepingClient(client)),
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
	if err := backend.WatchReadinessGates(ctx); err != nil {
		return err
	}
	if edges != nil {
		go probe.Serve(ctx, o.debugAddr, probe.DebugHandler(prober, edges))
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

// keeperConfig is the node keeper's part of the flags.
func (o *options) keeperConfig() keeper.Config {
	return keeper.Config{
		HostNode:    o.hostNode,
		VirtualNode: o.nodeName,
		StaleAfter:  o.keeperStaleAfter,
		OutageGrace: o.keeperOutageGrace,
		Interval:    o.keeperInterval,
	}
}

// runNodeKeeper is the outage guard (--node-keeper): a separate Deployment of this binary,
// pinned to the host, that keeps the virtual Node's Lease fresh while every guest-kubelet
// replica is down, for at most --keeper-outage-grace. See internal/keeper.
func runNodeKeeper(ctx context.Context, client kubernetes.Interface, cfg keeper.Config) error {
	log.G(ctx).WithField("node", cfg.VirtualNode).WithField("host", cfg.HostNode).
		WithField("staleAfter", cfg.StaleAfter.String()).WithField("outageGrace", cfg.OutageGrace.String()).
		Info("node keeper started")
	return keeper.New(client, cfg).Run(ctx, func(msg string, kv ...any) {
		entry := log.G(ctx).WithField("node", cfg.VirtualNode)
		for i := 0; i+1 < len(kv); i += 2 {
			entry = entry.WithField(fmt.Sprint(kv[i]), kv[i+1])
		}
		entry.Info(msg)
	})
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
