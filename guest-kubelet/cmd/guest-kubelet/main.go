// Command guest-kubelet registers a virtual Node and acts as its kubelet.
//
// M1: every guest bound to the virtual Node runs as a mirror pod on the real host (the node
// this process runs on), and the mirror's real status is copied back to the guest.
//
// M2: the guest kubelet runs each guest container's readinessProbe itself (the mirror has none)
// and reports the guest's Ready from the results.
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
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
	"github.com/edwinhr716/guest-kubelet/internal/group"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
	"github.com/edwinhr716/guest-kubelet/internal/prepull"
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

	// Client-side rate limit of the VK's API client.
	kubeAPIQPS   float64
	kubeAPIBurst int

	// Guest CPU/RAM budget (PENDING LEAD DECISION D-NS-9)
	guestBudget                      string
	budgetMarginCPU, budgetMarginMem string
	budgetRefresh                    time.Duration

	// M1: mirror backend
	cpuHeadroom, memHeadroom string
	mirrorOwnerRef           bool
	mirrorGroupLabel         string
	orphanGrace              time.Duration

	// D-NS-10: how nvidia.com/gpu reaches the mirror (pooled only)
	gpuMode, gpuDonorSelector string

	logFormat string

	// M1: surviving an outage
	providerIDFromHost bool
	leaderElect        bool
	leaseNamespace     string
	podName            string

	// D-VK-3 option b: where the real node's group is read (internal/group)
	groupSource string
	// D-VK-2 option c: one-shot deregistration.
	deregister  bool
	stopCleanup bool
	// VK-A7 on D-VK-2 c: replace a Node held Terminating by the finalizer when the VK returns.
	reclaimNode bool

	// M2: readiness
	readinessProbes bool
	// Pending lead decision D-NS-2: false = keep (today), true = ns-label.
	guestNodeLabel bool
	nodeLabels     string
	nodeTaints     string

	// VK-A7: admission and mirror memory
	gpuAllowlist       string
	prepullImages      bool
	gpuMemory          string
	mirrorMemoryFactor float64

	// M3/M4: suspend and resume through the snapshot agent (Q6)
	agentAddr          string
	agentPoll          time.Duration
	agentRPCTimeout    time.Duration
	agentRetryInitial  time.Duration
	agentRetryMax      time.Duration
	agentStatusPoll    time.Duration
	agentFaults        bool
	killBudget         time.Duration
	noticeWindow       time.Duration
	checkpointEstimate time.Duration
	restoreEstimate    time.Duration
	notReadyTimeout    time.Duration
	resumeReadyTimeout time.Duration

	// Pending lead decision D-NS-8: true = ns-cordon (NS stack default), false = skip (today).
	cordonWhileHeld bool
	// Cordon while the host has no donor pod, and re-queue the Pending guests stranded there.
	cordonWithoutDonor   bool
	requeueStrandedAfter time.Duration

	// M2 and M3 test hooks
	debugAddr string

	// VK-A6: host command server (D-NS-4 ns-push-vk)
	hostCommandPort  int
	hostCommandAllow string
	freezer          string
	hcAgentAddr      string
	hcAgentPort      int
	vacateMargin     time.Duration
	resumeBudget     time.Duration
	engineStart      time.Duration
	killTimeout      time.Duration
	fakeSuspendDelay time.Duration
	fakeResumeDelay  time.Duration

	// D-VK-5: which probes a guest may carry
	guestProbePolicy string
}

// edgeLogSize is how many Ready edges per guest the debug endpoint keeps.
const edgeLogSize = 256

// cordonPoll is how often the ns-cordon loop re-reads the hold, besides the pokes.
const cordonPoll = 500 * time.Millisecond

// requeuePoll is how often stranded guests are looked for while the host has no donor pod.
const requeuePoll = time.Second

// reclaimTimeout bounds the wait for a Terminating Node to go once its finalizer is removed.
const reclaimTimeout = 30 * time.Second

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
	flag.StringVar(&o.guestBudget, "guest-budget", provider.BudgetStatic,
		"static: advertise --cpu/--memory and cap mirror requests at the headroom flags; "+
			"computed: advertise host allocatable minus resident requests minus the margin, mirrors keep the guest's requests")
	flag.StringVar(&o.budgetMarginCPU, "budget-margin-cpu", "250m", "computed budget: cpu kept free on the host")
	flag.StringVar(&o.budgetMarginMem, "budget-margin-memory", "1Gi", "computed budget: memory kept free on the host")
	flag.DurationVar(&o.budgetRefresh, "budget-refresh", 30*time.Second, "computed budget: recompute interval (0 = once at start)")
	flag.StringVar(&o.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path; empty means in-cluster")
	flag.IntVar(&o.workers, "workers", 4, "pod sync workers")
	flag.Float64Var(&o.kubeAPIQPS, "kube-api-qps", 50,
		"client-side QPS limit of the API client (client-go's default of 5 starves host commands of API calls)")
	flag.IntVar(&o.kubeAPIBurst, "kube-api-burst", 100, "client-side burst of the API client (client-go default 10)")

	flag.StringVar(&o.cpuHeadroom, "mirror-cpu-headroom", "1", "cap on each mirror container's cpu request (0 = no cap)")
	flag.StringVar(&o.memHeadroom, "mirror-memory-headroom", "4Gi", "cap on each mirror container's memory request (0 = no cap)")
	flag.StringVar(&o.mirrorGroupLabel, "mirror-group-label", "plain", "what mirrors carry in timeslice.io/group: plain (the group name, which embeds the owner namespace) or token (an opaque hash; needs an orchestrator that matches tokens)")
	flag.BoolVar(&o.mirrorOwnerRef, "mirror-owner-ref", true, "make the guest the mirror's owner (false: mirrors survive guest force-deletion and can be re-adopted)")
	flag.DurationVar(&o.orphanGrace, "orphan-grace", 10*time.Minute, "how long a mirror without a guest is kept for re-adoption")
	flag.StringVar(&o.gpuMode, "gpu-mode", string(mirror.GPUModePooled),
		"how a guest's nvidia.com/gpu reaches the mirror: pooled (one pooled shadow resource for all the "+
			"donor's GPUs, timeslice.io/gpu-shadow; the real kubelet picks) is the only mode")
	flag.StringVar(&o.gpuDonorSelector, "gpu-donor-selector", mirror.DefaultGPUDonorSelector,
		"label selector of donor pods on the host whose GPUs mirrors share")
	flag.StringVar(&o.logFormat, "log-format", "json", "log format: json or text")

	// Off by default: GKE's ValidatingAdmissionPolicy validate-node-providerid denies a Node
	// whose providerID does not end in "/<node name>", so on GKE this flag makes the Node
	// create fail. Kept for clusters without that policy.
	flag.BoolVar(&o.providerIDFromHost, "provider-id-from-host", false, "copy the host Node's spec.providerID onto the virtual Node (denied on GKE)")
	flag.BoolVar(&o.leaderElect, "leader-elect", false, "run several replicas; only the Lease holder acts as the kubelet")
	flag.StringVar(&o.leaseNamespace, "leader-elect-namespace", os.Getenv("POD_NAMESPACE"), "namespace of the leader-election Lease (env POD_NAMESPACE)")
	flag.StringVar(&o.podName, "pod-name", os.Getenv("POD_NAME"), "leader-election identity (env POD_NAME)")
	flag.StringVar(&o.groupSource, "group-source", group.SourceNS,
		"host node labels that name its group: ns (timeslice.io/donor=true plus timeslice.io/group=<group>) or "+
			"either (also group.timeslice.io/<group>=true; both forms must agree)")
	flag.IntVar(&o.hostCommandPort, "host-command-port", 0, "port on --host-ip where the VK serves the orchestrator's "+
		"Vacate and Resume commands; 0 turns host commands off and mirrors start at once (M1)")
	flag.StringVar(&o.hostCommandAllow, "host-command-allow", "",
		"comma-separated IPs or CIDRs allowed to send host commands; empty allows every caller")
	flag.StringVar(&o.freezer, "freezer", freezerDelete,
		"how guests vacate the accelerator: agent = snapshot-agent SuspendAll/ResumeAll (D-NS-5 ns-host); "+
			"delete = delete the mirror (re-created on the next Resume); fake = test freezer")
	flag.StringVar(&o.hcAgentAddr, "snapshot-agent-addr", "",
		"--freezer=agent: snapshot-agent host:port; empty means --host-ip:--snapshot-agent-port")
	flag.IntVar(&o.hcAgentPort, "snapshot-agent-port", 9001, "--freezer=agent: snapshot-agent port on the real node")
	flag.DurationVar(&o.vacateMargin, "vacate-margin", 250*time.Millisecond, "taken off each Vacate deadline for the ack's way back")
	flag.DurationVar(&o.resumeBudget, "resume-budget", 30*time.Second,
		"bound on each guest Resume; also the least time a host-command Resume run gets")
	flag.DurationVar(&o.engineStart, "engine-start-budget", 10*time.Minute,
		"how long past its deadline a host-command Resume waits for a starting engine (a cold start: image pull, "+
			"model load) before it fails; the wait ends earlier when the mirror ends or a newer command comes")
	flag.DurationVar(&o.killTimeout, "kill-timeout", 30*time.Second, "bound on the kill sequence of one guest")
	flag.DurationVar(&o.fakeSuspendDelay, "fake-freezer-suspend-delay", 12500*time.Millisecond,
		"--freezer=fake: time a Suspend takes")
	flag.DurationVar(&o.fakeResumeDelay, "fake-freezer-resume-delay", 6*time.Second, "--freezer=fake: time a Resume takes")
	flag.BoolVar(&o.stopCleanup, "stop-cleanup", true,
		"on SIGTERM, delete the virtual Node if the VK's controller is gone (uninstall), else cordon it and mark it NotReady until a VK serves it again")
	flag.BoolVar(&o.deregister, "deregister", false,
		"one-shot: delete the virtual Node, remove its finalizer and exit (stop the serving VK first)")
	flag.BoolVar(&o.reclaimNode, "reclaim-terminating-node", true,
		"on start, replace the virtual Node if a delete (for example GKE during a VK outage) holds it Terminating: "+
			"remove our finalizer and register it again; guests stay bound by name. false leaves it Terminating")
	flag.BoolVar(&o.readinessProbes, "readiness-probes", true,
		"run the guests' readinessProbes (httpGet, tcpSocket) and report Ready from them; false copies the mirror's ready flags (M1)")
	flag.StringVar(&o.agentAddr, "agent-addr", "",
		"host:port of the node's snapshot agent (the deployment passes $(HOST_IP):9101); empty disables suspend/resume. "+
			"The guest kubelet never touches cgroups: every suspend, resume and kill is an agent call")
	flag.DurationVar(&o.agentPoll, "agent-poll", 100*time.Millisecond, "GetOperation poll interval (Q13 default)")
	flag.DurationVar(&o.agentRPCTimeout, "agent-rpc-timeout", 5*time.Second, "timeout of each agent RPC (Q13 default)")
	flag.DurationVar(&o.agentRetryInitial, "agent-retry-initial", time.Second,
		"first delay before sending a lost agent call again (Q13 default)")
	flag.DurationVar(&o.agentRetryMax, "agent-retry-max", 30*time.Second, "cap of the doubling retry delay (Q13 default)")
	flag.DurationVar(&o.agentStatusPoll, "agent-status-poll", 2*time.Second,
		"how often the agent's Status is read for the hold (D-NS-8) and to repair a mirror the agent reports SUSPENDED; 0 disables")
	flag.BoolVar(&o.agentFaults, "agent-fault-injection", false,
		"test hook: arm agent RPC faults (hang, crash, drop-ack, pending, unimplemented, refuse) through /debug/fault on --debug-addr")
	flag.DurationVar(&o.killBudget, "kill-budget", 3*time.Second,
		"K: the agent's deadline for a Kill; a suspend without a deadline gets now + N - K")
	flag.DurationVar(&o.noticeWindow, "notice-window", 30*time.Second, "N: the notice window")
	flag.DurationVar(&o.checkpointEstimate, "checkpoint-estimate", 13*time.Second,
		"one guest's suspend time, for admission (one restore + the sum of checkpoints must fit N - K); 0 disables admission")
	flag.DurationVar(&o.restoreEstimate, "restore-estimate", 6500*time.Millisecond,
		"one guest's resume time, for admission; 0 disables admission")
	flag.DurationVar(&o.notReadyTimeout, "suspend-notready-timeout", 5*time.Second,
		"how long a suspend waits for the guest's Ready=False to reach the API before the agent is called")
	flag.DurationVar(&o.resumeReadyTimeout, "resume-ready-timeout", 60*time.Second,
		"how long a resume waits for the guest's readiness probe to pass after the agent's Resume")
	flag.BoolVar(&o.cordonWhileHeld, "cordon-while-held", true,
		"set spec.unschedulable on the virtual Node while the donor holds the GPU (M3: while a guest is suspending "+
			"or suspended), so no new guest lands on it; false never touches spec.unschedulable")
	flag.BoolVar(&o.cordonWithoutDonor, "cordon-without-donor", true,
		"set spec.unschedulable on the virtual Node while the host has no donor pod (--gpu-donor-selector), "+
			"since no GPU can be lent there, and remove it when a donor pod is back; false waits for the era end")
	flag.DurationVar(&o.requeueStrandedAfter, "requeue-stranded-after", provider.DefaultRequeueAfter,
		"with --cordon-without-donor: once the host has had no donor pod this long, delete Pending guests that have a "+
			"controller so it re-creates them on another node (bare pods get a warning event only); 0 never deletes")
	// Off by default. The endpoint can force a guest Ready or freeze it, so only loopback addresses
	// are accepted.
	flag.StringVar(&o.debugAddr, "debug-addr", "",
		"loopback host:port for the test hooks (M2: /debug/readiness, /debug/ready-edges; "+
			"M3/M4: /debug/suspend, /debug/resume, /debug/suspend-all, /debug/resume-all, /debug/agent, /debug/fault); "+
			"empty disables them")
	flag.BoolVar(&o.guestNodeLabel, "guest-node-label", false,
		"also label the virtual Node timeslice.io/guest=true, for guests with a preferred node affinity (virtual-node=true stays)")
	flag.StringVar(&o.nodeLabels, "node-labels", "",
		"extra labels on the virtual Node, k=v,k2=v2 (keys the guest kubelet sets itself are refused); empty adds none")
	flag.StringVar(&o.nodeTaints, "node-taints", "",
		"extra taints on the virtual Node, k=v:Effect,k2:Effect, next to timeslice.io/guest; set at registration, so "+
			"only guests that tolerate them ever bind; empty adds none")

	flag.BoolVar(&o.prepullImages, "prepull-images", true,
		"pull an admitted guest's images onto the host at once (a short-lived <guest>-prepull pod), so the first "+
			"lend does not wait for the pull; false leaves the pull to the mirror")
	flag.StringVar(&o.gpuAllowlist, "gpu-allowlist", provider.DefaultGPUAllowlist,
		"comma-separated GPU models guests may use; a GPU guest is rejected unless the host Node's model label "+
			"(cloud.google.com/gke-accelerator or nvidia.com/gpu.product) is listed")
	flag.StringVar(&o.gpuMemory, "gpu-memory", provider.GPUMemoryAuto,
		"device memory of one host GPU (L4: 23034Mi, H100: 81559Mi), or auto (from the host's GPU model label, "+
			"fallback "+provider.FallbackGPUMemory+"); the device reserve is this times --mirror-memory-factor")
	flag.Float64Var(&o.mirrorMemoryFactor, "mirror-memory-factor", 1.1,
		"a GPU mirror container's memory limit = its limit + ceil(--gpu-memory x factor), "+
			"so Suspend can hold the device memory in the cgroup")
	flag.StringVar(&o.guestProbePolicy, "guest-probe-policy", string(provider.ProbePolicyA),
		"which probes a guest may carry (lead decision D-VK-5): a = only an httpGet or tcpSocket readinessProbe; "+
			"b = no probes; c = all (liveness dropped, the guest kubelet runs readiness and startup probes of any kind "+
			"and honours readinessGates)")
	flag.Parse()

	var handler slog.Handler = slog.NewJSONHandler(os.Stderr, nil)
	if o.logFormat == "text" {
		handler = slog.NewTextHandler(os.Stderr, nil)
	}
	log.L = vkslog.FromSlog(slog.New(handler))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err := run(ctx, &o)
	logStop(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.G(ctx).WithError(err).Error("guest-kubelet exited")
		os.Exit(1)
	}
}

func run(ctx context.Context, opts *options) error {
	if opts.deregister {
		return deregister(ctx, opts.nodeName, opts.hostNode, opts.kubeconfig)
	}
	if opts.hostIP == "" || opts.hostNode == "" {
		return fmt.Errorf("--host-ip and --host-node (env HOST_IP, NODE_NAME) are required")
	}
	if !group.ValidSource(opts.groupSource) {
		return fmt.Errorf("--group-source=%q: want one of %s", opts.groupSource, strings.Join(group.Sources, ", "))
	}
	if err := errors.Join(opts.checkDebug(), opts.checkProbePolicy(), opts.checkNodeExtras()); err != nil {
		return err
	}
	if err := checkGPUMode(ctx, opts); err != nil {
		return err
	}
	if opts.nodeName == "" {
		opts.nodeName = defaultNodeName(opts.hostNode)
	}
	client, err := vkClient(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		// Guard rail 4: on a signal, the process that served the Node cleans it up.
		if ctx.Err() != nil && roleRan.Load() && opts.stopCleanup {
			stopCleanup(ctx, client, opts)
		}
	}()
	if !opts.leaderElect {
		return runKubelet(ctx, client, opts)
	}
	return runWithLeaderElection(ctx, client, opts)
}

// checkNodeExtras refuses a bad --node-labels or --node-taints before anything starts.
func (o *options) checkNodeExtras() error {
	_, errL := provider.ParseNodeLabels(o.nodeLabels)
	_, errT := provider.ParseNodeTaints(o.nodeTaints)
	return errors.Join(errL, errT)
}

func defaultNodeName(hostNode string) string {
	return "vk-" + hostNode[strings.LastIndex(hostNode, "-")+1:]
}

// deregister is the VK's own removal of its virtual Node: it deletes the
// Node and removes the finalizer (reason deregister), then exits. Run it after the serving VK
// has stopped, or its re-registration recreates the Node.
func deregister(ctx context.Context, name, hostNode, kubeconfig string) error {
	if name == "" && hostNode != "" {
		name = defaultNodeName(hostNode)
	}
	if name == "" {
		return fmt.Errorf("--deregister needs --node-name or --host-node")
	}
	client, err := nodeutil.ClientsetFromEnv(kubeconfig)
	if err != nil {
		return err
	}
	released, err := provider.ReleaseNode(ctx, client, name, provider.ReasonDeregister)
	if err != nil {
		return err
	}
	log.G(ctx).WithField("node", name).WithField("released", released).Info("deregistered")
	return nil
}

// checkDebug validates --debug-addr: loopback only. Without --readiness-probes it serves only the
// M3 hooks.
func (o *options) checkDebug() error {
	if o.debugAddr == "" {
		return nil
	}
	if err := probe.CheckLoopback(o.debugAddr); err != nil {
		return err
	}
	return nil
}

// pooledCapacityPoll is how often pooled mode re-reads the host's gpu-shadow allocatable.
const pooledCapacityPoll = 3 * time.Second

// checkGPUMode validates --log-format, --gpu-mode and --gpu-donor-selector.
func checkGPUMode(_ context.Context, opts *options) error {
	if opts.logFormat != "json" && opts.logFormat != "text" {
		return fmt.Errorf("--log-format must be json or text, got %q", opts.logFormat)
	}
	if _, err := mirror.ParseGPUMode(opts.gpuMode); err != nil {
		return fmt.Errorf("--gpu-mode: %w", err)
	}
	if opts.gpuDonorSelector == "" {
		return fmt.Errorf("--gpu-donor-selector must be a non-empty label selector")
	}
	if _, err := labels.Parse(opts.gpuDonorSelector); err != nil {
		return fmt.Errorf("--gpu-donor-selector %q: %w", opts.gpuDonorSelector, err)
	}
	return nil
}

// runWithLeaderElection is LWS's (or any controller-runtime manager's) leader election, but
// the thing being protected is the kubelet role for one Node. Only the Lease holder builds the
// virtual-kubelet Node; a standby takes over within about LeaseDuration of a crash, or at once
// when the leader shuts down cleanly (ReleaseOnCancel).
func runWithLeaderElection(ctx context.Context, client kubernetes.Interface, o *options) error {
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

// runKubelet runs the kubelet role: one node run for the life of the process.
func runKubelet(ctx context.Context, client kubernetes.Interface, o *options) error {
	return runNode(ctx, client, o)
}

// runNode is one node run: the M0 wiring plus the mirror backend.
func runNode(ctx context.Context, client kubernetes.Interface, o *options) error {
	host, err := client.CoreV1().Nodes().Get(ctx, o.hostNode, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get host node %s: %w", o.hostNode, err)
	}
	cfg := provider.NodeConfig{
		Name: o.nodeName, InternalIP: o.hostIP, KubeletPort: int32(o.kubeletPort),
		KubeletVersion: o.kubeletVersion, GPUs: o.gpus, GuestNodeLabel: o.guestNodeLabel,
		HostName: host.Name, HostUID: host.UID,
	}
	// Virtual node GPU capacity = the host's gpu-shadow allocatable, kept current below;
	// --gpus is ignored.
	cfg.GPUs = provider.HostAllocatable(host, api.PooledResource)
	if cfg.ExtraLabels, err = provider.ParseNodeLabels(o.nodeLabels); err != nil {
		return err
	}
	if cfg.ExtraTaints, err = provider.ParseNodeTaints(o.nodeTaints); err != nil {
		return err
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
			HostNode: o.hostNode, VirtualNode: o.nodeName,
			HostTaints: host.Spec.Taints, GuestTaintKey: provider.GuestTaintKey, OwnerRef: o.mirrorOwnerRef,
			GroupToken: o.mirrorGroupLabel == "token",
		},
		OrphanGrace:      o.orphanGrace,
		GPUDonorSelector: o.gpuDonorSelector,
		Gated:            o.hostCommandPort > 0,
	}
	if o.mirrorGroupLabel != "plain" && o.mirrorGroupLabel != "token" {
		return fmt.Errorf("--mirror-group-label: want plain or token, got %q", o.mirrorGroupLabel)
	}
	if mopts.CPUHeadroom, err = resource.ParseQuantity(o.cpuHeadroom); err != nil {
		return fmt.Errorf("--mirror-cpu-headroom: %w", err)
	}
	if mopts.MemoryHeadroom, err = resource.ParseQuantity(o.memHeadroom); err != nil {
		return fmt.Errorf("--mirror-memory-headroom: %w", err)
	}
	gpuMemValue, gpuMemFallback := provider.ResolveGPUMemory(o.gpuMemory, provider.HostGPUModel(host))
	if gpuMemFallback {
		log.G(ctx).WithField("hostGPUModel", provider.HostGPUModel(host)).WithField("gpuMemory", gpuMemValue).
			Warn("--gpu-memory=auto: unknown host GPU model, using the fallback; set --gpu-memory")
	}
	gpuMem, err := resource.ParseQuantity(gpuMemValue)
	if err != nil {
		return fmt.Errorf("--gpu-memory: %w", err)
	}
	if mopts.DeviceMemoryReserve, err = mirror.DeviceReserve(gpuMem, o.mirrorMemoryFactor); err != nil {
		return fmt.Errorf("--mirror-memory-factor: %w", err)
	}
	probePolicy, err := o.probePolicy()
	if err != nil {
		return err
	}
	policy := provider.AdmissionPolicy{
		GPUAllowlist: provider.ParseGPUAllowlist(o.gpuAllowlist),
		HostGPUModel: provider.HostGPUModel(host),
		Probes:       probePolicy,
	}
	log.G(ctx).WithField("hostGPUModel", policy.HostGPUModel).WithField("gpuAllowlist", policy.GPUAllowlist).
		WithField("guestProbePolicy", string(probePolicy)).
		WithField("deviceMemoryReserve", mopts.DeviceMemoryReserve.String()).Info("admission and mirror memory settings")
	bo := provider.BudgetOptions{Mode: o.guestBudget, Refresh: o.budgetRefresh}
	if bo.MarginCPU, err = resource.ParseQuantity(o.budgetMarginCPU); err != nil {
		return fmt.Errorf("--budget-margin-cpu: %w", err)
	}
	if bo.MarginMemory, err = resource.ParseQuantity(o.budgetMarginMem); err != nil {
		return fmt.Errorf("--budget-margin-memory: %w", err)
	}
	mopts.RealRequests = bo.Mode == provider.BudgetComputed

	nodeProvider, err := provider.SetupNode(ctx, client, host, &cfg, &bo)
	if err != nil {
		return err
	}
	nodeProvider.OnStart(func(ctx context.Context) {
		provider.FollowHostGPUs(ctx, client, o.hostNode, api.PooledResource, pooledCapacityPoll, nodeProvider)
	})
	nodeSpec := *nodeProvider.Node()
	if err := ensureProviderID(ctx, client, o.nodeName, cfg.ProviderID); err != nil {
		return err
	}
	// D-VK-2 option c: register the Node ourselves with the finalizer and the ownerReference to
	// the host (or add them to an existing Node), so the library finds it and only patches status.
	action, err := provider.EnsureNodeGuard(ctx, client, &nodeSpec)
	if err != nil {
		return err
	}
	roleRan.Store(true)
	if o.stopCleanup {
		go provider.KeepClearingStopped(ctx, client.CoreV1().Nodes(), o.nodeName, selfID, clearStoppedInterval)
	}
	// VK-A7: a Terminating Node cannot be un-deleted. On a live host (we run on it), swap it for
	// a fresh one before the library starts, so the outage leaves no trace on the Node.
	if action == provider.GuardTerminating && o.reclaimNode {
		if _, err := provider.ReclaimNode(ctx, client, &nodeSpec, reclaimTimeout); err != nil {
			return err
		}
		if _, err := provider.EnsureNodeGuard(ctx, client, &nodeSpec); err != nil {
			return err
		}
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
	// One injector for both agent clients (M4 --agent-addr and the host-command freezer), so
	// /debug/fault arms the path the VK really uses.
	var faults *hostcmd.FaultInjector
	if o.agentFaults {
		faults = hostcmd.NewFaultInjector()
		log.G(ctx).Warn("agent fault injection is on (test hook)")
	}
	if o.agentAddr != "" {
		// M4: every suspend, resume and kill is a snapshot-agent call with a deadline (Q6); the
		// guest kubelet never touches cgroups.
		var dialOpts []grpc.DialOption
		if faults != nil {
			dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(faults.Interceptor()))
		}
		ac, err := hostcmd.DialAgent(o.agentAddr, dialOpts...)
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()
		ac.Poll, ac.RPCTimeout = o.agentPoll, o.agentRPCTimeout
		ac.RetryInitial, ac.RetryMax = o.agentRetryInitial, o.agentRetryMax
		mopts.Suspend = mirror.SuspendOptions{
			Agent:              &hostcmd.AgentBackend{Client: ac},
			NotReadyTimeout:    o.notReadyTimeout,
			NoticeWindow:       o.noticeWindow,
			KillBudget:         o.killBudget,
			CheckpointEstimate: o.checkpointEstimate,
			RestoreEstimate:    o.restoreEstimate,
			AgentStatusPoll:    o.agentStatusPoll,
			ReadyCheck:         mirror.ProbeUntilReady(100*time.Millisecond, probeOnce),
			ReadyTimeout:       o.resumeReadyTimeout,
			Recorder:           recorder,
		}
		log.G(ctx).WithField("agent", o.agentAddr).WithField("killBudget", o.killBudget.String()).
			WithField("noticeWindow", o.noticeWindow.String()).Info("suspend and resume through the snapshot agent")
	}
	// D-NS-8 ns-cordon: poked by every suspend-state change and agent Status change, polled every
	// cordonPoll.
	var cordonPoke chan struct{}
	// Both suspend paths record the state on the mirror (M4 agent calls here, host commands in
	// hostcmd), so the cordon follows either, and re-derives from the mirrors after a restart (M5).
	holdWhileHeld := o.cordonWhileHeld && (o.agentAddr != "" || o.hostCommandPort > 0)
	if holdWhileHeld || o.cordonWithoutDonor {
		cordonPoke = make(chan struct{}, 1)
		poke := func() {
			select {
			case cordonPoke <- struct{}{}:
			default: // a poke is already pending
			}
		}
		if holdWhileHeld {
			mopts.Suspend.OnHoldChange = poke
		}
		if o.cordonWithoutDonor {
			mopts.OnDonorChange = poke
		}
	}
	mopts.Recorder = recorder

	// D-VK-3: the host node's labels name the group. Resolved at start and on every label
	// change; while there is none, no mirror is created and the VK Node gets GroupUnresolved.
	vkNodeRef := &corev1.ObjectReference{Kind: "Node", Name: o.nodeName, UID: types.UID(o.nodeName)}
	resolver := group.NewResolver(o.hostNode, o.groupSource, os.Stderr)
	resolver.OnChange = func(res group.Result) {
		if _, ok := res.Group(); !ok {
			recorder.Eventf(vkNodeRef, corev1.EventTypeWarning, group.EventGroupUnresolved,
				"host node %s resolves to no group (%s, --group-source=%s); no mirror will be created", o.hostNode, res.Reason, o.groupSource)
		}
	}
	if err := resolver.Start(ctx, client); err != nil {
		return err
	}
	mopts.Group = resolver.Current
	mopts.OnUnresolved = func(guest *corev1.Pod, reason string) {
		recorder.Eventf(vkNodeRef, corev1.EventTypeWarning, group.EventGroupUnresolved,
			"no mirror for guest %s/%s: host node %s resolves to no group (%s)", guest.Namespace, guest.Name, o.hostNode, reason)
	}
	hc, err := newHostCommandWiring(o, faults)
	if err != nil {
		return err
	}
	defer hc.close(ctx)

	// The prober reports verdict changes to the backend, which re-translates the guest's status.
	var backend *mirror.Backend
	var prober *probe.Manager
	if o.readinessProbes {
		popts := probe.Options{
			OnChange: func(namespace, name string) {
				if backend != nil {
					backend.Refresh(namespace, name)
				}
			},
			Recorder: recorder,
		}
		if probePolicy == provider.ProbePolicyC {
			if err := proberOptionsC(&popts, o.kubeconfig, client); err != nil {
				return err
			}
		}
		prober = probe.NewManager(ctx, popts)
		mopts.Prober = prober
	}
	// D-VK-5 c admits guests with readinessGates: the library's status writes must keep the gate
	// conditions their owner wrote.
	libClient := client
	if probePolicy == provider.ProbePolicyC {
		libClient = mirror.GateKeepingClient(client)
	}
	var edges *probe.EdgeLog
	if o.debugAddr != "" && prober != nil {
		edges = probe.NewEdgeLog(edgeLogSize)
	}
	n, err := nodeutil.NewNode(o.nodeName,
		func(pc nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			// pc.Pods lists the pods bound to the virtual node (the library's informer).
			backend = mirror.New(client, pc.Pods, &mopts)
			owner, err := hc.build(ctx, backend, resolver, client.CoreV1().Nodes(), host.UID)
			if err != nil {
				return nil, nil, err
			}
			prov := provider.New(backend)
			if owner != nil {
				prov = provider.NewWithCreateOwner(backend, owner)
			}
			prov = prov.WithAdmission(&provider.Admission{Policy: policy, Recorder: recorder, Rejected: rejected})
			if o.prepullImages {
				prov = prov.WithPrepull(prepull.New(ctx, client, host.Name))
			}
			if edges != nil {
				prov.SetNotifyHook(func(pod *corev1.Pod) { edges.Observe(pod) })
			}
			return prov, nodeProvider, nil
		},
		nodeutil.WithClient(libClient),
		func(c *nodeutil.NodeConfig) error {
			c.NodeSpec = nodeSpec
			c.NumWorkers = o.workers
			c.EventRecorder = recorder
			// The library resolves env vars before CreatePod. That fails on DaemonSet pods using
			// status.hostIP before our guest filter sees them. The real kubelet resolves env for
			// the mirror, so the guest kubelet never needs it.
			c.SkipDownwardAPIResolution = true
			c.HTTPListenAddr = fmt.Sprintf(":%d", o.kubeletPort) // no TLS config, so no server starts yet
			c.NodeStatusUpdateErrorHandler = reRegisterOnNotFound(client, nodeProvider.Node)
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
	if o.debugAddr != "" {
		go probe.Serve(ctx, o.debugAddr, debugHandler(backend, prober, edges, faults))
	}
	// M5: relist the guests and restore what lived only in memory before anything acts on them.
	if _, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(corev1.NamespaceAll), NodeName: o.nodeName, Host: backend,
		Server: hc.server(), Journal: hc.journal, IsGuest: provider.MatchesGuest,
		Kill: hc.killer(), KillBudget: o.killTimeout,
	}); err != nil {
		return err
	}
	if err := hc.start(ctx); err != nil {
		return err
	}
	if cordonPoke != nil {
		cordoner := provider.NewCordoner(client.CoreV1().Nodes(), o.nodeName,
			eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: path.Join(o.nodeName, "cordon")}))
		var hold provider.HoldFunc
		if holdWhileHeld {
			hold = backend.Hold
		}
		if o.cordonWithoutDonor {
			hold = provider.WithoutDonor(backend.HasDonor, hold)
		}
		log.G(ctx).WithField("node", o.nodeName).WithField("pollMs", cordonPoll.Milliseconds()).
			WithField("whileHeld", holdWhileHeld).WithField("withoutDonor", o.cordonWithoutDonor).Info("cordon: on")
		go cordoner.RunCordon(ctx, cordonPoll, hold, cordonPoke)
	}
	if o.cordonWithoutDonor && o.requeueStrandedAfter > 0 {
		rq := &provider.Requeuer{
			Pods: client.CoreV1(), Bound: backend.BoundPods, IsGuest: provider.MatchesGuest,
			HasDonor: backend.HasDonor, After: o.requeueStrandedAfter, Recorder: recorder,
		}
		go rq.Run(ctx, requeuePoll)
	}
	if probePolicy == provider.ProbePolicyC {
		if err := backend.WatchReadinessGates(ctx); err != nil {
			return err
		}
	}
	go func() {
		if err := n.WaitReady(ctx, 0); err == nil {
			hc.ready() // the guest informer has synced: host commands may act
			log.G(ctx).WithField("node", o.nodeName).WithField("host", o.hostNode).
				WithField("providerID", cfg.ProviderID).Info("node registered and controllers running")
			log.G(ctx).WithField("node", o.nodeName).WithField("labels", nodeSpec.Labels).Info("virtual node labels")
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
func reRegisterOnNotFound(client kubernetes.Interface, current func() *corev1.Node) node.ErrorHandler {
	return func(ctx context.Context, err error) error {
		if !apierrors.IsNotFound(err) {
			return err
		}
		fresh := current() // the template, with the current budget
		log.G(ctx).WithField("node", fresh.Name).Warn("Node object was deleted by someone else; re-registering")
		fresh.ResourceVersion = ""
		if _, err := client.CoreV1().Nodes().Create(ctx, fresh, metav1.CreateOptions{}); err != nil {
			return err
		}
		log.G(ctx).WithField("node", fresh.Name).WithField("finalizer", provider.NodeFinalizer).
			WithField("action", "re-registered").Info("node finalizer set")
		return nil
	}
}

// probeOnce is one readiness probe attempt with the M2 prober's semantics (the resume check).
func probeOnce(ctx context.Context, podIP string, c *corev1.Container) error {
	return probe.NetProber{}.Probe(ctx, probe.Target{PodIP: podIP, Container: c})
}
