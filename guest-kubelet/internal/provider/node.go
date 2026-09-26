package provider

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// GuestTaintKey keeps ordinary pods off the virtual node. Guests tolerate it by key.
	GuestTaintKey = "timeslice.io/guest"
	// VirtualNodeLabel marks the Node as virtual, for selectors and for humans.
	VirtualNodeLabel = "timeslice.io/virtual-node"
	// GPUResource is advertised as plain capacity so the default scheduler can fit guests.
	GPUResource corev1.ResourceName = "nvidia.com/gpu"
)

// NodeConfig is everything needed to describe the virtual Node.
type NodeConfig struct {
	Name           string
	InternalIP     string // the real host's IP (downward API status.hostIP)
	KubeletPort    int32  // advertised in daemonEndpoints; the real kubelet holds 10250
	KubeletVersion string
	CPU            resource.Quantity
	Memory         resource.Quantity
	Pods           resource.Quantity
	GPUs           int64
	// ProviderID, if set, is the real host's spec.providerID (gce://project/zone/instance).
	// The cloud node lifecycle controller deletes a NotReady Node whose instance it cannot
	// find; with the host's ID it would find the host's VM and leave the Node alone (M0 risk).
	// GKE denies it: the validate-node-providerid admission policy requires the providerID to
	// end in "/<node name>". Empty on GKE.
	ProviderID string
}

// NewNodeSpec builds the Node object that the library registers once at startup.
// After that the library only patches nodes/status; it never rewrites spec or labels.
func NewNodeSpec(cfg NodeConfig) corev1.Node {
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    cfg.CPU,
		corev1.ResourceMemory: cfg.Memory,
		corev1.ResourcePods:   cfg.Pods,
		GPUResource:           *resource.NewQuantity(cfg.GPUs, resource.DecimalSI),
	}
	now := metav1.Now()
	cond := func(t corev1.NodeConditionType, s corev1.ConditionStatus, reason, msg string) corev1.NodeCondition {
		return corev1.NodeCondition{Type: t, Status: s, Reason: reason, Message: msg,
			LastHeartbeatTime: now, LastTransitionTime: now}
	}

	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: cfg.Name,
			// Tells the cluster autoscaler never to pick this Node for scale-down. It matters
			// if the Node ever carries the host's providerID (the autoscaler would then map it
			// to the host's instance group); harmless otherwise.
			Annotations: map[string]string{"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true"},
			// No cloud.google.com/gke-nodepool label: the fake node must belong to no node pool,
			// so the autoscaler and auto-repair leave it alone.
			// No kubernetes.io/os label either: every GKE system DaemonSet that landed on the
			// M0 node (collector, fluentbit-gke, gcsfusecsi-node, gke-metrics-agent, pdcsi-node)
			// requires kubernetes.io/os=linux, so without it none of them is scheduled here.
			Labels: map[string]string{
				"type":                   "virtual-kubelet",
				VirtualNodeLabel:         "true",
				"kubernetes.io/role":     "agent",
				"kubernetes.io/hostname": cfg.Name,
				"kubernetes.io/arch":     "amd64",
				"node.kubernetes.io/exclude-from-external-load-balancers": "true",
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: cfg.ProviderID,
			Taints:     []corev1.Taint{{Key: GuestTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{
			Phase:       corev1.NodeRunning,
			Capacity:    capacity,
			Allocatable: capacity.DeepCopy(),
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: cfg.InternalIP},
				{Type: corev1.NodeHostName, Address: cfg.Name},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{
				KubeletEndpoint: corev1.DaemonEndpoint{Port: cfg.KubeletPort},
			},
			NodeInfo: corev1.NodeSystemInfo{
				OperatingSystem: "linux",
				Architecture:    "amd64",
				KubeletVersion:  cfg.KubeletVersion,
			},
			// Report healthy from the first write. The library refreshes LastHeartbeatTime.
			Conditions: []corev1.NodeCondition{
				cond(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "guest-kubelet is ready"),
				cond(corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "no memory pressure"),
				cond(corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "no disk pressure"),
				cond(corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "no PID pressure"),
				cond(corev1.NodeNetworkUnavailable, corev1.ConditionFalse, "RouteCreated", "guest-kubelet reports network available"),
			},
		},
	}
}

// NodeProvider is the node half of the provider. The library calls Ping every 10s and
// only writes node status if Ping succeeds. M0 has no backend to check, so it is always healthy.
type NodeProvider struct{}

// Ping reports the node as healthy while the process is alive.
func (NodeProvider) Ping(ctx context.Context) error { return ctx.Err() }

// NotifyNodeStatus would push status changes (capacity, cordon) to the library. M0 has none.
func (NodeProvider) NotifyNodeStatus(context.Context, func(*corev1.Node)) {}
