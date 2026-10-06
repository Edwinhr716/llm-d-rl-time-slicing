package donorcontroller

// The shadow device plugin path (pooled mode) adds two duties to the donor controller:
//
//   - the lendable set: on an owned donor host it records how many GPUs the group's donor pod
//     holds (timeslice.io/lendable-gpus). That is the API-level view of what the shadow plugin
//     advertises as timeslice.io/gpu-shadow and the guest kubelet then offers as nvidia.com/gpu
//     on the virtual Node. Like the plugin it fails closed: more than one donor pod holding GPUs
//     on one node lends nothing (0) and raises a Warning event. A difference between the two
//     views is logged, not acted on: the plugin, which reads the kubelet's pod-resources, wins.
//   - stale fences (--clear-stale-fences): the guest kubelet puts the NoSchedule taint
//     timeslice.io/gpu-fence=<virtual node> on the host while a mirror still runs on a GPU whose
//     donor left, and takes it off when the mirror is gone. If the guest kubelet goes away in
//     between (uninstalled, its virtual Node released), nobody takes it off and the host stays
//     closed to every pod that does not tolerate it. The controller removes such a taint once
//     the virtual Node it names has been absent for --vk-deregister-grace and no mirror pod
//     (label timeslice.io/role) is left on the host. It never adds the taint: only the guest
//     kubelet knows when a mirror stops.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/util/retry"
)

// Names of the shadow path.
const (
	// AnnotationLendableGPUs is the number of GPUs the owned group's donor pod holds on the host,
	// or 0 when it is ambiguous (more than one donor pod holds GPUs).
	AnnotationLendableGPUs = "timeslice.io/lendable-gpus"
	// GPUResource is the device plugin resource donors hold.
	GPUResource corev1.ResourceName = "nvidia.com/gpu"
	// ShadowResource is the shadow device plugin's pooled resource on the host.
	ShadowResource corev1.ResourceName = "timeslice.io/gpu-shadow"
	// FenceTaintKey is the guest kubelet's fence taint on the host; its value is the virtual Node.
	FenceTaintKey = "timeslice.io/gpu-fence"
	// RoleLabelKey marks the guest kubelet's mirror pods.
	RoleLabelKey = "timeslice.io/role"

	EventLendableAmbiguous = "LendableAmbiguous"
	EventStaleFenceCleared = "StaleFenceCleared"
)

// podGPUs is the nvidia.com/gpu a pod holds: the sum over its containers, limits first (an
// extended resource has equal requests and limits), and at least the largest init container's.
func podGPUs(pod *corev1.Pod) int64 {
	get := func(c *corev1.Container) int64 {
		if q, ok := c.Resources.Limits[GPUResource]; ok {
			return q.Value()
		}
		if q, ok := c.Resources.Requests[GPUResource]; ok {
			return q.Value()
		}
		return 0
	}
	var sum, initMax int64
	for i := range pod.Spec.Containers {
		sum += get(&pod.Spec.Containers[i])
	}
	for i := range pod.Spec.InitContainers {
		initMax = max(initMax, get(&pod.Spec.InitContainers[i]))
	}
	return max(sum, initMax)
}

// lendable returns the lendable GPU count of a host from its donor pods (all groups) and the
// names of the donor pods that hold GPUs. More than one holder is ambiguous: 0.
func lendable(donors donorSet) (int64, []string) {
	var total int64
	var holders []string
	for _, pods := range donors {
		for _, p := range pods {
			if n := podGPUs(p); n > 0 {
				total += n
				holders = append(holders, p.Namespace+"/"+p.Name)
			}
		}
	}
	if len(holders) > 1 {
		return 0, holders
	}
	return total, holders
}

// syncLendable writes AnnotationLendableGPUs on an owned host when it changed.
func (c *Controller) syncLendable(ctx context.Context, node *corev1.Node, st *nodeState, donors donorSet) {
	n, holders := lendable(donors)
	if len(holders) > 1 {
		if !st.ambiguous {
			st.ambiguous = true
			c.log.Warn("more than one donor pod holds GPUs on the node; lending none", "node", node.Name,
				"holders", strings.Join(holders, ","))
			c.event(ctx, node, corev1.EventTypeWarning, EventLendableAmbiguous,
				fmt.Sprintf("donor pods %s all hold GPUs on this node; nothing is lent (fail closed)", strings.Join(holders, ",")))
		}
	} else {
		st.ambiguous = false
	}
	val := strconv.FormatInt(n, 10)
	if node.Annotations[AnnotationLendableGPUs] != val {
		if err := c.patchNode(ctx, node.Name, nil, map[string]*string{AnnotationLendableGPUs: &val}); err != nil {
			c.log.Warn("writing lendable GPUs failed", "node", node.Name, "err", err)
			return
		}
		c.log.Info("lendable GPUs", "node", node.Name, "group", st.group, "gpus", n, "holders", strings.Join(holders, ","))
	}
	// Cross-check with what the shadow plugin advertises, once per change.
	if q, ok := node.Status.Allocatable[ShadowResource]; ok {
		sig := fmt.Sprintf("%d/%d", n, q.Value())
		if q.Value() != n && st.shadowSig != sig {
			c.log.Info("lendable GPUs differ from the shadow allocatable (the plugin wins)", "node", node.Name,
				"lendable", n, "shadow_allocatable", q.Value())
		}
		st.shadowSig = sig
	}
}

// fenceTaint returns the node's fence taint, if any.
func fenceTaint(node *corev1.Node) *corev1.Taint {
	for i := range node.Spec.Taints {
		if node.Spec.Taints[i].Key == FenceTaintKey {
			return &node.Spec.Taints[i]
		}
	}
	return nil
}

// clearStaleFences removes fence taints whose guest kubelet is gone (see the file comment).
func (c *Controller) clearStaleFences(ctx context.Context, now time.Time, nodes map[string]*corev1.Node) {
	seen := map[string]bool{}
	for name, node := range nodes {
		if isVirtual(node) {
			continue
		}
		taint := fenceTaint(node)
		if taint == nil {
			continue
		}
		seen[name] = true
		key := name + "|" + taint.Value
		if vk, ok := nodes[taint.Value]; ok && isVirtual(vk) {
			delete(c.fenceGone, name)
			continue
		}
		since, ok := c.fenceGone[name]
		if !ok || since.key != key {
			c.fenceGone[name] = fenceWait{key: key, since: now}
			c.log.Info("fence taint names a missing virtual node; waiting", "node", name, "virtual_node", taint.Value,
				"grace", c.cfg.VKDeregisterGrace.String())
			continue
		}
		if now.Sub(since.since) < c.cfg.VKDeregisterGrace {
			continue
		}
		mirrors, err := c.mirrorsOn(ctx, name)
		if err != nil {
			c.log.Warn("listing mirror pods failed; fence kept", "node", name, "err", err)
			continue
		}
		if len(mirrors) > 0 {
			if !since.held {
				c.log.Warn("stale fence kept: mirror pods still on the host", "node", name,
					"virtual_node", taint.Value, "mirrors", strings.Join(mirrors, ","))
				since.held = true
				c.fenceGone[name] = since
			}
			continue
		}
		if err := c.removeFence(ctx, name, taint.Value); err != nil {
			c.log.Warn("removing stale fence failed", "node", name, "err", err)
			continue
		}
		delete(c.fenceGone, name)
		c.log.Info("stale fence removed", "node", name, "virtual_node", taint.Value)
		c.event(ctx, node, corev1.EventTypeNormal, EventStaleFenceCleared,
			fmt.Sprintf("removed %s=%s: the virtual node is gone and no mirror pod is left", FenceTaintKey, taint.Value))
	}
	for name := range c.fenceGone {
		if !seen[name] {
			delete(c.fenceGone, name)
		}
	}
}

// mirrorsOn lists the live mirror pods (label timeslice.io/role) bound to the host. Terminating
// mirrors count: their GPU work may still run.
func (c *Controller) mirrorsOn(ctx context.Context, host string) ([]string, error) {
	list, err := c.cs.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: RoleLabelKey,
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", host).String(),
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for i := range list.Items {
		p := &list.Items[i]
		if p.Spec.NodeName != host || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if _, ok := p.Labels[RoleLabelKey]; !ok {
			continue
		}
		out = append(out, p.Namespace+"/"+p.Name)
	}
	return out, nil
}

// removeFence drops the fence taint with this value from the host (read, filter, update).
func (c *Controller) removeFence(ctx context.Context, host, value string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := c.cs.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err != nil {
			return err
		}
		kept := node.Spec.Taints[:0:0]
		for _, t := range node.Spec.Taints {
			if t.Key == FenceTaintKey && t.Value == value {
				continue
			}
			kept = append(kept, t)
		}
		if len(kept) == len(node.Spec.Taints) {
			return nil
		}
		node.Spec.Taints = kept
		_, err = c.cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{FieldManager: LabelledByValue})
		return err
	})
}

// fenceWait tracks one host's fence whose virtual Node is missing.
type fenceWait struct {
	key   string
	since time.Time
	held  bool
}
