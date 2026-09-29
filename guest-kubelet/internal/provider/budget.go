package provider

// Guest CPU/RAM budget: the VK Node advertises the host's allocatable minus the effective
// requests of the pods resident on the host minus a margin, and mirrors carry the guest's real
// requests, so the real kubelet's fit check on the host passes for every guest the scheduler
// binds to the VK Node.

import (
	"context"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// BudgetOptions are the budget flags.
type BudgetOptions struct {
	MarginCPU    resource.Quantity
	MarginMemory resource.Quantity
	// Refresh is how often the budget is recomputed; 0 computes it once at start.
	Refresh time.Duration
}

// Budget is the guest budget with the numbers it came from (for the log line).
type Budget struct {
	CPU, Memory                   resource.Quantity // what the VK Node advertises
	HostAllocCPU, HostAllocMemory resource.Quantity
	ResidentCPU, ResidentMemory   resource.Quantity
	MarginCPU, MarginMemory       resource.Quantity
	ExcludedMirrors               int
}

// GuestBudget is the budget function: the host's status.allocatable minus the effective
// requests of the pods bound to the host that are not Succeeded or Failed, minus the margin,
// clamped at 0. Mirrors labelled timeslice.io/mirror-node=<vkNode> are left out: the scheduler
// already books their guests on the VK Node, so counting them again would book them twice.
// Other VKs' mirrors, this VK's own pods and DaemonSets count as resident. Pods on other nodes
// are skipped here too, since a fake clientset ignores the field selector.
func GuestBudget(host *corev1.Node, pods []corev1.Pod, vkNode string, marginCPU, marginMem resource.Quantity) *Budget {
	budget := &Budget{
		HostAllocCPU:    host.Status.Allocatable.Cpu().DeepCopy(),
		HostAllocMemory: host.Status.Allocatable.Memory().DeepCopy(),
		ResidentCPU:     *resource.NewMilliQuantity(0, resource.DecimalSI),
		ResidentMemory:  *resource.NewQuantity(0, resource.BinarySI),
		MarginCPU:       marginCPU.DeepCopy(),
		MarginMemory:    marginMem.DeepCopy(),
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != host.Name || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Labels[mirror.LabelMirrorNode] == vkNode {
			budget.ExcludedMirrors++
			continue
		}
		budget.ResidentCPU.Add(effectiveRequest(pod, corev1.ResourceCPU))
		budget.ResidentMemory.Add(effectiveRequest(pod, corev1.ResourceMemory))
	}
	budget.CPU = clampedRest(&budget.HostAllocCPU, &budget.ResidentCPU, &marginCPU)
	budget.Memory = clampedRest(&budget.HostAllocMemory, &budget.ResidentMemory, &marginMem)
	return budget
}

// ResourceList is the budget as the VK Node's cpu and memory.
func (b *Budget) ResourceList() corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: b.CPU.DeepCopy(), corev1.ResourceMemory: b.Memory.DeepCopy()}
}

func clampedRest(alloc, resident, margin *resource.Quantity) resource.Quantity {
	rest := alloc.DeepCopy()
	rest.Sub(*resident)
	rest.Sub(*margin)
	if rest.Sign() < 0 {
		rest.Set(0)
	}
	return rest
}

// effectiveRequest is a pod's request for one resource as the kubelet's fit check computes it:
// max(sum of containers plus restartable init containers, the peak of the init sequence) plus
// overhead. A pod-level request (spec.resources) wins where set.
func effectiveRequest(pod *corev1.Pod, name corev1.ResourceName) resource.Quantity {
	total := resource.Quantity{}
	if pod.Spec.Resources != nil {
		if podLevel, ok := pod.Spec.Resources.Requests[name]; ok {
			total = podLevel.DeepCopy()
		}
	}
	if total.IsZero() {
		total = containerRequest(&pod.Spec, name)
	}
	if overhead, ok := pod.Spec.Overhead[name]; ok {
		total.Add(overhead)
	}
	return total
}

func containerRequest(spec *corev1.PodSpec, name corev1.ResourceName) resource.Quantity {
	sum := resource.Quantity{}
	for i := range spec.Containers {
		sum.Add(spec.Containers[i].Resources.Requests[name])
	}
	sidecars, initPeak := resource.Quantity{}, resource.Quantity{}
	for i := range spec.InitContainers {
		initCtr := &spec.InitContainers[i]
		req := initCtr.Resources.Requests[name]
		if initCtr.RestartPolicy != nil && *initCtr.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecars.Add(req) // a sidecar runs for the pod's whole life
			if sidecars.Cmp(initPeak) > 0 {
				initPeak = sidecars.DeepCopy()
			}
			continue
		}
		step := req.DeepCopy()
		step.Add(sidecars)
		if step.Cmp(initPeak) > 0 {
			initPeak = step
		}
	}
	sum.Add(sidecars)
	if initPeak.Cmp(sum) > 0 {
		return initPeak
	}
	return sum
}

// ComputeBudget reads the host Node and the pods bound to it and returns the budget.
func ComputeBudget(
	ctx context.Context, client kubernetes.Interface, hostNode, vkNode string, opts *BudgetOptions,
) (*Budget, error) {
	host, err := client.CoreV1().Nodes().Get(ctx, hostNode, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get host node %s: %w", hostNode, err)
	}
	pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", hostNode).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods on %s: %w", hostNode, err)
	}
	return GuestBudget(host, pods.Items, vkNode, opts.MarginCPU, opts.MarginMemory), nil
}

// SetupNode builds the VK Node template and its NodeProvider. The template's cpu and memory
// are the budget, computed before the Node is registered and, with a refresh, kept current
// afterwards. cfg.CPU and cfg.Memory are ignored.
func SetupNode(
	ctx context.Context, client kubernetes.Interface, hostNode string, cfg *NodeConfig, opts *BudgetOptions,
) (*NodeProvider, error) {
	budget, err := ComputeBudget(ctx, client, hostNode, cfg.Name, opts)
	if err != nil {
		return nil, err
	}
	withBudget := *cfg
	withBudget.CPU, withBudget.Memory = budget.CPU, budget.Memory
	logBudget(ctx, budget)
	spec := NewNodeSpec(withBudget)
	nodeProvider := NewNodeProvider(&spec)
	if opts.Refresh > 0 {
		refresh := *opts
		nodeProvider.OnStart(func(ctx context.Context) {
			refreshBudget(ctx, client, hostNode, cfg.Name, &refresh, nodeProvider)
		})
	}
	return nodeProvider, nil
}

// refreshBudget recomputes the budget every opts.Refresh and writes changes to the VK Node. A
// smaller budget only affects new binds; nothing already bound is evicted.
func refreshBudget(
	ctx context.Context, client kubernetes.Interface, hostNode, vkNode string, opts *BudgetOptions, nodeProvider *NodeProvider,
) {
	ticker := time.NewTicker(opts.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		budget, err := ComputeBudget(ctx, client, hostNode, vkNode, opts)
		if err != nil {
			log.G(ctx).WithError(err).Warn("guest budget refresh failed; keeping the last budget")
			continue
		}
		if nodeProvider.SetCPUMemory(budget.CPU, budget.Memory) {
			logBudget(ctx, budget)
		}
	}
}

func logBudget(ctx context.Context, budget *Budget) {
	log.G(ctx).WithField("cpu", budget.CPU.String()).WithField("memory", budget.Memory.String()).
		WithField("host_alloc_cpu", budget.HostAllocCPU.String()).
		WithField("host_alloc_memory", budget.HostAllocMemory.String()).
		WithField("resident_cpu", budget.ResidentCPU.String()).
		WithField("resident_memory", budget.ResidentMemory.String()).
		WithField("margin_cpu", budget.MarginCPU.String()).WithField("margin_memory", budget.MarginMemory.String()).
		WithField("excluded_mirrors", budget.ExcludedMirrors).
		Info("guest budget")
}
