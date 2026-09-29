package provider_test

import (
	"context"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

const mi = 1 << 20

func budgetPod(name, nodeName string, phase corev1.PodPhase, cpu, mem string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem),
				}},
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func budgetMirror(name, vkNode, cpu, mem string) corev1.Pod {
	pod := budgetPod(name, "host", corev1.PodRunning, cpu, mem)
	pod.Labels = map[string]string{mirror.LabelMirrorNode: vkNode, mirror.LabelMirrorOf: name}
	return pod
}

func budgetHost() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "host"},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32Gi"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("7910m"), corev1.ResourceMemory: resource.MustParse("28Gi"),
			},
		},
	}
}

// residentSet: counted trainer 4/12Gi, ds 100m/256Mi, init-heavy 2/1Gi (init peak wins), another
// VK's mirror 500m/1Gi = 6600m/14592Mi. Not counted: Succeeded, other node, this VK's mirror.
func residentSet() []corev1.Pod {
	initHeavy := budgetPod("init-heavy", "host", corev1.PodRunning, "100m", "256Mi")
	initHeavy.Spec.InitContainers = []corev1.Container{{Name: "i", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}}}
	return []corev1.Pod{
		budgetPod("trainer", "host", corev1.PodRunning, "4", "12Gi"),
		budgetPod("ds", "host", corev1.PodRunning, "100m", "256Mi"),
		initHeavy,
		budgetPod("done", "host", corev1.PodSucceeded, "2", "4Gi"),
		budgetPod("failed", "host", corev1.PodFailed, "2", "4Gi"),
		budgetPod("other", "other-node", corev1.PodRunning, "2", "4Gi"),
		budgetMirror("mirror-own", "vk", "1", "2Gi"),
		budgetMirror("mirror-other", "vk-other", "500m", "1Gi"),
	}
}

// The default margins.
var (
	marginCPU = resource.MustParse("250m")
	marginMem = resource.MustParse("1Gi")
)

func TestGuestBudget_Arithmetic(t *testing.T) {
	got := provider.GuestBudget(budgetHost(), residentSet(), "vk", marginCPU, marginMem)
	if got.CPU.MilliValue() != 1060 || got.Memory.Value() != 13056*mi {
		t.Errorf("budget = %s / %s, want 1060m / 13056Mi", got.CPU.String(), got.Memory.String())
	}
	if got.ResidentCPU.MilliValue() != 6600 || got.ResidentMemory.Value() != 14592*mi {
		t.Errorf("resident = %s / %s, want 6600m / 14592Mi", got.ResidentCPU.String(), got.ResidentMemory.String())
	}
	if got.ExcludedMirrors != 1 {
		t.Errorf("excluded mirrors = %d, want 1 (only this VK's own)", got.ExcludedMirrors)
	}
}

func TestGuestBudget_ClampAtZero(t *testing.T) {
	pods := append(residentSet(), budgetPod("huge", "host", corev1.PodRunning, "8", "40Gi"))
	got := provider.GuestBudget(budgetHost(), pods, "vk", marginCPU, marginMem)
	if got.CPU.Sign() != 0 || got.Memory.Sign() != 0 {
		t.Errorf("budget = %s / %s, want 0 / 0", got.CPU.String(), got.Memory.String())
	}
}

func TestGuestBudget_SidecarsAndOverhead(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	pod := budgetPod("p", "host", corev1.PodRunning, "1", "1Gi")
	pod.Spec.InitContainers = []corev1.Container{
		{Name: "sidecar", RestartPolicy: &always, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
		}}},
		{Name: "init", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("1Gi"),
		}}},
	}
	pod.Spec.Overhead = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi"),
	}
	// cpu: max(1 + 0.5, 3 + 0.5) + 0.1 = 3.6; memory: max(1Gi + 512Mi, 1Gi + 512Mi) + 64Mi = 1600Mi.
	got := provider.GuestBudget(budgetHost(), []corev1.Pod{pod}, "vk", resource.Quantity{}, resource.Quantity{})
	if got.ResidentCPU.MilliValue() != 3600 || got.ResidentMemory.Value() != 1600*mi {
		t.Errorf("resident = %s / %s, want 3600m / 1600Mi", got.ResidentCPU.String(), got.ResidentMemory.String())
	}
}

func setupNode(t *testing.T, cs *fake.Clientset, refresh time.Duration) *provider.NodeProvider {
	t.Helper()
	cfg := provider.NodeConfig{
		Name: "vk", InternalIP: "10.0.0.1", KubeletPort: 10260, GPUs: 1,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
	}
	opts := provider.BudgetOptions{MarginCPU: marginCPU, MarginMemory: marginMem, Refresh: refresh}
	np, err := provider.SetupNode(t.Context(), cs, "host", &cfg, &opts)
	if err != nil {
		t.Fatal(err)
	}
	return np
}

func newBudgetClient(t *testing.T) *fake.Clientset {
	t.Helper()
	cs := fake.NewClientset(budgetHost())
	pods := residentSet()
	for i := range pods {
		if _, err := cs.CoreV1().Pods("ns").Create(t.Context(), &pods[i], metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	cs.ClearActions()
	return cs
}

// alloc is a Node's allocatable cpu in m and memory in Mi.
type alloc struct{ cpuM, memMi int64 }

func allocOf(n *corev1.Node) alloc {
	return alloc{n.Status.Allocatable.Cpu().MilliValue(), n.Status.Allocatable.Memory().Value() / mi}
}

func TestGuestBudget_NodeUsesBudget(t *testing.T) {
	cs := newBudgetClient(t)
	vkNode := setupNode(t, cs, 0).Node()
	if got := allocOf(vkNode); got != (alloc{1060, 13056}) {
		t.Errorf("allocatable = %dm / %dMi, want 1060m / 13056Mi", got.cpuM, got.memMi)
	}
	if vkNode.Status.Capacity.Cpu().MilliValue() != 1060 || vkNode.Status.Capacity.Memory().Value() != 13056*mi {
		t.Errorf("capacity = %v, want the budget", vkNode.Status.Capacity)
	}
	if got := vkNode.Status.Allocatable[provider.GPUResource]; got.Value() != 1 {
		t.Errorf("gpu = %s, want unchanged 1", got.String())
	}
	if got := vkNode.Status.Allocatable[corev1.ResourcePods]; got.Value() != 20 {
		t.Errorf("pods = %s, want unchanged 20", got.String())
	}
}

// runController runs a real virtual-kubelet NodeController on the fake clientset, as main.go
// wires it (the provider is the node template's NodeProvider), with 1 s status updates.
func runController(t *testing.T, cs *fake.Clientset, np *provider.NodeProvider) {
	t.Helper()
	nc, err := node.NewNodeController(np, np.Node(), cs.CoreV1().Nodes(),
		node.WithNodeStatusUpdateInterval(time.Second), node.WithNodePingInterval(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := nc.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("node controller: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-nc.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("node controller not ready")
	}
}

func serverAlloc(t *testing.T, cs *fake.Clientset) alloc {
	t.Helper()
	n, err := cs.CoreV1().Nodes().Get(t.Context(), "vk", metav1.GetOptions{})
	if err != nil {
		return alloc{-1, -1}
	}
	return allocOf(n)
}

func waitAlloc(t *testing.T, cs *fake.Clientset, wantCPU, wantMi int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if serverAlloc(t, cs) == (alloc{wantCPU, wantMi}) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := serverAlloc(t, cs)
	t.Fatalf("VK Node allocatable = %dm / %dMi, want %dm / %dMi", got.cpuM, got.memMi, wantCPU, wantMi)
}

func holdsAlloc(t *testing.T, cs *fake.Clientset, wantCPU, wantMi int64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := serverAlloc(t, cs); got != (alloc{wantCPU, wantMi}) {
			t.Fatalf("VK Node allocatable moved to %dm / %dMi, want it to stay %dm / %dMi", got.cpuM, got.memMi, wantCPU, wantMi)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestGuestBudget_RefreshReachesNodeAndSurvivesStatusUpdates(t *testing.T) {
	cs := newBudgetClient(t)
	np := setupNode(t, cs, 300*time.Millisecond)
	runController(t, cs, np)
	waitAlloc(t, cs, 1060, 13056)

	// A new resident pod shrinks the budget on the next refresh.
	pod := budgetPod("trainer2", "host", corev1.PodRunning, "500m", "2Gi")
	if _, err := cs.CoreV1().Pods("ns").Create(t.Context(), &pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitAlloc(t, cs, 560, 11008)

	// This VK's own new mirror does not: its guest is already booked on the VK Node.
	own := budgetMirror("mirror-own-2", "vk", "1", "4Gi")
	if _, err := cs.CoreV1().Pods("ns").Create(t.Context(), &own, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Several refreshes and at least two periodic status updates: nothing resets the budget.
	holdsAlloc(t, cs, 560, 11008, 2500*time.Millisecond)
	if got := allocOf(np.Node()); got != (alloc{560, 11008}) {
		t.Errorf("template = %dm / %dMi, want 560m / 11008Mi", got.cpuM, got.memMi)
	}
}

// TestNodeProvider_UpdatePushesWholeTemplate covers the node-status path: a change is
// kept in the template and the whole template is pushed, so labels and taints survive.
func TestNodeProvider_UpdatePushesWholeTemplate(t *testing.T) {
	spec := provider.NewNodeSpec(provider.NodeConfig{
		Name: "vk", CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
	})
	np := provider.NewNodeProvider(&spec)
	if np.SetCPUMemory(resource.MustParse("8"), resource.MustParse("32Gi")) {
		t.Error("no change must report false")
	}
	pushed := make(chan *corev1.Node, 4)
	np.NotifyNodeStatus(t.Context(), func(n *corev1.Node) { pushed <- n })
	if !np.SetCPUMemory(resource.MustParse("2"), resource.MustParse("4Gi")) {
		t.Fatal("a change must report true")
	}
	select {
	case n := <-pushed:
		if got := allocOf(n); got != (alloc{2000, 4096}) {
			t.Errorf("pushed allocatable = %dm / %dMi, want 2000m / 4096Mi", got.cpuM, got.memMi)
		}
		if n.Status.Capacity.Cpu().MilliValue() != 2000 || n.Labels[provider.VirtualNodeLabel] != "true" {
			t.Errorf("pushed node lost capacity or labels: %+v", n)
		}
		if got := n.Status.Allocatable[corev1.ResourcePods]; got.Value() != 20 {
			t.Errorf("pods = %s, want unchanged 20", got.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no push")
	}
	np.Update(func(n *corev1.Node) bool { n.Spec.Unschedulable = true; return true })
	if n := <-pushed; !n.Spec.Unschedulable || n.Status.Allocatable.Cpu().MilliValue() != 2000 {
		t.Errorf("second push must carry both changes: %+v", n)
	}
	if n := np.Node(); !n.Spec.Unschedulable {
		t.Error("template must keep the update")
	}
}
