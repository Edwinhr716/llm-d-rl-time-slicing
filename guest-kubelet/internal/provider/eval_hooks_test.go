package provider //nolint:testpackage // evaluator hooks (D-NS-8, D-NS-9) are package-internal test API for evaluation harnesses

import (
	"context"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// evalNodeController builds the virtual Node's NodeController on cs as main.go does: the
// NewNodeSpec template, NodeProvider, the ReRegisterOnNotFound handler and, with cordon
// (--cordon-while-held), a Cordoner. Status and lease updates run every second instead of 10 s.
// setHeld is what the hold watcher calls on every poll; with cordon=false it does nothing.
// run is the NodeController's Run.
//
//nolint:gocritic // unnamedResult: the harness destructures three results, and nonamedreturns forbids naming them.
func evalNodeController(
	ctx context.Context, cs kubernetes.Interface, nodeName string, cordon bool,
) (func(bool), func(context.Context) error, error) {
	tmpl := NewNodeSpec(NodeConfig{
		Name: nodeName, InternalIP: "10.0.0.1", KubeletPort: 10260, KubeletVersion: "eval",
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
	})
	setHeld := func(bool) {}
	template := tmpl.DeepCopy
	if cordon {
		cordoner := NewCordoner(cs.CoreV1().Nodes(), &tmpl, &record.FakeRecorder{})
		setHeld = func(held bool) { cordoner.Set(ctx, held, "eval") }
		template = cordoner.Template
	}
	leases := cs.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nc, err := node.NewNodeController(NewNodeProvider(&tmpl), tmpl.DeepCopy(), cs.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1WithRenewInterval(leases, node.DefaultLeaseDuration, time.Second),
		node.WithNodeStatusUpdateInterval(time.Second),
		node.WithNodeStatusUpdateErrorHandler(ReRegisterOnNotFound(cs, template)),
	)
	if err != nil {
		return nil, nil, err
	}
	return setHeld, nc.Run, nil
}

// runEval starts evalNodeController and waits until the Node is registered.
func runEval(t *testing.T, cs *fake.Clientset, cordon bool) func(bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	setHeld, run, err := evalNodeController(ctx, cs, "vk-eval", cordon)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("NodeController: %v", err)
		}
	})
	for i := 0; i < 50; i++ {
		if _, err := cs.CoreV1().Nodes().Get(ctx, "vk-eval", metav1.GetOptions{}); err == nil {
			return setHeld
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the Node was not registered")
	return nil
}

func unschedulable(t *testing.T, cs *fake.Clientset) bool {
	t.Helper()
	got, err := cs.CoreV1().Nodes().Get(context.Background(), "vk-eval", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return got.Spec.Unschedulable
}

func statusPatches(cs *fake.Clientset) int {
	count := 0
	for _, a := range cs.Actions() {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "nodes" && a.GetSubresource() == "status" {
			count++
		}
	}
	return count
}

func TestCordonWhileHeld_NsCordon_SurvivesNodeControllerUpdates(t *testing.T) {
	cs := fake.NewClientset()
	setHeld := runEval(t, cs, true)
	if unschedulable(t, cs) {
		t.Fatal("cordoned before any hold")
	}
	setHeld(true)
	if !unschedulable(t, cs) {
		t.Fatal("held: not cordoned")
	}
	updatesBefore := statusPatches(cs)
	for i := 0; i < 14; i++ { // 3.5 s: about three status updates and lease renewals
		time.Sleep(250 * time.Millisecond)
		if !unschedulable(t, cs) {
			t.Fatalf("the cordon was lost after %v", time.Duration(i+1)*250*time.Millisecond)
		}
	}
	if n := statusPatches(cs) - updatesBefore; n < 2 {
		t.Errorf("%d status updates during the hold, want at least 2 for the check to mean anything", n)
	}
	setHeld(false)
	if unschedulable(t, cs) {
		t.Error("released: still cordoned")
	}
}

func TestCordonWhileHeld_Skip_NeverCordons(t *testing.T) {
	cs := fake.NewClientset()
	setHeld := runEval(t, cs, false)
	setHeld(true)
	time.Sleep(1500 * time.Millisecond)
	if unschedulable(t, cs) {
		t.Error("--cordon-while-held=false cordoned the Node")
	}
	for _, a := range cs.Actions() {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "nodes" && a.GetSubresource() == "" {
			t.Errorf("--cordon-while-held=false patched the Node spec: %v", a)
		}
	}
}

// Test hooks for the D-NS-9 evaluation (guest budget). The evaluator drops its own test file
// into this package and calls these. Names differ from D-NS-8's evalNodeController on purpose.

// evalBudget is the computed-mode budget function, unchanged.
func evalBudget(
	host *corev1.Node, pods []corev1.Pod, vkNode string, marginCPU, marginMem resource.Quantity,
) corev1.ResourceList {
	return GuestBudget(host, pods, vkNode, marginCPU, marginMem).ResourceList()
}

// evalMirrorResources returns one mirror container's resources for mode "static" or "computed",
// with the deploy/deployment.yaml headroom flags (--mirror-cpu-headroom=1, --mirror-memory-headroom=4Gi).
func evalMirrorResources(in corev1.ResourceRequirements, mode string) corev1.ResourceRequirements {
	guest := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "eval", Name: "guest", UID: "eval-uid"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Resources: in}}},
	}
	mirrorPod, err := mirror.Build(guest, mirror.Config{
		HostNode: "eval-host", VirtualNode: "eval-vk",
		CPUHeadroom: resource.MustParse("1"), MemoryHeadroom: resource.MustParse("4Gi"),
		RealRequests: mode == BudgetComputed,
	})
	if err != nil {
		return corev1.ResourceRequirements{}
	}
	return mirrorPod.Spec.Containers[0].Resources
}

// evalBudgetNodeController builds the NodeProvider and Node template as main.go does (SetupNode;
// static flags --cpu=8 --memory=32Gi) and a virtual-kubelet NodeController with a 1 s status
// interval. run registers the Node and blocks until ctx ends.
func evalBudgetNodeController(
	ctx context.Context, cs kubernetes.Interface, hostNode, vkNode, mode string,
	marginCPU, marginMem resource.Quantity, refresh time.Duration,
) (func(context.Context) error, error) {
	host, err := cs.CoreV1().Nodes().Get(ctx, hostNode, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	cfg := NodeConfig{
		Name: vkNode, InternalIP: "10.0.0.1", KubeletPort: 10260, KubeletVersion: "eval", GPUs: 1,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
	}
	opts := BudgetOptions{Mode: mode, MarginCPU: marginCPU, MarginMemory: marginMem, Refresh: refresh}
	nodeProvider, err := SetupNode(ctx, cs, host, &cfg, &opts)
	if err != nil {
		return nil, err
	}
	nc, err := node.NewNodeController(nodeProvider, nodeProvider.Node(), cs.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1(nodeutil.NodeLeaseV1Client(cs), node.DefaultLeaseDuration),
		node.WithNodeStatusUpdateInterval(time.Second),
		node.WithNodePingInterval(time.Second),
	)
	if err != nil {
		return nil, err
	}
	return nc.Run, nil
}

// TestEvalHooks_Smoke keeps the hooks compiling and honest: each returns what the option code does.
func TestEvalHooks_Smoke(t *testing.T) {
	host := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "host"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"),
		}},
	}
	got := evalBudget(host, nil, "vk", resource.MustParse("1"), resource.MustParse("1Gi"))
	if got.Cpu().MilliValue() != 3000 || got.Memory().Value() != 7<<30 {
		t.Errorf("evalBudget = %v, want cpu 3 memory 7Gi", got)
	}
	in := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6")}}
	if r := evalMirrorResources(in, BudgetStatic); r.Requests.Cpu().MilliValue() != 1000 {
		t.Errorf("static mirror cpu request = %v, want 1", r.Requests.Cpu())
	}
	if r := evalMirrorResources(in, BudgetComputed); r.Requests.Cpu().MilliValue() != 6000 {
		t.Errorf("computed mirror cpu request = %v, want 6", r.Requests.Cpu())
	}
	cs := fake.NewClientset(host)
	for _, mode := range []string{BudgetStatic, BudgetComputed} {
		run, err := evalBudgetNodeController(t.Context(), cs, "host", "vk-"+mode, mode,
			resource.MustParse("1"), resource.MustParse("1Gi"), time.Second)
		if err != nil || run == nil {
			t.Fatalf("%s: evalBudgetNodeController: %v", mode, err)
		}
	}
}
