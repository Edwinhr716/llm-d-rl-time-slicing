package provider //nolint:testpackage // evalNodeController is package-internal test API for evaluation harnesses

import (
	"context"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
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
	nc, err := node.NewNodeController(NodeProvider{}, tmpl.DeepCopy(), cs.CoreV1().Nodes(),
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
