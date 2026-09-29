package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

const vkName = "vk-test"

func vkTemplate() *corev1.Node {
	node := provider.NewNodeSpec(provider.NodeConfig{
		Name: vkName, InternalIP: "10.0.0.1", KubeletPort: 10260, KubeletVersion: "test",
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
	})
	return &node
}

// newCluster registers the virtual Node, optionally already cordoned and annotated.
func newCluster(t *testing.T, unschedulable bool, annotations map[string]string) *fake.Clientset {
	t.Helper()
	node := vkTemplate()
	node.Spec.Unschedulable = unschedulable
	for k, v := range annotations {
		node.Annotations[k] = v
	}
	return fake.NewClientset(node)
}

func getNode(t *testing.T, cs *fake.Clientset) *corev1.Node {
	t.Helper()
	node, err := cs.CoreV1().Nodes().Get(context.Background(), vkName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func nodePatches(cs *fake.Clientset) []k8stesting.PatchAction {
	var out []k8stesting.PatchAction
	for _, a := range cs.Actions() {
		if p, ok := a.(k8stesting.PatchAction); ok && a.GetResource().Resource == "nodes" && a.GetSubresource() == "" {
			out = append(out, p)
		}
	}
	return out
}

func events(rec *record.FakeRecorder) []string {
	var out []string
	for len(rec.Events) > 0 {
		out = append(out, <-rec.Events)
	}
	return out
}

func TestCordonAndUncordon(t *testing.T) {
	cs := newCluster(t, false, nil)
	rec := record.NewFakeRecorder(10)
	cordoner := provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), rec)
	ctx := context.Background()

	cordoner.Set(ctx, true, "STATE_LOCKED")
	node := getNode(t, cs)
	if !node.Spec.Unschedulable || node.Annotations[provider.CordonedByAnnotation] != provider.CordonedByValue {
		t.Fatalf("held: unschedulable=%v annotations=%v", node.Spec.Unschedulable, node.Annotations)
	}
	if tmpl := cordoner.Template(); !tmpl.Spec.Unschedulable {
		t.Error("held: the template must carry the cordon")
	}
	if got := events(rec); len(got) != 1 || !strings.HasPrefix(got[0], "Normal Cordoned") {
		t.Errorf("held: events %v, want one Cordoned", got)
	}

	cordoner.Set(ctx, true, "STATE_LOCKED") // no change: no API call
	cordoner.Set(ctx, false, "STATE_IDLE_YIELDED")
	node = getNode(t, cs)
	if node.Spec.Unschedulable {
		t.Error("released: still unschedulable")
	}
	if _, ok := node.Annotations[provider.CordonedByAnnotation]; ok {
		t.Error("released: the cordoned-by annotation must be removed")
	}
	if tmpl := cordoner.Template(); tmpl.Spec.Unschedulable || tmpl.Annotations[provider.CordonedByAnnotation] != "" {
		t.Error("released: the template must not carry the cordon")
	}
	if got := events(rec); len(got) != 1 || !strings.HasPrefix(got[0], "Normal Uncordoned") {
		t.Errorf("released: events %v, want one Uncordoned", got)
	}
	if n := len(nodePatches(cs)); n != 2 {
		t.Errorf("%d Node patches, want 2 (one per change)", n)
	}
}

func TestCordonPatchesOnlyUnschedulable(t *testing.T) {
	cs := newCluster(t, false, nil)
	before := getNode(t, cs)
	provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), nil).Set(context.Background(), true, "STATE_LOCKED")
	patches := nodePatches(cs)
	if len(patches) != 1 {
		t.Fatalf("%d patches, want 1", len(patches))
	}
	var body struct {
		Metadata map[string]json.RawMessage `json:"metadata"`
		Spec     map[string]json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(patches[0].GetPatch(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Spec) != 1 || body.Spec["unschedulable"] == nil {
		t.Errorf("spec patch %v, want only unschedulable", body.Spec)
	}
	for k := range body.Metadata {
		if k != "annotations" && k != "resourceVersion" {
			t.Errorf("metadata patch touches %q", k)
		}
	}
	after := getNode(t, cs)
	if len(after.Spec.Taints) != len(before.Spec.Taints) || after.Labels[provider.VirtualNodeLabel] != "true" {
		t.Error("taints or labels changed")
	}
}

func TestCordonLeavesAdminCordonAlone(t *testing.T) {
	cs := newCluster(t, true, nil) // kubectl cordon: unschedulable, no annotation
	rec := record.NewFakeRecorder(10)
	cordoner := provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), rec)
	cordoner.Set(context.Background(), true, "STATE_LOCKED")
	cordoner.Set(context.Background(), false, "STATE_IDLE")
	node := getNode(t, cs)
	if !node.Spec.Unschedulable {
		t.Error("the guest kubelet removed an admin cordon")
	}
	if _, ok := node.Annotations[provider.CordonedByAnnotation]; ok {
		t.Error("the guest kubelet claimed an admin cordon")
	}
	if n := len(nodePatches(cs)); n != 0 {
		t.Errorf("%d patches, want 0", n)
	}
	if got := events(rec); len(got) != 0 {
		t.Errorf("events %v, want none", got)
	}
}

func TestCordonRestartKeepsCordon(t *testing.T) {
	// The previous guest kubelet cordoned the Node and restarted during the hold.
	cs := newCluster(t, true, map[string]string{provider.CordonedByAnnotation: provider.CordonedByValue})
	provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), nil).Set(context.Background(), true, "STATE_LOCKED")
	if !getNode(t, cs).Spec.Unschedulable {
		t.Error("a restart during the hold uncordoned the Node")
	}
	if n := len(nodePatches(cs)); n != 0 {
		t.Errorf("%d patches, want 0", n)
	}
	// ...and the hold ends after the restart: the new process removes its predecessor's cordon.
	cs2 := newCluster(t, true, map[string]string{provider.CordonedByAnnotation: provider.CordonedByValue})
	provider.NewCordoner(cs2.CoreV1().Nodes(), vkTemplate(), nil).Set(context.Background(), false, "STATE_IDLE")
	if getNode(t, cs2).Spec.Unschedulable {
		t.Error("the hold ended but the Node stayed cordoned")
	}
}

func TestCordonRetriesAfterError(t *testing.T) {
	cs := newCluster(t, false, nil)
	failures := 1
	cs.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, errors.New("apiserver unavailable")
		}
		return false, nil, nil
	})
	cordoner := provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), nil)
	cordoner.Set(context.Background(), true, "STATE_LOCKED")
	if getNode(t, cs).Spec.Unschedulable {
		t.Fatal("the patch failed, the Node cannot be cordoned")
	}
	cordoner.Set(context.Background(), true, "STATE_LOCKED") // next poll
	if !getNode(t, cs).Spec.Unschedulable {
		t.Error("the next poll did not retry the cordon")
	}
}

func TestCordonReRegisterKeepsCordon(t *testing.T) {
	cs := newCluster(t, false, nil)
	cordoner := provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), nil)
	ctx := context.Background()
	cordoner.Set(ctx, true, "STATE_LOCKED")
	if err := cs.CoreV1().Nodes().Delete(ctx, vkName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	notFound := apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, vkName)
	if err := provider.ReRegisterOnNotFound(cs, cordoner.Template)(ctx, notFound); err != nil {
		t.Fatal(err)
	}
	node := getNode(t, cs)
	if !node.Spec.Unschedulable || node.Annotations[provider.CordonedByAnnotation] != provider.CordonedByValue {
		t.Errorf("re-registered during the hold: unschedulable=%v annotations=%v", node.Spec.Unschedulable, node.Annotations)
	}
}

// runNodeController runs the virtual Node's NodeController on cs the way main.go wires it (the
// NewNodeSpec template, NodeProvider, ReRegisterOnNotFound with the Cordoner's template), with
// status and lease updates every second instead of 10 s. It waits until the Node is registered.
func runNodeController(t *testing.T, cs *fake.Clientset, cordoner *provider.Cordoner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	leases := cs.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nc, err := vknode.NewNodeController(provider.NodeProvider{}, vkTemplate(), cs.CoreV1().Nodes(),
		vknode.WithNodeEnableLeaseV1WithRenewInterval(leases, vknode.DefaultLeaseDuration, time.Second),
		vknode.WithNodeStatusUpdateInterval(time.Second),
		vknode.WithNodeStatusUpdateErrorHandler(provider.ReRegisterOnNotFound(cs, cordoner.Template)),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- nc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("NodeController: %v", err)
		}
	})
	for range 50 {
		if _, err := cs.CoreV1().Nodes().Get(ctx, vkName, metav1.GetOptions{}); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the Node was not registered")
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

func TestCordonSurvivesNodeControllerUpdates(t *testing.T) {
	cs := fake.NewClientset()
	cordoner := provider.NewCordoner(cs.CoreV1().Nodes(), vkTemplate(), nil)
	runNodeController(t, cs, cordoner)
	ctx := context.Background()
	if getNode(t, cs).Spec.Unschedulable {
		t.Fatal("cordoned before any hold")
	}
	cordoner.Set(ctx, true, "STATE_LOCKED")
	if !getNode(t, cs).Spec.Unschedulable {
		t.Fatal("held: not cordoned")
	}
	updatesBefore := statusPatches(cs)
	for i := range 14 { // 3.5 s: about three status updates and lease renewals
		time.Sleep(250 * time.Millisecond)
		if !getNode(t, cs).Spec.Unschedulable {
			t.Fatalf("the cordon was lost after %v", time.Duration(i+1)*250*time.Millisecond)
		}
	}
	if n := statusPatches(cs) - updatesBefore; n < 2 {
		t.Errorf("%d status updates during the hold, want at least 2 for the check to mean anything", n)
	}
	cordoner.Set(ctx, false, "STATE_IDLE_YIELDED")
	if getNode(t, cs).Spec.Unschedulable {
		t.Error("released: still cordoned")
	}
}
