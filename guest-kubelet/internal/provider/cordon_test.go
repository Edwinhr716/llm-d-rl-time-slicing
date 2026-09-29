package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

const cordonNode = "vk-cordon"

// cordonCluster registers the virtual Node, optionally already cordoned and annotated.
func cordonCluster(t *testing.T, unschedulable bool, annotations map[string]string) *fake.Clientset {
	t.Helper()
	node := provider.NewNodeSpec(provider.NodeConfig{
		Name: cordonNode, InternalIP: "10.0.0.1", KubeletPort: 10260, KubeletVersion: "test",
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
	})
	node.Spec.Unschedulable = unschedulable
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	for k, v := range annotations {
		node.Annotations[k] = v
	}
	return fake.NewClientset(&node)
}

func cordonGet(t *testing.T, cs *fake.Clientset) *corev1.Node {
	t.Helper()
	node, err := cs.CoreV1().Nodes().Get(context.Background(), cordonNode, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func countNodeActions(cs *fake.Clientset, verb string) int {
	n := 0
	for _, a := range cs.Actions() {
		if a.GetVerb() == verb && a.GetResource().Resource == "nodes" && a.GetSubresource() == "" {
			n++
		}
	}
	return n
}

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for len(rec.Events) > 0 {
		out = append(out, <-rec.Events)
	}
	return out
}

func TestCordonWhileHeld_NsCordon_CordonAndUncordon(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	rec := record.NewFakeRecorder(10)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, rec)
	ctx := context.Background()

	c.Set(ctx, true, "1 guest(s) suspended")
	node := cordonGet(t, cs)
	if !node.Spec.Unschedulable || node.Annotations[provider.CordonedByAnnotation] != provider.CordonedByValue {
		t.Fatalf("held: unschedulable=%v annotations=%v", node.Spec.Unschedulable, node.Annotations)
	}
	if got := drainEvents(rec); len(got) != 1 || !strings.HasPrefix(got[0], "Normal Cordoned") {
		t.Errorf("held: events %v, want one Cordoned", got)
	}

	c.Set(ctx, true, "1 guest(s) suspended") // no change within the resync: no API call
	c.Set(ctx, false, "no guest suspended")
	node = cordonGet(t, cs)
	if node.Spec.Unschedulable {
		t.Error("released: still unschedulable")
	}
	if _, ok := node.Annotations[provider.CordonedByAnnotation]; ok {
		t.Error("released: the cordoned-by annotation must be removed")
	}
	if got := drainEvents(rec); len(got) != 1 || !strings.HasPrefix(got[0], "Normal Uncordoned") {
		t.Errorf("released: events %v, want one Uncordoned", got)
	}
	if n := countNodeActions(cs, "patch"); n != 2 {
		t.Errorf("%d Node patches, want 2 (one per change)", n)
	}
}

func TestCordonWhileHeld_NsCordon_PatchesOnlyUnschedulable(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	before := cordonGet(t, cs)
	provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil).Set(context.Background(), true, "held")
	var patches []k8stesting.PatchAction
	for _, a := range cs.Actions() {
		if p, ok := a.(k8stesting.PatchAction); ok && a.GetResource().Resource == "nodes" {
			patches = append(patches, p)
		}
	}
	if len(patches) != 1 {
		t.Fatalf("%d patches, want 1", len(patches))
	}
	if patches[0].GetSubresource() != "" {
		t.Errorf("patched subresource %q, want the Node itself", patches[0].GetSubresource())
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
	after := cordonGet(t, cs)
	if len(after.Spec.Taints) != len(before.Spec.Taints) || after.Labels[provider.VirtualNodeLabel] != "true" {
		t.Error("taints or labels changed")
	}
}

func TestCordonWhileHeld_NsCordon_LeavesAdminCordonAlone(t *testing.T) {
	cs := cordonCluster(t, true, nil) // kubectl cordon: unschedulable, no annotation
	rec := record.NewFakeRecorder(10)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, rec)
	c.Set(context.Background(), true, "held")
	c.Set(context.Background(), false, "free")
	node := cordonGet(t, cs)
	if !node.Spec.Unschedulable {
		t.Error("the guest kubelet removed an admin cordon")
	}
	if _, ok := node.Annotations[provider.CordonedByAnnotation]; ok {
		t.Error("the guest kubelet claimed an admin cordon")
	}
	if n := countNodeActions(cs, "patch"); n != 0 {
		t.Errorf("%d patches, want 0", n)
	}
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("events %v, want none", got)
	}
}

func TestCordonWhileHeld_NsCordon_RestartKeepsCordon(t *testing.T) {
	// The previous guest kubelet cordoned the Node and restarted while a guest was suspended.
	owned := map[string]string{provider.CordonedByAnnotation: provider.CordonedByValue}
	cs := cordonCluster(t, true, owned)
	provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil).Set(context.Background(), true, "held")
	if !cordonGet(t, cs).Spec.Unschedulable {
		t.Error("a restart during the hold uncordoned the Node")
	}
	if n := countNodeActions(cs, "patch"); n != 0 {
		t.Errorf("%d patches, want 0", n)
	}
	// ...and the guest was resumed while the VK was down: the new process removes the cordon.
	cs2 := cordonCluster(t, true, owned)
	provider.NewCordoner(cs2.CoreV1().Nodes(), cordonNode, nil).Set(context.Background(), false, "free")
	if cordonGet(t, cs2).Spec.Unschedulable {
		t.Error("the hold ended but the Node stayed cordoned")
	}
}

func TestCordonWhileHeld_NsCordon_RetriesAfterError(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	failures := 1
	cs.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, errors.New("apiserver unavailable")
		}
		return false, nil, nil
	})
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil)
	c.Set(context.Background(), true, "held")
	if cordonGet(t, cs).Spec.Unschedulable {
		t.Fatal("the patch failed, the Node cannot be cordoned")
	}
	c.Set(context.Background(), true, "held") // next poll
	if !cordonGet(t, cs).Spec.Unschedulable {
		t.Error("the next poll did not retry the cordon")
	}
}

func TestCordonWhileHeld_NsCordon_ResyncRepairsLostCordon(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil)
	c.Resync = time.Nanosecond // every Set reads the Node again
	ctx := context.Background()
	c.Set(ctx, true, "held")
	// Someone uncordons the Node during the hold (or it was registered again without it).
	node := cordonGet(t, cs)
	node.Spec.Unschedulable = false
	delete(node.Annotations, provider.CordonedByAnnotation)
	if _, err := cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	c.Set(ctx, true, "held")
	if !cordonGet(t, cs).Spec.Unschedulable {
		t.Error("the resync did not restore the cordon")
	}
}

func TestCordonWhileHeld_NsCordon_NoReadWithinResync(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil)
	for range 10 {
		c.Set(context.Background(), true, "held")
	}
	if n := countNodeActions(cs, "get"); n != 1 {
		t.Errorf("%d Node reads for 10 polls within the resync, want 1", n)
	}
}

func TestCordonWhileHeld_NsCordon_RunFollowsHoldAndPokes(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil)
	var held atomic.Bool
	poke := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		// A long interval: only the pokes can make the loop react within the test.
		c.RunCordon(ctx, time.Hour, func() (bool, string) { return held.Load(), "test" }, poke)
		close(done)
	}()
	waitUnschedulable := func(want bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for cordonGet(t, cs).Spec.Unschedulable != want {
			if time.Now().After(deadline) {
				t.Fatalf("unschedulable never became %v", want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	held.Store(true)
	poke <- struct{}{}
	waitUnschedulable(true)
	held.Store(false)
	poke <- struct{}{}
	waitUnschedulable(false)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunCordon did not return after cancel")
	}
}

func TestCordonWhileHeld_Skip_NeverTouchesNode(t *testing.T) {
	// Option skip builds no Cordoner: the Node keeps what it was registered with.
	cs := cordonCluster(t, false, nil)
	if cordonGet(t, cs).Spec.Unschedulable {
		t.Error("a registered virtual Node must start schedulable")
	}
	if n := countNodeActions(cs, "patch"); n != 0 {
		t.Errorf("%d patches without a Cordoner, want 0", n)
	}
}
