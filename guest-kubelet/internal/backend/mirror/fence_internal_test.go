package mirror

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

func hostNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "real-node"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule},
		}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}},
	}
}

func donorPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "donor", UID: "donor-uid",
			Labels: map[string]string{"timeslice.io/donor": "true"},
		},
		Spec:   corev1.PodSpec{NodeName: "real-node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func noDRACalls(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		if action.GetResource().Group == "resource.k8s.io" {
			t.Errorf("made a resource.k8s.io call: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func getNode(t *testing.T, hn *harness) *corev1.Node {
	t.Helper()
	node, err := hn.client.CoreV1().Nodes().Get(context.Background(), "real-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func fenceTaint(node *corev1.Node) string {
	for _, taint := range node.Spec.Taints {
		if taint.Key == FenceTaintKey {
			return taint.Value
		}
	}
	return ""
}

// firstAction returns the index of the first action that matches, or -1.
func firstAction(actions []k8stesting.Action, match func(k8stesting.Action) bool) int {
	for i, action := range actions {
		if match(action) {
			return i
		}
	}
	return -1
}

func isFenceTaintPatch(action k8stesting.Action) bool {
	patch, isPatch := action.(k8stesting.PatchAction)
	return isPatch && action.GetResource().Resource == "nodes" &&
		strings.Contains(string(patch.GetPatch()), FenceTaintKey)
}

func isMirrorDelete(action k8stesting.Action) bool {
	del, isDelete := action.(k8stesting.DeleteAction)
	return isDelete && del.GetName() == "vllm-m"
}

func TestDonorGoneFencesAndStopsMirror(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	hn := newHarness(t, ref(pooledOptions(rec)), pooledHost(2), donorPod())
	guest := testGuest()
	hn.addGuest(guest)
	if err := hn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	hn.waitInformer("guest-uid")
	ctx := context.Background()
	hn.client.ClearActions()

	// The donor is deleted (grace period running): deletionTimestamp is set.
	donor, err := hn.client.CoreV1().Pods("ns").Get(ctx, "donor", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	donor.DeletionTimestamp = &now
	if _, err := hn.client.CoreV1().Pods("ns").Update(ctx, donor, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && hn.mirror("vllm-m") != nil {
		time.Sleep(20 * time.Millisecond)
	}
	if hn.mirror("vllm-m") != nil {
		t.Fatal("mirror must be stopped when its donor goes")
	}
	// The taint must have gone on before the mirror delete.
	actions := hn.client.Actions()
	taintAt, deleteAt := firstAction(actions, isFenceTaintPatch), firstAction(actions, isMirrorDelete)
	if taintAt < 0 || deleteAt < 0 || taintAt > deleteAt {
		t.Errorf("want the fence taint (action %d) before the mirror delete (action %d)", taintAt, deleteAt)
	}
	// Once no mirror holds the GPU, the taint comes off.
	for time.Now().Before(deadline) {
		if node := getNode(t, hn); fenceTaint(node) == "" && len(node.Spec.Taints) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if node := getNode(t, hn); fenceTaint(node) != "" || len(node.Spec.Taints) != 1 {
		t.Errorf("fence taint must be removed and other taints kept: %v", node.Spec.Taints)
	}
	var sawDonorGone bool
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, EventDonorGone) {
			sawDonorGone = true
		}
	}
	if !sawDonorGone {
		t.Error("want a DonorGone event on the guest")
	}
	noDRACalls(t, hn.client.Actions())
}

func TestStaleFenceTaintRemovedAtStart(t *testing.T) {
	node := pooledHost(2)
	node.Spec.Taints = append(node.Spec.Taints,
		corev1.Taint{Key: FenceTaintKey, Value: "vk-x", Effect: corev1.TaintEffectNoSchedule})
	hn := newHarness(t, ref(pooledOptions(nil)), node, donorPod())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fenceTaint(getNode(t, hn)) == "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("a fence taint left by an earlier run must be removed when nothing is fenced")
}

func TestForeignFenceTaintLeftAlone(t *testing.T) {
	node := pooledHost(2)
	node.Spec.Taints = append(node.Spec.Taints,
		corev1.Taint{Key: FenceTaintKey, Value: "vk-other", Effect: corev1.TaintEffectNoSchedule})
	hn := newHarness(t, ref(pooledOptions(nil)), node, donorPod())
	hn.b.reconcileFence(context.Background())
	if cur := getNode(t, hn); fenceTaint(cur) != "vk-other" {
		t.Errorf("another guest kubelet's fence must stay: %v", cur.Spec.Taints)
	}
}
