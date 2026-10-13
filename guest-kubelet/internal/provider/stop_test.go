package provider

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func vnode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{VirtualNodeLabel: "true"},
			Finalizers: []string{NodeFinalizer}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

func TestMarkStoppedThenClear(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientset(vnode("vk-a"))
	nodes := c.CoreV1().Nodes()
	if err := MarkStopped(ctx, nodes, "vk-a", "vk-pod-1"); err != nil {
		t.Fatal(err)
	}
	n, _ := nodes.Get(ctx, "vk-a", metav1.GetOptions{})
	if !n.Spec.Unschedulable || n.Annotations[StoppedByAnnotation] != "vk-pod-1" || n.Annotations[CordonedByAnnotation] != CordonedByValue {
		t.Fatalf("not cordoned: %+v %+v", n.Spec, n.Annotations)
	}
	if len(n.Finalizers) != 1 {
		t.Errorf("restart keeps the finalizer: %v", n.Finalizers)
	}
	if n.Status.Conditions[0].Status != corev1.ConditionFalse || n.Status.Conditions[0].Reason != ReasonStopped {
		t.Errorf("ready = %+v", n.Status.Conditions)
	}
	// The stopping pod itself never clears its own mark.
	if cleared, _ := ClearStopped(ctx, nodes, "vk-a", "vk-pod-1"); cleared {
		t.Error("cleared by the pod that stopped")
	}
	if cleared, err := ClearStopped(ctx, nodes, "vk-a", "vk-pod-2"); err != nil || !cleared {
		t.Fatalf("ClearStopped = %v, %v", cleared, err)
	}
	n, _ = nodes.Get(ctx, "vk-a", metav1.GetOptions{})
	if n.Spec.Unschedulable || n.Annotations[StoppedByAnnotation] != "" || n.Annotations[CordonedByAnnotation] != "" {
		t.Fatalf("not cleared: %+v %+v", n.Spec, n.Annotations)
	}
}

func TestClearStoppedLeavesAdminCordon(t *testing.T) {
	ctx := context.Background()
	n := vnode("vk-a")
	n.Spec.Unschedulable = true
	n.Annotations = map[string]string{StoppedByAnnotation: "old"}
	c := fake.NewClientset(n)
	if _, err := ClearStopped(ctx, c.CoreV1().Nodes(), "vk-a", "new"); err != nil {
		t.Fatal(err)
	}
	got, _ := c.CoreV1().Nodes().Get(ctx, "vk-a", metav1.GetOptions{})
	if !got.Spec.Unschedulable {
		t.Error("removed a cordon the guest kubelet did not set")
	}
}

func TestMarkStoppedRefusesRealNode(t *testing.T) {
	real := vnode("gke-x")
	real.Labels = nil
	c := fake.NewClientset(real)
	if err := MarkStopped(context.Background(), c.CoreV1().Nodes(), "gke-x", "p"); err == nil {
		t.Fatal("marked a real Node")
	}
	if err := MarkStopped(context.Background(), c.CoreV1().Nodes(), "missing", "p"); err != nil {
		t.Fatalf("missing Node: %v", err)
	}
}

func TestCleanupReleasedNode(t *testing.T) {
	ctx := context.Background()
	mirror := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: "g-m", UID: "m1",
		Labels: map[string]string{"timeslice.io/mirror-node": "vk-a"}}, Spec: corev1.PodSpec{NodeName: "host-a"}}
	otherMirror := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: "h-m", UID: "m2",
		Labels: map[string]string{"timeslice.io/mirror-node": "vk-b"}}, Spec: corev1.PodSpec{NodeName: "host-b"}}
	now := metav1.Now()
	terminating := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: "g", UID: "g1",
		DeletionTimestamp: &now, Finalizers: []string{"test/hold"}}, Spec: corev1.PodSpec{NodeName: "vk-a"}}
	pending := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: "p", UID: "p1"},
		Spec: corev1.PodSpec{NodeName: "vk-a"}}
	elsewhere := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: "e", UID: "e1"},
		Spec: corev1.PodSpec{NodeName: "vk-b"}}
	c := fake.NewClientset(mirror, otherMirror, terminating, pending, elsewhere)

	mirrors, guests, err := CleanupReleasedNode(ctx, c.CoreV1(), "vk-a")
	if err != nil || mirrors != 1 || guests != 2 {
		t.Fatalf("CleanupReleasedNode = %d mirrors, %d guests, %v; want 1, 2, nil", mirrors, guests, err)
	}
	var forced, graceful []string
	for _, a := range c.Actions() {
		del, ok := a.(k8stesting.DeleteAction)
		if !ok {
			continue
		}
		if g := del.GetDeleteOptions().GracePeriodSeconds; g != nil && *g == 0 {
			forced = append(forced, del.GetName())
		} else {
			graceful = append(graceful, del.GetName())
		}
	}
	slices.Sort(forced)
	if !slices.Equal(forced, []string{"g", "p"}) || !slices.Equal(graceful, []string{"g-m"}) {
		t.Errorf("force-deleted %v (want [g p]), deleted with grace %v (want [g-m])", forced, graceful)
	}
	for _, name := range []string{"h-m", "e"} {
		if _, err := c.CoreV1().Pods("batch").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("%s belongs to another virtual Node and must stay: %v", name, err)
		}
	}
	// Nothing left: a second run deletes nothing and is not an error.
	if m, g, err := CleanupReleasedNode(ctx, c.CoreV1(), "vk-a"); err != nil || m != 0 || g != 0 {
		t.Errorf("second run = %d, %d, %v", m, g, err)
	}
}
