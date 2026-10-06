package provider

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
