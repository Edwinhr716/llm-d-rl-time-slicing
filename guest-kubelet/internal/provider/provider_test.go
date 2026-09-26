package provider

import (
	"context"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func guestPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		Spec: corev1.PodSpec{
			Containers:  []corev1.Container{{Name: "c", Image: "busybox"}},
			Tolerations: []corev1.Toleration{{Key: GuestTaintKey, Operator: corev1.TolerationOpExists}},
		},
	}
}

func newTestProvider() (*Provider, *[]*corev1.Pod) {
	p := New("10.0.0.1")
	var got []*corev1.Pod
	p.NotifyPods(context.Background(), func(pod *corev1.Pod) { got = append(got, pod) })
	return p, &got
}

func TestIsGuest(t *testing.T) {
	blanket := guestPod("ds")
	blanket.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	if IsGuest(blanket) {
		t.Error("a blanket {operator: Exists} toleration must not make a pod a guest")
	}
	if !IsGuest(guestPod("g")) {
		t.Error("a pod tolerating the guest taint by key is a guest")
	}
}

func TestCreatePodReportsRunningWithFakeIP(t *testing.T) {
	p, got := newTestProvider()
	ctx := context.Background()

	if err := p.CreatePod(ctx, guestPod("a")); err != nil {
		t.Fatal(err)
	}
	if err := p.CreatePod(ctx, guestPod("b")); err != nil {
		t.Fatal(err)
	}

	if len(*got) != 2 {
		t.Fatalf("want 2 notifications, got %d", len(*got))
	}
	a := (*got)[0]
	if a.Status.Phase != corev1.PodRunning || a.Status.PodIP != "198.18.0.1" || a.Status.HostIP != "10.0.0.1" {
		t.Errorf("unexpected status: phase=%s podIP=%s hostIP=%s", a.Status.Phase, a.Status.PodIP, a.Status.HostIP)
	}
	if !a.Status.ContainerStatuses[0].Ready {
		t.Error("container should be ready")
	}
	if b := (*got)[1]; b.Status.PodIP != "198.18.0.2" {
		t.Errorf("second pod should get the next IP, got %s", b.Status.PodIP)
	}
	if st, err := p.GetPodStatus(ctx, "ns", "a"); err != nil || st.Phase != corev1.PodRunning {
		t.Errorf("GetPodStatus: %v, %v", st, err)
	}
}

func TestCreatePodIgnoresNonGuest(t *testing.T) {
	p, got := newTestProvider()
	ds := guestPod("ds")
	ds.Spec.Tolerations = nil

	if err := p.CreatePod(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 0 {
		t.Error("non-guest pod must not produce a status update")
	}
	if _, err := p.GetPod(context.Background(), "ns", "ds"); !errdefs.IsNotFound(err) {
		t.Errorf("want NotFound, got %v", err)
	}
}

func TestDeletePodReportsTerminated(t *testing.T) {
	p, got := newTestProvider()
	ctx := context.Background()
	if err := p.CreatePod(ctx, guestPod("a")); err != nil {
		t.Fatal(err)
	}

	if err := p.DeletePod(ctx, guestPod("a")); err != nil {
		t.Fatal(err)
	}
	last := (*got)[len(*got)-1]
	if last.Status.Phase != corev1.PodSucceeded || last.Status.ContainerStatuses[0].State.Terminated == nil {
		t.Errorf("want Succeeded with terminated containers, got %+v", last.Status)
	}
	if _, err := p.GetPod(ctx, "ns", "a"); !errdefs.IsNotFound(err) {
		t.Errorf("want NotFound after delete, got %v", err)
	}
	if err := p.DeletePod(ctx, guestPod("a")); !errdefs.IsNotFound(err) {
		t.Errorf("second delete: want NotFound, got %v", err)
	}
}

func TestNewNodeSpec(t *testing.T) {
	n := NewNodeSpec(NodeConfig{
		Name: "vk-test", InternalIP: "10.0.0.1", KubeletPort: 10260, GPUs: 1,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
	})
	if n.Labels[VirtualNodeLabel] != "true" || n.Labels["type"] != "virtual-kubelet" {
		t.Errorf("labels: %v", n.Labels)
	}
	if _, ok := n.Labels["cloud.google.com/gke-nodepool"]; ok {
		t.Error("the virtual node must not claim a GKE node pool")
	}
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != GuestTaintKey {
		t.Errorf("taints: %v", n.Spec.Taints)
	}
	if g := n.Status.Capacity[GPUResource]; g.Value() != 1 {
		t.Errorf("gpu capacity: %v", g)
	}
	if n.Status.DaemonEndpoints.KubeletEndpoint.Port != 10260 {
		t.Error("kubelet port should be 10260")
	}
}
