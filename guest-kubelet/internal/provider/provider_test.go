package provider

import (
	"context"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
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

func dsPod(name string) *corev1.Pod {
	p := guestPod(name)
	p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	return p
}

// fakeBackend records calls.
type fakeBackend struct {
	created, deleted []string
	cb               func(*corev1.Pod)
}

func (f *fakeBackend) Create(_ context.Context, g *corev1.Pod) error {
	f.created = append(f.created, g.Name)
	return nil
}
func (f *fakeBackend) Delete(_ context.Context, g *corev1.Pod) error {
	f.deleted = append(f.deleted, g.Name)
	return nil
}
func (f *fakeBackend) Get(ns, name string) (*corev1.Pod, error) {
	return nil, errdefs.NotFoundf("%s/%s", ns, name)
}
func (f *fakeBackend) List() ([]*corev1.Pod, error)           { return nil, nil }
func (f *fakeBackend) SetStatusCallback(cb func(*corev1.Pod)) { f.cb = cb }

func TestIsGuest(t *testing.T) {
	if IsGuest(dsPod("ds")) {
		t.Error("a blanket {operator: Exists} toleration must not make a pod a guest")
	}
	if !IsGuest(guestPod("g")) {
		t.Error("a pod tolerating the guest taint by key is a guest")
	}
}

func TestCreateAndDeleteOnlyGuests(t *testing.T) {
	b := &fakeBackend{}
	p := New(b)
	ctx := context.Background()
	for _, pod := range []*corev1.Pod{guestPod("g"), dsPod("ds")} {
		if err := p.CreatePod(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.created) != 1 || b.created[0] != "g" {
		t.Errorf("only the guest should reach the backend, got %v", b.created)
	}
	if err := p.DeletePod(ctx, dsPod("ds")); !errdefs.IsNotFound(err) {
		t.Errorf("deleting a non-guest: want NotFound, got %v", err)
	}
	if err := p.DeletePod(ctx, guestPod("g")); err != nil || len(b.deleted) != 1 {
		t.Errorf("guest delete: err=%v deleted=%v", err, b.deleted)
	}
}

func TestNotifyPodsFiltersNonGuests(t *testing.T) {
	b := &fakeBackend{}
	p := New(b)
	var got []string
	p.NotifyPods(context.Background(), func(pod *corev1.Pod) { got = append(got, pod.Name) })
	b.cb(guestPod("g"))
	b.cb(dsPod("ds"))
	if len(got) != 1 || got[0] != "g" {
		t.Errorf("notified %v", got)
	}
}

func TestGuestOnlyRecorder(t *testing.T) {
	fake := record.NewFakeRecorder(10)
	r := GuestOnlyRecorder{EventRecorder: fake}
	r.Event(dsPod("ds"), corev1.EventTypeNormal, "ProviderCreateSuccess", "x")
	r.Eventf(guestPod("g"), corev1.EventTypeNormal, "ProviderCreateSuccess", "%s", "x")
	r.Event(&corev1.Node{}, corev1.EventTypeNormal, "NodeReady", "x")
	if n := len(fake.Events); n != 2 {
		t.Errorf("want 2 events (guest + node), got %d", n)
	}
}

func TestNewNodeSpec(t *testing.T) {
	n := NewNodeSpec(NodeConfig{
		Name: "vk-test", InternalIP: "10.0.0.1", KubeletPort: 10260, GPUs: 1,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
		ProviderID: "gce://p/z/i",
	})
	if n.Labels[VirtualNodeLabel] != "true" || n.Labels["type"] != "virtual-kubelet" {
		t.Errorf("labels: %v", n.Labels)
	}
	for _, l := range []string{"cloud.google.com/gke-nodepool", "kubernetes.io/os"} {
		if _, ok := n.Labels[l]; ok {
			t.Errorf("the virtual node must not have label %s", l)
		}
	}
	if n.Spec.ProviderID != "gce://p/z/i" {
		t.Errorf("providerID: %q", n.Spec.ProviderID)
	}
	if n.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] != "true" {
		t.Error("scale-down must be disabled")
	}
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != GuestTaintKey {
		t.Errorf("taints: %v", n.Spec.Taints)
	}
	if g := n.Status.Capacity[GPUResource]; g.Value() != 1 {
		t.Errorf("gpu capacity: %v", g)
	}
}
