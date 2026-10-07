package provider_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

const pooled corev1.ResourceName = "timeslice.io/gpu-shadow"

// Pooled mode: the virtual node's nvidia.com/gpu follows the host's gpu-shadow allocatable.
func TestFollowHostGPUs(t *testing.T) {
	host := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "host"},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{}},
	}
	cs := fake.NewClientset(host)
	spec := provider.NewNodeSpec(provider.NodeConfig{
		Name: "vk", CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
	})
	np := provider.NewNodeProvider(&spec)
	pushed := make(chan *corev1.Node, 16)
	np.NotifyNodeStatus(t.Context(), func(n *corev1.Node) { pushed <- n })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go provider.FollowHostGPUs(ctx, cs, "host", pooled, 20*time.Millisecond, np)

	wantGPUs := func(want int64) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case n := <-pushed:
				c, a := n.Status.Capacity[provider.GPUResource], n.Status.Allocatable[provider.GPUResource]
				if c.Value() == want && a.Value() == want {
					return
				}
			case <-deadline:
				t.Fatalf("virtual node GPUs never became %d", want)
			}
		}
	}
	wantGPUs(0) // no donor: the plugin advertises nothing
	host.Status.Allocatable[pooled] = resource.MustParse("2")
	if _, err := cs.CoreV1().Nodes().Update(t.Context(), host, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	wantGPUs(2)
	if np.SetGPUs(2) {
		t.Error("no change must report false")
	}
	if provider.HostAllocatable(host, pooled) != 2 || provider.HostAllocatable(host, "x/y") != 0 {
		t.Error("HostAllocatable")
	}
}
