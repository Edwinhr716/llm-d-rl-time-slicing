package provider

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
)

// Tests for pending lead decision D-NS-2 (--guest-node-label): option keep (false, the default)
// and option ns-label (true).

// evalGuestNodeController is evaluation hook H3 for D-NS-2. It builds the virtual Node as main.go
// does (main.go's default capacity, --guest-node-label=guestNodeLabel) and returns a function
// that runs the virtual-kubelet NodeController against cs until its context ends. Node status
// and the node Lease are renewed every second.
func evalGuestNodeController(
	_ context.Context, cs kubernetes.Interface, nodeName string, guestNodeLabel bool,
) (func(context.Context) error, error) {
	spec := NewNodeSpec(NodeConfig{
		Name: nodeName, InternalIP: "10.0.0.1", KubeletPort: 10260, KubeletVersion: "v1.35.8-guest-kubelet-m1",
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
		GuestNodeLabel: guestNodeLabel,
	})
	leases := cs.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nc, err := node.NewNodeController(NewNodeProvider(&spec), &spec, cs.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1WithRenewInterval(leases, node.DefaultLeaseDuration, time.Second),
		node.WithNodeStatusUpdateInterval(time.Second),
		node.WithNodePingInterval(time.Second),
	)
	if err != nil {
		return nil, err
	}
	return nc.Run, nil
}

// runGuestNodeController runs the hook until the Node exists and returns a stop function that
// waits for the controller to exit.
func runGuestNodeController(t *testing.T, cs kubernetes.Interface, guestNodeLabel bool) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run, err := evalGuestNodeController(ctx, cs, "vk-test", guestNodeLabel)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("NodeController: %v", err)
		}
	}()
	stop := func() {
		cancel()
		<-done
	}
	for range 100 {
		if _, err := cs.CoreV1().Nodes().Get(ctx, "vk-test", metav1.GetOptions{}); err == nil {
			return stop
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	t.Fatal("the Node was never registered")
	return nil
}

// registeredLabels returns the labels of the Node as the API server has it.
func registeredLabels(t *testing.T, cs kubernetes.Interface) map[string]string {
	t.Helper()
	got, err := cs.CoreV1().Nodes().Get(context.Background(), "vk-test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return got.Labels
}

func wantNodeLabels(t *testing.T, labels map[string]string, guestNodeLabel bool) {
	t.Helper()
	if labels[VirtualNodeLabel] != "true" {
		t.Errorf("%s: got %q, want \"true\" in both options", VirtualNodeLabel, labels[VirtualNodeLabel])
	}
	guest, ok := labels[GuestNodeLabel]
	switch {
	case guestNodeLabel && guest != "true":
		t.Errorf("ns-label: %s = %q, want \"true\"", GuestNodeLabel, guest)
	case !guestNodeLabel && ok:
		t.Errorf("keep: the Node must not carry %s, got %q", GuestNodeLabel, guest)
	}
}

func TestGuestNodeLabel_KeyIsTheTaintKey(t *testing.T) {
	if GuestNodeLabel != "timeslice.io/guest" {
		t.Errorf("GuestNodeLabel = %q, want timeslice.io/guest", GuestNodeLabel)
	}
	if GuestNodeLabel != GuestTaintKey {
		t.Errorf("GuestNodeLabel = %q, want the taint key %q", GuestNodeLabel, GuestTaintKey)
	}
}

func TestGuestNodeLabel_Keep_NodeSpec(t *testing.T) {
	wantNodeLabels(t, NewNodeSpec(NodeConfig{Name: "vk-test"}).Labels, false)
}

func TestGuestNodeLabel_NSLabel_NodeSpec(t *testing.T) {
	spec := NewNodeSpec(NodeConfig{Name: "vk-test", GuestNodeLabel: true})
	wantNodeLabels(t, spec.Labels, true)
	if spec.Labels["type"] != "virtual-kubelet" || len(spec.Spec.Taints) != 1 || spec.Spec.Taints[0].Key != GuestTaintKey {
		t.Errorf("ns-label must only add a label: labels %v taints %v", spec.Labels, spec.Spec.Taints)
	}
}

// The labels survive NodeController status and Lease updates, and a restart with the same flag.
func testGuestNodeLabelRegistered(t *testing.T, guestNodeLabel bool) {
	t.Helper()
	cs := fake.NewClientset()
	stop := runGuestNodeController(t, cs, guestNodeLabel)
	wantNodeLabels(t, registeredLabels(t, cs), guestNodeLabel)
	time.Sleep(2500 * time.Millisecond) // two status updates
	wantNodeLabels(t, registeredLabels(t, cs), guestNodeLabel)
	stop()

	stop = runGuestNodeController(t, cs, guestNodeLabel)
	defer stop()
	wantNodeLabels(t, registeredLabels(t, cs), guestNodeLabel)
	time.Sleep(1500 * time.Millisecond)
	wantNodeLabels(t, registeredLabels(t, cs), guestNodeLabel)
}

func TestGuestNodeLabel_Keep_Registered(t *testing.T) { testGuestNodeLabelRegistered(t, false) }

func TestGuestNodeLabel_NSLabel_Registered(t *testing.T) { testGuestNodeLabelRegistered(t, true) }
