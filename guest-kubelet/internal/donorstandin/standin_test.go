package donorstandin_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/donorstandin"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/edwinhr716/guest-kubelet/internal/testutil"
)

const (
	vkNode   = "vk-abcd"
	hostName = "host-abcd"
)

func hostNode(uid types.UID) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: hostName, UID: uid}}
}

func virtualNode() *corev1.Node {
	node := provider.NewNodeSpec(provider.NodeConfig{Name: vkNode, HostName: hostName, HostUID: "host-uid"})
	return &node
}

type fixture struct {
	client  *fake.Clientset
	standIn *donorstandin.StandIn
	ctx     context.Context
	logs    *bytes.Buffer
}

func newFixture(t *testing.T, objs ...*corev1.Node) fixture {
	t.Helper()
	client := testutil.NewClient()
	for _, obj := range objs {
		if err := client.Tracker().Add(obj); err != nil {
			t.Fatal(err)
		}
	}
	standIn, err := donorstandin.New(client, donorstandin.Config{VirtualNode: vkNode, HostNode: hostName})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	ctx := log.WithLogger(context.Background(), vkslog.FromSlog(slog.New(slog.NewJSONHandler(&buf, nil))))
	return fixture{client: client, standIn: standIn, ctx: ctx, logs: &buf}
}

func (fx fixture) check(t *testing.T, want bool) {
	t.Helper()
	got, err := fx.standIn.Check(fx.ctx)
	if err != nil || got != want {
		t.Fatalf("Check() = %v, %v; want %v, nil", got, err, want)
	}
}

func (fx fixture) vkNodeGone(t *testing.T) bool {
	t.Helper()
	_, err := fx.client.CoreV1().Nodes().Get(fx.ctx, vkNode, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return apierrors.IsNotFound(err)
}

func (fx fixture) deleteHost(t *testing.T) {
	t.Helper()
	if err := fx.client.CoreV1().Nodes().Delete(fx.ctx, hostName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestDonorStandIn_HostPresentDoesNothing(t *testing.T) {
	fx := newFixture(t, hostNode("host-uid"), virtualNode())
	fx.check(t, false)
	fx.check(t, false)
	if fx.vkNodeGone(t) {
		t.Error("the virtual Node must stay while the host exists")
	}
}

func TestDonorStandIn_HostGoneReleasesNode(t *testing.T) {
	fx := newFixture(t, hostNode("host-uid"), virtualNode())
	fx.check(t, false)
	fx.deleteHost(t)
	fx.check(t, true)
	if !fx.vkNodeGone(t) {
		t.Error("the virtual Node must be gone after the host is")
	}
	for _, want := range []string{
		`"msg":"node finalizer removed"`, `"msg":"virtual node released"`,
		`"reason":"host-gone"`, `"node":"` + vkNode + `"`,
	} {
		if !strings.Contains(fx.logs.String(), want) {
			t.Errorf("logs lack %s: %s", want, fx.logs.String())
		}
	}
	fx.check(t, false) // nothing left to release; no error
}

func TestDonorStandIn_HostRecreatedReleasesNode(t *testing.T) {
	fx := newFixture(t, hostNode("host-uid"), virtualNode())
	fx.check(t, false)
	fx.deleteHost(t)
	if _, err := fx.client.CoreV1().Nodes().Create(fx.ctx, hostNode("host-uid-2"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	fx.check(t, true)
	if !fx.vkNodeGone(t) {
		t.Error("a recreated host is a new machine; the old virtual Node must go")
	}
}

// With the ownerReference, the garbage collector may delete the virtual Node first; the
// finalizer holds it until the stand-in releases it.
func TestDonorStandIn_ReleasesNodeAlreadyBeingDeleted(t *testing.T) {
	fx := newFixture(t, hostNode("host-uid"), virtualNode())
	fx.check(t, false)
	if err := fx.client.CoreV1().Nodes().Delete(fx.ctx, vkNode, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if fx.vkNodeGone(t) {
		t.Fatal("the finalizer must hold the virtual Node")
	}
	fx.deleteHost(t)
	fx.check(t, true)
	if !fx.vkNodeGone(t) {
		t.Error("the virtual Node must be gone after release")
	}
}

func TestDonorStandIn_NoVirtualNode(t *testing.T) {
	fx := newFixture(t)
	fx.check(t, false)
}

func TestDonorStandIn_RefusesRealNode(t *testing.T) {
	notVirtual := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: vkNode}}
	fx := newFixture(t, notVirtual)
	if _, err := fx.standIn.Check(fx.ctx); err == nil {
		t.Error("want an error: the named Node is not a virtual Node")
	}
	if fx.vkNodeGone(t) {
		t.Error("a Node without the virtual-node label must never be deleted")
	}
}

func TestDonorStandIn_NeedsBothNames(t *testing.T) {
	client := testutil.NewClient()
	for _, cfg := range []donorstandin.Config{{VirtualNode: vkNode}, {HostNode: hostName}} {
		if _, err := donorstandin.New(client, cfg); err == nil {
			t.Errorf("New(%+v): want an error", cfg)
		}
	}
}
