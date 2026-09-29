package provider_test

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/edwinhr716/guest-kubelet/internal/testutil"
)

const (
	vkNode  = "vk-abcd"
	hostOld = types.UID("host-uid-old")
	hostUID = types.UID("host-uid")
)

func guardedSpec() corev1.Node {
	return provider.NewNodeSpec(provider.NodeConfig{Name: vkNode, InternalIP: "10.0.0.1", HostName: "host", HostUID: hostUID})
}

// logCtx returns a context whose logger writes JSON lines to the returned buffer.
func logCtx() (context.Context, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := vkslog.FromSlog(slog.New(slog.NewJSONHandler(&buf, nil)))
	return log.WithLogger(context.Background(), logger), &buf
}

func getNode(t *testing.T, client kubernetes.Interface) *corev1.Node {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(context.Background(), vkNode, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", vkNode, err)
	}
	return node
}

func hasHostOwner(node *corev1.Node, uid types.UID) bool {
	return slices.ContainsFunc(node.OwnerReferences, func(ref metav1.OwnerReference) bool {
		return ref.Kind == "Node" && ref.Name == "host" && ref.UID == uid
	})
}

func TestNodeGuard_SpecCarriesFinalizerAndHostOwner(t *testing.T) {
	spec := guardedSpec()
	if !slices.Contains(spec.Finalizers, provider.NodeFinalizer) {
		t.Errorf("finalizers = %v, want %s", spec.Finalizers, provider.NodeFinalizer)
	}
	if len(spec.OwnerReferences) != 1 || !hasHostOwner(&spec, hostUID) {
		t.Errorf("ownerReferences = %+v, want one to Node/host", spec.OwnerReferences)
	}
	ref := spec.OwnerReferences[0]
	if ref.APIVersion != "v1" || ref.Controller != nil || ref.BlockOwnerDeletion != nil {
		t.Errorf("owner ref must be a plain v1 reference, got %+v", ref)
	}
	noHost := provider.NewNodeSpec(provider.NodeConfig{Name: vkNode})
	if len(noHost.OwnerReferences) != 0 {
		t.Errorf("no host UID: want no ownerReferences, got %+v", noHost.OwnerReferences)
	}
}

func TestNodeGuard_EnsureCreatesNode(t *testing.T) {
	client := testutil.NewClient()
	ctx, logs := logCtx()
	spec := guardedSpec()
	action, err := provider.EnsureNodeGuard(ctx, client, &spec)
	if err != nil || action != provider.GuardCreated {
		t.Fatalf("action=%q err=%v, want created", action, err)
	}
	node := getNode(t, client)
	if !slices.Contains(node.Finalizers, provider.NodeFinalizer) || !hasHostOwner(node, hostUID) {
		t.Errorf("created Node lacks the guard: finalizers=%v owners=%+v", node.Finalizers, node.OwnerReferences)
	}
	if !strings.Contains(logs.String(), `"msg":"node finalizer set"`) ||
		!strings.Contains(logs.String(), `"finalizer":"`+provider.NodeFinalizer+`"`) {
		t.Errorf("missing log line, got %s", logs.String())
	}
}

func TestNodeGuard_EnsureAddsToExistingNode(t *testing.T) {
	old := guardedSpec()
	old.Finalizers = []string{"other.io/keep"}
	other := metav1.OwnerReference{APIVersion: "example.io/v1", Kind: "Thing", Name: "t", UID: "thing-uid"}
	old.OwnerReferences = []metav1.OwnerReference{provider.HostOwnerRef("host", hostOld), other}
	client := testutil.NewClient(&old)
	spec := guardedSpec()
	ctx := context.Background()

	action, err := provider.EnsureNodeGuard(ctx, client, &spec)
	if err != nil || action != provider.GuardUpdated {
		t.Fatalf("action=%q err=%v, want updated", action, err)
	}
	node := getNode(t, client)
	if !slices.Contains(node.Finalizers, provider.NodeFinalizer) || !slices.Contains(node.Finalizers, "other.io/keep") {
		t.Errorf("finalizers = %v", node.Finalizers)
	}
	if !hasHostOwner(node, hostUID) || hasHostOwner(node, hostOld) || !slices.Contains(node.OwnerReferences, other) {
		t.Errorf("ownerReferences = %+v, want the new host UID only, other owners kept", node.OwnerReferences)
	}
	if action, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil || action != provider.GuardPresent {
		t.Errorf("second ensure: action=%q err=%v, want present", action, err)
	}
}

// An outage: the cloud node lifecycle controller deletes the Node while no VK runs. The
// finalizer holds it (Terminating), so it still exists for its guests, and a returning VK
// leaves it alone.
func TestNodeGuard_OutageDeleteIsHeld(t *testing.T) {
	client := testutil.NewClient()
	ctx := context.Background()
	spec := guardedSpec()
	if _, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Nodes().Delete(ctx, vkNode, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	node := getNode(t, client)
	if node.DeletionTimestamp == nil {
		t.Fatal("want the Node held with a deletionTimestamp")
	}
	action, err := provider.EnsureNodeGuard(ctx, client, &spec)
	if err != nil || action != provider.GuardTerminating {
		t.Errorf("action=%q err=%v, want terminating", action, err)
	}
	if after := getNode(t, client); !slices.Equal(after.Finalizers, node.Finalizers) {
		t.Errorf("a terminating Node must be left alone: %v -> %v", node.Finalizers, after.Finalizers)
	}
}

func TestNodeGuard_ReleaseDeregister(t *testing.T) {
	client := testutil.NewClient()
	ctx, logs := logCtx()
	spec := guardedSpec()
	if _, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil {
		t.Fatal(err)
	}
	released, err := provider.ReleaseNode(ctx, client, vkNode, provider.ReasonDeregister)
	if err != nil || !released {
		t.Fatalf("released=%v err=%v", released, err)
	}
	if _, err := client.CoreV1().Nodes().Get(ctx, vkNode, metav1.GetOptions{}); err == nil {
		t.Error("the Node must be gone after release")
	}
	if !strings.Contains(logs.String(), `"msg":"node finalizer removed"`) ||
		!strings.Contains(logs.String(), `"reason":"deregister"`) {
		t.Errorf("missing removal log line, got %s", logs.String())
	}
	if released, err := provider.ReleaseNode(ctx, client, vkNode, provider.ReasonDeregister); err != nil || released {
		t.Errorf("release of a missing Node: released=%v err=%v, want false, nil", released, err)
	}
}

func TestNodeGuard_ReleaseKeepsOtherFinalizers(t *testing.T) {
	node := guardedSpec()
	node.Finalizers = append(node.Finalizers, "other.io/keep")
	now := metav1.Now()
	node.DeletionTimestamp = &now
	client := testutil.NewClient(&node)
	released, err := provider.ReleaseNode(context.Background(), client, vkNode, provider.ReasonHostGone)
	if err != nil || !released {
		t.Fatalf("released=%v err=%v", released, err)
	}
	if got := getNode(t, client).Finalizers; !slices.Equal(got, []string{"other.io/keep"}) {
		t.Errorf("finalizers = %v, want only other.io/keep", got)
	}
}

func TestNodeGuard_ReleaseRefusesRealNode(t *testing.T) {
	hostNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: vkNode, Finalizers: []string{provider.NodeFinalizer}}}
	client := testutil.NewClient(hostNode)
	if _, err := provider.ReleaseNode(context.Background(), client, vkNode, provider.ReasonHostGone); err == nil {
		t.Error("want an error for a Node without the virtual-node label")
	}
	if node := getNode(t, client); node.DeletionTimestamp != nil || len(node.Finalizers) != 1 {
		t.Errorf("a real Node must be untouched, got %+v", node.ObjectMeta)
	}
}
