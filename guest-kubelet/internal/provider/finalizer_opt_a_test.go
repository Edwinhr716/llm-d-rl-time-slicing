package provider_test

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/edwinhr716/guest-kubelet/internal/testutil"
)

// D-VK-2 option a (--node-finalizer=false, the default on this branch): the virtual Node gets
// no finalizer, so a delete by anyone completes; the host ownerReference stays.

func unguardedSpec() provider.NodeConfig {
	return provider.NodeConfig{Name: vkNode, InternalIP: "10.0.0.1", HostName: "host", HostUID: hostUID}
}

func TestNodeFinalizer_A_SpecHasNoFinalizer(t *testing.T) {
	spec := provider.NewNodeSpec(unguardedSpec())
	if len(spec.Finalizers) != 0 {
		t.Errorf("finalizers = %v, want none", spec.Finalizers)
	}
	if !hasHostOwner(&spec, hostUID) {
		t.Errorf("ownerReferences = %+v, want the host", spec.OwnerReferences)
	}
}

func TestNodeFinalizer_A_EnsureCreatesWithoutFinalizer(t *testing.T) {
	client := testutil.NewClient()
	ctx, logs := logCtx()
	spec := provider.NewNodeSpec(unguardedSpec())
	action, err := provider.EnsureNodeGuard(ctx, client, &spec)
	if err != nil || action != provider.GuardCreated {
		t.Fatalf("action=%q err=%v, want created", action, err)
	}
	node := getNode(t, client)
	if len(node.Finalizers) != 0 || !hasHostOwner(node, hostUID) {
		t.Errorf("finalizers=%v owners=%+v, want no finalizer and the host owner", node.Finalizers, node.OwnerReferences)
	}
	if !strings.Contains(logs.String(), `"msg":"node registered without finalizer"`) ||
		strings.Contains(logs.String(), provider.NodeFinalizer) {
		t.Errorf("log lines: %s", logs.String())
	}
}

func TestNodeFinalizer_A_EnsureNeverAddsFinalizer(t *testing.T) {
	old := provider.NewNodeSpec(unguardedSpec())
	old.OwnerReferences = nil
	client := testutil.NewClient(&old)
	spec := provider.NewNodeSpec(unguardedSpec())
	action, err := provider.EnsureNodeGuard(context.Background(), client, &spec)
	if err != nil || action != provider.GuardUpdated {
		t.Fatalf("action=%q err=%v, want updated (owner added)", action, err)
	}
	node := getNode(t, client)
	if len(node.Finalizers) != 0 || !hasHostOwner(node, hostUID) {
		t.Errorf("finalizers=%v owners=%+v", node.Finalizers, node.OwnerReferences)
	}
	if action, err := provider.EnsureNodeGuard(context.Background(), client, &spec); err != nil || action != provider.GuardPresent {
		t.Errorf("second call: action=%q err=%v, want present", action, err)
	}
}

func TestNodeFinalizer_A_DeleteCompletes(t *testing.T) {
	spec := provider.NewNodeSpec(unguardedSpec())
	client := testutil.NewClient(&spec)
	if err := client.CoreV1().Nodes().Delete(context.Background(), vkNode, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.CoreV1().Nodes().Get(context.Background(), vkNode, metav1.GetOptions{}); err == nil {
		t.Errorf("Node still there after a delete; option a must not hold it")
	}
}
