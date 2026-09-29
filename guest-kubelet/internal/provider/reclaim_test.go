package provider_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/edwinhr716/guest-kubelet/internal/testutil"
)

// heldNode is the virtual Node as a VK outage leaves it: deleted by someone else, held by the
// finalizer, owned by the live host.
func heldNode() *corev1.Node {
	node := guardedSpec()
	node.UID = "vk-uid-1"
	now := metav1.Now()
	node.DeletionTimestamp = &now
	return &node
}

func boundGuest() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "guest-0", Namespace: "default", UID: "guest-uid"},
		Spec:       corev1.PodSpec{NodeName: vkNode},
	}
}

// The outage path end to end: the cloud node lifecycle controller deletes the Node while no
// VK runs, the finalizer holds it, and the returning VK replaces it with a fresh Node that has
// no deletionTimestamp and carries the guard again. The guest bound to the Node is untouched.
func TestReclaim_OutageDeleteIsReplaced(t *testing.T) {
	client := testutil.NewClient(boundGuest())
	ctx, logs := logCtx()
	spec := guardedSpec()
	if _, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Nodes().Delete(ctx, vkNode, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if action, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil || action != provider.GuardTerminating {
		t.Fatalf("after the outage delete: action=%q err=%v, want terminating", action, err)
	}

	got, err := provider.ReclaimNode(ctx, client, &spec, time.Second)
	if err != nil || got != provider.ReclaimReplaced {
		t.Fatalf("reclaim=%q err=%v, want replaced", got, err)
	}
	node := getVKNode(t, client)
	if node.DeletionTimestamp != nil {
		t.Errorf("the reclaimed Node must not be Terminating: %v", node.DeletionTimestamp)
	}
	if !slices.Equal(node.Finalizers, []string{provider.NodeFinalizer}) || !hasHostOwner(node, hostUID) {
		t.Errorf("reclaimed Node lacks the guard: finalizers=%v owners=%+v", node.Finalizers, node.OwnerReferences)
	}
	if node.Labels[provider.VirtualNodeLabel] != "true" || len(node.Spec.Taints) != 1 {
		t.Errorf("reclaimed Node is not the spec: labels=%v taints=%v", node.Labels, node.Spec.Taints)
	}
	if action, err := provider.EnsureNodeGuard(ctx, client, &spec); err != nil || action != provider.GuardPresent {
		t.Errorf("ensure after reclaim: action=%q err=%v, want present", action, err)
	}
	if _, err := client.CoreV1().Pods("default").Get(ctx, "guest-0", metav1.GetOptions{}); err != nil {
		t.Errorf("the bound guest must survive a reclaim: %v", err)
	}
	for _, want := range []string{`"msg":"node finalizer removed"`, `"reason":"reclaim"`, `"msg":"node reclaimed"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %s in logs: %s", want, logs.String())
		}
	}
}

func TestReclaim_FreshUIDReplacesOld(t *testing.T) {
	client := testutil.NewClient(heldNode())
	spec := guardedSpec()
	got, err := provider.ReclaimNode(context.Background(), client, &spec, time.Second)
	if err != nil || got != provider.ReclaimReplaced {
		t.Fatalf("reclaim=%q err=%v, want replaced", got, err)
	}
	if node := getVKNode(t, client); node.UID == "vk-uid-1" || node.DeletionTimestamp != nil {
		t.Errorf("want a new Node object, got uid=%s deletionTimestamp=%v", node.UID, node.DeletionTimestamp)
	}
}

func TestReclaim_NothingToDo(t *testing.T) {
	ctx := context.Background()
	spec := guardedSpec()
	if got, err := provider.ReclaimNode(ctx, testutil.NewClient(), &spec, time.Second); err != nil || got != provider.ReclaimNone {
		t.Errorf("missing Node: reclaim=%q err=%v, want none", got, err)
	}
	live := guardedSpec()
	live.UID = "vk-uid-1"
	client := testutil.NewClient(&live)
	if got, err := provider.ReclaimNode(ctx, client, &spec, time.Second); err != nil || got != provider.ReclaimNone {
		t.Errorf("live Node: reclaim=%q err=%v, want none", got, err)
	}
	if node := getVKNode(t, client); node.UID != "vk-uid-1" {
		t.Errorf("a live Node must be left alone, uid now %s", node.UID)
	}
}

// Nodes that are not plainly the VK's own on a live host stay held for the donor controller.
func TestReclaim_SkipsNodesNotOurs(t *testing.T) {
	cases := map[string]func(*corev1.Node){
		"other finalizer": func(n *corev1.Node) { n.Finalizers = append(n.Finalizers, "other.io/keep") },
		"recreated host": func(n *corev1.Node) {
			n.OwnerReferences = []metav1.OwnerReference{provider.HostOwnerRef("host", hostOld)}
		},
		"no host owner":       func(n *corev1.Node) { n.OwnerReferences = nil },
		"no virtual label":    func(n *corev1.Node) { delete(n.Labels, provider.VirtualNodeLabel) },
		"finalizer not there": func(n *corev1.Node) { n.Finalizers = []string{"other.io/keep"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			held := heldNode()
			mutate(held)
			client := testutil.NewClient(held)
			ctx, logs := logCtx()
			spec := guardedSpec()
			got, err := provider.ReclaimNode(ctx, client, &spec, time.Second)
			if err != nil || got != provider.ReclaimSkipped {
				t.Fatalf("reclaim=%q err=%v, want skipped", got, err)
			}
			node := getVKNode(t, client)
			if node.UID != "vk-uid-1" || node.DeletionTimestamp == nil || !slices.Equal(node.Finalizers, held.Finalizers) {
				t.Errorf("a skipped Node must stay held as it was, got %+v", node.ObjectMeta)
			}
			if !strings.Contains(logs.String(), `"msg":"terminating node not reclaimed`) {
				t.Errorf("missing skip log line: %s", logs.String())
			}
		})
	}
}

// The VK needs the host's UID to tell a live host from a recreated one.
func TestReclaim_SkipsWithoutHostUID(t *testing.T) {
	client := testutil.NewClient(heldNode())
	spec := provider.NewNodeSpec(provider.NodeConfig{Name: vkNode, InternalIP: "10.0.0.1"})
	got, err := provider.ReclaimNode(context.Background(), client, &spec, time.Second)
	if err != nil || got != provider.ReclaimSkipped {
		t.Errorf("reclaim=%q err=%v, want skipped", got, err)
	}
}

// If the old Node does not go (something else still holds it), the VK stops with an error
// instead of registering over it; its restart retries.
func TestReclaim_TimesOutWhenOldNodeStays(t *testing.T) {
	client := testutil.NewClient(heldNode())
	client.PrependReactor("update", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		return true, update.GetObject(), nil // accepted, never applied
	})
	spec := guardedSpec()
	if _, err := provider.ReclaimNode(context.Background(), client, &spec, 300*time.Millisecond); err == nil {
		t.Fatal("want a timeout error")
	}
	if node := getVKNode(t, client); node.UID != "vk-uid-1" {
		t.Errorf("no new Node may be registered over the old one, uid now %s", node.UID)
	}
}

// A stale read (the Node changed after the VK looked at it) is a conflict: nothing is let go.
func TestReclaim_ConflictLetsNothingGo(t *testing.T) {
	held := heldNode()
	held.ResourceVersion = "5"
	client := testutil.NewClient(held)
	client.PrependReactor("update", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, vkNode, errors.New("stale read"))
	})
	spec := guardedSpec()
	if _, err := provider.ReclaimNode(context.Background(), client, &spec, time.Second); err == nil {
		t.Fatal("want the conflict returned")
	}
	if node := getVKNode(t, client); node.DeletionTimestamp == nil || len(node.Finalizers) != 1 {
		t.Errorf("the Node must stay held, got %+v", node.ObjectMeta)
	}
}
