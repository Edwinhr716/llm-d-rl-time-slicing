package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Tests for the D-NS-11 startup log hook: the controller kind the VK reports for itself.

func vkPod(ownerKind string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "guest-kubelet-x", Namespace: "guest-kubelet-proto"}}
	if ownerKind != "" {
		isController := true
		pod.OwnerReferences = []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "ConfigMap", Name: "not-a-controller", UID: "u0"},
			{APIVersion: "apps/v1", Kind: ownerKind, Name: "guest-kubelet", UID: "u1", Controller: &isController},
		}
	}
	return pod
}

func TestPlacementNSDS_ControllerKind(t *testing.T) {
	cases := map[string]string{
		"DaemonSet":  "DaemonSet",  // ns-ds
		"ReplicaSet": "Deployment", // keep: a Deployment's pods are owned by its ReplicaSet
		"":           "none",
	}
	for owner, want := range cases {
		if got := controllerKind(vkPod(owner)); got != want {
			t.Errorf("owner %q: controllerKind = %q, want %q", owner, got, want)
		}
	}
}

func TestPlacementNSDS_LookupController(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset(vkPod("DaemonSet"))
	if got := lookupController(ctx, client, "guest-kubelet-proto", "guest-kubelet-x"); got != "DaemonSet" {
		t.Errorf("own pod: %q, want DaemonSet", got)
	}
	if got := lookupController(ctx, client, "guest-kubelet-proto", "missing"); got != "unknown" {
		t.Errorf("missing pod: %q, want unknown", got)
	}
	if got := lookupController(ctx, client, "", ""); got != "unknown" {
		t.Errorf("no POD_NAME: %q, want unknown", got)
	}
}

func TestPlacementNSDS_StartStopLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	o := &options{
		hostNode: "host-pool-abcd", nodeName: "vk-abcd",
		leaseNamespace: "guest-kubelet-proto", podName: "guest-kubelet-x",
	}
	logStart(ctx, fake.NewClientset(vkPod("DaemonSet")), o)
	if started.host != "host-pool-abcd" || started.node != "vk-abcd" {
		t.Errorf("started = %+v", started)
	}
	logStop(ctx) // not cancelled: no line
	cancel()
	logStop(ctx) // after a signal: "vk stopping" with deregister=false
}
