package main

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

const stopNS = "guest-kubelet-proto"

func ctrlRef(kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, UID: types.UID("u-" + name), Controller: &yes}}
}

func deletingVKPod(kind, owner string) *corev1.Pod {
	now := metav1.Now()
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "vk-pod", Namespace: stopNS, DeletionTimestamp: &now,
		Finalizers: []string{"test/hold"}}}
	if kind != "" {
		p.OwnerReferences = ctrlRef(kind, owner)
	}
	return p
}

func TestDecideStop(t *testing.T) {
	now := metav1.Now()
	zero, one := int32(0), int32(1)
	rs := func(replicas *int32, deleting bool) *appsv1.ReplicaSet {
		r := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "vk-rs", Namespace: stopNS,
			OwnerReferences: ctrlRef("Deployment", "vk")}, Spec: appsv1.ReplicaSetSpec{Replicas: replicas}}
		if deleting {
			r.DeletionTimestamp, r.Finalizers = &now, []string{"test/hold"}
		}
		return r
	}
	dep := func(replicas *int32) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: stopNS},
			Spec: appsv1.DeploymentSpec{Replicas: replicas}}
	}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "vk-ds", Namespace: stopNS},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"timeslice.io/donor": "true"}}}}}
	host := func(donor bool) *corev1.Node {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host", Labels: map[string]string{}}}
		if donor {
			n.Labels["timeslice.io/donor"] = "true"
		}
		return n
	}
	live := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "vk-pod", Namespace: stopNS, OwnerReferences: ctrlRef("ReplicaSet", "vk-rs")}}
	dsRef, rsRef := &ctrlRef("DaemonSet", "vk-ds")[0], &ctrlRef("ReplicaSet", "vk-rs")[0]
	cases := map[string]struct {
		objs  []runtime.Object
		want  stopAction
		known *metav1.OwnerReference
	}{
		"container restart":        {[]runtime.Object{live, rs(&one, false), dep(&one)}, stopRestart, nil},
		"own pod gone":             {nil, stopUninstall, nil},
		"rollout or pod delete":    {[]runtime.Object{deletingVKPod("ReplicaSet", "vk-rs"), rs(&one, false), dep(&one)}, stopRestart, nil},
		"deployment deleted":       {[]runtime.Object{deletingVKPod("ReplicaSet", "vk-rs"), rs(&one, false)}, stopUninstall, nil},
		"replicaset being deleted": {[]runtime.Object{deletingVKPod("ReplicaSet", "vk-rs"), rs(&one, true), dep(&one)}, stopUninstall, nil},
		"replicaset deleted":       {[]runtime.Object{deletingVKPod("ReplicaSet", "vk-rs")}, stopUninstall, nil},
		"scaled to zero":           {[]runtime.Object{deletingVKPod("ReplicaSet", "vk-rs"), rs(&zero, false), dep(&zero)}, stopUninstall, nil},
		"daemonset still selects":  {[]runtime.Object{deletingVKPod("DaemonSet", "vk-ds"), ds, host(true)}, stopRestart, nil},
		"daemonset unbinds host":   {[]runtime.Object{deletingVKPod("DaemonSet", "vk-ds"), ds, host(false)}, stopUninstall, nil},
		"daemonset deleted":        {[]runtime.Object{deletingVKPod("DaemonSet", "vk-ds"), host(true)}, stopUninstall, nil},
		"bare pod deleted":         {[]runtime.Object{deletingVKPod("", "")}, stopUninstall, nil},
		// force delete: the pod object is gone before the signal; judge the controller from start
		"force-deleted, daemonset still selects": {[]runtime.Object{ds, host(true)}, stopRestart, dsRef},
		"force-deleted, daemonset unbinds host":  {[]runtime.Object{ds, host(false)}, stopUninstall, dsRef},
		"force-deleted, daemonset deleted":       {[]runtime.Object{host(true)}, stopUninstall, dsRef},
		"force-deleted, deployment wants pods":   {[]runtime.Object{rs(&one, false), dep(&one)}, stopRestart, rsRef},
		"force-deleted, deployment deleted":      {[]runtime.Object{rs(&one, false)}, stopUninstall, rsRef},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientset(tc.objs...)
			got, why := decideStop(context.Background(), c, stopNS, "vk-pod", "host", tc.known)
			if got != tc.want {
				t.Errorf("decideStop = %s (%s), want %s", got, why, tc.want)
			}
		})
	}
	if got, _ := decideStop(context.Background(), fake.NewClientset(), "", "", "host", nil); got != stopRestart {
		t.Errorf("unknown pod: %s", got)
	}
}
