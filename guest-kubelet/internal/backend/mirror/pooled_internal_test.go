package mirror

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

func pooledHost(alloc int64) *corev1.Node {
	n := hostNode()
	n.Status.Allocatable[api.PooledResource] = *resource.NewQuantity(alloc, resource.DecimalSI)
	return n
}

func pooledOptions(rec record.EventRecorder) Options {
	return Options{
		Config: testConfig(), OrphanGrace: time.Minute,
		GPUDonorSelector: DefaultGPUDonorSelector, Recorder: rec,
		FenceInterval: 50 * time.Millisecond,
	}
}

func gpuGuest(name, uid string, k int64) *corev1.Pod {
	g := testGuest()
	g.Name, g.UID = name, types.UID(uid)
	q := *resource.NewQuantity(k, resource.DecimalSI)
	g.Spec.Containers[0].Resources.Requests[GPUResource] = q
	g.Spec.Containers[0].Resources.Limits[GPUResource] = q
	return g
}

// pooledMirror is another guest's mirror holding qty pooled devices.
func pooledMirror(t *testing.T, name string, qty int64, terminating bool) runtime.Object {
	t.Helper()
	cfg := pooledOptions(nil).Config
	m, err := BuildWithGPU(cpuGuest(name+"-uid"), &cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.Name = name
	m.Spec.Containers[0].Resources.Limits = corev1.ResourceList{api.PooledResource: *resource.NewQuantity(qty, resource.DecimalSI)}
	m.Status.Phase = corev1.PodRunning
	if terminating {
		now := metav1.Now()
		m.DeletionTimestamp = &now
		m.Finalizers = []string{"test/hold"}
	}
	return m
}

func waitMirrorCached(t *testing.T, hn *harness, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := hn.b.mirrors.Pods("ns").Get(name); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mirror %s never reached the informer", name)
}

func TestPooledMirrorAsksForPooledResource(t *testing.T) {
	hn := newHarness(t, ref(pooledOptions(nil)), pooledHost(2), donorPod())
	guest := gpuGuest("vllm", "guest-uid", 2)
	hn.addGuest(guest)
	if err := hn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	mir := hn.mirror("vllm-m")
	if mir == nil {
		t.Fatal("mirror not created")
	}
	c := mir.Spec.Containers[0].Resources
	if q := c.Limits[api.PooledResource]; q.Value() != 2 {
		t.Errorf("want limit %s: 2, got %v", api.PooledResource, c.Limits)
	}
	if q := c.Requests[api.PooledResource]; q.Value() != 2 {
		t.Errorf("want request %s: 2, got %v", api.PooledResource, c.Requests)
	}
	if _, ok := c.Limits[GPUResource]; ok {
		t.Errorf("nvidia.com/gpu must be swapped out: %v", c.Limits)
	}
	if mir.Annotations[AnnotationGPUDonorUID] != "donor-uid" {
		t.Errorf("want donor uid for the fence, got %q", mir.Annotations[AnnotationGPUDonorUID])
	}
	if PooledQtyOf(mir) != 2 {
		t.Errorf("PooledQtyOf=%d", PooledQtyOf(mir))
	}
	if len(mir.Spec.ResourceClaims) != 0 {
		t.Errorf("no claim in pooled mode: %v", mir.Spec.ResourceClaims)
	}
	noDRACalls(t, hn.client.Actions())
}

func TestPooledCapacity(t *testing.T) {
	cases := map[string]struct {
		alloc   int64
		objs    func(t *testing.T) []runtime.Object
		k       int64
		wantErr bool
	}{
		"fits":                     {alloc: 2, k: 2},
		"no donor: allocatable 0":  {alloc: 0, k: 1, wantErr: true},
		"too many":                 {alloc: 2, k: 3, wantErr: true},
		"running mirror holds one": {alloc: 2, k: 2, wantErr: true, objs: func(t *testing.T) []runtime.Object { return []runtime.Object{pooledMirror(t, "other-m", 1, false)} }},
		"one left":                 {alloc: 2, k: 1, objs: func(t *testing.T) []runtime.Object { return []runtime.Object{pooledMirror(t, "other-m", 1, false)} }},
		// A terminating mirror still holds its devices until the real kubelet stops it.
		"terminating mirror counts": {alloc: 2, k: 1, wantErr: true, objs: func(t *testing.T) []runtime.Object { return []runtime.Object{pooledMirror(t, "other-m", 2, true)} }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := record.NewFakeRecorder(10)
			objs := []runtime.Object{pooledHost(tc.alloc), donorPod()}
			if tc.objs != nil {
				objs = append(objs, tc.objs(t)...)
			}
			hn := newHarness(t, ref(pooledOptions(rec)), objs...)
			if tc.objs != nil {
				waitMirrorCached(t, hn, "other-m")
			}
			guest := gpuGuest("vllm", "guest-uid", tc.k)
			hn.addGuest(guest)
			err := hn.b.Create(context.Background(), guest)
			if !tc.wantErr {
				if err != nil || hn.mirror("vllm-m") == nil {
					t.Fatalf("want a mirror, got err=%v", err)
				}
				return
			}
			var r *refusalError
			if !errors.As(err, &r) || !strings.Contains(r.reason, "allocatable") {
				t.Fatalf("want a capacity refusal, got %v", err)
			}
			for _, leak := range []string{"donor", "other-m", "real-node", "allocatable"} {
				if strings.Contains(err.Error(), leak) {
					t.Errorf("guest-visible error %q contains %q", err.Error(), leak)
				}
			}
			if hn.mirror("vllm-m") != nil {
				t.Error("fail closed: no mirror over capacity")
			}
		})
	}
}

// Two guests created back to back: the second must count the first before the informer has
// its mirror (pooledAssigned).
func TestPooledCountsJustCreatedMirror(t *testing.T) {
	hn := newHarness(t, ref(pooledOptions(nil)), pooledHost(1), donorPod())
	a, b := gpuGuest("a", "a-uid", 1), gpuGuest("b", "b-uid", 1)
	hn.addGuest(a)
	hn.addGuest(b)
	if err := hn.b.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := hn.b.Create(context.Background(), b); err == nil {
		t.Fatal("the second 1-GPU mirror must not fit an allocatable of 1")
	}
	if hn.mirror("b-m") != nil {
		t.Error("no mirror for b")
	}
}

func TestCheckPooledGuest(t *testing.T) {
	g := gpuGuest("vllm", "u", 1)
	g.Spec.Containers = append(g.Spec.Containers, *g.Spec.Containers[0].DeepCopy())
	g.Spec.Containers[1].Name = "second"
	if k, err := CheckPooledGuest(g); err != nil || k != 2 {
		t.Errorf("two 1-GPU containers: want k=2, got %d %v", k, err)
	}
	priv := gpuGuest("vllm", "u", 1)
	priv.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: ref(true)}
	if _, err := CheckPooledGuest(priv); err == nil {
		t.Error("privileged guest must be refused in pooled mode")
	}
	initGPU := gpuGuest("vllm", "u", 1)
	initGPU.Spec.InitContainers = []corev1.Container{*initGPU.Spec.Containers[0].DeepCopy()}
	if _, err := CheckPooledGuest(initGPU); err == nil {
		t.Error("init container GPU must be refused")
	}
	// Build: each GPU container gets its own quantity.
	cfg := pooledOptions(nil).Config
	m, err := BuildWithGPU(g, &cfg, &GPUAttachment{Qty: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := range m.Spec.Containers {
		if q := m.Spec.Containers[i].Resources.Limits[api.PooledResource]; q.Value() != 1 {
			t.Errorf("container %d: want %s: 1, got %v", i, api.PooledResource, m.Spec.Containers[i].Resources.Limits)
		}
	}
	if mode, err := ParseGPUMode("pooled"); err != nil || mode != GPUModePooled {
		t.Errorf("ParseGPUMode(pooled) = %q %v", mode, err)
	}
}
