package mirror

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type harness struct {
	t       *testing.T
	client  *fake.Clientset
	guests  cache.Indexer
	b       *Backend

	mu      sync.Mutex // the informer goroutine emits too
	emitted []*corev1.Pod
}

func (h *harness) emittedPods() []*corev1.Pod {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*corev1.Pod(nil), h.emitted...)
}

func newHarness(t *testing.T, opts Options, objs ...runtime.Object) *harness {
	t.Helper()
	client := fake.NewClientset(objs...)
	// The API server sets UIDs; the fake does not.
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		p := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		if p.UID == "" {
			p.UID = uuid.NewUUID()
		}
		return false, nil, nil
	})
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	h := &harness{t: t, client: client, guests: idx}
	h.b = New(client, corev1listers.NewPodLister(idx), opts)
	h.b.SetStatusCallback(func(p *corev1.Pod) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.emitted = append(h.emitted, p)
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := h.b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return h
}

func testOptions() Options {
	cfg := testConfig()
	cfg.OwnerRef = false
	return Options{Config: cfg, ReserveClaim: true, OrphanGrace: time.Minute}
}

func cpuGuest(uid string) *corev1.Pod {
	g := testGuest()
	g.UID = types.UID(uid)
	g.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
	}
	return g
}

func (h *harness) addGuest(g *corev1.Pod) {
	if err := h.guests.Add(g); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) mirror(name string) *corev1.Pod {
	m, err := h.client.CoreV1().Pods("ns").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return m
}

// waitInformer waits until the backend's informer has the mirror with this guest UID.
func (h *harness) waitInformer(guestUID string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m, err := h.b.mirrors.Pods("ns").Get("vllm-m"); err == nil && m.Labels[LabelMirrorOf] == guestUID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("informer never saw mirror for %s", guestUID)
}

func TestCreateThenGet(t *testing.T) {
	h := newHarness(t, testOptions())
	g := cpuGuest("g1")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if h.mirror("vllm-m") == nil {
		t.Fatal("mirror not created")
	}
	if len(h.emittedPods()) == 0 {
		t.Error("create should emit a status")
	}
	h.waitInformer("g1")
	got, err := h.b.Get("ns", "vllm")
	if err != nil || got.UID != "g1" {
		t.Fatalf("Get: %v %v", got, err)
	}
	// A second create (the library retrying, or a restart) is a no-op.
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if list, _ := h.b.List(); len(list) != 1 {
		t.Errorf("List: %d", len(list))
	}
}

func TestReadoptOrphanWithSameSpec(t *testing.T) {
	old := cpuGuest("old-uid")
	orphan, _ := Build(old, testOptions().Config)
	orphan.UID = "mirror-uid"
	orphan.Status.PodIP = "10.9.9.9"
	orphan.Status.Phase = corev1.PodRunning
	h := newHarness(t, testOptions(), orphan)

	g := cpuGuest("new-uid") // same name and containers, new UID (StatefulSet re-creation)
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	m := h.mirror("vllm-m")
	if m.UID != "mirror-uid" || m.Labels[LabelMirrorOf] != "new-uid" || m.Status.PodIP != "10.9.9.9" {
		t.Errorf("want the same mirror re-adopted, got uid=%s of=%s ip=%s", m.UID, m.Labels[LabelMirrorOf], m.Status.PodIP)
	}
}

func TestReplaceOrphanWithDifferentSpec(t *testing.T) {
	old := cpuGuest("old-uid")
	old.Spec.Containers[0].Image = "other:1"
	orphan, _ := Build(old, testOptions().Config)
	orphan.UID = "mirror-uid"
	h := newHarness(t, testOptions(), orphan)

	g := cpuGuest("new-uid")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err == nil {
		t.Error("want a retry error while the stale mirror is replaced")
	}
	if h.mirror("vllm-m") != nil {
		t.Error("stale mirror should be deleted")
	}
}

func TestGPUMirrorIsReservedOnClaim(t *testing.T) {
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "shared-gpu"},
		Status:     resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{}},
	}
	h := newHarness(t, testOptions(), claim)
	g := testGuest() // requests nvidia.com/gpu
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	c, _ := h.client.ResourceV1().ResourceClaims("ns").Get(context.Background(), "shared-gpu", metav1.GetOptions{})
	if len(c.Status.ReservedFor) != 1 || c.Status.ReservedFor[0].Name != "vllm-m" {
		t.Errorf("reservedFor: %+v", c.Status.ReservedFor)
	}
}

func TestUnallocatedClaimFails(t *testing.T) {
	claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "shared-gpu"}}
	h := newHarness(t, testOptions(), claim)
	g := testGuest()
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err == nil {
		t.Error("want an error when the claim is not allocated")
	}
}

func TestDeleteUsesGuestGrace(t *testing.T) {
	h := newHarness(t, testOptions())
	g := cpuGuest("g1")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	h.waitInformer("g1")
	grace := int64(7)
	g = g.DeepCopy() // objects in a lister are shared with the informer goroutine
	g.DeletionGracePeriodSeconds = &grace
	h.client.ClearActions()
	if err := h.b.Delete(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, a := range h.client.Actions() {
		if d, ok := a.(k8stesting.DeleteAction); ok && d.GetName() == "vllm-m" {
			o := d.GetDeleteOptions()
			found = o.GracePeriodSeconds != nil && *o.GracePeriodSeconds == 7
		}
	}
	if !found {
		t.Errorf("mirror delete with grace 7 not seen: %v", h.client.Actions())
	}
}

func TestDeleteWithoutMirrorReportsTerminated(t *testing.T) {
	h := newHarness(t, testOptions())
	g := cpuGuest("g1")
	h.addGuest(g)
	if err := h.b.Delete(context.Background(), g); err == nil {
		t.Error("want NotFound")
	}
	if e := h.emittedPods(); len(e) != 1 || e[0].Status.Phase != corev1.PodSucceeded {
		t.Errorf("want one terminal status, got %d", len(e))
	}
}

func TestOrphanCollectedAfterGrace(t *testing.T) {
	orphan, _ := Build(cpuGuest("gone"), testOptions().Config)
	orphan.UID = "mirror-uid"
	h := newHarness(t, testOptions(), orphan)
	h.waitInformer("gone")
	now := time.Now()
	h.b.collectOrphans(context.Background(), now)
	if h.mirror("vllm-m") == nil {
		t.Fatal("orphan deleted before the grace period")
	}
	h.b.collectOrphans(context.Background(), now.Add(2*time.Minute))
	if h.mirror("vllm-m") != nil {
		t.Error("orphan should be deleted after the grace period")
	}
}

func TestGuestRemovedWhenMirrorStops(t *testing.T) {
	h := newHarness(t, testOptions())
	g := cpuGuest("g1")
	now := metav1.Now()
	g.DeletionTimestamp = &now
	g.Name = "vllm"
	if _, err := h.client.CoreV1().Pods("ns").Create(context.Background(), g, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h.addGuest(g)
	h.b.finishGuestDeletion(context.Background(), g)
	if _, err := h.client.CoreV1().Pods("ns").Get(context.Background(), "vllm", metav1.GetOptions{}); err == nil {
		t.Error("guest should be deleted once its mirror is gone")
	}
}
