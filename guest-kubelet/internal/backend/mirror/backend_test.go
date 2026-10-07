package mirror

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/edwinhr716/guest-kubelet/internal/group"
)

type harness struct {
	t      *testing.T
	client *fake.Clientset
	guests cache.Indexer
	b      *Backend

	mu      sync.Mutex // the informer goroutine emits too
	emitted []*corev1.Pod
	// writeBack stores every emitted guest in the guest lister, as the library's status
	// write plus its informer would.
	writeBack bool
}

func (h *harness) emittedPods() []*corev1.Pod {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*corev1.Pod(nil), h.emitted...)
}

func newHarness(t *testing.T, opts *Options, objs ...runtime.Object) *harness {
	t.Helper()
	return newHarnessReact(t, opts, nil, objs...)
}

// newHarnessReact is newHarness with extra reactors, added before the backend starts: the fence
// loop and the informers use the client from then on, and the fake's reactor chain is not safe
// to change while they do.
func newHarnessReact(t *testing.T, opts *Options, react func(*fake.Clientset), objs ...runtime.Object) *harness {
	t.Helper()
	if !hasNode(objs) {
		// GPU guests need the host's pooled shadow allocatable (attachPooled).
		objs = append([]runtime.Object{pooledHost(8)}, objs...)
	}
	client := fake.NewClientset(objs...)
	// The API server sets UIDs; the fake does not.
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		p := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		if p.UID == "" {
			p.UID = uuid.NewUUID()
		}
		return false, nil, nil
	})
	if react != nil {
		react(client)
	}
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	h := &harness{t: t, client: client, guests: idx}
	h.b = New(client, corev1listers.NewPodLister(idx), opts)
	h.b.SetStatusCallback(func(p *corev1.Pod) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.emitted = append(h.emitted, p)
		if h.writeBack {
			if err := idx.Update(p); err != nil {
				t.Error(err)
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := h.b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return h
}

func hasNode(objs []runtime.Object) bool {
	for _, o := range objs {
		if _, ok := o.(*corev1.Node); ok {
			return true
		}
	}
	return false
}

func testOptions() Options {
	cfg := testConfig()
	cfg.OwnerRef = false
	return Options{Config: cfg, OrphanGrace: time.Minute}
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
	h := newHarness(t, ref(testOptions()))
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
	orphan, _ := Build(old, ref(testOptions().Config))
	orphan.UID = "mirror-uid"
	orphan.Status.PodIP = "10.9.9.9"
	orphan.Status.Phase = corev1.PodRunning
	h := newHarness(t, ref(testOptions()), orphan)

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
	orphan, _ := Build(old, ref(testOptions().Config))
	orphan.UID = "mirror-uid"
	h := newHarness(t, ref(testOptions()), orphan)

	g := cpuGuest("new-uid")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err == nil {
		t.Error("want a retry error while the stale mirror is replaced")
	}
	if h.mirror("vllm-m") != nil {
		t.Error("stale mirror should be deleted")
	}
}

func TestDeleteUsesGuestGrace(t *testing.T) {
	h := newHarness(t, ref(testOptions()))
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
	h := newHarness(t, ref(testOptions()))
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
	orphan, _ := Build(cpuGuest("gone"), ref(testOptions().Config))
	orphan.UID = "mirror-uid"
	h := newHarness(t, ref(testOptions()), orphan)
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
	h := newHarness(t, ref(testOptions()))
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

// D-VK-3 option b: the mirror carries the host node's group and is refused while there is none.

func TestBuildSetsGroupLabel(t *testing.T) {
	cfg := testConfig()
	cfg.Group = "team-a.rc-1.trainers"
	m, err := Build(testGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if m.Labels[LabelGroup] != "team-a.rc-1.trainers" || m.Labels[LabelMirrorOf] != "guest-uid" {
		t.Errorf("labels: %v", m.Labels)
	}
	cfg.Group = ""
	m, err = Build(testGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Labels[LabelGroup]; ok {
		t.Errorf("no group: want no %s label, got %v", LabelGroup, m.Labels)
	}
}

func TestCreateRefusedWhileGroupUnresolved(t *testing.T) {
	res := group.Result{Reason: group.ReasonGroupWithoutDonor}
	var refused []string
	opts := testOptions()
	opts.Group = func() group.Result { return res }
	opts.OnUnresolved = func(g *corev1.Pod, reason string) { refused = append(refused, g.Name+":"+reason) }
	h := newHarness(t, &opts)
	g := cpuGuest("g1")
	h.addGuest(g)

	err := h.b.Create(context.Background(), g)
	if !errors.Is(err, ErrGroupUnresolved) {
		t.Fatalf("want ErrGroupUnresolved, got %v", err)
	}
	if h.mirror("vllm-m") != nil {
		t.Fatal("no mirror may exist while the group is unresolved")
	}
	if len(refused) != 1 || refused[0] != "vllm:"+group.ReasonGroupWithoutDonor {
		t.Errorf("OnUnresolved calls: %v", refused)
	}

	// The donor controller finishes writing the pair; the library's retry now succeeds.
	res = group.Result{Groups: []string{"team-a.rc-1.trainers"}, Reason: group.ReasonOK}
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if m := h.mirror("vllm-m"); m == nil || m.Labels[LabelGroup] != "team-a.rc-1.trainers" {
		t.Fatalf("mirror with group label expected, got %v", m)
	}
}

func TestReadoptionRefreshesGroupLabel(t *testing.T) {
	cfg := testOptions().Config
	cfg.Group = "old.rc-0.trainers"
	orphan, err := Build(cpuGuest("old-uid"), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	orphan.UID = "mirror-uid"
	orphan.Status.Phase = corev1.PodRunning
	opts := testOptions()
	newGroup := group.Result{Groups: []string{"new.rc-1.trainers"}, Reason: group.ReasonOK}
	opts.Group = func() group.Result { return newGroup }
	h := newHarness(t, &opts, orphan)
	g := cpuGuest("new-uid")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if m := h.mirror("vllm-m"); m.UID != "mirror-uid" || m.Labels[LabelGroup] != "new.rc-1.trainers" {
		t.Errorf("adopted mirror: uid=%s group=%s", m.UID, m.Labels[LabelGroup])
	}
}

// Lead decision D-VK-5 option c: a readiness-gate change on the guest re-emits its status at
// once, without a mirror event.
func TestGuestProbePolicy_C_WatchReadinessGates(t *testing.T) {
	harn := newHarness(t, ref(testOptions()))
	guest := cpuGuest("g1") // gate timeslice.io/serving, not set
	ctx := t.Context()
	if _, err := harn.client.CoreV1().Pods("ns").Create(ctx, guest, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	harn.addGuest(guest)
	if err := harn.b.Create(ctx, guest); err != nil {
		t.Fatal(err)
	}
	harn.waitInformer("g1")
	if err := harn.b.WatchReadinessGates(ctx); err != nil {
		t.Fatal(err)
	}
	before := len(harn.emittedPods())
	updated := guest.DeepCopy()
	updated.Status.Conditions = []corev1.PodCondition{{Type: "timeslice.io/serving", Status: corev1.ConditionTrue}}
	if _, err := harn.client.CoreV1().Pods("ns").UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, pod := range harn.emittedPods()[before:] {
			if c := findCondition(pod.Status.Conditions, "timeslice.io/serving"); c != nil && c.Status == corev1.ConditionTrue {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a gate change must re-emit the guest's status with the new gate")
}
