package mirror

import (
	"context"
	"errors"
	"strconv"
	"strings"
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

func TestGPUMirrorIsReservedOnClaim(t *testing.T) {
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "shared-gpu"},
		Status:     resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{}},
	}
	h := newHarness(t, ref(testOptions()), claim)
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
	h := newHarness(t, ref(testOptions()), claim)
	g := testGuest()
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err == nil {
		t.Error("want an error when the claim is not allocated")
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

// fakeFreezer records calls and, at each Suspend, whether the guest was already NotReady in
// the guest lister.
type fakeFreezer struct {
	mu          sync.Mutex
	calls       []string
	suspendErr  error
	resumeErr   error
	readyAtCall []bool
	guestReady  func() bool
}

func (f *fakeFreezer) Suspend(_ context.Context, _ *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "freeze:"+strconv.FormatInt(epoch, 10))
	f.readyAtCall = append(f.readyAtCall, f.guestReady())
	return f.suspendErr
}

func (f *fakeFreezer) Resume(_ context.Context, _ *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "thaw:"+strconv.FormatInt(epoch, 10))
	return f.resumeErr
}

func (f *fakeFreezer) Frozen(*corev1.Pod) (bool, error) { return false, nil }

func (f *fakeFreezer) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeFreezer) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// suspendHarness runs a Ready CPU guest with a running mirror and a fake freezer.
func suspendHarness(t *testing.T) (*harness, *fakeFreezer) {
	t.Helper()
	ff := &fakeFreezer{}
	opts := testOptions()
	opts.Suspend = SuspendOptions{
		Freezer: ff, NotReadyTimeout: 2 * time.Second, FreezeTimeout: time.Second, ReadyTimeout: time.Second,
		ReadyCheck: func(context.Context, *corev1.Pod, *corev1.Pod) error { ff.record("readycheck"); return nil },
	}
	hrn := newHarness(t, &opts)
	guest := cpuGuest("g1")
	guest.Spec.ReadinessGates = nil // an unset gate would hold Ready false throughout
	hrn.addGuest(guest)
	ff.guestReady = func() bool {
		cur, err := hrn.b.guests.Pods("ns").Get("vllm")
		return err == nil && IsReady(cur)
	}
	if err := hrn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	mirrorPod := hrn.mirror("vllm-m")
	mirrorPod.Status = runningMirror().Status
	if _, err := hrn.client.CoreV1().Pods("ns").UpdateStatus(context.Background(), mirrorPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cur, err := hrn.b.mirrors.Pods("ns").Get("vllm-m"); err == nil && cur.Status.PodIP != "" {
			break
		}
		if time.Now().After(deadline) {
			hrn.t.Fatal("informer never saw the running mirror")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// From here on the guest lister follows what the backend emits. Start from Ready, as the
	// library last wrote it.
	hrn.mu.Lock()
	defer hrn.mu.Unlock()
	ready := guest.DeepCopy()
	ready.Status = runningMirror().Status
	if err := hrn.guests.Update(ready); err != nil {
		t.Fatal(err)
	}
	hrn.writeBack = true
	return hrn, ff
}

// settled waits until the last emitted guest satisfies ok and returns it. The informer delivers
// the mirror updates asynchronously, so the last status is only final once it has caught up.
func (h *harness) settled(what string, ok func(*corev1.Pod) bool) *corev1.Pod {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if e := h.emittedPods(); len(e) > 0 && ok(e[len(e)-1]) {
			return e[len(e)-1]
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("last emitted guest never became %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func notReady(p *corev1.Pod) bool { return !IsReady(p) }

func TestSuspendResume_NotReadyBeforeFreeze(t *testing.T) {
	hrn, ff := suspendHarness(t)
	res, err := hrn.b.Suspend(context.Background(), "ns", "vllm")
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateSuspended || res.Epoch != 1 {
		t.Fatalf("suspend result: %+v", res)
	}
	if len(ff.readyAtCall) != 1 || ff.readyAtCall[0] {
		t.Fatalf("the freeze must come after NotReady is visible: ready at freeze = %v", ff.readyAtCall)
	}
	mirrorPod := hrn.mirror("vllm-m")
	if mirrorPod.Annotations[AnnotationSuspendState] != StateSuspended || mirrorPod.Annotations[AnnotationGuestEpoch] != "1" ||
		mirrorPod.Annotations[AnnotationSuspendStateSince] == "" {
		t.Fatalf("mirror annotations: %v", mirrorPod.Annotations)
	}
	st := hrn.settled("suspended", func(p *corev1.Pod) bool {
		c := findCondition(p.Status.Conditions, ConditionSuspended)
		return c != nil && c.Status == corev1.ConditionTrue
	}).Status
	if IsReady(&corev1.Pod{Status: st}) || st.Phase != corev1.PodRunning {
		t.Fatalf("suspended guest must be Running and NotReady: %+v", st)
	}
	cs := st.ContainerStatuses[0]
	if cs.Ready || cs.State.Waiting == nil || cs.State.Waiting.Reason != StateSuspended || cs.RestartCount != 2 {
		t.Fatalf("container status: %+v", cs)
	}
	if c := findCondition(st.Conditions, ConditionSuspended); c == nil || c.Status != corev1.ConditionTrue {
		t.Fatalf("condition %s: %+v", ConditionSuspended, st.Conditions)
	}

	// A second suspend is a no-op.
	if res, err := hrn.b.Suspend(context.Background(), "ns", "vllm"); err != nil || !res.Noop {
		t.Fatalf("second suspend: %+v %v", res, err)
	}

	res, err = hrn.b.Resume(context.Background(), "ns", "vllm")
	if err != nil {
		t.Fatal(err)
	}
	if res.Epoch != 2 || res.State != "Running" {
		t.Fatalf("resume result: %+v", res)
	}
	if got, want := strings.Join(ff.callList(), ","), "freeze:1,thaw:2,readycheck"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	mirrorPod = hrn.mirror("vllm-m")
	if _, ok := mirrorPod.Annotations[AnnotationSuspendState]; ok || mirrorPod.Annotations[AnnotationGuestEpoch] != "2" {
		t.Fatalf("mirror annotations after resume: %v", mirrorPod.Annotations)
	}
	st = hrn.settled("ready", IsReady).Status
	if !IsReady(&corev1.Pod{Status: st}) || st.ContainerStatuses[0].State.Running == nil ||
		findCondition(st.Conditions, ConditionSuspended) != nil {
		t.Fatalf("resumed guest must be Ready and running: %+v", st)
	}
}

func TestSuspend_FreezeFailureRevertsToRunning(t *testing.T) {
	h, ff := suspendHarness(t)
	ff.suspendErr = errors.New("stuck task")
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error")
	}
	if got := strings.Join(ff.callList(), ","); got != "freeze:1,thaw:1" {
		t.Fatalf("a failed freeze must be undone with a thaw: calls = %s", got)
	}
	m := h.mirror("vllm-m")
	if _, ok := m.Annotations[AnnotationSuspendState]; ok {
		t.Fatalf("state must be cleared: %v", m.Annotations)
	}
	h.settled("ready again", IsReady)
}

func TestSuspend_ThawFailureAfterFreezeFailureStaysNotReady(t *testing.T) {
	h, ff := suspendHarness(t)
	ff.suspendErr, ff.resumeErr = errors.New("stuck task"), errors.New("no thaw")
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error")
	}
	if s := h.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != StateSuspending {
		t.Fatalf("state = %q, want %s", s, StateSuspending)
	}
	h.settled("NotReady", notReady)
}

func TestResume_ReadyCheckFailureStaysNotReady(t *testing.T) {
	h, _ := suspendHarness(t)
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	h.b.opts.Suspend.ReadyCheck = func(context.Context, *corev1.Pod, *corev1.Pod) error { return errors.New("503") }
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error")
	}
	if s := h.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != StateResuming {
		t.Fatalf("state = %q, want %s", s, StateResuming)
	}
	h.settled("NotReady", notReady)
}

func TestSuspend_NoFreezerConfigured(t *testing.T) {
	h := newHarness(t, ref(testOptions()))
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error without a freeze backend")
	}
}

func TestDelete_ThawsSuspendedMirrorFirst(t *testing.T) {
	h, ff := suspendHarness(t)
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if m, err := h.b.mirrors.Pods("ns").Get("vllm-m"); err == nil && m.Annotations[AnnotationSuspendState] == StateSuspended {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("informer never saw Suspended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.b.Delete(context.Background(), g.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ff.callList(), ","); got != "freeze:1,thaw:1" {
		t.Fatalf("calls = %s, want the delete to thaw first", got)
	}
}
