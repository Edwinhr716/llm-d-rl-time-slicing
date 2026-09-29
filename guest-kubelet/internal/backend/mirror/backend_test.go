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

// fakeFreezer stands in for the snapshot agent. It records calls and, at each Suspend, whether
// the guest was already NotReady in the guest lister.
type fakeFreezer struct {
	mu          sync.Mutex
	calls       []string
	suspendErr  error
	resumeErr   error
	killErr     error
	readyAtCall []bool
	guestReady  func() bool
	deadlines   []time.Duration // time left until each call's deadline
	frozen      bool            // the host fact Frozen reports
}

func (f *fakeFreezer) noteDeadline(ctx context.Context) {
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, time.Until(d))
	} else {
		f.deadlines = append(f.deadlines, -1)
	}
}

func (f *fakeFreezer) Suspend(ctx context.Context, _ *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "suspend:"+strconv.FormatInt(epoch, 10))
	f.readyAtCall = append(f.readyAtCall, f.guestReady())
	f.noteDeadline(ctx)
	return f.suspendErr
}

func (f *fakeFreezer) Resume(ctx context.Context, _ *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "resume:"+strconv.FormatInt(epoch, 10))
	f.noteDeadline(ctx)
	return f.resumeErr
}

func (f *fakeFreezer) Kill(ctx context.Context, _ *corev1.Pod, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "kill")
	f.noteDeadline(ctx)
	return f.killErr
}

// Frozen is the host fact (HostFact): what the agent says now. It records no call.
func (f *fakeFreezer) Frozen(context.Context, *corev1.Pod) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frozen, nil
}

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

// suspendHarness runs a Ready CPU guest with a running mirror and a fake agent.
func suspendHarness(t *testing.T) (*harness, *fakeFreezer) {
	t.Helper()
	ff := &fakeFreezer{}
	opts := testOptions()
	opts.Suspend = SuspendOptions{
		Freezer: ff, NotReadyTimeout: 2 * time.Second, SuspendTimeout: 20 * time.Second,
		ResumeTimeout: 15 * time.Second, KillTimeout: 5 * time.Second, ReadyTimeout: time.Second,
		ReadyCheck: func(context.Context, *corev1.Pod, *corev1.Pod) error { ff.record("readycheck"); return nil },
	}
	hrn := newHarness(t, opts)
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

func TestSuspendResume_NotReadyBeforeAgentSuspend(t *testing.T) {
	hrn, ff := suspendHarness(t)
	res, err := hrn.b.Suspend(context.Background(), "ns", "vllm")
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateSuspended || res.Epoch != 1 {
		t.Fatalf("suspend result: %+v", res)
	}
	if len(ff.readyAtCall) != 1 || ff.readyAtCall[0] {
		t.Fatalf("the agent suspend must come after NotReady is visible: ready at call = %v", ff.readyAtCall)
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
	if got, want := strings.Join(ff.callList(), ","), "suspend:1,resume:2,readycheck"; got != want {
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

// suspended suspends the guest and waits until the informer holds the Suspended mirror, which
// the next call reads.
func (h *harness) suspended() {
	h.t.Helper()
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
		h.t.Fatal(err)
	}
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.b.waitInformerState(context.Background(), g, StateSuspended, 5*time.Second); err != nil {
		h.t.Fatal(err)
	}
}

// readyAfter reports the first emitted guest from index i on that is Ready, if any.
func (h *harness) readyAfter(i int) *corev1.Pod {
	for _, p := range h.emittedPods()[i:] {
		if IsReady(p) {
			return p
		}
	}
	return nil
}

func killedGuest(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed && p.Status.Reason == ReasonKilled && !IsReady(p)
}

// waitMirrorGone waits until the mirror informer no longer has the guest's mirror.
func (h *harness) waitMirrorGone(name string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := h.b.mirrors.Pods("ns").Get(name); err != nil {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("mirror %s never went away", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSuspend_AgentDeadlines(t *testing.T) {
	h, ff := suspendHarness(t)
	h.suspended()
	// A caller's earlier deadline wins over --agent-suspend-timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.b.Resume(ctx, "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	if len(ff.deadlines) != 2 || ff.deadlines[0] < 19*time.Second || ff.deadlines[0] > 20*time.Second ||
		ff.deadlines[1] <= 0 || ff.deadlines[1] > 3*time.Second {
		t.Fatalf("deadlines left at each call = %v, want about 20s then at most 3s", ff.deadlines)
	}
}

func TestSuspend_AgentFailureRunsKillSequence(t *testing.T) {
	h, ff := suspendHarness(t)
	from := len(h.emittedPods())
	ff.suspendErr = errors.New("agent Suspend of job x: DeadlineMissed")
	res, err := h.b.Suspend(context.Background(), "ns", "vllm")
	if !errors.Is(err, ErrKilled) || !res.Killed || res.State != StateKilled {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if got := strings.Join(ff.callList(), ","); got != "suspend:1,kill" {
		t.Fatalf("calls = %s, want the agent kill after the failed suspend", got)
	}
	if h.mirror("vllm-m") != nil {
		t.Fatal("the kill sequence must delete the mirror")
	}
	h.waitMirrorGone("vllm-m")
	h.settled("Failed (killed)", killedGuest)
	if p := h.readyAfter(from); p != nil {
		t.Fatalf("the guest was reported Ready after the suspend began: %+v", p.Status)
	}
}

func TestSuspend_KillFailureStillDeletesMirror(t *testing.T) {
	h, ff := suspendHarness(t)
	ff.suspendErr, ff.killErr = errors.New("agent hangs"), errors.New("agent hangs")
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); !errors.Is(err, ErrKilled) {
		t.Fatalf("want the kill sequence, got %v", err)
	}
	if h.mirror("vllm-m") != nil {
		t.Fatal("the mirror must be deleted even when the agent kill fails")
	}
	h.settled("Failed (killed)", killedGuest)
}

func TestSuspend_KillSequenceUsesNormalGrace(t *testing.T) {
	h, ff := suspendHarness(t)
	ff.suspendErr = errors.New("refused")
	var grace []*int64
	h.client.PrependReactor("delete", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if d, ok := a.(k8stesting.DeleteActionImpl); ok {
			grace = append(grace, d.DeleteOptions.GracePeriodSeconds)
		}
		return false, nil, nil
	})
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); !errors.Is(err, ErrKilled) {
		t.Fatalf("want the kill sequence, got %v", err)
	}
	if len(grace) != 1 || grace[0] != nil {
		t.Fatalf("the mirror must be deleted once with its normal grace period, never forced: %v", grace)
	}
}

func TestSuspend_NotReadyNotConfirmedRevertsWithoutAgentCall(t *testing.T) {
	h, ff := suspendHarness(t)
	h.b.opts.Suspend.NotReadyTimeout = 200 * time.Millisecond
	h.mu.Lock()
	h.writeBack = false // the API never shows the guest NotReady
	h.mu.Unlock()
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil || errors.Is(err, ErrKilled) {
		t.Fatalf("want a plain error, got %v", err)
	}
	if got := ff.callList(); len(got) != 0 {
		t.Fatalf("the agent must not be called before NotReady is confirmed: %v", got)
	}
	if m := h.mirror("vllm-m"); m == nil || m.Annotations[AnnotationSuspendState] != "" {
		t.Fatalf("the guest must be back to Running: %v", m)
	}
	h.settled("ready again", IsReady)
}

func TestResume_AgentFailureRunsKillSequence(t *testing.T) {
	h, ff := suspendHarness(t)
	h.suspended()
	from := len(h.emittedPods())
	ff.resumeErr = errors.New("agent Resume of job x: Refused (FAILED_PRECONDITION): cannot resume job x in state JOB_STATE_FAULTED")
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); !errors.Is(err, ErrKilled) {
		t.Fatalf("want the kill sequence, got %v", err)
	}
	if got := strings.Join(ff.callList(), ","); got != "suspend:1,resume:2,kill" {
		t.Fatalf("calls = %s", got)
	}
	h.settled("Failed (killed)", killedGuest)
	if p := h.readyAfter(from); p != nil {
		t.Fatalf("a guest whose resume failed was reported Ready: %+v", p.Status)
	}
}

func TestResume_ReadyCheckFailureRunsKillSequence(t *testing.T) {
	h, ff := suspendHarness(t)
	h.suspended()
	from := len(h.emittedPods())
	h.b.opts.Suspend.ReadyCheck = func(context.Context, *corev1.Pod, *corev1.Pod) error { return errors.New("503") }
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); !errors.Is(err, ErrKilled) {
		t.Fatalf("want the kill sequence, got %v", err)
	}
	if got := strings.Join(ff.callList(), ","); got != "suspend:1,resume:2,kill" {
		t.Fatalf("calls = %s", got)
	}
	h.settled("Failed (killed)", killedGuest)
	if p := h.readyAfter(from); p != nil {
		t.Fatalf("a guest that failed its ready check was reported Ready: %+v", p.Status)
	}
}

func TestSuspend_HalfDoneKillIsFinished(t *testing.T) {
	h, ff := suspendHarness(t)
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	// A guest kubelet that stopped mid kill sequence left the mirror Killing.
	if _, err := h.b.setSuspendState(context.Background(), g, StateKilling, false); err != nil {
		t.Fatal(err)
	}
	if err := h.b.waitInformerState(context.Background(), g, StateKilling, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); !errors.Is(err, ErrKilled) {
		t.Fatalf("want the kill sequence finished, got %v", err)
	}
	if got := strings.Join(ff.callList(), ","); got != "kill" {
		t.Fatalf("calls = %s", got)
	}
	if h.mirror("vllm-m") != nil {
		t.Fatal("mirror must be deleted")
	}
}

func TestTranslate_KilledMirrorIsFailed(t *testing.T) {
	h, _ := suspendHarness(t)
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.b.setSuspendState(context.Background(), g, StateKilled, false)
	if err != nil {
		t.Fatal(err)
	}
	if p := h.b.translate(g, m); !killedGuest(p) {
		t.Fatalf("a Killed mirror must translate to a Failed guest: %+v", p.Status)
	}
	h.settled("Failed (killed)", killedGuest)
}

// The agent's Kill can stop the mirror before the kill sequence records Killed. The library
// never updates a Failed guest again, so a stopped Killing mirror must already translate to
// the kill reason, not to the mirror's own Failed status.
func TestTranslate_StoppedKillingMirrorIsKilled(t *testing.T) {
	h, _ := suspendHarness(t)
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	mirrorPod, err := h.b.setSuspendState(context.Background(), g, StateKilling, false)
	if err != nil {
		t.Fatal(err)
	}
	if p := h.b.translate(g, mirrorPod); p.Status.Phase == corev1.PodFailed || IsReady(p) {
		t.Fatalf("a running Killing mirror must translate to a live NotReady guest: %+v", p.Status)
	}
	stopped := mirrorPod.DeepCopy()
	stopped.Status.Phase = corev1.PodFailed
	if p := h.b.translate(g, stopped); !killedGuest(p) {
		t.Fatalf("a stopped Killing mirror must translate to a killed guest: %+v", p.Status)
	}
	deleting := mirrorPod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if p := h.b.translate(g, deleting); !killedGuest(p) {
		t.Fatalf("a deleted Killing mirror must translate to a killed guest: %+v", p.Status)
	}
}

// Suspend and Resume return only once the informer shows their final state, so a call issued
// right after one is judged on that state (a resume right after a suspend is not refused as
// still Suspending).
func TestSuspendResume_InformerShowsTheFinalStateOnReturn(t *testing.T) {
	hrn, _ := suspendHarness(t)
	informerState := func() string {
		m, err := hrn.b.mirrors.Pods("ns").Get("vllm-m")
		if err != nil {
			t.Fatal(err)
		}
		return m.Annotations[AnnotationSuspendState]
	}
	for i := range 5 {
		if _, err := hrn.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
			t.Fatalf("suspend %d: %v", i, err)
		}
		if got := informerState(); got != StateSuspended {
			t.Fatalf("after suspend %d the informer shows %q, want %s", i, got, StateSuspended)
		}
		if _, err := hrn.b.Resume(context.Background(), "ns", "vllm"); err != nil {
			t.Fatalf("resume right after suspend %d: %v", i, err)
		}
		if got := informerState(); got != "" {
			t.Fatalf("after resume %d the informer shows %q, want running", i, got)
		}
	}
}

func TestSuspend_NoFreezerConfigured(t *testing.T) {
	h := newHarness(t, testOptions())
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error without a snapshot agent")
	}
}

func TestDelete_KillsSuspendedMirrorFirst(t *testing.T) {
	h, ff := suspendHarness(t)
	h.suspended()
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.b.Delete(context.Background(), g.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ff.callList(), ","); got != "suspend:1,kill" {
		t.Fatalf("calls = %s, want the delete to have the agent kill first", got)
	}
}

func TestDelete_RunningMirrorIsNotKilled(t *testing.T) {
	h, ff := suspendHarness(t)
	g, err := h.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.b.Delete(context.Background(), g.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if got := ff.callList(); len(got) != 0 {
		t.Fatalf("a running mirror gets the normal SIGTERM, no agent kill: %v", got)
	}
}

// Lead decision D-VK-5 option c: a readiness-gate change on the guest re-emits its status at
// once, without a mirror event.
func TestGuestProbePolicy_C_WatchReadinessGates(t *testing.T) {
	harn := newHarness(t, testOptions())
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
