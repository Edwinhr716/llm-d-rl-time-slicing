package mirror //nolint:testpackage // uses the package-internal harness and Backend state

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
)

// Tests for PENDING LEAD DECISION D-VK-6 (--mirror-identity). TestMirrorIdentity_Readopt_* and
// TestMirrorIdentity_Incarnation_* belong to one option each; the rest hold for both.

func identityOptions(id Identity) Options {
	o := testOptions()
	o.Identity = id
	return o
}

func readyGuest(uid string) *corev1.Pod {
	g := cpuGuest(uid)
	g.Status.Phase = corev1.PodRunning
	g.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return g
}

func noOwnerRef() *Config {
	cfg := testOptions().Config
	return &cfg
}

func liveMirror(t *testing.T, g *corev1.Pod, cfg *Config, uid types.UID, attempt int) *corev1.Pod {
	t.Helper()
	m, err := Build(g, *cfg)
	if err != nil {
		t.Fatal(err)
	}
	m.UID = uid
	m.Labels[LabelJobID] = JobID(g.UID, attempt)
	m.Status.Phase = corev1.PodRunning
	return m
}

// mirrorDeletes returns the options of every delete call for the mirror vllm-m.
func (h *harness) mirrorDeletes() []metav1.DeleteOptions {
	var out []metav1.DeleteOptions
	for _, a := range h.client.Actions() {
		if d, ok := a.(k8stesting.DeleteAction); ok && d.GetName() == "vllm-m" {
			out = append(out, d.GetDeleteOptions())
		}
	}
	return out
}

func (h *harness) waitGone(name string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := h.b.mirrors.Pods("ns").Get(name); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("informer still has %s", name)
}

func checkNormalGrace(t *testing.T, ds []metav1.DeleteOptions) {
	t.Helper()
	for _, d := range ds {
		if d.GracePeriodSeconds != nil {
			t.Errorf("mirror delete must use normal grace, got GracePeriodSeconds=%d", *d.GracePeriodSeconds)
		}
	}
}

func TestJobID(t *testing.T) {
	uid := types.UID("0f1e-2d3c")
	if got := JobID(uid, 2); got != "0f1e-2d3c-2" {
		t.Errorf("JobID: %s", got)
	}
	if n, ok := AttemptOf("0f1e-2d3c-7", uid); !ok || n != 7 {
		t.Errorf("AttemptOf: %d %v", n, ok)
	}
	for _, bad := range []string{"", "other-2", "0f1e-2d3c-", "0f1e-2d3c-x", "0f1e-2d3c-0"} {
		if _, ok := AttemptOf(bad, uid); ok {
			t.Errorf("AttemptOf(%q) should fail", bad)
		}
	}
}

func TestMirrorIdentity_Parse(t *testing.T) {
	for _, s := range []string{"readopt", "incarnation"} {
		if i, err := ParseIdentity(s); err != nil || string(i) != s {
			t.Errorf("ParseIdentity(%q): %v %v", s, i, err)
		}
	}
	for _, s := range []string{"", "Readopt", "new"} {
		if _, err := ParseIdentity(s); err == nil {
			t.Errorf("ParseIdentity(%q) should fail", s)
		}
	}
}

func TestCreateSetsJobID(t *testing.T) {
	for _, id := range []Identity{IdentityReadopt, IdentityIncarnation} {
		h := newHarness(t, identityOptions(id))
		g := cpuGuest("g1")
		h.addGuest(g)
		if err := h.b.Create(context.Background(), g); err != nil {
			t.Fatal(err)
		}
		if m := h.mirror("vllm-m"); m.Labels[LabelJobID] != "g1-1" {
			t.Errorf("%s: job-id %q, want g1-1", id, m.Labels[LabelJobID])
		}
	}
}

func TestMirrorIdentity_Readopt_RestartKeepsMirrorAndJobID(t *testing.T) {
	g := readyGuest("g1")
	m := liveMirror(t, g, noOwnerRef(), "mirror-uid", 1)
	opts := identityOptions(IdentityReadopt)
	h := buildHarness(t, &opts, g, m)
	h.addGuest(g)
	h.start()

	if ds := h.mirrorDeletes(); len(ds) != 0 {
		t.Fatalf("readopt must not delete the mirror: %v", ds)
	}
	if got, err := h.b.Get("ns", "vllm"); err != nil || got.UID != "g1" {
		t.Fatalf("Get after restart: %v %v", got, err)
	}
	// The library calls CreatePod only when Get fails; a racing call is still a no-op.
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	cur := h.mirror("vllm-m")
	if cur.UID != "mirror-uid" || cur.Labels[LabelJobID] != "g1-1" {
		t.Errorf("want same mirror and job id, got uid=%s jobID=%s", cur.UID, cur.Labels[LabelJobID])
	}
}

// Also covers D-VK-1 --mirror-owner-ref=true on the adoption path.
func TestMirrorIdentity_Readopt_GuestRecreatedKeepsJobID(t *testing.T) {
	opts := identityOptions(IdentityReadopt)
	opts.OwnerRef = true
	orphan := liveMirror(t, cpuGuest("old-uid"), &opts.Config, "mirror-uid", 1)
	h := newHarness(t, opts, orphan)

	g := cpuGuest("new-uid")
	h.addGuest(g)
	if err := h.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	m := h.mirror("vllm-m")
	if m.UID != "mirror-uid" || m.Labels[LabelMirrorOf] != "new-uid" || m.Labels[LabelJobID] != "old-uid-1" {
		t.Errorf("want the orphan adopted with its job id, got uid=%s of=%s jobID=%s",
			m.UID, m.Labels[LabelMirrorOf], m.Labels[LabelJobID])
	}
	if len(m.OwnerReferences) != 1 || m.OwnerReferences[0].UID != "new-uid" {
		t.Errorf("ownerRefs: %v", m.OwnerReferences)
	}
}

func TestMirrorIdentity_Incarnation_RestartReplacesMirror(t *testing.T) {
	guest := readyGuest("g1")
	old := liveMirror(t, guest, noOwnerRef(), "old-mirror", 1)
	opts := identityOptions(IdentityIncarnation)
	hs := buildHarness(t, &opts, guest, old)
	hs.addGuest(guest)
	hs.start()

	ds := hs.mirrorDeletes()
	if len(ds) != 1 || ds[0].Preconditions == nil || *ds[0].Preconditions.UID != "old-mirror" {
		t.Fatalf("want one delete of the old mirror, got %v", ds)
	}
	checkNormalGrace(t, ds)
	if hs.mirror("vllm-m") != nil {
		t.Fatal("Start must return only after the old mirror is gone")
	}
	cur, err := hs.client.CoreV1().Pods("ns").Get(context.Background(), "vllm", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c := findCondition(cur.Status.Conditions, corev1.PodReady)
	if c == nil || c.Status != corev1.ConditionFalse || c.Reason != ReasonMirrorReplaced {
		t.Errorf("guest must be NotReady while its mirror is replaced: %+v", c)
	}
	if _, err := hs.b.Get("ns", "vllm"); err == nil {
		t.Error("Get must report no mirror, so the library creates the new one")
	}

	if err := hs.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	m := hs.mirror("vllm-m")
	if m == nil || m.UID == "old-mirror" || m.Labels[LabelJobID] != "g1-2" {
		t.Fatalf("want a new mirror with job id g1-2, got %+v", m)
	}

	// The old mirror's delete event must not fail the guest.
	deadline := time.Now().Add(5 * time.Second)
	for hs.b.isRetired("old-mirror") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hs.b.isRetired("old-mirror") {
		t.Fatal("delete event for the old mirror never seen")
	}
	for _, p := range hs.emittedPods() {
		if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
			t.Errorf("guest reported terminal (%s, %s) by the replacement", p.Status.Phase, p.Status.Reason)
		}
	}
}

func TestMirrorIdentity_Incarnation_CreateWaitsForOldMirror(t *testing.T) {
	guest := readyGuest("g1")
	old := liveMirror(t, guest, noOwnerRef(), "old-mirror", 3)
	now := metav1.Now()
	old.DeletionTimestamp = &now // deleted, but its processes are still stopping
	opts := identityOptions(IdentityIncarnation)
	opts.ReplaceWait = 300 * time.Millisecond
	hs := buildHarness(t, &opts, guest, old)
	var stuck atomic.Bool
	stuck.Store(true)
	hs.client.PrependReactor("delete", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		d, ok := a.(k8stesting.DeleteAction)
		return ok && stuck.Load() && d.GetName() == "vllm-m", nil, nil
	})
	hs.addGuest(guest)
	hs.start() // returns after ReplaceWait with the old mirror still there

	if err := hs.b.Create(context.Background(), guest); err == nil {
		t.Error("want a retry error while the old mirror is still terminating")
	}
	if m := hs.mirror("vllm-m"); m == nil || m.UID != "old-mirror" {
		t.Fatalf("no new mirror may exist beside the old one, got %+v", m)
	}
	if _, err := hs.b.Get("ns", "vllm"); err == nil {
		t.Error("a retired mirror must not count as the guest's mirror")
	}
	checkNormalGrace(t, hs.mirrorDeletes())

	// The real kubelet finishes; the pod object goes away.
	stuck.Store(false)
	if err := hs.client.CoreV1().Pods("ns").Delete(context.Background(), "vllm-m", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	hs.waitGone("vllm-m")
	if err := hs.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	if m := hs.mirror("vllm-m"); m.UID == "old-mirror" || m.Labels[LabelJobID] != "g1-4" {
		t.Errorf("want a new mirror with job id g1-4, got uid=%s jobID=%s", m.UID, m.Labels[LabelJobID])
	}
}

func TestMirrorIdentity_Incarnation_RestartRetiresOrphan(t *testing.T) {
	orphan := liveMirror(t, cpuGuest("gone"), noOwnerRef(), "orphan-uid", 1)
	hs := newHarness(t, identityOptions(IdentityIncarnation), orphan)
	if hs.mirror("vllm-m") != nil {
		t.Error("an orphan from the previous incarnation must be deleted at startup")
	}
	checkNormalGrace(t, hs.mirrorDeletes())
}

func TestMirrorIdentity_Incarnation_OrphanIsNotAdopted(t *testing.T) {
	opts := identityOptions(IdentityIncarnation)
	hs := newHarness(t, opts)
	orphan := liveMirror(t, cpuGuest("old-uid"), &opts.Config, "", 1)
	orphan, err := hs.client.CoreV1().Pods("ns").Create(context.Background(), orphan, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hs.waitInformer("old-uid")

	g := cpuGuest("new-uid") // same name and containers: readopt would adopt it
	hs.addGuest(g)
	if err := hs.b.Create(context.Background(), g); err == nil {
		t.Error("want a retry error while the orphan is deleted")
	}
	if hs.mirror("vllm-m") != nil {
		t.Fatal("the orphan must be deleted, not adopted")
	}
	checkNormalGrace(t, hs.mirrorDeletes())
	hs.waitGone("vllm-m")
	if err := hs.b.Create(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if m := hs.mirror("vllm-m"); m.UID == orphan.UID || m.Labels[LabelJobID] != "new-uid-1" {
		t.Errorf("want a new mirror with job id new-uid-1, got uid=%s jobID=%s", m.UID, m.Labels[LabelJobID])
	}
}
