package mirror

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func suspendedCond(p *corev1.Pod) bool {
	c := findCondition(p.Status.Conditions, ConditionSuspended)
	return c != nil && c.Status == corev1.ConditionTrue
}

func TestLoopFreezer_RecordsTheSuspendState(t *testing.T) {
	hrn, ff := suspendHarness(t)
	lf, err := hrn.b.LoopFreezer()
	if err != nil {
		t.Fatal(err)
	}
	if err := lf.Suspend(context.Background(), hrn.mirror("vllm-m"), 5); err != nil {
		t.Fatal(err)
	}
	if s := hrn.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != StateSuspended {
		t.Fatalf("state = %q, want %s", s, StateSuspended)
	}
	hrn.settled("suspended", suspendedCond)
	if err := lf.Resume(context.Background(), hrn.mirror("vllm-m"), 6); err != nil {
		t.Fatal(err)
	}
	if _, ok := hrn.mirror("vllm-m").Annotations[AnnotationSuspendState]; ok {
		t.Fatalf("state must be cleared after the resume: %v", hrn.mirror("vllm-m").Annotations)
	}
	// The loop, not the freezer, runs the engine check: no readycheck call.
	if got, want := strings.Join(ff.callList(), ","), "freeze:5,thaw:6"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	hrn.settled("not suspended", func(p *corev1.Pod) bool { return !suspendedCond(p) })
}

func TestLoopFreezer_FreezeFailureThawsAndClears(t *testing.T) {
	hrn, ff := suspendHarness(t)
	ff.suspendErr = errors.New("stuck task")
	lf, err := hrn.b.LoopFreezer()
	if err != nil {
		t.Fatal(err)
	}
	if err := lf.Suspend(context.Background(), hrn.mirror("vllm-m"), 3); err == nil {
		t.Fatal("want an error")
	}
	if got := strings.Join(ff.callList(), ","); got != "freeze:3,thaw:3" {
		t.Fatalf("calls = %s", got)
	}
	if _, ok := hrn.mirror("vllm-m").Annotations[AnnotationSuspendState]; ok {
		t.Fatalf("state must be cleared: %v", hrn.mirror("vllm-m").Annotations)
	}
}

func TestLoopFreezer_ThawFailureStaysResuming(t *testing.T) {
	hrn, ff := suspendHarness(t)
	lf, err := hrn.b.LoopFreezer()
	if err != nil {
		t.Fatal(err)
	}
	if err := lf.Suspend(context.Background(), hrn.mirror("vllm-m"), 1); err != nil {
		t.Fatal(err)
	}
	ff.resumeErr = errors.New("no thaw")
	if err := lf.Resume(context.Background(), hrn.mirror("vllm-m"), 2); err == nil {
		t.Fatal("want an error")
	}
	if s := hrn.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != StateResuming {
		t.Fatalf("state = %q, want %s", s, StateResuming)
	}
}

func TestLoopFreezer_NeedsAFreezeBackend(t *testing.T) {
	hrn := newHarness(t, testOptions())
	if _, err := hrn.b.LoopFreezer(); !errors.Is(err, ErrNoFreezer) {
		t.Fatalf("err = %v, want ErrNoFreezer", err)
	}
	if _, err := hrn.b.HostFrozen(&corev1.Pod{}); !errors.Is(err, ErrNoFreezer) {
		t.Fatalf("HostFrozen err = %v, want ErrNoFreezer", err)
	}
}

func TestAttempt(t *testing.T) {
	m := func(uid, job string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelMirrorOf: uid, LabelJobID: job}}}
	}
	cases := []struct {
		m    *corev1.Pod
		want int
		ok   bool
	}{
		{m("u-1", "u-1-0"), 0, true},
		{m("u-1", "u-1-12"), 12, true},
		{m("u-1", ""), 0, false},
		{m("u-1", "other-3"), 0, false},
		{m("u-1", "u-1-x"), 0, false},
		{m("", "u-1-3"), 0, false},
	}
	for _, tc := range cases {
		if got, ok := Attempt(tc.m); got != tc.want || ok != tc.ok {
			t.Errorf("Attempt(%v) = %d, %t; want %d, %t", tc.m.Labels, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRelistHelpers(t *testing.T) {
	hrn, _ := suspendHarness(t)
	ctx := context.Background()
	mirrors, err := hrn.b.ListMirrors(ctx)
	if err != nil || len(mirrors) != 1 || mirrors[0].Name != "vllm-m" {
		t.Fatalf("ListMirrors = %v, %v", mirrors, err)
	}
	// The guest is not in the API yet: gone.
	if g, err := hrn.b.GuestNow(ctx, mirrors[0]); err != nil || g != nil {
		t.Fatalf("GuestNow without the guest = %v, %v", g, err)
	}
	guest, err := hrn.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	other := guest.DeepCopy()
	other.UID = "someone-else"
	other.ResourceVersion = ""
	if _, err := hrn.client.CoreV1().Pods("ns").Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if g, err := hrn.b.GuestNow(ctx, mirrors[0]); err != nil || g != nil {
		t.Fatalf("GuestNow with another pod of the same name = %v, %v", g, err)
	}
	if err := hrn.client.CoreV1().Pods("ns").Delete(ctx, "vllm", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	same := guest.DeepCopy()
	same.ResourceVersion = ""
	if _, err := hrn.client.CoreV1().Pods("ns").Create(ctx, same, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if g, err := hrn.b.GuestNow(ctx, mirrors[0]); err != nil || g == nil || g.UID != guest.UID {
		t.Fatalf("GuestNow = %v, %v", g, err)
	}

	if _, err := hrn.b.RecordSuspendState(ctx, guest, StateSuspended); err != nil {
		t.Fatal(err)
	}
	fs, err := hrn.b.FreezeStateOf(ctx, "ns", "vllm")
	if err != nil {
		t.Fatal(err)
	}
	if fs.Mirror != "vllm-m" || fs.State != StateSuspended || fs.Epoch != 0 || fs.Frozen || fs.HostError != "" {
		t.Fatalf("FreezeStateOf = %+v", fs)
	}
	if _, err := hrn.b.FreezeStateOf(ctx, "ns", "nope"); err == nil {
		t.Fatal("want an error for a guest without a mirror")
	}
}

func TestAdopt_RebuildsTheGate(t *testing.T) {
	opts := testOptions()
	opts.Gated = true
	hrn := newHarness(t, opts)
	uid := types.UID("g1")
	hrn.b.Adopt(uid, 3, false, ReasonSuspended)
	hrn.b.Adopt(uid, 1, false, ReasonSuspended) // a lower attempt never lowers the counter
	hrn.b.mu.Lock()
	attempt, released, reason := hrn.b.gate.attempts[uid], hrn.b.gate.released[uid], hrn.b.gate.reason[uid]
	hrn.b.mu.Unlock()
	if attempt != 3 || released || reason != ReasonSuspended {
		t.Fatalf("gate = %d %t %q", attempt, released, reason)
	}
	hrn.b.Adopt(uid, 3, true, "")
	hrn.b.mu.Lock()
	released, reason = hrn.b.gate.released[uid], hrn.b.gate.reason[uid]
	hrn.b.mu.Unlock()
	if !released || reason != "" {
		t.Fatalf("gate after release = %t %q", released, reason)
	}
}
