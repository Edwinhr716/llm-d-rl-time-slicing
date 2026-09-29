package mirror

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

func runningMirror() *corev1.Pod {
	now := metav1.Now()
	return &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning, PodIP: "10.1.2.3", PodIPs: []corev1.PodIP{{IP: "10.1.2.3"}},
		HostIP: "10.0.0.9", StartTime: &now, QOSClass: corev1.PodQOSBurstable,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "vllm", Ready: true, RestartCount: 2,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		}},
	}}
}

func TestTranslateStatus(t *testing.T) {
	g := testGuest()
	g.Spec.ReadinessGates = nil
	g.Status.QOSClass = corev1.PodQOSGuaranteed
	out := TranslateStatus(g, runningMirror())
	s := out.Status
	if s.Phase != corev1.PodRunning || s.PodIP != "10.1.2.3" || s.HostIP != "10.0.0.9" || s.StartTime == nil {
		t.Errorf("status: %+v", s)
	}
	if s.ContainerStatuses[0].RestartCount != 2 || s.ContainerStatuses[0].State.Running == nil {
		t.Errorf("container status: %+v", s.ContainerStatuses)
	}
	if s.QOSClass != corev1.PodQOSGuaranteed {
		t.Errorf("qosClass must stay the guest's, got %s", s.QOSClass)
	}
	if g.Status.PodIP != "" {
		t.Error("TranslateStatus modified the guest")
	}
}

func TestReadinessGateHoldsReady(t *testing.T) {
	g := testGuest() // has gate timeslice.io/serving, not set
	s := TranslateStatus(g, runningMirror()).Status
	if c := findCondition(s.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionFalse {
		t.Errorf("Ready must be false while a gate is unset: %+v", s.Conditions)
	}
	g.Status.Conditions = []corev1.PodCondition{{Type: "timeslice.io/serving", Status: corev1.ConditionTrue}}
	s = TranslateStatus(g, runningMirror()).Status
	if c := findCondition(s.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("Ready must be true once the gate is true: %+v", s.Conditions)
	}
	if findCondition(s.Conditions, "timeslice.io/serving") == nil {
		t.Error("gate condition must be kept")
	}
}

func TestTerminalStatus(t *testing.T) {
	g := testGuest()
	out := TerminalStatus(g, runningMirror(), ReasonGuestDeleted).Status
	if out.Phase != corev1.PodSucceeded || out.ContainerStatuses[0].State.Terminated == nil {
		t.Errorf("guest delete: %+v", out)
	}
	out = TerminalStatus(g, nil, ReasonMirrorDeleted).Status
	if out.Phase != corev1.PodFailed || len(out.ContainerStatuses) != 1 || out.ContainerStatuses[0].State.Terminated == nil {
		t.Errorf("mirror gone: %+v", out)
	}
}

func TestMarkNotReady(t *testing.T) {
	st := runningMirror().Status
	at := metav1.NewTime(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC))
	MarkNotReady(&st, st.Conditions, "Suspending", "msg", at)
	ready := findCondition(st.Conditions, corev1.PodReady)
	if ready == nil || ready.Status != corev1.ConditionFalse || ready.Reason != "Suspending" ||
		!ready.LastTransitionTime.Equal(&at) {
		t.Fatalf("Ready: %+v", ready)
	}
	if c := findCondition(st.Conditions, corev1.ContainersReady); c == nil || c.Status != corev1.ConditionFalse {
		t.Fatalf("ContainersReady must be added as False: %+v", st.Conditions)
	}
	if !st.ContainerStatuses[0].Ready {
		t.Fatal("container ready flags are the caller's")
	}
	// Already False on the guest: the transition time stays, the reason follows.
	prev := append([]corev1.PodCondition(nil), st.Conditions...)
	fresh := runningMirror().Status
	MarkNotReady(&fresh, prev, "Suspended", "msg2", metav1.NewTime(at.Add(time.Minute)))
	ready = findCondition(fresh.Conditions, corev1.PodReady)
	if !ready.LastTransitionTime.Equal(&at) || ready.Reason != "Suspended" {
		t.Fatalf("Ready after second mark: %+v", ready)
	}
}

// A failed readiness probe and a suspend give the same NotReady (MarkNotReady); only the
// reason differs.
func TestTranslateStatusWith_ProbeNotReadyUsesMarkNotReady(t *testing.T) {
	guest := testGuest()
	guest.Spec.ReadinessGates = nil
	st := TranslateStatusWith(guest, runningMirror(), fixedVerdict(false)).Status
	for _, ct := range []corev1.PodConditionType{corev1.PodReady, corev1.ContainersReady} {
		c := findCondition(st.Conditions, ct)
		if c == nil || c.Status != corev1.ConditionFalse || c.Reason != ReasonContainersNotReady {
			t.Fatalf("%s: %+v", ct, c)
		}
	}
	// Suspended wins over a passing probe: the verdict is applied first, the suspend state last.
	st = TranslateStatusWith(guest, suspendedMirror(StateSuspended), fixedVerdict(true)).Status
	if c := findCondition(st.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionFalse ||
		c.Reason != StateSuspended || st.ContainerStatuses[0].Ready {
		t.Fatalf("suspended guest with a passing probe: %+v", st)
	}
}

type fixedVerdict bool

//nolint:gocritic // unnamedResult: the interface's (ready, has) pair; nonamedreturns forbids naming them
func (v fixedVerdict) ContainerReady(*corev1.Pod, string) (bool, bool) { return bool(v), true }

func suspendedMirror(state string) *corev1.Pod {
	m := runningMirror()
	m.Annotations = map[string]string{
		AnnotationSuspendState: state, AnnotationGuestEpoch: "3",
		AnnotationSuspendStateSince: "2026-09-30T12:00:00Z",
	}
	return m
}

func TestTranslateStatus_SuspendStates(t *testing.T) {
	guest := testGuest()
	guest.Spec.ReadinessGates = nil
	for _, state := range []string{StateSuspending, StateSuspended, StateResuming, StateKilling} {
		st := TranslateStatus(guest, suspendedMirror(state)).Status
		if IsReady(&corev1.Pod{Status: st}) || st.Phase != corev1.PodRunning {
			t.Errorf("%s: want Running and NotReady, got %+v", state, st)
		}
		c := findCondition(st.Conditions, ConditionSuspended)
		want := corev1.ConditionFalse
		if state == StateSuspended {
			want = corev1.ConditionTrue
		}
		if c == nil || c.Status != want || c.Reason != state {
			t.Errorf("%s: condition %+v", state, c)
		}
		cs := st.ContainerStatuses[0]
		waiting := cs.State.Waiting != nil && cs.State.Waiting.Reason == StateSuspended
		if waiting != (state == StateSuspended) || cs.RestartCount != 2 {
			t.Errorf("%s: container %+v", state, cs)
		}
	}
	if st := TranslateStatus(guest, runningMirror()).Status; findCondition(st.Conditions, ConditionSuspended) != nil {
		t.Error("a running guest has no suspended condition")
	}
}

func TestProbeUntilReady(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" || calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	g := testGuest()
	g.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.Parse(port)},
	}}
	m := runningMirror()
	m.Status.PodIP = host

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ProbeUntilReady(5*time.Millisecond, netProbeOnce)(ctx, g, m); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("want 3 attempts, got %d", calls.Load())
	}

	short, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	g.Spec.Containers[0].ReadinessProbe.HTTPGet.Path = "/never"
	if err := ProbeUntilReady(5*time.Millisecond, netProbeOnce)(short, g, m); err == nil {
		t.Fatal("want an error when the probe never passes")
	}

	// No httpGet/tcpSocket readiness probe (testGuest's exec probe is not one): passes at once.
	if err := ProbeUntilReady(time.Millisecond, netProbeOnce)(context.Background(), testGuest(), m); err != nil {
		t.Fatal(err)
	}
}

func netProbeOnce(ctx context.Context, podIP string, c *corev1.Container) error {
	return probe.NetProber{}.Probe(ctx, probe.Target{PodIP: podIP, Container: c})
}
