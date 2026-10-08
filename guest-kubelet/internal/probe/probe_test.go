package probe_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

func TestThresholds(t *testing.T) {
	t.Parallel()
	th := probe.NewThresholds(2, 3)
	steps := []struct {
		success, ready, changed bool
	}{
		{true, false, false}, // starts not ready; one success is not enough
		{true, true, true},
		{false, true, false},
		{true, true, false}, // a success resets the failure run
		{false, true, false},
		{false, true, false},
		{false, false, true},
		{false, false, false},
	}
	for i, step := range steps {
		ready, changed := th.Observe(step.success)
		if ready != step.ready || changed != step.changed {
			t.Fatalf("step %d: got (%t, %t), want (%t, %t)", i, ready, changed, step.ready, step.changed)
		}
	}
}

func TestThresholdsDefaults(t *testing.T) {
	t.Parallel()
	th := probe.NewThresholds(0, 0) // 1 and 3
	if ready, changed := th.Observe(true); !ready || !changed {
		t.Fatal("one success must be enough by default")
	}
	for i := range 2 {
		if ready, _ := th.Observe(false); !ready {
			t.Fatalf("failure %d flipped the verdict before the default threshold of 3", i+1)
		}
	}
	if ready, changed := th.Observe(false); ready || !changed {
		t.Fatal("third failure must flip the verdict")
	}
}

func TestResolvePort(t *testing.T) {
	t.Parallel()
	ctr := &corev1.Container{Name: "c", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}}}
	cases := []struct {
		port    intstr.IntOrString
		want    int
		wantErr bool
	}{
		{intstr.FromInt32(9000), 9000, false},
		{intstr.FromString("http"), 8000, false},
		{intstr.FromString("7000"), 7000, false},
		{intstr.FromString("metrics"), 0, true},
		{intstr.FromInt32(0), 0, true},
		{intstr.FromInt32(70000), 0, true},
	}
	for _, tc := range cases {
		got, err := probe.ResolvePort(tc.port, ctr)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("ResolvePort(%v): got (%d, %v), want %d (error %t)", tc.port.String(), got, err, tc.want, tc.wantErr)
		}
	}
}

func TestSupported(t *testing.T) {
	t.Parallel()
	ok := []*corev1.Probe{
		{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{}}},
		{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{}}},
	}
	for _, p := range ok {
		if err := probe.Supported(p); err != nil {
			t.Errorf("Supported(%+v) = %v", p.ProbeHandler, err)
		}
	}
	bad := []*corev1.Probe{
		{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{}}},
		{ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{}}},
		{},
	}
	for _, p := range bad {
		if err := probe.Supported(p); !errors.Is(err, probe.ErrUnsupported) {
			t.Errorf("Supported(%+v) = %v, want ErrUnsupported", p.ProbeHandler, err)
		}
	}
}

// serverPort starts an HTTP server answering with status code and returns its port.
func serverPort(t *testing.T, code int) int {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" || r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func httpTarget(port int) probe.Target {
	return probe.Target{PodIP: "127.0.0.1", Container: &corev1.Container{
		Name: "c",
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "healthz", Port: intstr.FromString(strconv.Itoa(port))},
		}},
	}}
}

func TestNetProberHTTP(t *testing.T) {
	t.Parallel()
	for code, wantOK := range map[int]bool{
		http.StatusOK: true, http.StatusFound: true, http.StatusNotFound: false, http.StatusInternalServerError: false,
	} {
		err := probe.NetProber{}.Probe(t.Context(), httpTarget(serverPort(t, code)))
		if (err == nil) != wantOK {
			t.Errorf("HTTP %d: got %v, want success %t", code, err, wantOK)
		}
	}
}

func TestNetProberTCP(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, isTCP := ln.Addr().(*net.TCPAddr)
	if !isTCP {
		t.Fatalf("listener address %T", ln.Addr())
	}
	port := addr.Port
	target := probe.Target{PodIP: "127.0.0.1", Container: &corev1.Container{
		Name: "c",
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(strconv.Itoa(port))},
		}},
	}}
	if err := (probe.NetProber{}).Probe(t.Context(), target); err != nil {
		t.Errorf("open port: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (probe.NetProber{}).Probe(t.Context(), target); err == nil {
		t.Error("closed port must fail")
	}
}

// fakeProber succeeds while ok is true.
type fakeProber struct{ ok atomic.Bool }

func (f *fakeProber) Probe(context.Context, probe.Target) error {
	if f.ok.Load() {
		return nil
	}
	return errors.New("down")
}

// changes counts OnChange calls.
type changes struct {
	mu    sync.Mutex
	count int
}

func (c *changes) add(string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
}

func (c *changes) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func probedGuest(handler corev1.ProbeHandler) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "ns", UID: "uid-1"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "server", ReadinessProbe: &corev1.Probe{ProbeHandler: handler, PeriodSeconds: 1, FailureThreshold: 1}},
			{Name: "plain"},
		}},
	}
}

func runningMirror(containerID string) *corev1.Pod {
	return &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning, PodIP: "10.1.2.3",
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "server", ContainerID: containerID,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
		}},
	}}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestManager(t *testing.T) {
	t.Parallel()
	fake := &fakeProber{}
	calls := &changes{}
	mgr := probe.NewManager(t.Context(), probe.Options{OnChange: calls.add, Prober: fake})
	guest := probedGuest(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8000)}})
	ready := func() bool {
		verdict, has := mgr.ContainerReady(guest, "server")
		if !has {
			t.Fatal("a probed container must have a verdict")
		}
		return verdict
	}

	if _, has := mgr.ContainerReady(guest, "plain"); has {
		t.Error("a container without a probe must have no verdict")
	}
	mgr.Sync(guest, runningMirror("c1"))
	if ready() {
		t.Fatal("verdict must start not ready")
	}
	fake.ok.Store(true)
	waitFor(t, "ready", ready)
	fake.ok.Store(false)
	waitFor(t, "not ready", func() bool { return !ready() })
	if calls.get() < 2 {
		t.Errorf("OnChange called %d times, want at least 2", calls.get())
	}

	// Overrides win over the probes, and clearing one hands readiness back.
	forced := true
	mgr.SetOverride("ns", "g", &forced)
	if !ready() {
		t.Error("override true must make the container ready")
	}
	if verdict, has := mgr.ContainerReady(guest, "plain"); !verdict || !has {
		t.Error("override applies to every container")
	}
	mgr.SetOverride("ns", "g", nil)
	if ready() {
		t.Error("cleared override must return to the probe verdict")
	}

	// A restarted container (new container ID) starts over from not ready.
	mgr.Sync(guest, runningMirror("c2"))
	if ready() {
		t.Error("a restarted container must start not ready")
	}

	// A stopped container loses its verdict at once.
	fake.ok.Store(true)
	waitFor(t, "ready again", ready)
	stopped := runningMirror("c2")
	stopped.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
	}
	mgr.Sync(guest, stopped)
	if ready() {
		t.Error("a container that stopped must not be ready")
	}
	mgr.Forget(guest.UID, "ns", "g")
}

func TestManagerUnsupported(t *testing.T) {
	t.Parallel()
	rec := record.NewFakeRecorder(10)
	mgr := probe.NewManager(t.Context(), probe.Options{Recorder: rec, Prober: &fakeProber{}})
	guest := probedGuest(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}})
	mgr.Sync(guest, runningMirror("c1"))
	mgr.Sync(guest, runningMirror("c1"))
	if verdict, has := mgr.ContainerReady(guest, "server"); verdict || !has {
		t.Errorf("exec probe: got (%t, %t), want not ready", verdict, has)
	}
	if len(rec.Events) != 1 {
		t.Fatalf("want exactly one event, got %d", len(rec.Events))
	}
	if ev := <-rec.Events; !strings.Contains(ev, probe.ReasonProbeUnsupported) {
		t.Errorf("event: %s", ev)
	}
}
