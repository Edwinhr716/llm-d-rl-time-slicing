package probe_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

// Tests for the exec and grpc handlers and the startup gate.

// startupProber answers startup probes (tcpSocket in these tests) from startupOK and every other
// probe from readyOK.
type startupProber struct{ startupOK, readyOK atomic.Bool }

func (s *startupProber) Probe(_ context.Context, target probe.Target) error {
	flag := &s.readyOK
	if target.Spec().TCPSocket != nil {
		flag = &s.startupOK
	}
	if flag.Load() {
		return nil
	}
	return errors.New("down")
}

func startupGuest(withReadiness bool) *corev1.Pod {
	guest := probedGuest(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8000)}})
	server := &guest.Spec.Containers[0]
	server.StartupProbe = &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8000)}},
		PeriodSeconds: 1, FailureThreshold: 30,
	}
	if !withReadiness {
		server.ReadinessProbe = nil
	}
	return guest
}

func TestStartupGate(t *testing.T) {
	t.Parallel()
	fake := &startupProber{}
	fake.readyOK.Store(true)
	mgr := probe.NewManager(t.Context(), probe.Options{Prober: fake})
	guest := startupGuest(true)
	ready := func() bool {
		verdict, has := mgr.ContainerReady(guest, "server")
		if !has {
			t.Fatal("a gated container must have a verdict")
		}
		return verdict
	}
	mgr.Sync(guest, runningMirror("c1"))
	// The readiness probe would pass, but the startup probe has not: not ready.
	time.Sleep(1500 * time.Millisecond) // at least one failed startup attempt and one period
	if ready() {
		t.Fatal("ready before the startup probe passed")
	}
	fake.startupOK.Store(true)
	waitFor(t, "ready after startup", ready)
	// Once started, the startup probe no longer matters; the readiness probe does.
	fake.startupOK.Store(false)
	fake.readyOK.Store(false)
	waitFor(t, "not ready from the readiness probe", func() bool { return !ready() })
	// A restarted container is gated again.
	fake.readyOK.Store(true)
	mgr.Sync(guest, runningMirror("c2"))
	if ready() {
		t.Error("a restarted container must wait for its startup probe again")
	}
	mgr.Forget(guest.UID, "ns", "g")
}

// A container with only a startupProbe is ready once the probe passes (as with the kubelet).
func TestStartupOnly(t *testing.T) {
	t.Parallel()
	fake := &startupProber{}
	calls := &changes{}
	mgr := probe.NewManager(t.Context(), probe.Options{Prober: fake, OnChange: calls.add})
	guest := startupGuest(false)
	mgr.Sync(guest, runningMirror("c1"))
	if verdict, has := mgr.ContainerReady(guest, "server"); verdict || !has {
		t.Fatalf("before startup: got (%t, %t), want (false, true)", verdict, has)
	}
	fake.startupOK.Store(true)
	waitFor(t, "ready after startup", func() bool {
		verdict, has := mgr.ContainerReady(guest, "server")
		return verdict && has
	})
	if calls.get() == 0 {
		t.Error("passing the startup probe must call OnChange")
	}
}

// fakeExecer records exec calls and succeeds while ok is true.
type fakeExecer struct {
	ok    atomic.Bool
	mu    sync.Mutex
	calls []string
}

func (f *fakeExecer) Exec(_ context.Context, namespace, pod, container string, command []string) error {
	f.mu.Lock()
	f.calls = append(f.calls, namespace+"/"+pod+"/"+container+":"+command[0])
	f.mu.Unlock()
	if f.ok.Load() {
		return nil
	}
	return errors.New("exit status 1")
}

func (f *fakeExecer) seen(call string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.calls, call)
}

func TestAllProberExec(t *testing.T) {
	t.Parallel()
	execer := &fakeExecer{}
	execReady := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
	if err := (probe.AllProber{}).Probe(t.Context(), probe.Target{Container: &corev1.Container{
		Name: "c", ReadinessProbe: execReady,
	}}); !errors.Is(err, probe.ErrUnsupported) {
		t.Errorf("exec without an executor: got %v, want ErrUnsupported", err)
	}

	mgr := probe.NewManager(t.Context(), probe.Options{Prober: probe.AllProber{Exec: execer}})
	guest := probedGuest(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"check"}}})
	mirror := runningMirror("c1")
	mirror.Namespace, mirror.Name = "ns", "g-m"
	mgr.Sync(guest, mirror)
	ready := func() bool {
		verdict, has := mgr.ContainerReady(guest, "server")
		return verdict && has
	}
	waitFor(t, "an exec in the mirror", func() bool { return execer.seen("ns/g-m/server:check") })
	if ready() {
		t.Fatal("a failing exec probe must not make the container ready")
	}
	execer.ok.Store(true)
	waitFor(t, "ready from the exec probe", ready)
	mgr.Forget(guest.UID, "ns", "g")
}

func TestAllProberGRPC(t *testing.T) {
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
	srv := grpc.NewServer()
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		if err := <-served; err != nil {
			t.Errorf("grpc server: %v", err)
		}
	})

	target := func(service string) probe.Target {
		action := &corev1.GRPCAction{Port: int32(addr.Port)} //nolint:gosec // a listener port fits in int32
		if service != "" {
			action.Service = &service
		}
		return probe.Target{PodIP: "127.0.0.1", Container: &corev1.Container{
			Name: "c", ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{GRPC: action}},
		}}
	}
	prober := probe.AllProber{}
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	if err := prober.Probe(t.Context(), target("")); err != nil {
		t.Errorf("serving: %v", err)
	}
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	if err := prober.Probe(t.Context(), target("")); err == nil {
		t.Error("not serving must fail")
	}
	hs.SetServingStatus("engine", healthpb.HealthCheckResponse_SERVING)
	if err := prober.Probe(t.Context(), target("engine")); err != nil {
		t.Errorf("named service serving: %v", err)
	}
	if err := prober.Probe(t.Context(), target("unknown")); err == nil {
		t.Error("an unknown service must fail")
	}
}
