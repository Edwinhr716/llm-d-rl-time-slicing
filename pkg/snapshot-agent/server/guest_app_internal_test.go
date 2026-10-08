// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// residual is the VRAM the fake vLLM keeps while it sleeps.
const residual = int64(512) << 20

// fakeVLLM serves vLLM's dev-mode sleep API for one guest process: a sleep
// leaves that process with residual bytes of VRAM, a wake up with 2 GiB.
type fakeVLLM struct {
	fx  *guestFixture
	pid int
	// sleepResidual is the VRAM left by a sleep (residual by default).
	sleepResidual int64

	mu       sync.Mutex
	sleeping bool
	calls    []string
	// onCall, if set, runs at each call, before it is answered.
	onCall func(call string)
}

func (v *fakeVLLM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	call := r.Method + " " + r.URL.Path
	if r.URL.RawQuery != "" {
		call += "?" + r.URL.RawQuery
	}
	if v.onCall != nil {
		v.onCall(call)
	}
	v.mu.Lock()
	v.calls = append(v.calls, call)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/is_sleeping":
		fmt.Fprintf(w, `{"is_sleeping": %v}`, v.sleeping)
	case r.Method == http.MethodPost && r.URL.Path == "/sleep":
		v.sleeping = true
		v.fx.setVRAM(v.pid, v.sleepResidual)
	case r.Method == http.MethodPost && r.URL.Path == "/wake_up":
		v.sleeping = false
		v.fx.setVRAM(v.pid, 2*gib)
	case r.Method == http.MethodPost && r.URL.Path == "/reset_prefix_cache":
	default:
		w.WriteHeader(http.StatusNotFound)
	}
	v.mu.Unlock()
}

func (v *fakeVLLM) getCalls() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.calls...)
}

func (v *fakeVLLM) asleep() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sleeping
}

// sleepNow puts the fake to sleep as a past Suspend would have.
func (v *fakeVLLM) sleepNow() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sleeping = true
	v.fx.setVRAM(v.pid, v.sleepResidual)
}

// setVRAM sets the VRAM NVML lists for pid on GPU-0; 0 removes the process.
func (f *guestFixture) setVRAM(pid int, bytes int64) {
	f.gpu.mu.Lock()
	defer f.gpu.mu.Unlock()
	out := f.gpu.procs[:0:0]
	for _, p := range f.gpu.procs {
		if p.PID != pid {
			out = append(out, p)
		}
	}
	if bytes > 0 {
		out = append(out, gpuProcess{PID: pid, UsedBytes: uint64(bytes)})
	}
	f.gpu.procs = out
}

// declareApp makes mirror an application-aware guest served by a fake vLLM
// for pid: the annotation the guest kubelet copies from the guest pod, the
// serving port and the pod IP.
func declareApp(t *testing.T, fx *guestFixture, mirror *corev1.Pod, pid int) *fakeVLLM {
	t.Helper()
	v := &fakeVLLM{fx: fx, pid: pid, sleepResidual: residual}
	srv := httptest.NewServer(v)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	fx.mu.Lock()
	defer fx.mu.Unlock()
	mirror.Annotations = map[string]string{"timeslice.io/backend": "app_endpoint"}
	mirror.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: int32(port)}} //nolint:gosec // a test port
	mirror.Status.PodIP = "127.0.0.1"
	return v
}

// newAppGuest is the guest fixture with its guest declared application-aware
// on a node without cuda-checkpoint.
func newAppGuest(t *testing.T) (*guestFixture, *fakeVLLM) {
	t.Helper()
	fx := newGuestFixture(t)
	v := declareApp(t, fx, fx.mirror, guestPID)
	fx.backend.avail = errors.New("cuda-checkpoint: executable file not found")
	fx.g.app = newAppGuestBackend()
	return fx, v
}

var (
	appSuspendCalls = []string{"GET /is_sleeping", "POST /reset_prefix_cache", "POST /sleep?level=1", "GET /is_sleeping"}
	appResumeCalls  = []string{"GET /is_sleeping", "POST /wake_up"}
)

// Bug: the guest pipelines were wired to cuda-checkpoint only, so a guest
// that declares the app_endpoint backend was cuda-checkpointed (and refused
// on a node without cuda-checkpoint). It must be put to sleep and woken
// through its API, with no cuda-checkpoint call.
func TestGuestApp_SuspendResumeCycles(t *testing.T) {
	fx, vllm := newAppGuest(t)
	srv := newGuestServer(t, fx)
	ctx := context.Background()
	var want []string
	for cycle := int64(0); cycle < 2; cycle++ {
		sresp, err := srv.Suspend(ctx, &pb.SuspendRequest{JobId: guestJob, Epoch: 2*cycle + 1, Deadline: deadlineIn(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		op := waitGuestOp(t, srv, sresp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED {
			t.Fatalf("cycle %d suspend: %v", cycle, op)
		}
		if op.GetHostBytesPinned() != 1320 || op.GetSnapshotDeviceBytes() != 2*gib {
			t.Errorf("cycle %d suspend bytes: host %d device %d", cycle, op.GetHostBytesPinned(), op.GetSnapshotDeviceBytes())
		}
		if !fx.frozen(t) || !vllm.asleep() {
			t.Fatalf("cycle %d: frozen %v asleep %v after Suspend", cycle, fx.frozen(t), vllm.asleep())
		}
		rresp, err := srv.Resume(ctx, &pb.ResumeRequest{JobId: guestJob, Epoch: 2*cycle + 2, Deadline: deadlineIn(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		op = waitGuestOp(t, srv, rresp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_RESUMED {
			t.Fatalf("cycle %d resume: %v", cycle, op)
		}
		if fx.frozen(t) || vllm.asleep() {
			t.Fatalf("cycle %d: frozen %v asleep %v after Resume", cycle, fx.frozen(t), vllm.asleep())
		}
		if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_RUNNING {
			t.Errorf("cycle %d: job %v", cycle, js)
		}
		want = append(append(want, appSuspendCalls...), appResumeCalls...)
	}
	if got := vllm.getCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("vLLM calls %v, want %v", got, want)
	}
	if got := fx.backend.getCalls(); len(got) != 0 {
		t.Errorf("cuda-checkpoint ran on an app_endpoint guest: %v", got)
	}
}

// Bug: verify required a suspended guest to hold no VRAM, so a sleeping
// vLLM, which keeps its CUDA context, failed verify (and was killed).
func TestGuestApp_FrozenSleepingGuestVerifies(t *testing.T) {
	fx, vllm := newAppGuest(t)
	vllm.sleepNow()
	if err := fx.g.cgroups.Freeze(context.Background(), fx.podDir); err != nil {
		t.Fatal(err)
	}
	srv := newGuestServer(t, fx)
	sresp, err := srv.Suspend(context.Background(), &pb.SuspendRequest{JobId: guestJob, Epoch: 1, Deadline: deadlineIn(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	op := waitGuestOp(t, srv, sresp.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("suspend of a frozen, sleeping guest: %v", op)
	}
	if got := fx.backend.getCalls(); len(got) != 0 {
		t.Errorf("cuda-checkpoint ran: %v", got)
	}
}

// Bug: restart recovery classified a frozen guest that holds any VRAM as
// FAULTED, so an agent restart while an application guest slept faulted it.
func TestGuestApp_RecoverFrozenSleepingGuest(t *testing.T) {
	fx, vllm := newAppGuest(t)
	vllm.sleepNow()
	if err := fx.g.cgroups.Freeze(context.Background(), fx.podDir); err != nil {
		t.Fatal(err)
	}
	srv, cb := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
	if n := cb.getState.Load(); n != 0 {
		t.Errorf("cuda-checkpoint --get-state ran %d times on a frozen guest", n)
	}
	checkOutcome(t, resumeCall(t, srv, 5), pb.Outcome_OUTCOME_RESUMED)
	if fx.frozen(t) || vllm.asleep() {
		t.Errorf("frozen %v asleep %v after Resume", fx.frozen(t), vllm.asleep())
	}
}

// A frozen application guest over the residual cap is still FAULTED.
func TestGuestApp_RecoverFrozenOverResidualFaults(t *testing.T) {
	fx, vllm := newAppGuest(t)
	vllm.sleepResidual = appResidualMaxBytes + 1
	vllm.sleepNow()
	if err := fx.g.cgroups.Freeze(context.Background(), fx.podDir); err != nil {
		t.Fatal(err)
	}
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID, helperPID})
}

// A restart between the sleep and the freeze leaves a thawed guest that
// sleeps: SAVED, so that a re-issued Suspend freezes it without a second
// sleep.
func TestGuestApp_RecoverThawedSleepingIsSaved(t *testing.T) {
	fx, vllm := newAppGuest(t)
	vllm.sleepNow()
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SAVED, []int{guestPID})
	checkOutcome(t, suspendCall(t, srv, 5), pb.Outcome_OUTCOME_SUSPENDED)
	if !fx.frozen(t) {
		t.Error("not frozen after the re-issued Suspend")
	}
	for _, c := range vllm.getCalls() {
		if strings.HasPrefix(c, "POST /sleep") {
			t.Errorf("a sleeping guest was put to sleep again: %v", vllm.getCalls())
		}
	}
}

// A sleeping guest that still holds more than the residual cap fails
// verify.
func TestGuestApp_ResidualOverCapFailsVerify(t *testing.T) {
	fx, vllm := newAppGuest(t)
	vllm.sleepResidual = appResidualMaxBytes + 1
	_, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
	if sm.ErrorReasonOf(err) != pb.ErrorReason_VERIFY_FAILED {
		t.Fatalf("suspend: %v, want VERIFY_FAILED", err)
	}
}

// The in-flight connections are drained before the sleep, until an abort
// resets none, and aborted once more after the freeze.
func TestGuestApp_DrainsBeforeSleep(t *testing.T) {
	fx, vllm := newAppGuest(t)
	var mu sync.Mutex
	var events []string
	resets := []int{3, 1, 0, 0}
	fx.g.abortConns = func(_ string, _ []int, _ time.Time) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		if len(resets) > 0 {
			n, resets = resets[0], resets[1:]
		}
		events = append(events, fmt.Sprintf("abort=%d", n))
		return n, nil
	}
	vllm.onCall = func(call string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, call)
	}
	if _, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"GET /is_sleeping", "abort=3", "abort=1", "abort=0", "POST /reset_prefix_cache",
		"POST /sleep?level=1", "GET /is_sleeping", "abort=0",
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(events, want) {
		t.Errorf("events %v, want %v", events, want)
	}
}

// Resume of an awake guest does not wake it again (a re-issued Resume).
func TestGuestApp_ResumeAwakeIsIdempotent(t *testing.T) {
	fx, vllm := newAppGuest(t)
	if _, err := fx.g.resume(context.Background(), guestJob); err != nil {
		t.Fatal(err)
	}
	if got, want := vllm.getCalls(), []string{"GET /is_sleeping"}; !reflect.DeepEqual(got, want) {
		t.Errorf("vLLM calls %v, want %v", got, want)
	}
}

// One agent suspends a cuda-checkpointed guest and an application-aware
// guest on the same node, each with its own backend.
func TestGuestApp_MixedBackendsOnOneNode(t *testing.T) {
	fx := newGuestFixture(t)
	fx.g.app = newAppGuestBackend()
	const appJob, appPID = "guest-uid-2-0", 200
	fx.addGuest(t, appJob, "uid-2", appPID, "GPU-0")
	fx.mu.Lock()
	appMirror := fx.extraMirrors[appJob]
	fx.mu.Unlock()
	vllm := declareApp(t, fx, appMirror, appPID)
	// The cuda guest's checkpoint frees only its own VRAM.
	fx.backend.afterCkpt = func() { fx.setVRAM(guestPID, 0) }
	fx.backend.afterRest = func() { fx.setVRAM(guestPID, 2*gib) }

	ctx := context.Background()
	for _, job := range []string{guestJob, appJob} {
		res, err := fx.g.suspend(ctx, job, time.Now().Add(time.Minute))
		if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED {
			t.Fatalf("suspend %s: %v %v", job, res, err)
		}
	}
	for _, job := range []string{guestJob, appJob} {
		if _, err := fx.g.resume(ctx, job); err != nil {
			t.Fatalf("resume %s: %v", job, err)
		}
	}
	wantCuda := []string{"checkpoint [100] locked=0", "restore [100] checkpointed"}
	if got := fx.backend.getCalls(); !reflect.DeepEqual(got, wantCuda) {
		t.Errorf("cuda-checkpoint calls %v, want %v", got, wantCuda)
	}
	wantApp := append(append([]string(nil), appSuspendCalls...), appResumeCalls...)
	if got, want := vllm.getCalls(), wantApp; !reflect.DeepEqual(got, want) {
		t.Errorf("vLLM calls %v, want %v", got, want)
	}
}

func TestGuestAppConfig(t *testing.T) {
	fx := newGuestFixture(t)
	base := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: fx.mirror.ObjectMeta,
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "vllm", Ports: []corev1.ContainerPort{{ContainerPort: 8000}},
			}}},
			Status: corev1.PodStatus{PodIP: "10.1.2.3"},
		}
	}
	appAnn := func(config string) map[string]string {
		return map[string]string{"timeslice.io/backend": "app_endpoint", "timeslice.io/backend-config": config}
	}
	const localEndpoints = `{"app": "APP_VLLM", "endpoints": ` +
		`["http://localhost:9000/v", "http://0.0.0.0:9001", "http://guest.ns.svc:8000"]}`
	cases := []struct {
		name    string
		ann     map[string]string
		mutate  func(p *corev1.Pod)
		want    []string // endpoints; nil with no error means cuda
		mode    pb.SuspendMode
		wantErr string
	}{
		{name: "no annotation is cuda"},
		{name: "cuda", ann: map[string]string{"timeslice.io/backend": "cuda"}},
		{
			name:    "unknown backend",
			ann:     map[string]string{"timeslice.io/backend": "direct_memory"},
			wantErr: "cuda or app_endpoint",
		},
		{
			name: "defaults",
			ann:  map[string]string{"timeslice.io/backend": "app_endpoint"},
			want: []string{"http://10.1.2.3:8000"},
			mode: pb.SuspendMode_SUSPEND_MODE_OFFLOAD,
		},
		{
			name: "localhost endpoint points at the pod",
			ann: map[string]string{
				"timeslice.io/backend":        "app_endpoint",
				"timeslice.io/backend-config": localEndpoints,
			},
			want: []string{"http://10.1.2.3:9000/v", "http://10.1.2.3:9001", "http://guest.ns.svc:8000"},
			mode: pb.SuspendMode_SUSPEND_MODE_OFFLOAD,
		},
		{
			name:    "discard refused",
			ann:     appAnn(`{"mode": "SUSPEND_MODE_DISCARD"}`),
			wantErr: "DISCARD",
		},
		{
			name:    "sglang refused",
			ann:     appAnn(`{"app": "APP_SGLANG"}`),
			wantErr: "only APP_VLLM",
		},
		{
			name:    "malformed config",
			ann:     appAnn(`{"endpoints": "x"`),
			wantErr: "parse",
		},
		{
			name:    "no pod IP",
			ann:     map[string]string{"timeslice.io/backend": "app_endpoint"},
			mutate:  func(p *corev1.Pod) { p.Status.PodIP = "" },
			wantErr: "no pod IP",
		},
		{
			name:   "IPv6 pod IP",
			ann:    map[string]string{"timeslice.io/backend": "app_endpoint"},
			mutate: func(p *corev1.Pod) { p.Status.PodIP = "fd00::5" },
			want:   []string{"http://[fd00::5]:8000"},
			mode:   pb.SuspendMode_SUSPEND_MODE_OFFLOAD,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			p.Annotations = tc.ann
			if tc.mutate != nil {
				tc.mutate(p)
			}
			cfg, err := fx.g.guestAppConfig(context.Background(), p)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == nil {
				if cfg != nil {
					t.Fatalf("config %v, want cuda (nil)", cfg)
				}
				return
			}
			app := cfg.GetAppEndpoint()
			if !reflect.DeepEqual(app.GetEndpoints(), tc.want) || app.GetMode() != tc.mode || app.GetApp() != pb.App_APP_VLLM {
				t.Errorf("config %v, want endpoints %v mode %s", app, tc.want, tc.mode)
			}
		})
	}
}

// The guest client keeps no idle connection: the after-freeze abort resets
// every connection into the guest's serving port.
func TestNewAppGuestBackend_NoKeepAlive(t *testing.T) {
	var mu sync.Mutex
	remotes := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		mu.Unlock()
		fmt.Fprint(w, `{"is_sleeping": false}`)
	}))
	defer srv.Close()
	b := newAppGuestBackend()
	cfg := &pb.BackendConfig{Backend: &pb.BackendConfig_AppEndpoint{AppEndpoint: &pb.AppEndpointConfig{
		App: pb.App_APP_VLLM, Endpoints: []string{srv.URL},
	}}}
	for i := 0; i < 3; i++ {
		if _, err := b.IsSuspended(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 3 {
		t.Errorf("3 calls used %d connections, want 3 (no keep-alive)", len(remotes))
	}
}
