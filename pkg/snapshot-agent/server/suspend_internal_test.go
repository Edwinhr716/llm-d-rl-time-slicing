package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

const (
	gib       = int64(1) << 30
	guestJob  = "guest-uid-1-0"
	guestPID  = 100 // CUDA process
	helperPID = 101 // no CUDA context
)

type fakeBackend struct {
	mu        sync.Mutex
	states    map[int]string // a PID not listed has no CUDA context
	avail     error
	ckptErr   error
	calls     []string
	afterCkpt func()
	afterRest func()
}

func (f *fakeBackend) Available() error { return f.avail }

func (f *fakeBackend) GetState(_ context.Context, pid int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.states[pid]; ok {
		return s, nil
	}
	return "", fmt.Errorf("pid %d: %w", pid, backends.ErrNotCudaProcess)
}

func (f *fakeBackend) GuestCheckpoint(_ context.Context, pids []int, locked map[int]bool, beforeRun func() error) error {
	if beforeRun != nil {
		if err := beforeRun(); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("checkpoint %v locked=%d", pids, len(locked)))
	if f.ckptErr != nil {
		f.mu.Unlock()
		return f.ckptErr
	}
	for _, p := range pids {
		f.states[p] = backends.CudaStateCheckpointed
	}
	f.mu.Unlock()
	if f.afterCkpt != nil {
		f.afterCkpt()
	}
	return nil
}

func (f *fakeBackend) GuestRestore(_ context.Context, pids []int, states map[int]string) error {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("restore %v %v", pids, states[pids[0]]))
	for _, p := range pids {
		f.states[p] = backends.CudaStateRunning
	}
	f.mu.Unlock()
	if f.afterRest != nil {
		f.afterRest()
	}
	return nil
}

func (f *fakeBackend) getCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fakeGPU struct {
	mu      sync.Mutex
	devices []gpuDevice
	procs   []gpuProcess
}

func (f *fakeGPU) Devices() ([]gpuDevice, error) { return f.devices, nil }

// Processes lists the fake processes; one without a GPU UUID is on GPU-0.
func (f *fakeGPU) Processes() ([]gpuProcess, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]gpuProcess(nil), f.procs...)
	for i := range out {
		if out[i].GPUUUID == "" {
			out[i].GPUUUID = "GPU-0"
		}
	}
	return out, nil
}

func (f *fakeGPU) set(procs ...gpuProcess) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.procs = procs
}

type guestFixture struct {
	t       *testing.T
	g       *guestPipeline
	backend *fakeBackend
	gpu     *fakeGPU
	mirror  *corev1.Pod
	guest   *corev1.Pod
	podDir  string
	root    string

	mu sync.Mutex
	// podDirs are the pod cgroups the fake kernel applies cgroup.freeze to.
	podDirs []string
	// extraMirrors and extraGuests hold the guests added by addGuest,
	// by job ID and by guest pod name.
	extraMirrors map[string]*corev1.Pod
	extraGuests  map[string]*corev1.Pod
	// scrubs records the GPUs scrubbed, in order; scrubErr and
	// scrubNonzero make the fake scrub fail.
	scrubs       []string
	scrubErr     error
	scrubNonzero int64
	// onScrub, if set, runs inside each scrub.
	onScrub func()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// newGuestFixture builds a fake cgroup hierarchy with one guest pod whose
// container runs a CUDA process holding 2 GiB and a helper process, and a
// fake kernel that applies cgroup.freeze to cgroup.events.
func newGuestFixture(t *testing.T) *guestFixture {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "cgroup.controllers"), "cpu memory\n")
	podDir := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", "kubepods-burstable-poduid_1.slice")
	writeFile(t, filepath.Join(podDir, "cgroup.events"), "populated 1\nfrozen 0\n")
	writeFile(t, filepath.Join(podDir, "cgroup.freeze"), "0")
	writeFile(t, filepath.Join(podDir, "memory.max"), fmt.Sprint(30*gib))
	writeFile(t, filepath.Join(podDir, "memory.current"), fmt.Sprint(1*gib))
	writeFile(t, filepath.Join(podDir, "memory.stat"), "anon 1000\nfile 5000\nshmem 20\nkernel 300\n")
	writeFile(t, filepath.Join(podDir, "cri-containerd-sandbox.scope", "cgroup.procs"), "7\n")
	writeFile(t, filepath.Join(podDir, "cri-containerd-c1.scope", "cgroup.procs"), fmt.Sprintf("%d\n%d\n", guestPID, helperPID))

	fx := &guestFixture{
		t: t, podDir: podDir, root: root, podDirs: []string{podDir},
		extraMirrors: map[string]*corev1.Pod{}, extraGuests: map[string]*corev1.Pod{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			fx.mu.Lock()
			dirs := append([]string(nil), fx.podDirs...)
			fx.mu.Unlock()
			for _, dir := range dirs {
				if v, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze")); err == nil {
					ev := "populated 1\nfrozen " + strings.TrimSpace(string(v)) + "\n"
					events := filepath.Join(dir, "cgroup.events")
					if cur, err := os.ReadFile(events); err != nil || string(cur) != ev {
						if err := os.WriteFile(events, []byte(ev), 0o600); err != nil {
							t.Error(err)
						}
					}
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	isController := true
	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror", Namespace: "ns", UID: types.UID("uid-1"),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "guest", Controller: &isController}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm"}}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "vllm", ContainerID: "containerd://c1"}},
		},
	}
	guest := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "guest", Namespace: "ns"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}},
	}
	gpu := &fakeGPU{
		devices: []gpuDevice{{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-0"}},
		procs:   []gpuProcess{{PID: guestPID, UsedBytes: uint64(2 * gib)}, {PID: 999, UsedBytes: uint64(5 * gib)}},
	}
	backend := &fakeBackend{states: map[int]string{guestPID: backends.CudaStateRunning}}
	backend.afterCkpt = func() { gpu.set(gpuProcess{PID: 999, UsedBytes: uint64(5 * gib)}) }
	backend.afterRest = func() {
		gpu.set(gpuProcess{PID: guestPID, UsedBytes: uint64(2 * gib)}, gpuProcess{PID: 999, UsedBytes: uint64(5 * gib)})
	}
	fx.backend, fx.gpu, fx.mirror, fx.guest = backend, gpu, mirror, guest
	allowlist, err := scrub.ParseAllowlist(scrub.DefaultQualified)
	if err != nil {
		t.Fatal(err)
	}
	fx.g = &guestPipeline{
		mirror: func(jobID string) (*corev1.Pod, bool) {
			fx.mu.Lock()
			defer fx.mu.Unlock()
			if p, ok := fx.extraMirrors[jobID]; ok {
				return p, true
			}
			if fx.mirror == nil || jobID != guestJob {
				return nil, false
			}
			return fx.mirror, true
		},
		getPod: func(_ context.Context, ns, name string) (*corev1.Pod, error) {
			fx.mu.Lock()
			defer fx.mu.Unlock()
			if p, ok := fx.extraGuests[name]; ok && p.Namespace == ns {
				return p, nil
			}
			if fx.guest == nil || ns != fx.guest.Namespace || name != fx.guest.Name {
				return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
			}
			return fx.guest, nil
		},
		cgroups: cgroup.New(root),
		backend: backend,
		gpu:     gpu,
		scrubCfg: ScrubConfig{
			Policy: scrub.DefaultPolicy, Mode: scrub.DefaultMode, Allowlist: allowlist, MarginMiB: scrub.DefaultMarginMiB,
		},
		scrub:     fx.fakeScrub,
		scrubGate: newScrubGate(),
		now:       time.Now,
		records:   map[string]*guestRecord{},
	}
	return fx
}

// fakeScrub records the GPU and fails as configured.
func (f *guestFixture) fakeScrub(_ context.Context, gpuUUID string) (scrub.Result, error) {
	if f.onScrub != nil {
		f.onScrub()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scrubs = append(f.scrubs, gpuUUID)
	if f.scrubErr != nil {
		return scrub.Result{}, f.scrubErr
	}
	return scrub.Result{
		Decision: scrub.ActionScrub, GPUUUID: gpuUUID, BytesScrubbed: 20 * uint64(gib),
		ReadbackNonzeroWords: f.scrubNonzero,
	}, nil
}

func (f *guestFixture) scrubbed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.scrubs...)
}

// addGuest adds a second guest (mirror, guest pod, pod cgroup appended to
// f.podDirs) whose one CUDA process pid holds 2 GiB on gpuUUID.
func (f *guestFixture) addGuest(t *testing.T, jobID, uid string, pid int, gpuUUID string) {
	t.Helper()
	podDir := filepath.Join(f.root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+strings.ReplaceAll(uid, "-", "_")+".slice")
	writeFile(t, filepath.Join(podDir, "cgroup.events"), "populated 1\nfrozen 0\n")
	writeFile(t, filepath.Join(podDir, "cgroup.freeze"), "0")
	writeFile(t, filepath.Join(podDir, "memory.max"), fmt.Sprint(30*gib))
	writeFile(t, filepath.Join(podDir, "memory.current"), fmt.Sprint(1*gib))
	writeFile(t, filepath.Join(podDir, "memory.stat"), "anon 1000\nshmem 20\nkernel 300\n")
	container := "c-" + uid
	writeFile(t, filepath.Join(podDir, "cri-containerd-"+container+".scope", "cgroup.procs"), fmt.Sprintf("%d\n", pid))

	isController := true
	guestName := "guest-" + uid
	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror-" + uid, Namespace: "ns", UID: types.UID(uid),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: guestName, Controller: &isController}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm"}}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "vllm", ContainerID: "containerd://" + container}},
		},
	}
	guest := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: guestName, Namespace: "ns"},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}},
	}
	f.mu.Lock()
	f.podDirs = append(f.podDirs, podDir)
	f.extraMirrors[jobID] = mirror
	f.extraGuests[guestName] = guest
	f.mu.Unlock()

	f.backend.mu.Lock()
	f.backend.states[pid] = backends.CudaStateRunning
	f.backend.mu.Unlock()
	f.gpu.mu.Lock()
	f.gpu.procs = append(f.gpu.procs, gpuProcess{PID: pid, UsedBytes: uint64(2 * gib), GPUUUID: gpuUUID})
	f.gpu.mu.Unlock()
}

func (f *guestFixture) frozenDir(t *testing.T, dir string) bool {
	t.Helper()
	frozen, err := f.g.cgroups.Frozen(dir)
	if err != nil {
		t.Fatal(err)
	}
	return frozen
}

func (f *guestFixture) frozen(t *testing.T) bool {
	t.Helper()
	frozen, err := f.g.cgroups.Frozen(f.podDir)
	if err != nil {
		t.Fatal(err)
	}
	return frozen
}

func deadlineIn(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(time.Now().Add(d)) }

func newGuestServer(t *testing.T, f *guestFixture) *Server {
	t.Helper()
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil)
	srv.guest = f.g
	srv.state.RegisterJob(guestJob, "group-1")
	if err := srv.state.TransitionToRunning(guestJob, []int{guestPID}); err != nil {
		t.Fatal(err)
	}
	return srv
}

func jobStatus(t *testing.T, srv *Server) *pb.JobStatus {
	t.Helper()
	st, err := srv.Status(context.Background(), &pb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range st.GetJobStatuses() {
		if j.GetJobId() == guestJob {
			return j
		}
	}
	t.Fatalf("job %s not in Status", guestJob)
	return nil
}

func TestGuest_SuspendResumeCycles(t *testing.T) {
	fx := newGuestFixture(t)
	srv := newGuestServer(t, fx)
	ctx := context.Background()

	for cycle := int64(0); cycle < 3; cycle++ {
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
		if !fx.frozen(t) {
			t.Fatalf("cycle %d: cgroup not frozen after Suspend", cycle)
		}
		if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_SUSPENDED || js.GetDeviceBytes() != 2*gib {
			t.Errorf("cycle %d: job %v", cycle, js)
		}

		rresp, err := srv.Resume(ctx, &pb.ResumeRequest{JobId: guestJob, Epoch: 2*cycle + 2, Deadline: deadlineIn(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		op = waitGuestOp(t, srv, rresp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_RESUMED {
			t.Fatalf("cycle %d resume: %v", cycle, op)
		}
		if fx.frozen(t) {
			t.Fatalf("cycle %d: cgroup frozen after Resume", cycle)
		}
		if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_RUNNING {
			t.Errorf("cycle %d: job %v", cycle, js)
		}
	}
	want := []string{
		"checkpoint [100] locked=0", "restore [100] checkpointed",
		"checkpoint [100] locked=0", "restore [100] checkpointed",
		"checkpoint [100] locked=0", "restore [100] checkpointed",
	}
	if got := fx.backend.getCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("backend calls %v, want %v", got, want)
	}
}

func TestGuest_SuspendRequestValidation(t *testing.T) {
	f := newGuestFixture(t)
	srv := newGuestServer(t, f)
	ctx := context.Background()
	if _, err := srv.Suspend(ctx, &pb.SuspendRequest{Epoch: 1, Deadline: deadlineIn(time.Minute)}); err == nil {
		t.Error("missing job_id must be refused")
	}
	if _, err := srv.Suspend(ctx, &pb.SuspendRequest{JobId: guestJob, Epoch: 1}); err == nil {
		t.Error("missing deadline must be refused")
	}
	if _, err := srv.Resume(ctx, &pb.ResumeRequest{Epoch: 1, Deadline: deadlineIn(time.Minute)}); err == nil {
		t.Error("missing job_id must be refused")
	}
	srv.guest = nil
	if _, err := srv.Suspend(ctx, &pb.SuspendRequest{JobId: guestJob, Epoch: 1, Deadline: deadlineIn(time.Minute)}); err == nil {
		t.Error("unconfigured pipelines must be refused")
	}
}

// TestGuest_PreconditionFailureFaults checks that a failed precondition
// fails the operation with its reason, leaves the job FAULTED (D-AGENT-9
// default) and does not touch the guest.
func TestGuest_PreconditionFailureFaults(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(f *guestFixture)
		reason pb.ErrorReason
	}{
		{"guest Ready", func(f *guestFixture) {
			f.guest.Status.Conditions[0].Status = corev1.ConditionTrue
		}, pb.ErrorReason_PRECONDITION_READINESS},
		{"no owner", func(f *guestFixture) { f.mirror.OwnerReferences = nil }, pb.ErrorReason_PRECONDITION_READINESS},
		{"readiness probe", func(f *guestFixture) {
			f.mirror.Spec.Containers[0].ReadinessProbe = &corev1.Probe{}
		}, pb.ErrorReason_PRECONDITION_PROBES},
		{"readiness gate", func(f *guestFixture) {
			f.mirror.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "x"}}
		}, pb.ErrorReason_PRECONDITION_PROBES},
		{"no memory limit", func(f *guestFixture) {
			writeFile(f.t, filepath.Join(f.podDir, "memory.max"), "max\n")
		}, pb.ErrorReason_PRECONDITION_MEMORY},
		{"not enough memory", func(f *guestFixture) {
			// 2 GiB x 1.1 does not fit in 3 GiB - 1 GiB.
			writeFile(f.t, filepath.Join(f.podDir, "memory.max"), fmt.Sprint(3*gib))
		}, pb.ErrorReason_PRECONDITION_MEMORY},
		{"unqualified GPU", func(f *guestFixture) {
			f.gpu.devices = []gpuDevice{{Name: "Tesla T4", DriverVersion: "580.1"}}
		}, pb.ErrorReason_PRECONDITION_NODE},
		{"unqualified driver", func(f *guestFixture) {
			f.gpu.devices = []gpuDevice{{Name: "NVIDIA L4", DriverVersion: "570.86.15"}}
		}, pb.ErrorReason_PRECONDITION_NODE},
		{"no cuda-checkpoint", func(f *guestFixture) { f.backend.avail = errors.New("not found") }, pb.ErrorReason_PRECONDITION_NODE},
		{"infeasible deadline", func(f *guestFixture) {
			f.gpu.set(gpuProcess{PID: guestPID, UsedBytes: uint64(20 * gib)})
			writeFile(f.t, filepath.Join(f.podDir, "memory.max"), fmt.Sprint(60*gib))
		}, pb.ErrorReason_DEADLINE_INFEASIBLE},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newGuestFixture(t)
			tc.setup(f)
			srv := newGuestServer(t, f)
			// 5 s: enough for 2 GiB (1.2 s + 2 s), not for 20 GiB (12 s + 2 s).
			req := &pb.SuspendRequest{JobId: guestJob, Epoch: 1, Deadline: deadlineIn(5 * time.Second)}
			resp, err := srv.Suspend(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			op := waitGuestOp(t, srv, resp.GetOperationId())
			if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED || op.GetErrorReason() != tc.reason {
				t.Fatalf("got %v, want FAILED %s", op, tc.reason)
			}
			if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_FAULTED {
				t.Errorf("job %v, want FAULTED", js)
			}
			if calls := f.backend.getCalls(); len(calls) != 0 {
				t.Errorf("backend touched: %v", calls)
			}
			if f.frozen(t) {
				t.Error("guest frozen")
			}
		})
	}
}

func TestGuest_GuestPodGonePassesReadiness(t *testing.T) {
	f := newGuestFixture(t)
	f.guest = nil
	res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
	if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("got %v, %v", res, err)
	}
}

func TestGuest_SuspendReleased(t *testing.T) {
	t.Run("no mirror", func(t *testing.T) {
		f := newGuestFixture(t)
		f.mirror = nil
		res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if err != nil || res.Outcome != pb.Outcome_OUTCOME_RELEASED {
			t.Fatalf("got %v, %v", res, err)
		}
	})
	t.Run("no pod cgroup", func(t *testing.T) {
		f := newGuestFixture(t)
		f.mirror.UID = "uid-other"
		res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if err != nil || res.Outcome != pb.Outcome_OUTCOME_RELEASED {
			t.Fatalf("got %v, %v", res, err)
		}
	})
	t.Run("no process", func(t *testing.T) {
		f := newGuestFixture(t)
		writeFile(t, filepath.Join(f.podDir, "cri-containerd-c1.scope", "cgroup.procs"), "")
		res, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if err != nil || res.Outcome != pb.Outcome_OUTCOME_RELEASED {
			t.Fatalf("got %v, %v", res, err)
		}
		if calls := f.backend.getCalls(); len(calls) != 0 {
			t.Errorf("backend touched: %v", calls)
		}
	})
}

func TestGuest_SuspendFailures(t *testing.T) {
	t.Run("verify: VRAM still held", func(t *testing.T) {
		f := newGuestFixture(t)
		f.backend.afterCkpt = nil // NVML still lists the guest with 2 GiB
		_, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_VERIFY_FAILED {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("verify: not available counts as held", func(t *testing.T) {
		f := newGuestFixture(t)
		f.backend.afterCkpt = func() { f.gpu.set(gpuProcess{PID: guestPID, UsedBytes: nvmlNotAvailable}) }
		_, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_VERIFY_FAILED || !strings.Contains(err.Error(), "not available") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("backend error", func(t *testing.T) {
		f := newGuestFixture(t)
		f.backend.ckptErr = errors.New("cuda-checkpoint failed")
		_, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_BACKEND_ERROR {
			t.Fatalf("got %v", err)
		}
		if f.frozen(t) {
			t.Error("frozen after a failed checkpoint")
		}
	})
	t.Run("deadline exceeded in backend", func(t *testing.T) {
		f := newGuestFixture(t)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		defer cancel()
		f.backend.ckptErr = context.DeadlineExceeded
		err := f.g.ensureCheckpointed(ctx, guestJob, []int{guestPID}, 0, 0, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_DEADLINE_EXCEEDED {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("budget re-checked under the lock", func(t *testing.T) {
		f := newGuestFixture(t)
		// The measured checkpoint takes 10 s; only 5 s are left.
		f.g.update(guestJob, func(r *guestRecord) { r.lastCheckpoint = 10 * time.Second })
		err := f.g.ensureCheckpointed(context.Background(), guestJob, []int{guestPID}, 2*gib, 0, time.Now().Add(5*time.Second))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_DEADLINE_INFEASIBLE {
			t.Fatalf("got %v", err)
		}
		if calls := f.backend.getCalls(); len(calls) != 0 {
			t.Errorf("backend touched: %v", calls)
		}
	})
	t.Run("failed cuda state", func(t *testing.T) {
		f := newGuestFixture(t)
		f.backend.states[guestPID] = backends.CudaStateFailed
		_, err := f.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_BACKEND_ERROR {
			t.Fatalf("got %v", err)
		}
	})
}

// TestGuest_SuspendIdempotent checks that a Suspend of a guest that is
// already checkpointed and frozen (a re-issue after an aborted operation)
// only verifies.
func TestGuest_SuspendIdempotent(t *testing.T) {
	f := newGuestFixture(t)
	ctx := context.Background()
	if _, err := f.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	res, err := f.g.suspend(ctx, guestJob, time.Now().Add(time.Minute))
	if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED || res.DeviceBytes != 2*gib {
		t.Fatalf("got %v, %v", res, err)
	}
	if calls := f.backend.getCalls(); len(calls) != 1 {
		t.Errorf("backend calls %v, want one checkpoint", calls)
	}
}

func TestGuest_Resume(t *testing.T) {
	t.Run("thaws before restore", func(t *testing.T) {
		f := newGuestFixture(t)
		ctx := context.Background()
		if _, err := f.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		f.backend.afterRest = func() {
			if f.frozen(t) {
				t.Error("restore ran on a frozen guest")
			}
			f.gpu.set(gpuProcess{PID: guestPID, UsedBytes: uint64(2 * gib)})
		}
		res, err := f.g.resume(ctx, guestJob)
		if err != nil || res.DeviceBytes != 2*gib {
			t.Fatalf("got %v, %v", res, err)
		}
	})
	t.Run("verify: no context", func(t *testing.T) {
		f := newGuestFixture(t)
		ctx := context.Background()
		if _, err := f.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		f.backend.afterRest = nil
		if _, err := f.g.resume(ctx, guestJob); sm.ErrorReasonOf(err) != pb.ErrorReason_VERIFY_FAILED {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("guest gone", func(t *testing.T) {
		f := newGuestFixture(t)
		f.mirror = nil
		if _, err := f.g.resume(context.Background(), guestJob); sm.ErrorReasonOf(err) != pb.ErrorReason_BACKEND_ERROR {
			t.Fatalf("got %v", err)
		}
	})
}

func TestMirrorPod(t *testing.T) {
	done := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	live := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if p, ok := mirrorPod([]*corev1.Pod{done, live}); !ok || p != live {
		t.Error("want the live pod")
	}
	if p, ok := mirrorPod([]*corev1.Pod{done}); !ok || p != done {
		t.Error("want the only pod")
	}
	if _, ok := mirrorPod(nil); ok {
		t.Error("want none")
	}
}
