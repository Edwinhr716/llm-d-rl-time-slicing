package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

const (
	guestJob2 = "guest-uid-2-0"
	guestPID2 = 200
)

// hostFixture is a guest fixture with a second guest (pod cgroup
// fx.podDirs[1]), and NVML listing each guest process only while
// cuda-checkpoint reports it running.
func hostFixture(t *testing.T, gpu2 string) *guestFixture {
	t.Helper()
	fx := newGuestFixture(t)
	fx.gpu.devices = []gpuDevice{
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-0"},
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-1"},
	}
	fx.addGuest(t, guestJob2, "uid-2", guestPID2, gpu2)
	gpuOf := map[int]string{guestPID: "GPU-0", guestPID2: gpu2}
	syncNVML := func() {
		fx.backend.mu.Lock()
		var procs []gpuProcess
		for _, pid := range []int{guestPID, guestPID2} {
			if fx.backend.states[pid] == backends.CudaStateRunning {
				procs = append(procs, gpuProcess{PID: pid, UsedBytes: uint64(2 * gib), GPUUUID: gpuOf[pid]})
			}
		}
		fx.backend.mu.Unlock()
		fx.gpu.set(procs...)
	}
	fx.backend.afterCkpt = syncNVML
	fx.backend.afterRest = syncNVML
	return fx
}

// suspendAllReq and resumeAllReq are host calls for the background role
// with a one-minute deadline.
func suspendAllReq(epoch int64) *pb.SuspendAllRequest {
	return &pb.SuspendAllRequest{Role: "background", Epoch: epoch, Deadline: deadlineIn(time.Minute)}
}

func resumeAllReq(epoch int64) *pb.ResumeAllRequest {
	return &pb.ResumeAllRequest{Role: "background", Epoch: epoch, Deadline: deadlineIn(time.Minute)}
}

func newHostServer(t *testing.T, fx *guestFixture) *Server {
	t.Helper()
	lister := func(role string) []string {
		if role == "background" {
			return []string{guestJob, guestJob2}
		}
		return nil
	}
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil, sm.WithTargetLister(lister))
	srv.guest = fx.g
	for jobID, pid := range map[string]int{guestJob: guestPID, guestJob2: guestPID2} {
		srv.state.RegisterJob(jobID, "group-1")
		if err := srv.state.TransitionToRunning(jobID, []int{pid}); err != nil {
			t.Fatal(err)
		}
	}
	return srv
}

func jobStates(t *testing.T, srv *Server) map[string]pb.JobState {
	t.Helper()
	st, err := srv.Status(context.Background(), &pb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]pb.JobState{}
	for _, j := range st.GetJobStatuses() {
		out[j.GetJobId()] = j.GetState()
	}
	return out
}

func checkTargets(t *testing.T, op *pb.GetOperationResponse, outcome pb.Outcome) {
	t.Helper()
	ids := make([]string, 0, len(op.GetTargets()))
	for _, tr := range op.GetTargets() {
		ids = append(ids, tr.GetJobId())
		if tr.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || tr.GetOutcome() != outcome {
			t.Errorf("target %v, want COMPLETE %s", tr, outcome)
		}
	}
	if want := []string{guestJob, guestJob2}; !reflect.DeepEqual(ids, want) {
		t.Errorf("targets %v, want %v", ids, want)
	}
}

// TestHostOp_SuspendAllResumeAllRunPipelines checks that the SuspendAll and
// ResumeAll RPCs run the real guest pipelines on every background guest on
// the node, and that per-guest Suspend and Resume still work afterwards.
func TestHostOp_SuspendAllResumeAllRunPipelines(t *testing.T) {
	fx := hostFixture(t, "GPU-0")
	podDir2 := fx.podDirs[1]
	srv := newHostServer(t, fx)
	ctx := context.Background()

	for cycle := int64(0); cycle < 2; cycle++ {
		sresp, err := srv.SuspendAll(ctx, suspendAllReq(2*cycle+1))
		if err != nil {
			t.Fatal(err)
		}
		op := waitGuestOp(t, srv, sresp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED {
			t.Fatalf("cycle %d SuspendAll: %v", cycle, op)
		}
		checkTargets(t, op, pb.Outcome_OUTCOME_SUSPENDED)
		if !fx.frozen(t) || !fx.frozenDir(t, podDir2) {
			t.Fatalf("cycle %d: not every guest is frozen after SuspendAll", cycle)
		}
		for id, st := range jobStates(t, srv) {
			if st != pb.JobState_JOB_STATE_SUSPENDED {
				t.Errorf("cycle %d: job %s is %s after SuspendAll", cycle, id, st)
			}
		}
		if procs, err := fx.gpu.Processes(); err != nil || len(procs) != 0 {
			t.Errorf("cycle %d: NVML lists %v while suspended", cycle, procs)
		}

		rresp, err := srv.ResumeAll(ctx, resumeAllReq(2*cycle+2))
		if err != nil {
			t.Fatal(err)
		}
		op = waitGuestOp(t, srv, rresp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != pb.Outcome_OUTCOME_RESUMED {
			t.Fatalf("cycle %d ResumeAll: %v", cycle, op)
		}
		checkTargets(t, op, pb.Outcome_OUTCOME_RESUMED)
		if fx.frozen(t) || fx.frozenDir(t, podDir2) {
			t.Fatalf("cycle %d: a guest is frozen after ResumeAll", cycle)
		}
		for id, st := range jobStates(t, srv) {
			if st != pb.JobState_JOB_STATE_RUNNING {
				t.Errorf("cycle %d: job %s is %s after ResumeAll", cycle, id, st)
			}
		}
	}
	if n := len(fx.backend.getCalls()); n != 8 {
		t.Errorf("backend calls %v, want 4 checkpoints and 4 restores", fx.backend.getCalls())
	}

	// Per-guest Suspend and Resume still work, and touch only their guest.
	sresp, err := srv.Suspend(ctx, &pb.SuspendRequest{JobId: guestJob, Epoch: 5, Deadline: deadlineIn(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	op := waitGuestOp(t, srv, sresp.GetOperationId())
	if op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED || len(op.GetTargets()) != 0 {
		t.Fatalf("per-guest Suspend: %v", op)
	}
	if !fx.frozen(t) || fx.frozenDir(t, podDir2) {
		t.Fatal("per-guest Suspend must freeze only its guest")
	}
	rresp, err := srv.Resume(ctx, &pb.ResumeRequest{JobId: guestJob, Epoch: 6, Deadline: deadlineIn(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if op = waitGuestOp(t, srv, rresp.GetOperationId()); op.GetOutcome() != pb.Outcome_OUTCOME_RESUMED {
		t.Fatalf("per-guest Resume: %v", op)
	}
	if fx.frozen(t) {
		t.Fatal("guest frozen after per-guest Resume")
	}
}

// TestHostOp_SuspendAllTargetFailure checks that one guest failing its
// precondition fails the host operation with its reason while the other
// guest is still suspended.
func TestHostOp_SuspendAllTargetFailure(t *testing.T) {
	fx := hostFixture(t, "GPU-0")
	podDir2 := fx.podDirs[1]
	fx.guest.Status.Conditions[0].Status = "True"
	srv := newHostServer(t, fx)
	resp, err := srv.SuspendAll(context.Background(), suspendAllReq(1))
	if err != nil {
		t.Fatal(err)
	}
	op := waitGuestOp(t, srv, resp.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED || op.GetErrorReason() != pb.ErrorReason_PRECONDITION_READINESS {
		t.Fatalf("SuspendAll: %v", op)
	}
	if fx.frozen(t) || !fx.frozenDir(t, podDir2) {
		t.Error("want the Ready guest untouched and the other guest frozen")
	}
	if st := jobStates(t, srv); st[guestJob] != pb.JobState_JOB_STATE_FAULTED || st[guestJob2] != pb.JobState_JOB_STATE_SUSPENDED {
		t.Errorf("job states %v", st)
	}
}

func TestHostOp_SuspendAllValidation(t *testing.T) {
	fx := hostFixture(t, "GPU-0")
	srv := newHostServer(t, fx)
	ctx := context.Background()
	noRole := &pb.SuspendAllRequest{Epoch: 1, Deadline: deadlineIn(time.Minute)}
	if _, err := srv.SuspendAll(ctx, noRole); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing role: %v", err)
	}
	if _, err := srv.ResumeAll(ctx, &pb.ResumeAllRequest{Role: "background", Epoch: 1}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing deadline: %v", err)
	}
	resp, err := srv.SuspendAll(ctx, suspendAllReq(5))
	if err != nil {
		t.Fatal(err)
	}
	waitGuestOp(t, srv, resp.GetOperationId())
	_, err = srv.ResumeAll(ctx, resumeAllReq(4))
	if sm.ErrorReasonOf(err) != pb.ErrorReason_STALE_EPOCH {
		t.Errorf("lower epoch: %v, want STALE_EPOCH", err)
	}
	srv.guest = nil
	if _, err := srv.SuspendAll(ctx, suspendAllReq(9)); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unconfigured SuspendAll: %v", err)
	}
	if _, err := srv.ResumeAll(ctx, resumeAllReq(9)); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unconfigured ResumeAll: %v", err)
	}
}

// TestScrub_NsScrub_SuspendAllScrubsEachGuestGPU checks that under ns-scrub
// a SuspendAll scrubs every GPU a guest held, one scrub at a time.
func TestScrub_NsScrub_SuspendAllScrubsEachGuestGPU(t *testing.T) {
	fx := hostFixture(t, "GPU-1")
	fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
	var running, overlap atomic.Int32
	fx.onScrub = func() {
		if running.Add(1) > 1 {
			overlap.Add(1)
		}
		time.Sleep(5 * time.Millisecond)
		running.Add(-1)
	}
	srv := newHostServer(t, fx)
	resp, err := srv.SuspendAll(context.Background(), suspendAllReq(1))
	if err != nil {
		t.Fatal(err)
	}
	if op := waitGuestOp(t, srv, resp.GetOperationId()); op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("SuspendAll: %v", op)
	}
	got := fx.scrubbed()
	sort.Strings(got)
	if want := []string{"GPU-0", "GPU-1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("scrubbed %v, want %v", got, want)
	}
	if overlap.Load() != 0 {
		t.Error("two scrubs ran at once")
	}
}

func TestScrub_Keep_SuspendNeverScrubs(t *testing.T) {
	fx := newGuestFixture(t)
	if _, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := fx.scrubbed(); len(got) != 0 {
		t.Errorf("keep scrubbed %v", got)
	}
}

// TestScrub_NsScrub_SuspendScrubsGuestGPUAfterVerify checks that ns-scrub
// scrubs only the GPU the guest held, after the guest is checkpointed and
// frozen, and that it lifts the allowlist refusal of keep.
func TestScrub_NsScrub_SuspendScrubsGuestGPUAfterVerify(t *testing.T) {
	fx := newGuestFixture(t)
	fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
	fx.gpu.devices = []gpuDevice{
		{Name: "Tesla T4", DriverVersion: "570.1", UUID: "GPU-0"},
		{Name: "Tesla T4", DriverVersion: "570.1", UUID: "GPU-1"},
	}
	fx.gpu.set(gpuProcess{PID: guestPID, UsedBytes: uint64(2 * gib), GPUUUID: "GPU-0"},
		gpuProcess{PID: 999, UsedBytes: uint64(5 * gib), GPUUUID: "GPU-1"})
	var mu sync.Mutex
	var frozenAtScrub, checkpointedAtScrub bool
	fx.onScrub = func() {
		mu.Lock()
		defer mu.Unlock()
		frozenAtScrub = fx.frozen(t)
		fx.backend.mu.Lock()
		checkpointedAtScrub = fx.backend.states[guestPID] == backends.CudaStateCheckpointed
		fx.backend.mu.Unlock()
	}
	res, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
	if err != nil || res.Outcome != pb.Outcome_OUTCOME_SUSPENDED {
		t.Fatalf("got %v, %v", res, err)
	}
	if got := fx.scrubbed(); !reflect.DeepEqual(got, []string{"GPU-0"}) {
		t.Errorf("scrubbed %v, want [GPU-0]", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !frozenAtScrub || !checkpointedAtScrub {
		t.Errorf("scrub ran before checkpoint and freeze (frozen %v, checkpointed %v)", frozenAtScrub, checkpointedAtScrub)
	}
}

func TestScrub_NsScrub_ScrubFailureFailsSuspend(t *testing.T) {
	t.Run("scrub error", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
		fx.scrubErr = errors.New("cuMemAlloc failed")
		srv := newGuestServer(t, fx)
		resp, err := srv.Suspend(context.Background(), &pb.SuspendRequest{JobId: guestJob, Epoch: 1, Deadline: deadlineIn(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		op := waitGuestOp(t, srv, resp.GetOperationId())
		if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED || op.GetErrorReason() != pb.ErrorReason_BACKEND_ERROR {
			t.Fatalf("got %v, want FAILED BACKEND_ERROR", op)
		}
		if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_FAULTED {
			t.Errorf("job %v, want FAULTED", js)
		}
	})
	t.Run("nonzero read back", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
		fx.scrubNonzero = 3
		_, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute))
		if sm.ErrorReasonOf(err) != pb.ErrorReason_VERIFY_FAILED {
			t.Fatalf("got %v, want VERIFY_FAILED", err)
		}
	})
}

func TestScrub_NsScrub_BudgetCountsScrub(t *testing.T) {
	fx := newGuestFixture(t)
	if d := fx.g.estimate(guestJob, 0, true, 2) - fx.g.estimate(guestJob, 0, true, 0); d != 2*scrubEstimate {
		t.Errorf("two scrubs add %s, want %s", d, 2*scrubEstimate)
	}
}

// TestScrub_NsScrub_RunScrubSubprocess checks that the default scrubFunc
// runs "<binary> scrub" with the policy flags and reads its SCRUBJSON line.
func TestScrub_NsScrub_RunScrubSubprocess(t *testing.T) {
	allowlist, err := scrub.ParseAllowlist(scrub.DefaultQualified)
	if err != nil {
		t.Fatal(err)
	}
	script := func(body string) string {
		path := filepath.Join(t.TempDir(), "agent")
		writeFile(t, path, "#!/bin/sh\n"+body+"\n")
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cfg := ScrubConfig{Policy: scrub.PolicyNsScrub, Mode: scrub.DefaultMode, Allowlist: allowlist, MarginMiB: 256}

	t.Run("scrub", func(t *testing.T) {
		argsFile := filepath.Join(t.TempDir(), "args")
		cfg.Command = script(`echo "$@" > ` + argsFile + `
echo 'SCRUBJSON {"decision":"scrub","gpu_uuid":"GPU-7","bytes_scrubbed":1024,"readback_nonzero_words":0}'`)
		res, err := cfg.runScrub(context.Background(), "GPU-7")
		if err != nil || res.Decision != scrub.ActionScrub || res.BytesScrubbed != 1024 || res.GPUUUID != "GPU-7" {
			t.Fatalf("got %+v, %v", res, err)
		}
		args, err := os.ReadFile(argsFile)
		if err != nil {
			t.Fatal(err)
		}
		want := "scrub --boundary=suspend --scrub-policy=ns-scrub --scrub=unqualified " +
			"--vram-zeroing-qualified=NVIDIA L4:580 --gpu-uuid=GPU-7 --margin-mib=256\n"
		if string(args) != want {
			t.Errorf("args %q, want %q", args, want)
		}
	})
	t.Run("error in SCRUBJSON", func(t *testing.T) {
		cfg.Command = script(`echo 'SCRUBJSON {"decision":"scrub","error":"cuMemAlloc failed"}'; exit 1`)
		if _, err := cfg.runScrub(context.Background(), "GPU-7"); err == nil || !strings.Contains(err.Error(), "cuMemAlloc") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("refused", func(t *testing.T) {
		cfg.Command = script(`echo 'SCRUBJSON {"decision":"refuse","reason":"PRECONDITION_NODE"}'; exit 3`)
		if _, err := cfg.runScrub(context.Background(), "GPU-7"); err == nil {
			t.Fatal("want an error")
		}
	})
	t.Run("no SCRUBJSON", func(t *testing.T) {
		cfg.Command = script(`echo boom >&2; exit 1`)
		if _, err := cfg.runScrub(context.Background(), "GPU-7"); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("got %v", err)
		}
	})
}
