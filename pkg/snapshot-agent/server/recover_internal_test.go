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
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// countingBackend counts GetState calls: none may reach a frozen guest.
type countingBackend struct {
	*fakeBackend
	getState atomic.Int32
}

func (c *countingBackend) GetState(ctx context.Context, pid int) (string, error) {
	c.getState.Add(1)
	return c.fakeBackend.GetState(ctx, pid)
}

// restartedServer is a fresh agent after a restart: the job is registered
// by the watcher as IDLE and recovery runs before any RPC.
func restartedServer(t *testing.T, fx *guestFixture) (*Server, *countingBackend) {
	t.Helper()
	cb := &countingBackend{fakeBackend: fx.backend}
	fx.g.backend = cb
	fx.g.records = map[string]*guestRecord{}
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil)
	srv.guest = fx.g
	srv.state.RegisterJob(guestJob, "group-1")
	srv.state.SeedEpoch(guestJob, 4)
	srv.recoverJobs(context.Background(), []string{guestJob})
	return srv, cb
}

// setGuestState sets the guest process's cuda-checkpoint state; "" means no CUDA context.
func (f *guestFixture) setGuestState(state string) {
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	if state == "" {
		delete(f.backend.states, guestPID)
		return
	}
	f.backend.states[guestPID] = state
}

func checkRecovered(t *testing.T, srv *Server, state pb.JobState, pids []int) {
	t.Helper()
	js := jobStatus(t, srv)
	if js.GetState() != state {
		t.Fatalf("recovered state %s, want %s (%v)", js.GetState(), state, js)
	}
	if js.GetEpoch() != 4 {
		t.Errorf("epoch %d, want 4 from the mirror annotation", js.GetEpoch())
	}
	got, err := srv.state.GetJobPIDs(guestJob)
	if err != nil {
		got = nil // a job without PIDs
	}
	if !reflect.DeepEqual(got, pids) {
		t.Errorf("recovered pids %v, want %v", got, pids)
	}
}

func suspendCall(t *testing.T, srv *Server, epoch int64) *pb.GetOperationResponse {
	t.Helper()
	req := &pb.SuspendRequest{JobId: guestJob, Epoch: epoch, Deadline: deadlineIn(time.Minute)}
	resp, err := srv.Suspend(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return waitGuestOp(t, srv, resp.GetOperationId())
}

func resumeCall(t *testing.T, srv *Server, epoch int64) *pb.GetOperationResponse {
	t.Helper()
	req := &pb.ResumeRequest{JobId: guestJob, Epoch: epoch, Deadline: deadlineIn(time.Minute)}
	resp, err := srv.Resume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return waitGuestOp(t, srv, resp.GetOperationId())
}

func checkOutcome(t *testing.T, op *pb.GetOperationResponse, outcome pb.Outcome) {
	t.Helper()
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || op.GetOutcome() != outcome {
		t.Fatalf("operation %v, want COMPLETE with %s", op, outcome)
	}
}

func TestRecover_Running(t *testing.T) {
	fx := newGuestFixture(t)
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_RUNNING, []int{guestPID})
	checkOutcome(t, suspendCall(t, srv, 5), pb.Outcome_OUTCOME_SUSPENDED)
}

// A restart after a Suspend finished: frozen, no guest VRAM.
func TestRecover_Suspended(t *testing.T) {
	fx := newGuestFixture(t)
	if _, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	srv, cb := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
	if n := cb.getState.Load(); n != 0 {
		t.Errorf("cuda-checkpoint --get-state ran %d times on a frozen guest", n)
	}
	js := jobStatus(t, srv)
	if js.GetLastOutcome() != pb.Outcome_OUTCOME_SUSPENDED || js.GetHostBytesPinned() != 1320 {
		t.Errorf("job %v", js)
	}
	// A re-issued Suspend completes without a second checkpoint; Resume
	// thaws and restores.
	checkOutcome(t, suspendCall(t, srv, 5), pb.Outcome_OUTCOME_SUSPENDED)
	checkOutcome(t, resumeCall(t, srv, 6), pb.Outcome_OUTCOME_RESUMED)
	if fx.frozen(t) {
		t.Error("frozen after Resume")
	}
	want := []string{"checkpoint [100] locked=0", "restore [100] checkpointed"}
	if got := fx.backend.getCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("backend calls %v, want %v", got, want)
	}
}

// A restart mid-suspend, after the checkpoint and before the freeze.
func TestRecover_MidSuspendSaved(t *testing.T) {
	fx := newGuestFixture(t)
	fx.setGuestState(backends.CudaStateCheckpointed)
	fx.gpu.set(gpuProcess{PID: 999, UsedBytes: uint64(5 * gib)})
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SAVED, []int{guestPID})
	// The next Suspend continues: no checkpoint, freeze, verify.
	checkOutcome(t, suspendCall(t, srv, 5), pb.Outcome_OUTCOME_SUSPENDED)
	if !fx.frozen(t) {
		t.Error("not frozen after Suspend")
	}
	if got := fx.backend.getCalls(); len(got) != 0 {
		t.Errorf("backend calls %v, want none", got)
	}
}

// A restart mid-resume, after the thaw and before the restore.
func TestRecover_MidResumeSaved(t *testing.T) {
	fx := newGuestFixture(t)
	ctx := context.Background()
	if _, err := fx.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := fx.g.cgroups.Thaw(ctx, fx.podDir); err != nil {
		t.Fatal(err)
	}
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SAVED, []int{guestPID})
	checkOutcome(t, resumeCall(t, srv, 5), pb.Outcome_OUTCOME_RESUMED)
	if js := jobStatus(t, srv); js.GetState() != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("job %v", js)
	}
}

// A restart mid-resume, after the restore: the guest runs again.
func TestRecover_MidResumeRestored(t *testing.T) {
	fx := newGuestFixture(t)
	ctx := context.Background()
	if _, err := fx.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.g.resume(ctx, guestJob); err != nil {
		t.Fatal(err)
	}
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_RUNNING, []int{guestPID})
	// The re-issued Resume completes at once.
	checkOutcome(t, resumeCall(t, srv, 5), pb.Outcome_OUTCOME_RESUMED)
}

func TestRecover_Faulted(t *testing.T) {
	t.Run("frozen with guest VRAM", func(t *testing.T) {
		fx := newGuestFixture(t)
		if err := fx.g.cgroups.Freeze(context.Background(), fx.podDir); err != nil {
			t.Fatal(err)
		}
		srv, cb := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID, helperPID})
		if n := cb.getState.Load(); n != 0 {
			t.Errorf("cuda-checkpoint --get-state ran %d times on a frozen guest", n)
		}
	})
	t.Run("frozen, NVML does not know the guest's VRAM", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.gpu.set(gpuProcess{PID: guestPID, UsedBytes: nvmlNotAvailable})
		if err := fx.g.cgroups.Freeze(context.Background(), fx.podDir); err != nil {
			t.Fatal(err)
		}
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID, helperPID})
	})
	t.Run("locked", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.setGuestState(backends.CudaStateLocked)
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID})
	})
	t.Run("one process running, one locked", func(t *testing.T) {
		// A restore that stopped between restore and unlock.
		fx := newGuestFixture(t)
		fx.backend.states[helperPID] = backends.CudaStateLocked
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{helperPID})
	})
	t.Run("failed", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.setGuestState(backends.CudaStateFailed)
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID})
	})
	t.Run("one process checkpointed, one failed", func(t *testing.T) {
		// Checkpointed ranks first: the next Resume restores it.
		fx := newGuestFixture(t)
		fx.setGuestState(backends.CudaStateCheckpointed)
		fx.backend.states[helperPID] = backends.CudaStateFailed
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_SAVED, []int{guestPID})
	})
}

func TestRecover_Idle(t *testing.T) {
	t.Run("no CUDA process", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.setGuestState("")
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_IDLE, nil)
	})
	t.Run("no process", func(t *testing.T) {
		fx := newGuestFixture(t)
		writeFile(t, filepath.Join(fx.podDir, "cri-containerd-c1.scope", "cgroup.procs"), "")
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_IDLE, nil)
	})
	t.Run("no mirror pod", func(t *testing.T) {
		fx := newGuestFixture(t)
		fx.mirror = nil
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_IDLE, nil)
	})
}

// A job whose cgroups cannot be read stays as the watcher registered it.
func TestRecover_Unreadable(t *testing.T) {
	fx := newGuestFixture(t)
	fx.setGuestState(backends.CudaStateCheckpointed)
	// Without cgroup.freeze the fake kernel stops writing cgroup.events.
	if err := os.Remove(filepath.Join(fx.podDir, "cgroup.freeze")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := os.Remove(filepath.Join(fx.podDir, "cgroup.events")); err != nil {
		t.Fatal(err)
	}
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_IDLE, nil)
}

func TestRecover_NoGuestPipeline(t *testing.T) {
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil)
	srv.state.RegisterJob(guestJob, "group-1")
	srv.state.SeedEpoch(guestJob, 4)
	srv.recoverJobs(context.Background(), []string{guestJob})
	checkRecovered(t, srv, pb.JobState_JOB_STATE_IDLE, nil)
}

// nsScrubFixture is a guest fixture under ns-scrub whose guest process has
// the device node /dev/nvidia0 (GPU-0), with GPU discovery from device
// nodes on; the other tenant runs on GPU-1.
func nsScrubFixture(t *testing.T) *guestFixture {
	t.Helper()
	fx := newGuestFixture(t)
	fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
	fx.gpu.devices = []gpuDevice{
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-0", Minor: 0},
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-1", Minor: 1},
	}
	guest := gpuProcess{PID: guestPID, UsedBytes: uint64(2 * gib), GPUUUID: "GPU-0"}
	other := gpuProcess{PID: 999, UsedBytes: uint64(5 * gib), GPUUUID: "GPU-1"}
	fx.gpu.set(guest, other)
	fx.backend.afterCkpt = func() { fx.gpu.set(other) }
	fx.backend.afterRest = func() { fx.gpu.set(guest, other) }
	fx.g.procRoot = t.TempDir()
	for _, n := range []string{"nvidia0", "nvidiactl", "nvidia-uvm", "null"} {
		writeFile(t, filepath.Join(fx.g.procRoot, fmt.Sprint(guestPID), "root", "dev", n), "")
	}
	return fx
}

// checkpointAndFreeze leaves the guest as a Suspend killed between its
// freeze and its scrub: checkpointed, frozen, nothing scrubbed.
func (f *guestFixture) checkpointAndFreeze(t *testing.T) {
	t.Helper()
	f.setGuestState(backends.CudaStateCheckpointed)
	f.backend.afterCkpt()
	if err := f.g.cgroups.Freeze(context.Background(), f.podDir); err != nil {
		t.Fatal(err)
	}
}

func checkScrubbed(t *testing.T, fx *guestFixture, want []string) {
	t.Helper()
	if got := fx.scrubbed(); !reflect.DeepEqual(got, want) {
		t.Errorf("scrubbed %v, want %v", got, want)
	}
}

// TestRecover_Scrub_Interrupted is a restart between the freeze and the
// scrub: recovery scrubs the guest's GPU before the guest counts as
// SUSPENDED.
func TestRecover_Scrub_Interrupted(t *testing.T) {
	fx := nsScrubFixture(t)
	fx.checkpointAndFreeze(t)
	checkScrubbed(t, fx, nil)
	srv, cb := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
	checkScrubbed(t, fx, []string{"GPU-0"})
	if n := cb.getState.Load(); n != 0 {
		t.Errorf("cuda-checkpoint --get-state ran %d times on a frozen guest", n)
	}
}

// Nothing records that a scrub finished, so a restart after a complete
// Suspend scrubs once more.
func TestRecover_Scrub_AfterCompleteSuspend(t *testing.T) {
	fx := nsScrubFixture(t)
	if _, err := fx.g.suspend(context.Background(), guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	checkScrubbed(t, fx, []string{"GPU-0"})
	srv, _ := restartedServer(t, fx)
	checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
	checkScrubbed(t, fx, []string{"GPU-0", "GPU-0"})
}

func TestRecover_Scrub_FailureIsFaulted(t *testing.T) {
	t.Run("scrub error", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.checkpointAndFreeze(t)
		fx.scrubErr = errors.New("cuMemAlloc failed")
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID, helperPID})
	})
	t.Run("nonzero read back", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.checkpointAndFreeze(t)
		fx.scrubNonzero = 1
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_FAULTED, []int{guestPID, helperPID})
	})
}

func TestRecover_Scrub_Skipped(t *testing.T) {
	t.Run("another process is on the GPU", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.checkpointAndFreeze(t)
		fx.gpu.set(gpuProcess{PID: 999, UsedBytes: uint64(5 * gib), GPUUUID: "GPU-0"})
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
		checkScrubbed(t, fx, nil)
	})
	t.Run("keep policy", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.g.scrubCfg.Policy = scrub.DefaultPolicy
		fx.checkpointAndFreeze(t)
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
		checkScrubbed(t, fx, nil)
	})
	t.Run("the guest process is gone from procfs", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.checkpointAndFreeze(t)
		if err := os.RemoveAll(filepath.Join(fx.g.procRoot, fmt.Sprint(guestPID))); err != nil {
			t.Fatal(err)
		}
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_SUSPENDED, []int{guestPID, helperPID})
		checkScrubbed(t, fx, nil)
	})
}

// A guest that is already checkpointed holds no VRAM, so NVML lists none
// of its GPUs; the Suspend that follows a restart still scrubs its GPU.
func TestRecover_Scrub_SuspendOfCheckpointedGuest(t *testing.T) {
	t.Run("restart between checkpoint and freeze", func(t *testing.T) {
		fx := nsScrubFixture(t)
		fx.setGuestState(backends.CudaStateCheckpointed)
		fx.backend.afterCkpt()
		srv, _ := restartedServer(t, fx)
		checkRecovered(t, srv, pb.JobState_JOB_STATE_SAVED, []int{guestPID})
		checkOutcome(t, suspendCall(t, srv, 5), pb.Outcome_OUTCOME_SUSPENDED)
		checkScrubbed(t, fx, []string{"GPU-0"})
	})
	t.Run("re-issued Suspend of a suspended guest", func(t *testing.T) {
		fx := nsScrubFixture(t)
		srv := newGuestServer(t, fx)
		checkOutcome(t, suspendCall(t, srv, 1), pb.Outcome_OUTCOME_SUSPENDED)
		checkOutcome(t, suspendCall(t, srv, 2), pb.Outcome_OUTCOME_SUSPENDED)
		checkScrubbed(t, fx, []string{"GPU-0", "GPU-0"})
	})
}

// TestRecover_RestartMidSuspendAll is a SuspendAll with epoch 3 cut by a
// restart: the first guest was suspended but its scrub may not have run,
// the second was checkpointed but not frozen. Recovery finds SUSPENDED
// (scrubbed again) and SAVED, refuses a late SuspendAll as a whole, and the
// re-issued SuspendAll suspends and scrubs both.
func TestRecover_RestartMidSuspendAll(t *testing.T) {
	fx := hostFixture(t, "GPU-1")
	fx.g.scrubCfg.Policy = scrub.PolicyNsScrub
	fx.gpu.devices = []gpuDevice{
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-0", Minor: 0},
		{Name: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-1", Minor: 1},
	}
	fx.g.procRoot = t.TempDir()
	writeFile(t, filepath.Join(fx.g.procRoot, fmt.Sprint(guestPID), "root", "dev", "nvidia0"), "")
	writeFile(t, filepath.Join(fx.g.procRoot, fmt.Sprint(guestPID2), "root", "dev", "nvidia1"), "")
	ctx := context.Background()
	if _, err := fx.g.suspend(ctx, guestJob, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	fx.backend.mu.Lock()
	fx.backend.states[guestPID2] = backends.CudaStateCheckpointed
	fx.backend.mu.Unlock()
	fx.backend.afterCkpt()
	checkScrubbed(t, fx, []string{"GPU-0"})

	// The restart.
	fx.g.records = map[string]*guestRecord{}
	lister := func(role string) []string {
		if role == "background" {
			return []string{guestJob, guestJob2}
		}
		return nil
	}
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil, sm.WithTargetLister(lister))
	srv.guest = fx.g
	for _, id := range []string{guestJob, guestJob2} {
		srv.state.RegisterJob(id, "group-1")
		srv.state.SeedEpoch(id, 3)
	}
	srv.recoverJobs(ctx, []string{guestJob, guestJob2})
	srv.seedHostEpochs(ctx, map[string]int64{"background": 3})
	want := map[string]pb.JobState{guestJob: pb.JobState_JOB_STATE_SUSPENDED, guestJob2: pb.JobState_JOB_STATE_SAVED}
	if got := jobStates(t, srv); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered %v, want %v", got, want)
	}
	checkScrubbed(t, fx, []string{"GPU-0", "GPU-0"})

	if _, err := srv.SuspendAll(ctx, suspendAllReq(2)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late SuspendAll: got %v, want FailedPrecondition (STALE_EPOCH)", err)
	}
	resp, err := srv.SuspendAll(ctx, suspendAllReq(3))
	if err != nil {
		t.Fatal(err)
	}
	checkTargets(t, waitGuestOp(t, srv, resp.GetOperationId()), pb.Outcome_OUTCOME_SUSPENDED)
	want = map[string]pb.JobState{guestJob: pb.JobState_JOB_STATE_SUSPENDED, guestJob2: pb.JobState_JOB_STATE_SUSPENDED}
	if got := jobStates(t, srv); !reflect.DeepEqual(got, want) {
		t.Errorf("after SuspendAll %v, want %v", got, want)
	}
	if !fx.frozenDir(t, fx.podDirs[0]) || !fx.frozenDir(t, fx.podDirs[1]) {
		t.Error("a guest is not frozen after SuspendAll")
	}
	got := fx.scrubbed()
	sort.Strings(got)
	if w := []string{"GPU-0", "GPU-0", "GPU-0", "GPU-1"}; !reflect.DeepEqual(got, w) {
		t.Errorf("scrubbed %v, want %v", got, w)
	}
	// One checkpoint only, from before the restart: the second guest was
	// already checkpointed.
	if calls := fx.backend.getCalls(); len(calls) != 1 {
		t.Errorf("backend calls %v, want the one checkpoint before the restart", calls)
	}

	resp2, err := srv.ResumeAll(ctx, resumeAllReq(4))
	if err != nil {
		t.Fatal(err)
	}
	checkTargets(t, waitGuestOp(t, srv, resp2.GetOperationId()), pb.Outcome_OUTCOME_RESUMED)
	if fx.frozenDir(t, fx.podDirs[0]) || fx.frozenDir(t, fx.podDirs[1]) {
		t.Error("a guest is frozen after ResumeAll")
	}
}
