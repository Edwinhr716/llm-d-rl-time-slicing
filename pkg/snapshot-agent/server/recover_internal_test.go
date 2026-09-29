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
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
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
