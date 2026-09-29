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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	killJob     = "guest-kill"
	killGroup   = "group-1"
	killPodUID  = "11111111-2222-3333-4444-555555555555"
	killCtr     = "abc123"
	killPause   = "pause99"
	killPodPath = "kubepods.slice/kubepods-burstable.slice/" +
		"kubepods-burstable-pod11111111_2222_3333_4444_555555555555.slice"
)

// fakePods is a podSource.
type fakePods struct {
	mu   sync.Mutex
	pods []*corev1.Pod
}

func (f *fakePods) PodsForJob(jobID string) []*corev1.Pod {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*corev1.Pod
	for _, pod := range f.pods {
		if pod.Labels[podutils.JobIDLabel] == jobID {
			out = append(out, pod.DeepCopy())
		}
	}
	return out
}

func (f *fakePods) edit(change func(pods []*corev1.Pod) []*corev1.Pod) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods = change(f.pods)
}

// killGPU is the UUID of the GPU fakeNVML lists every PID on.
const killGPU = "GPU-11111111-aaaa-bbbb-cccc-000000000001"

// fakeNVML stands in for NVML: the PIDs it lists, or an error.
type fakeNVML struct {
	mu     sync.Mutex
	listed map[int]bool
	err    error
}

func (f *fakeNVML) pids(context.Context) (gpuProcs, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make(gpuProcs, len(f.listed))
	for pid := range f.listed {
		out[pid] = []string{killGPU}
	}
	return out, nil
}

func (f *fakeNVML) set(listed map[int]bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = listed
	f.err = err
}

// kernelOpts says how the fake kernel answers a write to cgroup.kill.
type kernelOpts struct {
	// ignoreKill leaves every process running.
	ignoreKill bool
	// gpuAfter and gpuErrAfter are what NVML reports once the processes
	// exited. The default lists only another tenant's PID.
	gpuAfter    map[int]bool
	gpuErrAfter error
}

// killEnv is a server with a fake cgroup tree holding one guest pod: its
// container has PIDs 101 and 102, and the pause sandbox PID 7. NVML lists
// 101 and another tenant's 999.
type killEnv struct {
	srv  *Server
	root string
	pods *fakePods
	gpu  *fakeNVML
}

func writeCgroupFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func guestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror", Namespace: "guests", UID: types.UID(killPodUID),
			Labels: map[string]string{podutils.JobIDLabel: killJob},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "vllm", ContainerID: "containerd://" + killCtr}},
		},
	}
}

func newKillEnv(t *testing.T, opts kernelOpts) *killEnv {
	t.Helper()
	root := t.TempDir()
	writeCgroupFile(t, root, "cgroup.controllers", "cpu memory pids")
	writeCgroupFile(t, root, killPodPath+"/cgroup.kill", "")
	writeCgroupFile(t, root, killPodPath+"/cri-containerd-"+killCtr+".scope/cgroup.procs", "101\n102\n")
	writeCgroupFile(t, root, killPodPath+"/cri-containerd-"+killPause+".scope/cgroup.procs", "7\n")

	env := &killEnv{
		srv:  NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil),
		root: root,
		pods: &fakePods{pods: []*corev1.Pod{guestPod()}},
		gpu:  &fakeNVML{listed: map[int]bool{101: true, 999: true}},
	}
	env.srv.killer = &killer{
		cgroups:   cgroup.New(root),
		pods:      env.pods,
		gpuPIDs:   env.gpu.pids,
		poll:      5 * time.Millisecond,
		scrubGate: newScrubGate(),
		scrubRun: func(context.Context, *scrub.Options) (scrub.Result, error) {
			return scrub.Result{}, errors.New("scrub called under keep")
		},
	}
	if opts.gpuAfter == nil {
		opts.gpuAfter = map[int]bool{999: true}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		env.kernel(t, opts, stop)
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	return env
}

// kernel stands in for the kernel's cgroup.kill: once 1 is written, every
// process in the pod exits and NVML changes to what opts says.
func (e *killEnv) kernel(t *testing.T, opts kernelOpts, stop <-chan struct{}) {
	t.Helper()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		if opts.ignoreKill {
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.root, killPodPath, "cgroup.kill"))
		if err != nil || strings.TrimSpace(string(data)) != "1" {
			continue
		}
		for _, ctr := range []string{killCtr, killPause} {
			path := filepath.Join(e.root, killPodPath, "cri-containerd-"+ctr+".scope", "cgroup.procs")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Error(err)
			}
		}
		e.gpu.set(opts.gpuAfter, opts.gpuErrAfter)
		return
	}
}

// setJob brings the job to state through the state manager's API.
func (e *killEnv) setJob(t *testing.T, state pb.JobState) {
	t.Helper()
	st := e.srv.state
	st.RegisterJob(killJob, killGroup)
	if state == pb.JobState_JOB_STATE_IDLE {
		return
	}
	if err := st.TransitionToRunning(killJob, []int{101, 102}); err != nil {
		t.Fatal(err)
	}
	var (
		opID string
		err  error
	)
	switch state {
	case pb.JobState_JOB_STATE_RUNNING:
		return
	case pb.JobState_JOB_STATE_SAVED:
		opID, err = st.StartSnapshot(killJob, killGroup, func() error { return nil })
	case pb.JobState_JOB_STATE_SUSPENDED, pb.JobState_JOB_STATE_FAULTED:
		opID, err = st.StartGuestOp(killJob, sm.OpTypeSuspend, 1, time.Now().Add(time.Minute),
			func(context.Context) (sm.GuestResult, error) {
				if state == pb.JobState_JOB_STATE_FAULTED {
					return sm.GuestResult{}, errors.New("checkpoint failed")
				}
				return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
			})
	default:
		t.Fatalf("setJob: unsupported state %s", state)
	}
	if err != nil {
		t.Fatal(err)
	}
	waitGuestOp(t, e.srv, opID)
	if got := e.jobStatus(t).GetState(); got != state {
		t.Fatalf("setJob: want %s, got %s", state, got)
	}
}

func (e *killEnv) kill(t *testing.T, within time.Duration) string {
	t.Helper()
	resp, err := e.srv.Kill(context.Background(), &pb.KillRequest{
		JobId: killJob, Deadline: timestamppb.New(time.Now().Add(within)), Reason: "T reached",
	})
	if err != nil {
		t.Fatalf("Kill: %v", err)
	}
	return resp.GetOperationId()
}

func (e *killEnv) jobStatus(t *testing.T) *pb.JobStatus {
	t.Helper()
	for _, st := range e.srv.state.GetJobStatus() {
		if st.GetJobId() == killJob {
			return st
		}
	}
	t.Fatalf("job %s not found", killJob)
	return nil
}

func (e *killEnv) checkKillWritten(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(e.root, killPodPath, "cgroup.kill"))
	if err != nil || string(data) != "1" {
		t.Fatalf("cgroup.kill = %q, %v; want 1", data, err)
	}
}

func (e *killEnv) checkKilled(t *testing.T, opID string) {
	t.Helper()
	resp := waitGuestOp(t, e.srv, opID)
	if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || resp.GetOutcome() != pb.Outcome_OUTCOME_KILLED {
		t.Fatalf("want COMPLETE/KILLED, got %s/%s (%s: %s)",
			resp.GetStatus(), resp.GetOutcome(), resp.GetErrorReason(), resp.GetError())
	}
	st := e.jobStatus(t)
	if st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetLastOutcome() != pb.Outcome_OUTCOME_KILLED {
		t.Fatalf("want IDLE/KILLED, got %s/%s", st.GetState(), st.GetLastOutcome())
	}
}

func (e *killEnv) checkUnconfirmed(t *testing.T, opID string) {
	t.Helper()
	resp := waitGuestOp(t, e.srv, opID)
	if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		resp.GetErrorReason() != pb.ErrorReason_KILL_UNCONFIRMED {
		t.Fatalf("want FAILED/KILL_UNCONFIRMED, got %s/%s (%s)", resp.GetStatus(), resp.GetErrorReason(), resp.GetError())
	}
	if st := e.jobStatus(t); st.GetState() != pb.JobState_JOB_STATE_FAULTED {
		t.Fatalf("want FAULTED, got %s/%s", st.GetState(), st.GetLastOutcome())
	}
}

// TestKill_FromAnyState: Kill ends IDLE with OUTCOME_KILLED from every job
// state, a running operation (TRANSITIONING) included.
func TestKill_FromAnyState(t *testing.T) {
	for _, state := range []pb.JobState{
		pb.JobState_JOB_STATE_IDLE,
		pb.JobState_JOB_STATE_RUNNING,
		pb.JobState_JOB_STATE_SAVED,
		pb.JobState_JOB_STATE_SUSPENDED,
		pb.JobState_JOB_STATE_FAULTED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			env := newKillEnv(t, kernelOpts{})
			env.setJob(t, state)
			env.checkKilled(t, env.kill(t, 5*time.Second))
			env.checkKillWritten(t)
		})
	}

	t.Run("JOB_STATE_TRANSITIONING", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		release := make(chan struct{})
		defer close(release)
		snapID, err := env.srv.state.StartSnapshot(killJob, killGroup, func() error {
			<-release
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		env.checkKilled(t, env.kill(t, 5*time.Second))
		if resp := waitGuestOp(t, env.srv, snapID); resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED {
			t.Fatalf("superseded Snapshot: want FAILED, got %s", resp.GetStatus())
		}
	})

	t.Run("unknown job", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		resp := waitGuestOp(t, env.srv, env.kill(t, 5*time.Second))
		if resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE || resp.GetOutcome() != pb.Outcome_OUTCOME_KILLED {
			t.Fatalf("want COMPLETE/KILLED, got %v", resp)
		}
	})
}

// TestKill_HungSuspendDoesNotBlockKill: a Suspend worker holds the node
// lock inside a checkpoint that ignores cancellation. Kill still confirms
// within its deadline, GetOperation answers meanwhile, a Snapshot waiting
// for the lock gives up at its deadline, and the Suspend returning later
// does not overwrite the killed job.
func TestKill_HungSuspendDoesNotBlockKill(t *testing.T) {
	env := newKillEnv(t, kernelOpts{})
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	cuda := backends.NewCudaCheckpoint()

	hung := make(chan struct{})
	holding := make(chan struct{})
	returned := make(chan struct{})
	suspendID, err := env.srv.state.StartGuestOp(killJob, sm.OpTypeSuspend, 1, time.Now().Add(time.Minute),
		func(ctx context.Context) (sm.GuestResult, error) {
			defer close(returned)
			if err := cuda.Acquire(ctx); err != nil {
				return sm.GuestResult{}, err
			}
			defer cuda.Release()
			close(holding)
			<-hung // Stuck in the driver: cancellation does not reach it.
			return sm.GuestResult{Outcome: pb.Outcome_OUTCOME_SUSPENDED}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	<-holding

	start := time.Now()
	env.checkKilled(t, env.kill(t, 3*time.Second))
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Kill took %s behind a hung Suspend", took)
	}
	env.checkKillWritten(t)
	if resp := waitGuestOp(t, env.srv, suspendID); resp.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED {
		t.Fatalf("superseded Suspend: want FAILED, got %s", resp.GetStatus())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	snapErr := cuda.Snapshot(ctx, backends.Request{JobID: "other", Config: backends.BuildCudaConfig([]string{"5"})})
	if !errors.Is(snapErr, context.DeadlineExceeded) {
		t.Fatalf("Snapshot behind the hung lock: want DeadlineExceeded, got %v", snapErr)
	}

	close(hung)
	<-returned
	time.Sleep(50 * time.Millisecond)
	if st := env.jobStatus(t); st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetLastOutcome() != pb.Outcome_OUTCOME_KILLED {
		t.Fatalf("the hung Suspend overwrote the kill: %s/%s", st.GetState(), st.GetLastOutcome())
	}
}

// TestKill_Unconfirmed: whatever keeps the kill from being confirmed within
// the deadline ends KILL_UNCONFIRMED with the job FAULTED.
func TestKill_Unconfirmed(t *testing.T) {
	t.Run("processes never exit; the pod going away clears the job", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{ignoreKill: true})
		env.setJob(t, pb.JobState_JOB_STATE_SUSPENDED)
		env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
		env.checkKillWritten(t)

		env.srv.state.RemoveJob(killJob)
		for _, st := range env.srv.state.GetJobStatus() {
			if st.GetJobId() == killJob {
				t.Fatalf("job still reported after its pod is gone: %v", st)
			}
		}
	})

	t.Run("NVML keeps listing a killed PID", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{gpuAfter: map[int]bool{101: true}})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
	})

	t.Run("NVML errors", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{gpuErrAfter: errors.New("NVML unavailable")})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
	})

	t.Run("live pod without a cgroup", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.pods.edit(func(pods []*corev1.Pod) []*corev1.Pod {
			pods[0].UID = "99999999-0000-0000-0000-000000000000"
			return pods
		})
		env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
	})

	t.Run("no pod watcher", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.srv.killer.pods = nil
		env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
	})

	t.Run("a deadline already passed still kills", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{ignoreKill: true})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.checkUnconfirmed(t, env.kill(t, -time.Second))
		env.checkKillWritten(t)
	})

	t.Run("a later Kill clears FAULTED", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{gpuErrAfter: errors.New("NVML unavailable")})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		env.checkUnconfirmed(t, env.kill(t, 100*time.Millisecond))

		env.gpu.set(map[int]bool{}, nil) // NVML recovers.
		env.checkKilled(t, env.kill(t, 5*time.Second))
	})
}

func TestKill_PodCases(t *testing.T) {
	t.Run("the pause sandbox is not waited for", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{ignoreKill: true})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		// The guest container already exited; only pause is left.
		writeCgroupFile(t, env.root, killPodPath+"/cri-containerd-"+killCtr+".scope/cgroup.procs", "")
		env.gpu.set(map[int]bool{}, nil)
		env.checkKilled(t, env.kill(t, 5*time.Second))
		env.checkKillWritten(t)
	})

	t.Run("terminal pod without a cgroup", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
		if err := os.RemoveAll(filepath.Join(env.root, "kubepods.slice")); err != nil {
			t.Fatal(err)
		}
		env.pods.edit(func(pods []*corev1.Pod) []*corev1.Pod {
			pods[0].Status.Phase = corev1.PodFailed
			return pods
		})
		env.checkKilled(t, env.kill(t, 5*time.Second))
	})

	t.Run("no pod left", func(t *testing.T) {
		env := newKillEnv(t, kernelOpts{})
		env.setJob(t, pb.JobState_JOB_STATE_FAULTED)
		env.pods.edit(func([]*corev1.Pod) []*corev1.Pod { return nil })
		env.checkKilled(t, env.kill(t, 5*time.Second))
	})
}

func TestKill_Validation(t *testing.T) {
	srv := NewServer(nil, backends.BackendNoop, "k8s", backends.NewChannelRegistry(), nil)
	for name, req := range map[string]*pb.KillRequest{
		"no job id":        {Deadline: timestamppb.New(time.Now().Add(time.Second))},
		"no deadline":      {JobId: killJob},
		"invalid deadline": {JobId: killJob, Deadline: &timestamppb.Timestamp{Nanos: -1}},
	} {
		if _, err := srv.Kill(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: want InvalidArgument, got %v", name, err)
		}
	}
}
