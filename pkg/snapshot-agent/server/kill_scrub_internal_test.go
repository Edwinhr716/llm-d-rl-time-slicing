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
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

// fakeScrubGPU runs the real scrub.Run decision with a fake GPU identity
// and a fake CUDA scrub, and records every scrub it was asked to run.
type fakeScrubGPU struct {
	env *killEnv

	mu    sync.Mutex
	runs  []scrub.Options
	execs []string
	// execErr and execDelay shape the fake CUDA scrub.
	execErr   error
	execDelay time.Duration
	// orderErr records a scrub that ran before the kill was confirmed.
	orderErr error
}

func (f *fakeScrubGPU) run(ctx context.Context, opts *scrub.Options) (scrub.Result, error) {
	f.mu.Lock()
	f.runs = append(f.runs, *opts)
	f.mu.Unlock()
	o := *opts
	o.Identify = func(uuid string) (scrub.Identity, error) {
		if uuid == "" {
			uuid = killGPU
		}
		return scrub.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.65.06", UUID: uuid}, nil
	}
	o.Exec = func(ctx context.Context, uuid string, margin uint64) (scrub.ExecResult, error) {
		f.checkConfirmed(ctx)
		f.mu.Lock()
		f.execs = append(f.execs, uuid)
		delay, err := f.execDelay, f.execErr
		f.mu.Unlock()
		time.Sleep(delay)
		return scrub.ExecResult{FreeBefore: 22 << 30, BytesScrubbed: 22<<30 - margin}, err
	}
	return scrub.Run(ctx, &o)
}

// checkConfirmed records an error if the scrub runs while a job process is
// still in the cgroup or NVML still lists one.
func (f *fakeScrubGPU) checkConfirmed(ctx context.Context) {
	data, err := os.ReadFile(filepath.Join(f.env.root, killPodPath, "cri-containerd-"+killCtr+".scope", "cgroup.procs"))
	listed, gpuErr := f.env.gpu.pids(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case err != nil:
		f.orderErr = err
	case strings.TrimSpace(string(data)) != "":
		f.orderErr = errors.New("scrub ran while the job's processes were alive")
	case gpuErr == nil && (len(listed[101]) > 0 || len(listed[102]) > 0):
		f.orderErr = errors.New("scrub ran while NVML still listed the job")
	}
}

func (f *fakeScrubGPU) snapshot() ([]scrub.Options, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]scrub.Options(nil), f.runs...), append([]string(nil), f.execs...), f.orderErr
}

func newScrubKillEnv(t *testing.T, policy scrub.Policy, mode scrub.Mode) (*killEnv, *fakeScrubGPU) {
	t.Helper()
	env := newKillEnv(t, kernelOpts{})
	allowlist, err := scrub.ParseAllowlist(scrub.DefaultQualified)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeScrubGPU{env: env}
	env.srv.killer.configure(KillConfig{Scrub: ScrubConfig{Policy: policy, Mode: mode, Allowlist: allowlist}})
	env.srv.killer.scrubRun = fake.run
	return env, fake
}

// TestKillScrub_NsScrub_ScrubsJobGPUAfterConfirm: under ns-scrub the Kill
// scrubs the GPU NVML listed the job on, once, only after the processes
// are gone and NVML no longer lists them, with the default margin, and
// then ends IDLE with OUTCOME_KILLED.
func TestKillScrub_NsScrub_ScrubsJobGPUAfterConfirm(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyNsScrub, scrub.DefaultMode)
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	env.checkKilled(t, env.kill(t, 5*time.Second))

	runs, execs, orderErr := fake.snapshot()
	if orderErr != nil {
		t.Fatal(orderErr)
	}
	if len(runs) != 1 || len(execs) != 1 || execs[0] != killGPU {
		t.Fatalf("want one scrub of %s, got runs %+v execs %v", killGPU, runs, execs)
	}
	if runs[0].Boundary != scrub.BoundaryKill || runs[0].Policy != scrub.PolicyNsScrub ||
		runs[0].GPUUUID != killGPU || runs[0].MarginMiB != scrub.DefaultMarginMiB {
		t.Errorf("unexpected scrub options: %+v", runs[0])
	}
}

// TestKillScrub_NsScrub_NoGPUHeldSkips: a SUSPENDED guest was
// checkpointed and holds no VRAM, so NVML lists none of its processes;
// there is nothing of it to scrub at the kill boundary.
func TestKillScrub_NsScrub_NoGPUHeldSkips(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyNsScrub, scrub.DefaultMode)
	env.setJob(t, pb.JobState_JOB_STATE_SUSPENDED)
	env.gpu.set(map[int]bool{999: true}, nil)
	env.checkKilled(t, env.kill(t, 5*time.Second))
	runs, _, orderErr := fake.snapshot()
	if orderErr != nil {
		t.Fatal(orderErr)
	}
	if len(runs) != 0 {
		t.Fatalf("scrubbed although the job held no GPU: %+v", runs)
	}
}

// TestKillScrub_NsScrub_UnknownGPUFallsBackToOnlyGPU: when NVML fails
// before the kill, the job's GPU is unknown; the scrub targets the only
// visible GPU (scrub.Run refuses a node with more than one).
func TestKillScrub_NsScrub_UnknownGPUFallsBackToOnlyGPU(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyNsScrub, scrub.DefaultMode)
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	env.gpu.set(nil, errors.New("NVML busy"))
	env.checkKilled(t, env.kill(t, 5*time.Second))
	runs, execs, orderErr := fake.snapshot()
	if orderErr != nil {
		t.Fatal(orderErr)
	}
	if len(runs) != 1 || runs[0].GPUUUID != "" || len(execs) != 1 {
		t.Fatalf("want one scrub of the only visible GPU, got runs %+v execs %v", runs, execs)
	}
}

// TestKillScrub_NsScrub_FailureIsUnconfirmed: a failed scrub fails closed.
// The job's processes are dead, but the GPU is not handed off: the Kill
// ends KILL_UNCONFIRMED with the job FAULTED.
func TestKillScrub_NsScrub_FailureIsUnconfirmed(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyNsScrub, scrub.DefaultMode)
	fake.execErr = errors.New("cuMemsetD8 failed")
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	opID := env.kill(t, 5*time.Second)
	env.checkUnconfirmed(t, opID)
	if resp := waitGuestOp(t, env.srv, opID); !strings.Contains(resp.GetError(), "scrub") {
		t.Errorf("error does not name the scrub: %s", resp.GetError())
	}
}

// TestKillScrub_NsScrub_PastDeadlineIsUnconfirmed: the scrub counts toward
// the Kill deadline; a scrub that ends after it leaves the kill unconfirmed.
func TestKillScrub_NsScrub_PastDeadlineIsUnconfirmed(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyNsScrub, scrub.DefaultMode)
	fake.execDelay = 400 * time.Millisecond
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	env.checkUnconfirmed(t, env.kill(t, 200*time.Millisecond))
}

// TestKillScrub_Keep_NeverScrubs: under keep (the default) the Kill never
// calls the scrub, not even to decide.
func TestKillScrub_Keep_NeverScrubs(t *testing.T) {
	env, fake := newScrubKillEnv(t, scrub.PolicyKeep, scrub.DefaultMode)
	env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	env.checkKilled(t, env.kill(t, 5*time.Second))
	runs, _, orderErr := fake.snapshot()
	if orderErr != nil {
		t.Fatal(orderErr)
	}
	if len(runs) != 0 {
		t.Fatalf("keep called the scrub: %+v", runs)
	}

	// The zero KillConfig is keep too.
	env2 := newKillEnv(t, kernelOpts{})
	env2.srv.killer.configure(KillConfig{})
	env2.setJob(t, pb.JobState_JOB_STATE_RUNNING)
	env2.checkKilled(t, env2.kill(t, 5*time.Second))
}

// TestKillScrub_Flag_FollowsMode: option flag decides per GPU through
// scrub.Decide. On a qualified L4:580 the default mode (unqualified) skips
// the scrub and always runs it.
func TestKillScrub_Flag_FollowsMode(t *testing.T) {
	for mode, wantExec := range map[scrub.Mode]int{
		scrub.ModeUnqualified: 0, scrub.ModeAlways: 1, scrub.ModeNever: 0,
	} {
		t.Run(string(mode), func(t *testing.T) {
			env, fake := newScrubKillEnv(t, scrub.PolicyFlag, mode)
			env.setJob(t, pb.JobState_JOB_STATE_RUNNING)
			env.checkKilled(t, env.kill(t, 5*time.Second))
			runs, execs, orderErr := fake.snapshot()
			if orderErr != nil {
				t.Fatal(orderErr)
			}
			if len(runs) != 1 || len(execs) != wantExec {
				t.Fatalf("want one decision and %d scrubs, got runs %+v execs %v", wantExec, runs, execs)
			}
		})
	}
}
