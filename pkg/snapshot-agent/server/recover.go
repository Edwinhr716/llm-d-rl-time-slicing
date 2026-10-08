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
	"log/slog"
	"sort"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// Restart recovery. The agent keeps no job state across restarts; the node
// is the record. Once at startup, after the watcher has synced and before
// any RPC is served (and before the watcher's GPU detection loop may
// promote a job), every local job's state is observed with the same
// level-triggered reads the pipelines use:
//
//   - processes come from the container cgroups' cgroup.procs, never from
//     NVML or memory;
//   - cgroup.events is read first, and cuda-checkpoint is never called on a
//     frozen cgroup: frozen with guest VRAM is FAULTED, frozen with no guest
//     VRAM is SUSPENDED;
//   - not frozen, cuda-checkpoint --get-state runs on each process: any
//     checkpointed is SAVED, else any locked, failed or unreadable is
//     FAULTED (a locked process next to a running one is a restore stopped
//     before its unlock), else any running is RUNNING (whatever NVML
//     shows); processes without CUDA are skipped, and no CUDA process is
//     IDLE.
//
// A scrub interrupted by the restart is redone: under a scrubbing D-NS-7
// policy, a frozen guest with no VRAM is scrubbed again, on the GPUs of its
// device nodes that no process is on, before it counts as SUSPENDED. A
// failed scrub leaves it FAULTED. Nothing records whether the scrub ran, so
// every restart scrubs those GPUs once more.
//
// Fences: each job's epoch comes from its mirror's guest-epoch annotation,
// which the watcher seeds when it registers the job; the SuspendAll and
// ResumeAll fence of each role is seeded with the highest annotation of the
// role's pods. A host operation in flight at the restart is lost with its
// ID; its targets are recovered one by one above, and the re-issued call
// (same epoch) continues each from its own state.

// recoveryScrubTimeout bounds the recovery scrub of one job.
const recoveryScrubTimeout = 2 * time.Minute

// observation is what recovery read for one job.
type observation struct {
	rec    sm.Recovered
	frozen bool
	// procs are the processes of the job's container cgroups.
	procs []int
	// states are the cuda-checkpoint states of the CUDA processes.
	states map[int]string
	// scrubbed are the GPUs the recovery scrub ran on.
	scrubbed []string
	// why says what made the state, for the log.
	why string
}

// observe reads one job's state from the node. An error means the node
// could not be read; the job is then left as the watcher registered it.
func (g *guestPipeline) observe(ctx context.Context, jobID string) (observation, error) {
	idle := sm.Recovered{State: pb.JobState_JOB_STATE_IDLE}
	target, err := g.resolve(jobID)
	if errors.Is(err, errGuestGone) {
		return observation{rec: idle, why: err.Error()}, nil
	}
	if err != nil {
		return observation{}, err
	}
	procs, err := target.procs(g.cgroups)
	if err != nil {
		return observation{}, fmt.Errorf("read cgroup.procs: %w", err)
	}
	frozen, err := g.cgroups.Frozen(target.podDir)
	if err != nil {
		return observation{}, fmt.Errorf("read cgroup.events: %w", err)
	}
	appCfg, err := g.guestAppConfig(ctx, target.pod)
	if err != nil {
		return observation{}, err
	}
	obs := observation{frozen: frozen, procs: procs}
	switch {
	case len(procs) == 0:
		obs.rec = idle
		obs.why = "no process in the container cgroups"
	case frozen:
		g.observeFrozen(ctx, jobID, target, appCfg != nil, &obs)
	default:
		g.observeThawed(ctx, &obs)
		if appCfg != nil {
			g.observeThawedApp(ctx, appCfg, &obs)
		}
	}
	return obs, nil
}

// observeFrozen classifies a frozen guest by NVML alone (the CLI may block
// on a frozen process), and redoes its scrub. A frozen application guest is
// asleep, and may keep up to appResidualMaxBytes of VRAM.
func (g *guestPipeline) observeFrozen(ctx context.Context, jobID string, target *guestTarget, app bool, obs *observation) {
	faulted := func(why string) {
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_FAULTED, PIDs: obs.procs}
		obs.why = why
	}
	gpuProcs, err := g.gpu.Processes()
	if err != nil {
		faulted(fmt.Sprintf("frozen, and NVML cannot show the guest holds no VRAM: %v", err))
		return
	}
	procSet := toSet(obs.procs)
	residualMax := int64(0)
	if app {
		residualMax = appResidualMaxBytes
	}
	if err := checkResidual(gpuProcs, procSet, residualMax); err != nil {
		faulted(fmt.Sprintf("frozen, and %v", err))
		return
	}
	scrubbed, err := g.recoveryScrub(ctx, jobID, obs.procs, app)
	obs.scrubbed = scrubbed
	if err != nil {
		faulted(fmt.Sprintf("frozen with no guest VRAM, but the recovery scrub failed: %v", err))
		return
	}
	var hostBytes int64
	if stat, err := g.cgroups.MemoryStat(target.podDir); err == nil {
		hostBytes = cgroup.HostBytes(stat)
	} else {
		slog.WarnContext(ctx, "Restart recovery: cannot read memory.stat of a frozen guest", "podDir", target.podDir, "error", err)
	}
	obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_SUSPENDED, PIDs: obs.procs, HostBytesPinned: hostBytes}
	obs.why = "frozen, and no guest process holds VRAM"
	if app {
		obs.why = "frozen application guest, within the residual VRAM of a sleeping application"
	}
}

// recoveryScrub redoes the Suspend-boundary scrub of a frozen guest under a
// scrubbing policy: on each GPU the policy scrubs, among the guest's device
// nodes, that no process is on (for a sleeping application guest, which NVML
// still lists, those no other process is on). It returns the GPUs scrubbed.
func (g *guestPipeline) recoveryScrub(ctx context.Context, jobID string, procs []int, app bool) ([]string, error) {
	if !g.scrubCfg.scrubs() {
		return nil, nil
	}
	policyGPUs, err := g.policyScrubGPUs()
	if err != nil {
		return nil, err
	}
	var gpus map[string]bool
	if app {
		gpus, err = g.exclusiveGuestGPUs(toSet(procs))
	} else {
		gpus, err = g.idleGuestGPUs(procs)
	}
	if err != nil {
		return nil, err
	}
	gpus = intersect(policyGPUs, gpus)
	if len(gpus) == 0 {
		return nil, nil
	}
	scrubCtx, cancel := context.WithTimeout(ctx, recoveryScrubTimeout)
	defer cancel()
	uuids := make([]string, 0, len(gpus))
	for u := range gpus {
		uuids = append(uuids, u)
	}
	sort.Strings(uuids)
	if err := g.scrubFreed(scrubCtx, jobID, gpus); err != nil {
		return nil, err
	}
	return uuids, nil
}

// observeThawed classifies a guest that is not frozen by the
// cuda-checkpoint state of each of its processes.
func (g *guestPipeline) observeThawed(ctx context.Context, obs *observation) {
	obs.states = map[int]string{}
	var checkpointed, running, stuck []int
	for _, pid := range obs.procs {
		state, err := g.backend.GetState(ctx, pid)
		if errors.Is(err, backends.ErrNotCudaProcess) {
			continue
		}
		if err != nil {
			// The process's state is unknown: it may be half-way through a
			// checkpoint or a restore.
			state = "unknown: " + err.Error()
		}
		obs.states[pid] = state
		switch state {
		case backends.CudaStateCheckpointed:
			checkpointed = append(checkpointed, pid)
		case backends.CudaStateRunning:
			running = append(running, pid)
		default:
			stuck = append(stuck, pid)
		}
	}
	switch {
	case len(checkpointed) > 0:
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_SAVED, PIDs: checkpointed}
		obs.why = "not frozen, and a process is checkpointed"
	case len(stuck) > 0:
		// A locked process next to a running one is a restore that stopped
		// before its unlock: the guest cannot serve, so it is not RUNNING.
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_FAULTED, PIDs: stuck}
		obs.why = "not frozen, no process is checkpointed, and a process is locked or failed"
	case len(running) > 0:
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_RUNNING, PIDs: running}
		obs.why = "not frozen, and every CUDA process is running"
	default:
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_IDLE}
		obs.why = "not frozen, and no CUDA process"
	}
}

// observeThawedApp reclassifies a thawed application guest that sleeps: a
// restart between the sleep and the freeze of a Suspend, or between the thaw
// and the wake up of a Resume. Like a checkpointed process that is not
// frozen, it is SAVED: a re-issued Suspend freezes it, a Resume wakes it.
func (g *guestPipeline) observeThawedApp(ctx context.Context, cfg *pb.BackendConfig, obs *observation) {
	if g.app == nil || obs.rec.State != pb.JobState_JOB_STATE_RUNNING {
		return
	}
	sleeping, err := g.app.IsSuspended(ctx, cfg)
	if err != nil {
		slog.WarnContext(ctx, "Restart recovery: cannot read an application guest's sleep state", "error", err)
		return
	}
	if sleeping {
		obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_SAVED, PIDs: obs.rec.PIDs}
		obs.why = "not frozen, and the application guest sleeps"
	}
}

// recoverJobs observes each job and sets its state. It runs once, before
// the gRPC server starts, so no operation can be running.
func (s *Server) recoverJobs(ctx context.Context, jobIDs []string) {
	if s.guest == nil {
		slog.WarnContext(ctx, "Restart recovery skipped: the guest pipelines are not configured", "jobs", len(jobIDs))
		return
	}
	start := time.Now()
	counts := map[string]int{}
	for _, jobID := range jobIDs {
		t0 := time.Now()
		obs, err := s.guest.observe(ctx, jobID)
		if err != nil {
			counts["unread"]++
			slog.ErrorContext(ctx, "Restart recovery: cannot observe the job; it stays as registered",
				"jobID", jobID, "error", err, "duration", time.Since(t0))
			continue
		}
		ok, err := s.state.RecoverJob(jobID, obs.rec)
		if err != nil || !ok {
			counts["skipped"]++
			slog.WarnContext(ctx, "Restart recovery: job not recovered", "jobID", jobID, "error", err)
			continue
		}
		counts[obs.rec.State.String()]++
		slog.InfoContext(ctx, "Restart recovery: job recovered",
			"jobID", jobID, "state", obs.rec.State, "why", obs.why, "frozen", obs.frozen,
			"procs", obs.procs, "cudaStates", obs.states, "pids", obs.rec.PIDs,
			"scrubbedGPUs", obs.scrubbed, "hostBytesPinned", obs.rec.HostBytesPinned,
			"duration", time.Since(t0))
	}
	slog.InfoContext(ctx, "Restart recovery done", "jobs", len(jobIDs), "states", counts, "duration", time.Since(start))
}

// seedHostEpochs raises the SuspendAll and ResumeAll fence of each role to
// the given epoch (the highest guest-epoch annotation of the role's pods).
func (s *Server) seedHostEpochs(ctx context.Context, epochs map[string]int64) {
	for role, epoch := range epochs {
		s.state.SeedHostEpoch(role, epoch)
		slog.InfoContext(ctx, "Restart recovery: host fence seeded", "role", role, "epoch", epoch)
	}
}
