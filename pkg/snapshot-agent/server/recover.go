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
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// Restart recovery. The agent keeps no job state across restarts; the node
// is the record. Once at startup, after the watcher has synced and before
// any RPC is served, every local job's state is observed with the same
// level-triggered reads the pipelines use:
//
//   - processes come from the container cgroups' cgroup.procs, never from
//     NVML or memory;
//   - cgroup.events is read first, and cuda-checkpoint is never called on a
//     frozen cgroup: frozen with no guest VRAM is SUSPENDED, frozen with
//     guest VRAM is FAULTED;
//   - not frozen, cuda-checkpoint --get-state runs on each process: any
//     checkpointed is SAVED, else any locked or failed is FAULTED, else any
//     running is RUNNING (whatever NVML shows); processes without CUDA are
//     skipped, and no CUDA process is IDLE.
//
// The epoch fence comes from the mirror's guest-epoch annotation, which the
// watcher seeds when it registers the job.

// observation is what recovery read for one job.
type observation struct {
	rec    sm.Recovered
	frozen bool
	// procs are the processes of the job's container cgroups.
	procs []int
	// states are the cuda-checkpoint states of the CUDA processes.
	states map[int]string
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
	obs := observation{frozen: frozen, procs: procs}
	if frozen {
		g.observeFrozen(target, &obs)
	} else {
		g.observeThawed(ctx, &obs)
	}
	return obs, nil
}

// observeFrozen classifies a frozen guest by NVML alone: the CLI may block
// on a frozen process.
func (g *guestPipeline) observeFrozen(target *guestTarget, obs *observation) {
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
	for _, p := range gpuProcs {
		if procSet[p.PID] && p.UsedBytes > 0 {
			faulted(fmt.Sprintf("frozen, and guest pid %d holds VRAM", p.PID))
			return
		}
	}
	var hostBytes int64
	if stat, err := g.cgroups.MemoryStat(target.podDir); err == nil {
		hostBytes = cgroup.HostBytes(stat)
	} else {
		slog.Warn("Restart recovery: cannot read memory.stat of a frozen guest", "podDir", target.podDir, "error", err)
	}
	obs.rec = sm.Recovered{State: pb.JobState_JOB_STATE_SUSPENDED, PIDs: obs.procs, HostBytesPinned: hostBytes}
	obs.why = "frozen, and no guest process holds VRAM"
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
			"hostBytesPinned", obs.rec.HostBytesPinned, "duration", time.Since(t0))
	}
	slog.InfoContext(ctx, "Restart recovery done", "jobs", len(jobIDs), "states", counts, "duration", time.Since(start))
}
