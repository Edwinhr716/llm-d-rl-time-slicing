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

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
)

// killPollInterval is how often Kill re-checks that the job's processes
// are gone and that NVML no longer lists them.
const killPollInterval = 20 * time.Millisecond

// podSource lists the pods on this node that carry a job ID. The watcher
// implements it from its informer cache.
type podSource interface {
	PodsForJob(jobID string) []*corev1.Pod
}

// killer kills a job's processes through the pod cgroups and confirms it.
// It never takes the backend's node lock, so a hung cuda-checkpoint or a
// queue of Suspends cannot delay it.
type killer struct {
	cgroups *cgroup.Manager
	// pods is nil until the watcher starts (k8s mode only).
	pods podSource
	// gpuPIDs lists the PIDs NVML reports on any device, with or without
	// memory.
	gpuPIDs func(ctx context.Context) (map[int]bool, error)
	poll    time.Duration
}

func newKiller() *killer {
	return &killer{
		cgroups: cgroup.New(cgroup.DefaultRoot),
		gpuPIDs: nvmlGPUPIDs,
		poll:    killPollInterval,
	}
}

// killTarget is one local pod of the job and its pod cgroup.
type killTarget struct {
	pod          *corev1.Pod
	cgroupPath   string
	containerIDs []string
}

// Kill kills a job from any state. It supersedes the job's running
// operation, kills every process in the job's pod cgroups and confirms,
// within the absolute deadline, that the containers' processes are gone
// and that NVML lists none of them. It returns an operation ID to poll:
// COMPLETE with OUTCOME_KILLED (the job is IDLE), or FAILED with
// KILL_UNCONFIRMED (the job stays FAULTED until its pod is gone or a later
// Kill confirms).
func (s *Server) Kill(ctx context.Context, req *pb.KillRequest) (*pb.KillResponse, error) {
	ctx = logging.WithServerMethod(ctx, "Kill")
	ctx = logging.WithJobID(ctx, req.GetJobId())

	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Kill: job_id is required")
	}
	var deadline time.Time
	if req.GetDeadline() != nil {
		if err := req.GetDeadline().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Kill of job %s: invalid deadline: %v", req.GetJobId(), err)
		}
		deadline = req.GetDeadline().AsTime()
	}
	slog.InfoContext(ctx, "Kill called", "reason", req.GetReason(), "deadline", deadline)

	worker := s.killer.worker(req.GetJobId())
	// The kill outlives this RPC: its context comes from the deadline, not from ctx.
	opID, err := s.state.StartKill(req.GetJobId(), deadline, req.GetReason(), worker) //nolint:contextcheck // see above
	if err != nil {
		slog.WarnContext(ctx, "Kill refused", "error", err)
		return nil, err
	}
	return &pb.KillResponse{OperationId: opID}, nil
}

// worker returns the Kill worker for jobID.
func (k *killer) worker(jobID string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return k.kill(ctx, jobID)
	}
}

// kill always issues the kill, even when the deadline has passed, and
// then polls for confirmation until the deadline.
func (k *killer) kill(ctx context.Context, jobID string) error {
	start := time.Now()
	targets, err := k.targets(jobID)
	if err != nil {
		return err
	}

	// Every PID seen in the pod cgroups, before and while killing: NVML
	// must list none of them afterwards.
	seen := make(map[int]bool)
	var killErrs []error
	for _, target := range targets {
		pids, procsErr := k.cgroups.Procs(target.cgroupPath)
		if procsErr != nil {
			killErrs = append(killErrs, procsErr)
		}
		for _, pid := range pids {
			seen[pid] = true
		}
		if err := k.cgroups.Kill(target.cgroupPath); err != nil {
			killErrs = append(killErrs, fmt.Errorf("pod %s/%s: %w", target.pod.Namespace, target.pod.Name, err))
		}
	}
	if err := errors.Join(killErrs...); err != nil {
		return err
	}
	killed := time.Now()

	err = k.pollUntil(ctx, "the job's processes to exit", func() (bool, error) {
		empty := true
		for _, target := range targets {
			pids, err := k.cgroups.ContainerProcs(target.cgroupPath, target.containerIDs)
			if err != nil {
				return false, err
			}
			for _, pid := range pids {
				seen[pid] = true
			}
			if len(pids) > 0 {
				empty = false
			}
		}
		return empty, nil
	})
	if err != nil {
		return err
	}
	exited := time.Now()

	err = k.pollUntil(ctx, "NVML to release the job's processes", func() (bool, error) {
		listed, err := k.gpuPIDs(ctx)
		if err != nil {
			return false, err
		}
		for pid := range seen {
			if listed[pid] {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Kill confirmed", "jobID", jobID, "pods", len(targets), "pids", len(seen),
		"killMs", killed.Sub(start).Milliseconds(),
		"exitMs", exited.Sub(killed).Milliseconds(),
		"gpuFreeMs", time.Since(exited).Milliseconds(),
		"totalMs", time.Since(start).Milliseconds())
	return nil
}

// targets finds the job's local pods and their pod cgroups. A pod that has
// no cgroup is skipped only when it is terminal; a live pod without one
// fails the kill, because nothing proves its processes are gone.
func (k *killer) targets(jobID string) ([]killTarget, error) {
	if k.pods == nil {
		return nil, errors.New("no pod watcher: Kill needs k8s deployment mode")
	}
	pods := k.pods.PodsForJob(jobID)
	targets := make([]killTarget, 0, len(pods))
	for _, pod := range pods {
		cgroupPath, err := k.cgroups.PodCgroupPath(string(pod.UID))
		if errors.Is(err, cgroup.ErrNotFound) && isTerminal(pod) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("pod %s/%s (phase %s): %w", pod.Namespace, pod.Name, pod.Status.Phase, err)
		}
		targets = append(targets, killTarget{pod: pod, cgroupPath: cgroupPath, containerIDs: containerIDs(pod)})
	}
	return targets, nil
}

// pollUntil calls done until it reports true or ctx ends. It checks before
// it looks at ctx, so a condition already met counts even at the deadline.
func (k *killer) pollUntil(ctx context.Context, what string, done func() (bool, error)) error {
	ticker := time.NewTicker(k.poll)
	defer ticker.Stop()
	var lastErr error
	for {
		ok, err := done()
		if ok {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("waiting for %s: %w (last error: %w)", what, ctx.Err(), lastErr)
			}
			return fmt.Errorf("waiting for %s: %w", what, ctx.Err())
		case <-ticker.C:
		}
	}
}

func isTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// containerIDs returns the runtime IDs of the pod's containers, init and
// ephemeral containers included. The pod sandbox has none.
func containerIDs(pod *corev1.Pod) []string {
	var ids []string
	for _, statuses := range [][]corev1.ContainerStatus{
		pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses, pod.Status.EphemeralContainerStatuses,
	} {
		for i := range statuses {
			if statuses[i].ContainerID != "" {
				ids = append(ids, statuses[i].ContainerID)
			}
		}
	}
	return ids
}

// nvmlGPUPIDs returns every PID NVML lists as a compute or graphics
// process on any device, whatever memory it reports.
func nvmlGPUPIDs(ctx context.Context) (map[int]bool, error) {
	if ret := podutils.NvmlInit(); ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer func() {
		if ret := podutils.NvmlShutdown(); ret != nvml.SUCCESS {
			slog.WarnContext(ctx, "Failed to shut down NVML", "error", nvml.ErrorString(ret))
		}
	}()
	count, ret := podutils.NvmlDeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to get device count: %v", nvml.ErrorString(ret))
	}
	listed := make(map[int]bool)
	for i := range count {
		device, ret := podutils.NvmlDeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("failed to get device %d: %v", i, nvml.ErrorString(ret))
		}
		for _, query := range []func() ([]nvml.ProcessInfo, nvml.Return){
			device.GetComputeRunningProcesses, device.GetGraphicsRunningProcesses,
		} {
			procs, ret := query()
			if ret == nvml.ERROR_NOT_SUPPORTED {
				continue
			}
			if ret != nvml.SUCCESS {
				return nil, fmt.Errorf("failed to list processes on device %d: %v", i, nvml.ErrorString(ret))
			}
			for _, proc := range procs {
				listed[int(proc.Pid)] = true
			}
		}
	}
	return listed, nil
}
