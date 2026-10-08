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
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
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

// KillConfig configures the Kill pipeline.
type KillConfig struct {
	// Scrub is the D-NS-7 handoff policy applied after a confirmed Kill.
	Scrub ScrubConfig
}

// gpuProcs maps a PID NVML lists as a compute or graphics process to the
// UUIDs of the GPUs it is listed on.
type gpuProcs map[int][]string

// killer kills a job's processes through the pod cgroups and confirms it.
// It never takes the backend's node lock, so a hung cuda-checkpoint or a
// queue of Suspends (per job or from a SuspendAll) cannot delay it.
type killer struct {
	cgroups *cgroup.Manager
	// pods is nil until the watcher starts (k8s mode only).
	pods podSource
	// gpuPIDs lists the PIDs NVML reports on any device, with or without
	// memory.
	gpuPIDs func(ctx context.Context) (gpuProcs, error)
	poll    time.Duration
	// scrubCfg and scrubRun run the D-NS-7 handoff scrub once the kill is
	// confirmed. scrubRun is the agent's single scrub path,
	// ScrubConfig.runScrubProcess (the Suspend pipeline runs the same one),
	// replaced in tests. scrubGate is shared with the Suspend pipeline.
	scrubCfg  ScrubConfig
	scrubRun  func(ctx context.Context, opts *scrub.Options) (scrub.Result, error)
	scrubGate scrubGate
}

func newKiller() *killer {
	k := &killer{
		cgroups:   cgroup.New(cgroup.DefaultRoot),
		gpuPIDs:   nvmlGPUPIDs,
		poll:      killPollInterval,
		scrubGate: newScrubGate(),
	}
	k.scrubRun = func(ctx context.Context, opts *scrub.Options) (scrub.Result, error) {
		return k.scrubCfg.runScrubProcess(ctx, opts)
	}
	return k
}

// configure applies cfg. Called by StartServer before the server serves.
func (k *killer) configure(cfg *KillConfig) {
	k.scrubCfg = cfg.Scrub
}

// killTarget is one local pod of the job and its pod cgroup.
type killTarget struct {
	pod        *corev1.Pod
	cgroupPath string
	// procDirs are the cgroups whose processes must be gone: the
	// containers' cgroups (the pause sandbox is left out), or the whole
	// pod cgroup when no container cgroup is known yet.
	procDirs []string
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
// then polls for confirmation until the deadline. Under a scrubbing D-NS-7
// policy it then scrubs the GPUs the job held, still within the deadline.
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
	}
	// The GPUs to scrub are the ones NVML lists the job's processes on
	// before they die. Only a scrubbing policy pays for this query.
	var devices []string
	var devicesErr error
	if k.scrubCfg.scrubs() && len(seen) > 0 {
		devices, devicesErr = k.jobDevices(ctx, seen)
	}
	for _, target := range targets {
		if err := k.cgroups.Kill(target.cgroupPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
			pids, err := k.cgroups.Procs(target.procDirs...)
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
			if _, ok := listed[pid]; ok {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	gpuFree := time.Now()

	scrubbed, err := k.scrubAfterKill(ctx, jobID, devices, devicesErr)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Kill confirmed", "jobID", jobID, "pods", len(targets), "pids", len(seen),
		"killMs", killed.Sub(start).Milliseconds(),
		"exitMs", exited.Sub(killed).Milliseconds(),
		"gpuFreeMs", gpuFree.Sub(exited).Milliseconds(),
		"scrubMs", time.Since(gpuFree).Milliseconds(), "scrubbedGPUs", scrubbed,
		"totalMs", time.Since(start).Milliseconds())
	return nil
}

// jobDevices returns the sorted UUIDs of the GPUs NVML lists any of pids on.
func (k *killer) jobDevices(ctx context.Context, pids map[int]bool) ([]string, error) {
	listed, err := k.gpuPIDs(ctx)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for pid := range pids {
		for _, uuid := range listed[pid] {
			set[uuid] = true
		}
	}
	devices := make([]string, 0, len(set))
	for uuid := range set {
		devices = append(devices, uuid)
	}
	sort.Strings(devices)
	return devices, nil
}

// scrubAfterKill applies the D-NS-7 policy at the kill boundary, once the
// job's processes are gone and NVML no longer lists them, to each GPU the
// job held. It returns how many GPUs were scrubbed. Under keep it does
// nothing. When the pre-kill NVML query failed, the GPU is not known: it
// falls back to the only visible GPU, and fails on a node with more than
// one, so an unscrubbed GPU is never handed off as scrubbed. A scrub error
// fails the kill (KILL_UNCONFIRMED): the next tenant must not get a GPU the
// policy says to scrub before the scrub has run.
func (k *killer) scrubAfterKill(ctx context.Context, jobID string, devices []string, devicesErr error) (int, error) {
	if !k.scrubCfg.scrubs() {
		return 0, nil
	}
	if devicesErr != nil {
		slog.WarnContext(ctx, "GPUs of the killed job unknown; scrubbing the only visible GPU",
			"jobID", jobID, "error", devicesErr)
		devices = []string{""}
	}
	if len(devices) == 0 {
		slog.InfoContext(ctx, "Kill scrub skipped: NVML listed none of the job's processes", "jobID", jobID)
		return 0, nil
	}
	margin := k.scrubCfg.MarginMiB
	if margin == 0 {
		margin = scrub.DefaultMarginMiB
	}
	if err := k.scrubGate.acquire(ctx); err != nil {
		return 0, err
	}
	defer k.scrubGate.release()
	scrubbed := 0
	for _, uuid := range devices {
		res, err := k.scrubRun(ctx, &scrub.Options{
			Policy: k.scrubCfg.Policy, Mode: k.scrubCfg.Mode, Boundary: scrub.BoundaryKill,
			Allowlist: k.scrubCfg.Allowlist, GPUUUID: uuid, MarginMiB: margin,
		})
		if err != nil {
			return scrubbed, fmt.Errorf("scrub of GPU %q after the kill: %w", uuid, err)
		}
		if res.Decision == scrub.ActionScrub {
			scrubbed++
		}
	}
	return scrubbed, nil
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
		targets = append(targets, killTarget{pod: pod, cgroupPath: cgroupPath, procDirs: k.procDirs(cgroupPath, pod)})
	}
	return targets, nil
}

// procDirs returns the cgroups of the pod's containers found under
// cgroupPath. A container whose cgroup is gone has exited and is left out.
// When none is found (no container ID yet, or every container cgroup is
// gone), it returns the pod cgroup itself: its processes include the pause
// sandbox, which cgroup.kill kills too, and a removed pod cgroup counts as
// empty.
func (k *killer) procDirs(cgroupPath string, pod *corev1.Pod) []string {
	var dirs []string
	for _, id := range containerIDs(pod) {
		dir, err := k.cgroups.ContainerCgroupPath(cgroupPath, id)
		if err != nil {
			continue
		}
		dirs = append(dirs, dir)
	}
	if len(dirs) == 0 {
		return []string{cgroupPath}
	}
	return dirs
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
// process on any device, whatever memory it reports, with the UUIDs of the
// devices it is listed on.
func nvmlGPUPIDs(ctx context.Context) (gpuProcs, error) {
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
	listed := make(gpuProcs)
	for i := range count {
		device, ret := podutils.NvmlDeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("failed to get device %d: %v", i, nvml.ErrorString(ret))
		}
		uuid := deviceUUID(device)
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
				pid := int(proc.Pid)
				if !containsString(listed[pid], uuid) {
					listed[pid] = append(listed[pid], uuid)
				}
			}
		}
	}
	return listed, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// deviceUUID returns the device's NVML UUID, or "" when it cannot be read.
// "" makes a scrub target the only visible GPU, and fail when there are
// more; the kill confirmation itself never depends on the UUID.
func deviceUUID(device podutils.DeviceInterface) string {
	withUUID, ok := device.(interface{ GetUUID() (string, nvml.Return) })
	if !ok {
		return ""
	}
	uuid, ret := withUUID.GetUUID()
	if ret != nvml.SUCCESS {
		return ""
	}
	return uuid
}
