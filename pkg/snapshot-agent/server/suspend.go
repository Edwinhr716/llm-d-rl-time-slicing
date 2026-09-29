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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Constants of the Suspend and Resume pipelines. They stay constants, not
// flags, until they are measured.
const (
	// memoryHeadroom is the factor applied to a guest's device bytes in
	// the memory precondition: the pod cgroup must have room for the
	// checkpointed device memory plus 10%.
	memoryHeadroom = 1.1
	// suspendMargin is the slack planBudget keeps before the deadline.
	suspendMargin = time.Second
	// checkpointPerGB estimates a first checkpoint of a job with no
	// measured one yet (12.5-13.3 s for 22.9 GB on L4).
	checkpointPerGB = 600 * time.Millisecond
	// freezeEstimate and verifyEstimate are the budget for the cgroup
	// freeze and the NVML verify.
	freezeEstimate = 500 * time.Millisecond
	verifyEstimate = 500 * time.Millisecond
	// scrubEstimate is the budget for one VRAM scrub of one GPU (context,
	// allocate, memset, read back, free) when the scrub policy scrubs.
	scrubEstimate = 2 * time.Second
)

// GuestConfig configures the Suspend and Resume pipelines.
type GuestConfig struct {
	// Scrub is the VRAM handling at the Suspend handoff (D-NS-7).
	Scrub ScrubConfig
	// CgroupRoot is the host cgroup v2 mount; empty means /sys/fs/cgroup.
	CgroupRoot string
}

// ScrubConfig is the VRAM handling at a GPU handoff, both at the Suspend
// boundary and after a confirmed Kill. PENDING LEAD DECISION D-NS-7: the
// policy is decided in scrub.Decide. Under keep (the default and the zero
// value) a GPU off the allowlist refuses Suspend with PRECONDITION_NODE and
// nothing is scrubbed; under ns-scrub every GPU the guest held VRAM on is
// scrubbed after the guest is checkpointed, frozen and verified, before the
// operation reports SUSPENDED, and after a Kill is confirmed. Both
// boundaries run the scrub through runScrubProcess, one at a time per node
// (scrubGate).
type ScrubConfig struct {
	Policy    scrub.Policy
	Mode      scrub.Mode
	Allowlist scrub.Allowlist
	// MarginMiB is the VRAM left unscrubbed so the scrub's allocation fits;
	// 0 means scrub.DefaultMarginMiB.
	MarginMiB uint64
	// Command is the binary run as "<Command> scrub ..."; empty means the
	// agent's own binary.
	Command string
}

// scrubs reports whether the policy can ever scrub. Under keep the Kill
// path does no scrub work at all, not even the NVML query that finds the
// job's GPUs.
func (c *ScrubConfig) scrubs() bool {
	return c.Policy == scrub.PolicyNsScrub || c.Policy == scrub.PolicyFlag
}

// scrubGate lets one VRAM scrub run on the node at a time, whichever
// boundary (Suspend or Kill) runs it. Waiting honours the caller's context,
// so a Kill never waits past its deadline behind a Suspend's scrub.
type scrubGate chan struct{}

func newScrubGate() scrubGate { return make(scrubGate, 1) }

func (g scrubGate) acquire(ctx context.Context) error {
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the node's scrub slot: %w", ctx.Err())
	}
}

func (g scrubGate) release() { <-g }

// scrubFunc scrubs the GPU with the given NVML UUID at the Suspend boundary.
type scrubFunc func(ctx context.Context, gpuUUID string) (scrub.Result, error)

// guestBackend is the part of the cuda-checkpoint backend the pipelines use.
type guestBackend interface {
	Available() error
	GetState(ctx context.Context, pid int) (string, error)
	GuestCheckpoint(ctx context.Context, pids []int, alreadyLocked map[int]bool, beforeRun func() error) error
	GuestRestore(ctx context.Context, pids []int, states map[int]string) error
}

// gpuDevice is one GPU as NVML reports it.
type gpuDevice struct {
	Name          string
	DriverVersion string
	UUID          string
	// Minor is the device minor number (/dev/nvidia<Minor>), or -1 when
	// NVML does not report it.
	Minor int
}

// gpuProcess is one process NVML lists on a GPU. UsedBytes is
// nvmlNotAvailable when the driver does not report it.
type gpuProcess struct {
	PID       int
	UsedBytes uint64
	// GPUUUID is the NVML UUID of the GPU the process is listed on.
	GPUUUID string
}

// nvmlNotAvailable is NVML's VALUE_NOT_AVAILABLE in an unsigned field.
const nvmlNotAvailable = ^uint64(0)

// gpuInspector queries NVML.
type gpuInspector interface {
	Devices() ([]gpuDevice, error)
	// Processes lists the compute and graphics processes of every GPU.
	Processes() ([]gpuProcess, error)
}

// guestPipeline runs the Suspend and Resume pipelines of guest jobs.
type guestPipeline struct {
	// mirror returns the job's mirror pod on this node from the watcher's
	// cache.
	mirror func(jobID string) (*corev1.Pod, bool)
	// getPod reads one pod from the API server.
	getPod   func(ctx context.Context, namespace, name string) (*corev1.Pod, error)
	cgroups  *cgroup.Manager
	backend  guestBackend
	gpu      gpuInspector
	scrubCfg ScrubConfig
	// scrub runs one VRAM scrub; scrubGate serializes scrubs node-wide
	// (shared with Kill), so that two guests suspended together, or a
	// Suspend and a Kill, never scrub a GPU at once.
	scrub     scrubFunc
	scrubGate scrubGate
	// procRoot is where /proc is mounted, to find a checkpointed guest's
	// GPUs from the device nodes in its mount namespace; empty turns that
	// off.
	procRoot string
	now      func() time.Time

	mu      sync.Mutex
	records map[string]*guestRecord
}

// guestRecord is what the pipelines remember about a job between calls.
type guestRecord struct {
	lastCheckpoint time.Duration
	deviceBytes    int64
}

func (g *guestPipeline) record(jobID string) guestRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r, ok := g.records[jobID]; ok {
		return *r
	}
	return guestRecord{}
}

func (g *guestPipeline) update(jobID string, f func(*guestRecord)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.records == nil {
		g.records = map[string]*guestRecord{}
	}
	r, ok := g.records[jobID]
	if !ok {
		r = &guestRecord{}
		g.records[jobID] = r
	}
	f(r)
}

// guestTarget is a job's mirror pod and its cgroups on this node.
type guestTarget struct {
	pod        *corev1.Pod
	podDir     string
	containers []string
}

// errGuestGone means the job has no mirror pod or pod cgroup on this node.
var errGuestGone = errors.New("guest is gone")

// resolve finds the job's mirror pod and cgroups.
func (g *guestPipeline) resolve(jobID string) (*guestTarget, error) {
	pod, ok := g.mirror(jobID)
	if !ok {
		return nil, fmt.Errorf("no mirror pod for job %s on this node: %w", jobID, errGuestGone)
	}
	podDir, err := g.cgroups.PodCgroupPath(string(pod.UID))
	if errors.Is(err, cgroup.ErrNotFound) {
		return nil, fmt.Errorf("pod %s/%s: %w: %w", pod.Namespace, pod.Name, errGuestGone, err)
	}
	if err != nil {
		return nil, err
	}
	t := &guestTarget{pod: pod, podDir: podDir}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.ContainerID == "" {
			continue
		}
		dir, err := g.cgroups.ContainerCgroupPath(podDir, cs.ContainerID)
		if errors.Is(err, cgroup.ErrNotFound) {
			continue // the container exited and its cgroup is gone
		}
		if err != nil {
			return nil, err
		}
		t.containers = append(t.containers, dir)
	}
	return t, nil
}

func (t *guestTarget) procs(m *cgroup.Manager) ([]int, error) {
	if len(t.containers) == 0 {
		return nil, nil
	}
	return m.Procs(t.containers...)
}

// Suspend makes a guest job give up its GPU: preconditions, deadline
// budget, cuda-checkpoint (lock + checkpoint), cgroup freeze, verify. It
// returns an operation ID at once; poll GetOperation for the outcome.
func (s *Server) Suspend(ctx context.Context, req *pb.SuspendRequest) (*pb.SuspendResponse, error) {
	ctx = logging.WithServerMethod(ctx, "Suspend")
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if s.guest == nil {
		return nil, status.Error(codes.FailedPrecondition, "guest pipelines are not configured on this agent")
	}
	deadline := timestampTime(req.GetDeadline())
	slog.InfoContext(ctx, "Suspend called", "jobID", req.GetJobId(), "epoch", req.GetEpoch(), "deadline", deadline)
	worker := func(wctx context.Context) (sm.GuestResult, error) {
		return s.guest.suspend(wctx, req.GetJobId(), deadline)
	}
	// The operation outlives the RPC: it runs under its own deadline context.
	opID, err := s.state.StartGuestOp( //nolint:contextcheck // outlives the RPC by design
		req.GetJobId(), sm.OpTypeSuspend, req.GetEpoch(), deadline, worker)
	if err != nil {
		return nil, err
	}
	return &pb.SuspendResponse{OperationId: opID}, nil
}

// Resume gives a suspended guest job its GPU back: thaw, cuda-checkpoint
// restore + unlock, verify. It returns an operation ID at once.
func (s *Server) Resume(ctx context.Context, req *pb.ResumeRequest) (*pb.ResumeResponse, error) {
	ctx = logging.WithServerMethod(ctx, "Resume")
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if s.guest == nil {
		return nil, status.Error(codes.FailedPrecondition, "guest pipelines are not configured on this agent")
	}
	deadline := timestampTime(req.GetDeadline())
	slog.InfoContext(ctx, "Resume called", "jobID", req.GetJobId(), "epoch", req.GetEpoch(), "deadline", deadline)
	worker := func(wctx context.Context) (sm.GuestResult, error) {
		return s.guest.resume(wctx, req.GetJobId())
	}
	// The operation outlives the RPC: it runs under its own deadline context.
	opID, err := s.state.StartGuestOp( //nolint:contextcheck // outlives the RPC by design
		req.GetJobId(), sm.OpTypeResume, req.GetEpoch(), deadline, worker)
	if err != nil {
		return nil, err
	}
	return &pb.ResumeResponse{OperationId: opID}, nil
}

// SuspendAll suspends every job of one role on this node (D-NS-5 ns-host):
// the state machine lists the targets from the watcher's pod cache when the
// call arrives, fences the call per role, and runs the same Suspend pipeline
// as Suspend on each target in parallel. It returns one operation ID at
// once; GetOperation reports one result per target.
func (s *Server) SuspendAll(ctx context.Context, req *pb.SuspendAllRequest) (*pb.SuspendAllResponse, error) {
	ctx = logging.WithServerMethod(ctx, "SuspendAll")
	if s.guest == nil {
		return nil, status.Error(codes.FailedPrecondition, "guest pipelines are not configured on this agent")
	}
	deadline := timestampTime(req.GetDeadline())
	slog.InfoContext(ctx, "SuspendAll called", "role", req.GetRole(), "epoch", req.GetEpoch(), "deadline", deadline)
	workerFor := func(jobID string) sm.GuestWorker {
		return func(wctx context.Context) (sm.GuestResult, error) {
			return s.guest.suspend(wctx, jobID, deadline)
		}
	}
	// The operation outlives the RPC: each target runs under its own
	// deadline context.
	opID, err := s.state.StartHostOp( //nolint:contextcheck // outlives the RPC by design
		req.GetRole(), sm.OpTypeSuspend, req.GetEpoch(), deadline, workerFor)
	if err != nil {
		return nil, err
	}
	return &pb.SuspendAllResponse{OperationId: opID}, nil
}

// ResumeAll resumes every job of one role on this node, found and fenced as
// for SuspendAll, with the same Resume pipeline as Resume on each target.
func (s *Server) ResumeAll(ctx context.Context, req *pb.ResumeAllRequest) (*pb.ResumeAllResponse, error) {
	ctx = logging.WithServerMethod(ctx, "ResumeAll")
	if s.guest == nil {
		return nil, status.Error(codes.FailedPrecondition, "guest pipelines are not configured on this agent")
	}
	deadline := timestampTime(req.GetDeadline())
	slog.InfoContext(ctx, "ResumeAll called", "role", req.GetRole(), "epoch", req.GetEpoch(), "deadline", deadline)
	workerFor := func(jobID string) sm.GuestWorker {
		return func(wctx context.Context) (sm.GuestResult, error) {
			return s.guest.resume(wctx, jobID)
		}
	}
	// The operation outlives the RPC: each target runs under its own
	// deadline context.
	opID, err := s.state.StartHostOp( //nolint:contextcheck // outlives the RPC by design
		req.GetRole(), sm.OpTypeResume, req.GetEpoch(), deadline, workerFor)
	if err != nil {
		return nil, err
	}
	return &pb.ResumeAllResponse{OperationId: opID}, nil
}

// suspend is the Suspend pipeline.
func (g *guestPipeline) suspend(ctx context.Context, jobID string, deadline time.Time) (sm.GuestResult, error) {
	released := sm.GuestResult{Outcome: pb.Outcome_OUTCOME_RELEASED}
	target, err := g.resolve(jobID)
	if errors.Is(err, errGuestGone) {
		slog.InfoContext(ctx, "Suspend: guest is gone; released", "jobID", jobID, "reason", err)
		return released, nil
	}
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	procs, err := target.procs(g.cgroups)
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	procSet := toSet(procs)

	vram, err := g.deviceBytes(procSet)
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	deviceBytes, guestGPUs := vram.bytes, vram.gpus
	scrubGPUs, err := g.checkPreconditions(ctx, target, deviceBytes)
	if err != nil {
		return sm.GuestResult{}, err
	}
	// A guest that is already checkpointed (a re-issued Suspend, or one
	// after a restart) holds no VRAM, so NVML lists none of its GPUs: they
	// come from its device nodes instead, those no process is on.
	if len(scrubGPUs) > 0 && len(guestGPUs) == 0 {
		if guestGPUs, err = g.idleGuestGPUs(procs); err != nil {
			return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
		}
	}
	// Only the GPUs the guest held VRAM on are scrubbed: another GPU may
	// belong to a tenant that is still running.
	scrubGPUs = intersect(scrubGPUs, guestGPUs)

	frozen, err := g.cgroups.Frozen(target.podDir)
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	estimate := g.estimate(jobID, deviceBytes, frozen, len(scrubGPUs))
	if err := g.checkBudget(deadline, estimate); err != nil {
		return sm.GuestResult{}, err
	}
	if len(procs) == 0 {
		slog.InfoContext(ctx, "Suspend: no process left; released", "jobID", jobID)
		return released, nil
	}

	if !frozen {
		if err := g.ensureCheckpointed(ctx, jobID, procs, deviceBytes, len(scrubGPUs), deadline); err != nil {
			return sm.GuestResult{}, err
		}
	}
	if err := g.cgroups.Freeze(ctx, target.podDir); err != nil {
		return sm.GuestResult{}, backendError(ctx, fmt.Errorf("freeze: %w", err))
	}
	hostBytes, err := g.verifySuspended(target, procSet)
	if err != nil {
		return sm.GuestResult{}, err
	}
	if err := g.scrubFreed(ctx, jobID, scrubGPUs); err != nil {
		return sm.GuestResult{}, err
	}
	if deviceBytes > 0 {
		g.update(jobID, func(r *guestRecord) { r.deviceBytes = deviceBytes })
	}
	return sm.GuestResult{
		Outcome:         pb.Outcome_OUTCOME_SUSPENDED,
		DeviceBytes:     g.record(jobID).deviceBytes,
		HostBytesPinned: hostBytes,
	}, nil
}

// checkPreconditions checks readiness, probes, memory and node. A failure
// fails the operation with the precondition's reason, and the job is left
// FAULTED. It returns the UUIDs of the GPUs the scrub policy scrubs at this
// Suspend.
func (g *guestPipeline) checkPreconditions(ctx context.Context, t *guestTarget, deviceBytes int64) (map[string]bool, error) {
	if err := g.checkReadiness(ctx, t.pod); err != nil {
		return nil, sm.NewOpError(pb.ErrorReason_PRECONDITION_READINESS, err)
	}
	if err := checkProbes(t.pod); err != nil {
		return nil, sm.NewOpError(pb.ErrorReason_PRECONDITION_PROBES, err)
	}
	if err := g.checkMemory(t.podDir, deviceBytes); err != nil {
		return nil, sm.NewOpError(pb.ErrorReason_PRECONDITION_MEMORY, err)
	}
	scrubGPUs, err := g.checkNode()
	if err != nil {
		return nil, sm.NewOpError(pb.ErrorReason_PRECONDITION_NODE, err)
	}
	return scrubGPUs, nil
}

// checkReadiness reads the owner guest pod once: it must not be Ready, so
// that no traffic is routed to the guest while it is frozen. A guest pod
// that no longer exists receives no traffic either.
func (g *guestPipeline) checkReadiness(ctx context.Context, mirror *corev1.Pod) error {
	owner := ownerPod(mirror)
	if owner == "" {
		return fmt.Errorf("mirror %s/%s has no owner pod", mirror.Namespace, mirror.Name)
	}
	pod, err := g.getPod(ctx, mirror.Namespace, owner)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get guest pod %s/%s: %w", mirror.Namespace, owner, err)
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return fmt.Errorf("guest pod %s/%s is Ready; it must be made NotReady before Suspend", pod.Namespace, pod.Name)
		}
	}
	return nil
}

// ownerPod returns the name of the pod that owns mirror, preferring the
// controller reference.
func ownerPod(mirror *corev1.Pod) string {
	name := ""
	for _, ref := range mirror.OwnerReferences {
		if ref.Kind != "Pod" {
			continue
		}
		if ref.Controller != nil && *ref.Controller {
			return ref.Name
		}
		if name == "" {
			name = ref.Name
		}
	}
	return name
}

// checkProbes refuses a mirror with probes or readiness gates: the kubelet
// would act on a frozen guest's failed probes.
func checkProbes(pod *corev1.Pod) error {
	if len(pod.Spec.ReadinessGates) > 0 {
		return fmt.Errorf("mirror %s/%s has readinessGates", pod.Namespace, pod.Name)
	}
	containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	for i := range containers {
		c := &containers[i]
		if c.LivenessProbe != nil || c.ReadinessProbe != nil || c.StartupProbe != nil {
			return fmt.Errorf("mirror %s/%s container %s has probes", pod.Namespace, pod.Name, c.Name)
		}
	}
	return nil
}

// checkMemory requires room in the pod cgroup for the device memory that
// cuda-checkpoint moves to host memory, plus headroom. A pod without a
// memory limit is refused: its checkpoint could push the node into OOM.
func (g *guestPipeline) checkMemory(podDir string, deviceBytes int64) error {
	if deviceBytes <= 0 {
		return nil
	}
	limit, unlimited, err := g.cgroups.MemoryMax(podDir)
	if err != nil {
		return fmt.Errorf("read memory.max: %w", err)
	}
	if unlimited {
		return errors.New("pod cgroup has no memory limit (memory.max is max)")
	}
	current, err := g.cgroups.MemoryCurrent(podDir)
	if err != nil {
		return fmt.Errorf("read memory.current: %w", err)
	}
	need := int64(float64(deviceBytes) * memoryHeadroom)
	if free := limit - current; free < need {
		return fmt.Errorf("pod cgroup has %d bytes free (memory.max %d - memory.current %d), need %d (device bytes %d x %.1f)",
			free, limit, current, need, deviceBytes, memoryHeadroom)
	}
	return nil
}

// checkNode requires cgroup v2 and the cuda-checkpoint binary, and asks the
// scrub policy (scrub.Decide at the Suspend boundary) about every GPU: a
// refusal (keep, on a GPU off --vram-zeroing-qualified) fails the
// precondition. It returns the UUIDs of the GPUs the policy scrubs.
func (g *guestPipeline) checkNode() (map[string]bool, error) {
	if !g.cgroups.IsV2() {
		return nil, fmt.Errorf("%s is not a cgroup v2 hierarchy", g.cgroups.Root)
	}
	if err := g.backend.Available(); err != nil {
		return nil, err
	}
	return g.policyScrubGPUs()
}

// policyScrubGPUs asks the scrub policy (scrub.Decide at the Suspend
// boundary) about every GPU and returns the UUIDs of those it scrubs; a
// refusal is an error.
func (g *guestPipeline) policyScrubGPUs() (map[string]bool, error) {
	devices, err := g.gpu.Devices()
	if err != nil {
		return nil, fmt.Errorf("query GPUs: %w", err)
	}
	if len(devices) == 0 {
		return nil, errors.New("no GPU on this node")
	}
	scrubGPUs := map[string]bool{}
	for _, d := range devices {
		qualified := g.scrubCfg.Allowlist.Qualified(d.Name, d.DriverVersion)
		switch decision := scrub.Decide(g.scrubCfg.Policy, g.scrubCfg.Mode, scrub.BoundarySuspend, qualified); decision.Action {
		case scrub.ActionRefuse:
			return nil, fmt.Errorf("GPU %q with driver %s is not on --vram-zeroing-qualified (scrub policy %s)",
				d.Name, d.DriverVersion, g.scrubCfg.Policy)
		case scrub.ActionScrub:
			scrubGPUs[d.UUID] = true
		}
	}
	return scrubGPUs, nil
}

// scrubFreed scrubs the VRAM the checkpointed guest freed on each GPU, one
// GPU at a time, after verify and before the operation reports SUSPENDED:
// the next tenant is only restored once the guest counts as vacated, so it
// never races the scrub. A scrub that fails fails the Suspend; one whose read
// back finds a nonzero word fails verify.
func (g *guestPipeline) scrubFreed(ctx context.Context, jobID string, gpus map[string]bool) error {
	if len(gpus) == 0 {
		return nil
	}
	uuids := make([]string, 0, len(gpus))
	for u := range gpus {
		uuids = append(uuids, u)
	}
	sort.Strings(uuids)
	if err := g.scrubGate.acquire(ctx); err != nil {
		return backendError(ctx, err)
	}
	defer g.scrubGate.release()
	for _, u := range uuids {
		res, err := g.scrub(ctx, u)
		if err != nil {
			return backendError(ctx, fmt.Errorf("scrub GPU %s: %w", u, err))
		}
		if res.ReadbackNonzeroWords > 0 {
			return sm.NewOpError(pb.ErrorReason_VERIFY_FAILED,
				fmt.Errorf("scrub GPU %s: read back %d nonzero words", u, res.ReadbackNonzeroWords))
		}
		slog.InfoContext(ctx, "Suspend: scrubbed", "jobID", jobID, "gpu", u, "decision", res.Decision,
			"bytes", res.BytesScrubbed, "coverage", res.Coverage, "ms", res.TTotalMs)
	}
	return nil
}

// scrubJSONPrefix starts the one result line of "snapshot-agent scrub".
const scrubJSONPrefix = "SCRUBJSON "

// runScrub is the default scrubFunc of the Suspend pipeline: one scrub at
// the Suspend boundary through runScrubProcess.
func (c *ScrubConfig) runScrub(ctx context.Context, gpuUUID string) (scrub.Result, error) {
	return c.runScrubProcess(ctx, &scrub.Options{
		Policy: c.Policy, Mode: c.Mode, Boundary: scrub.BoundarySuspend,
		Allowlist: c.Allowlist, GPUUUID: gpuUUID, MarginMiB: c.MarginMiB,
	})
}

// runScrubProcess is the agent's only scrub path, used at the Suspend and
// the Kill boundary. It runs "snapshot-agent scrub" as a child process, so
// that the scrub's CUDA context lives and dies with that process and never
// stays in the agent, and reads its SCRUBJSON line. opts gives the policy,
// boundary, GPU and margin; c gives the binary.
func (c *ScrubConfig) runScrubProcess(ctx context.Context, opts *scrub.Options) (scrub.Result, error) {
	bin := c.Command
	if bin == "" {
		exe, err := os.Executable()
		if err != nil {
			return scrub.Result{}, fmt.Errorf("find the agent binary: %w", err)
		}
		bin = exe
	}
	margin := opts.MarginMiB
	if margin == 0 {
		margin = scrub.DefaultMarginMiB
	}
	args := []string{
		"scrub", "--boundary=" + string(opts.Boundary),
		"--scrub-policy=" + string(opts.Policy), "--scrub=" + string(opts.Mode),
		"--vram-zeroing-qualified=" + opts.Allowlist.String(),
		"--gpu-uuid=" + opts.GPUUUID, "--margin-mib=" + strconv.FormatUint(margin, 10),
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, runErr := cmd.Output()
	res, parseErr := parseScrubJSON(out)
	switch {
	case parseErr != nil && runErr != nil:
		return res, fmt.Errorf("%w: %s", runErr, strings.TrimSpace(stderr.String()))
	case parseErr != nil:
		return res, parseErr
	case res.Error != "":
		return res, errors.New(res.Error)
	case runErr != nil:
		return res, fmt.Errorf("%w (decision %s)", runErr, res.Decision)
	}
	return res, nil
}

// parseScrubJSON reads the SCRUBJSON line of "snapshot-agent scrub".
func parseScrubJSON(out []byte) (scrub.Result, error) {
	for _, line := range strings.Split(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, scrubJSONPrefix); ok {
			var res scrub.Result
			if err := json.Unmarshal([]byte(rest), &res); err != nil {
				return scrub.Result{}, fmt.Errorf("decode SCRUBJSON: %w", err)
			}
			return res, nil
		}
	}
	return scrub.Result{}, errors.New("scrub printed no SCRUBJSON line")
}

func intersect(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		if b[k] {
			out[k] = true
		}
	}
	return out
}

// estimate is the time a Suspend still needs: checkpoint (the job's last
// measured checkpoint, or a per-GB estimate), freeze, verify, one scrub per
// GPU to scrub and a margin. A frozen guest is already checkpointed.
func (g *guestPipeline) estimate(jobID string, deviceBytes int64, frozen bool, scrubs int) time.Duration {
	est := freezeEstimate + verifyEstimate + suspendMargin + time.Duration(scrubs)*scrubEstimate
	if frozen {
		return est
	}
	if last := g.record(jobID).lastCheckpoint; last > 0 {
		return est + last
	}
	return est + time.Duration(float64(deviceBytes)/(1<<30)*float64(checkpointPerGB))
}

// checkBudget refuses with DEADLINE_INFEASIBLE when the estimate does not
// fit before the deadline.
func (g *guestPipeline) checkBudget(deadline time.Time, estimate time.Duration) error {
	if left := deadline.Sub(g.now()); left < estimate {
		return sm.NewOpError(pb.ErrorReason_DEADLINE_INFEASIBLE,
			fmt.Errorf("%s left before the deadline, suspend needs about %s", left.Round(time.Millisecond), estimate))
	}
	return nil
}

// ensureCheckpointed locks and checkpoints the guest's CUDA processes.
// Processes without a CUDA context are skipped; checkpointed ones are left
// alone. The budget is checked again once the node lock is held, since the
// wait for it may have used the time.
func (g *guestPipeline) ensureCheckpointed(
	ctx context.Context, jobID string, procs []int, deviceBytes int64, scrubs int, deadline time.Time,
) error {
	var targets []int
	locked := map[int]bool{}
	for _, pid := range procs {
		state, err := g.backend.GetState(ctx, pid)
		if errors.Is(err, backends.ErrNotCudaProcess) {
			continue
		}
		if err != nil {
			return backendError(ctx, err)
		}
		switch state {
		case backends.CudaStateRunning:
			targets = append(targets, pid)
		case backends.CudaStateLocked:
			targets = append(targets, pid)
			locked[pid] = true
		case backends.CudaStateCheckpointed:
		default:
			return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, fmt.Errorf("pid %d is in cuda-checkpoint state %q", pid, state))
		}
	}
	if len(targets) == 0 {
		return nil
	}
	t0 := time.Time{}
	err := g.backend.GuestCheckpoint(ctx, targets, locked, func() error {
		// The estimate here is the checkpoint itself plus what follows.
		if err := g.checkBudget(deadline, g.estimate(jobID, deviceBytes, false, scrubs)); err != nil {
			return err
		}
		t0 = g.now()
		return nil
	})
	if err != nil {
		if sm.ErrorReasonOf(err) != pb.ErrorReason_ERROR_REASON_UNSPECIFIED {
			return err
		}
		return backendError(ctx, err)
	}
	took := g.now().Sub(t0)
	g.update(jobID, func(r *guestRecord) { r.lastCheckpoint = took })
	slog.InfoContext(ctx, "Suspend: checkpointed", "jobID", jobID, "pids", targets, "duration", took, "deviceBytes", deviceBytes)
	return nil
}

// verifySuspended checks with its own NVML query that no guest process
// holds VRAM ("not available" counts as holding VRAM) and that the pod
// cgroup is frozen. It returns the host bytes the frozen guest pins.
func (g *guestPipeline) verifySuspended(target *guestTarget, procSet map[int]bool) (int64, error) {
	gpuProcs, err := g.gpu.Processes()
	if err != nil {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("query GPU processes: %w", err))
	}
	for _, p := range gpuProcs {
		if procSet[p.PID] && p.UsedBytes > 0 {
			used := fmt.Sprint(p.UsedBytes)
			if p.UsedBytes == nvmlNotAvailable {
				used = "not available"
			}
			return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("guest pid %d still holds VRAM (%s)", p.PID, used))
		}
	}
	frozen, err := g.cgroups.Frozen(target.podDir)
	if err != nil {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("read pod cgroup freeze state: %w", err))
	}
	if !frozen {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, errors.New("pod cgroup is not frozen"))
	}
	stat, err := g.cgroups.MemoryStat(target.podDir)
	if err != nil {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("read memory.stat: %w", err))
	}
	return cgroup.HostBytes(stat), nil
}

// resume is the Resume pipeline: thaw first, then restore, then verify.
// Toggling a frozen process blocks, so the thaw must come first.
func (g *guestPipeline) resume(ctx context.Context, jobID string) (sm.GuestResult, error) {
	t, err := g.resolve(jobID)
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	if err := g.cgroups.Thaw(ctx, t.podDir); err != nil {
		return sm.GuestResult{}, backendError(ctx, fmt.Errorf("thaw: %w", err))
	}
	procs, err := t.procs(g.cgroups)
	if err != nil {
		return sm.GuestResult{}, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
	}
	states := map[int]string{}
	var cudaPIDs []int
	for _, pid := range procs {
		state, err := g.backend.GetState(ctx, pid)
		if errors.Is(err, backends.ErrNotCudaProcess) {
			continue
		}
		if err != nil {
			return sm.GuestResult{}, backendError(ctx, err)
		}
		states[pid] = state
		cudaPIDs = append(cudaPIDs, pid)
	}
	if len(cudaPIDs) > 0 {
		if err := g.backend.GuestRestore(ctx, cudaPIDs, states); err != nil {
			return sm.GuestResult{}, backendError(ctx, err)
		}
	}
	deviceBytes, err := g.verifyResumed(toSet(cudaPIDs))
	if err != nil {
		return sm.GuestResult{}, err
	}
	slog.InfoContext(ctx, "Resume: restored", "jobID", jobID, "pids", cudaPIDs, "deviceBytes", deviceBytes)
	return sm.GuestResult{DeviceBytes: deviceBytes}, nil
}

// verifyResumed checks that NVML lists a restored guest process again, and
// returns the device bytes the guest holds.
func (g *guestPipeline) verifyResumed(cudaSet map[int]bool) (int64, error) {
	if len(cudaSet) == 0 {
		return 0, nil
	}
	gpuProcs, err := g.gpu.Processes()
	if err != nil {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("query GPU processes: %w", err))
	}
	var total int64
	found := false
	for _, p := range gpuProcs {
		if !cudaSet[p.PID] {
			continue
		}
		found = true
		if p.UsedBytes != nvmlNotAvailable {
			total += int64(p.UsedBytes) //nolint:gosec // VRAM sizes fit in int64
		}
	}
	if !found {
		return 0, sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, errors.New("no restored guest process has a GPU context"))
	}
	return total, nil
}

// guestVRAM is the VRAM a guest's processes hold, and the UUIDs of the GPUs
// they hold it on.
type guestVRAM struct {
	bytes int64
	gpus  map[string]bool
}

// deviceBytes sums the VRAM NVML reports for the given processes, and
// records the GPUs they are listed on.
func (g *guestPipeline) deviceBytes(procSet map[int]bool) (guestVRAM, error) {
	vram := guestVRAM{gpus: map[string]bool{}}
	if len(procSet) == 0 {
		return vram, nil
	}
	gpuProcs, err := g.gpu.Processes()
	if err != nil {
		return guestVRAM{}, fmt.Errorf("query GPU processes: %w", err)
	}
	for _, p := range gpuProcs {
		if !procSet[p.PID] {
			continue
		}
		vram.gpus[p.GPUUUID] = true
		if p.UsedBytes != nvmlNotAvailable {
			vram.bytes += int64(p.UsedBytes) //nolint:gosec // VRAM sizes fit in int64
		}
	}
	return vram, nil
}

// backendError classifies a pipeline step failure: DEADLINE_EXCEEDED when
// the deadline passed, BACKEND_ERROR otherwise.
func backendError(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return sm.NewOpError(pb.ErrorReason_DEADLINE_EXCEEDED, err)
	}
	return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, err)
}

func toSet(pids []int) map[int]bool {
	set := make(map[int]bool, len(pids))
	for _, p := range pids {
		set[p] = true
	}
	return set
}

// deviceGPUs returns the NVML UUIDs of the GPUs whose device nodes
// (/dev/nvidia<minor>) are in the mount namespace of any of pids, read
// through <procRoot>/<pid>/root/dev. It finds the GPUs of a checkpointed
// guest, which NVML no longer lists; a frozen process can be read this way.
// A process that is gone is skipped.
func (g *guestPipeline) deviceGPUs(pids []int, devices []gpuDevice) (map[string]bool, error) {
	out := map[string]bool{}
	if g.procRoot == "" || len(pids) == 0 {
		return out, nil
	}
	byMinor := make(map[int]string, len(devices))
	for _, d := range devices {
		if d.Minor >= 0 {
			byMinor[d.Minor] = d.UUID
		}
	}
	for _, pid := range pids {
		entries, err := os.ReadDir(filepath.Join(g.procRoot, strconv.Itoa(pid), "root", "dev"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("list the device nodes of pid %d: %w", pid, err)
		}
		for _, e := range entries {
			minor, ok := strings.CutPrefix(e.Name(), "nvidia")
			if !ok {
				continue
			}
			n, err := strconv.Atoi(minor)
			if err != nil || n < 0 {
				continue // nvidiactl, nvidia-uvm, nvidia-caps, ...
			}
			if uuid, ok := byMinor[n]; ok {
				out[uuid] = true
			}
		}
	}
	return out, nil
}

// idleGuestGPUs returns the GPUs among the guest's device nodes on which
// NVML lists no process: once the guest is checkpointed, the GPUs its
// freed VRAM is on, and no other tenant's.
func (g *guestPipeline) idleGuestGPUs(pids []int) (map[string]bool, error) {
	if g.procRoot == "" || len(pids) == 0 {
		return map[string]bool{}, nil
	}
	devices, err := g.gpu.Devices()
	if err != nil {
		return nil, fmt.Errorf("query GPUs: %w", err)
	}
	gpus, err := g.deviceGPUs(pids, devices)
	if err != nil || len(gpus) == 0 {
		return gpus, err
	}
	procs, err := g.gpu.Processes()
	if err != nil {
		return nil, fmt.Errorf("query GPU processes: %w", err)
	}
	for _, p := range procs {
		delete(gpus, p.GPUUUID)
	}
	return gpus, nil
}
