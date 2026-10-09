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

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
)

// Application-aware donors. The orchestrator parks a donor (a trainer that
// lends its GPU) with a Snapshot that carries no backend config, and brings
// it back with a Restore. By default the agent's default backend
// (cuda-checkpoint) runs. A donor whose pods are annotated
// timeslice.io/backend: app_channel is parked by the trainer itself instead:
// the agent sends the command over the workload channel the trainer
// registered (the trainer moves its model and optimizer to host memory and
// frees its cached GPU memory), then verifies that the donor's processes
// keep at most donorResidualMaxBytes of VRAM (their CUDA context and
// allocator residue) before the GPU is lent. The optional annotation
// timeslice.io/backend-config holds an AppChannelConfig in protojson.
const (
	// BackendAnnotationAppChannel is the timeslice.io/backend value of an
	// application-aware donor.
	BackendAnnotationAppChannel = "app_channel"
	// donorResidualMaxBytes is the most VRAM a parked application-aware
	// donor may still hold at verify; the same bound as a sleeping guest.
	donorResidualMaxBytes = appResidualMaxBytes
	// donorChannelWait bounds the wait for the donor's workload channel: the
	// trainer registers it at start-up and re-registers after a reconnect,
	// so a park may arrive a moment before the registration does.
	donorChannelWait = 30 * time.Second
	donorChannelPoll = 100 * time.Millisecond
)

// donorAppConfig returns the app_channel config of an application-aware
// donor from its pods' annotations, or nil for a cuda-checkpointed donor (no
// pod, or no annotation, or timeslice.io/backend: cuda). The pods of a donor
// must agree. Terminated pods are skipped.
func donorAppConfig(pods []*corev1.Pod) (*pb.BackendConfig, error) {
	var kind, raw string
	var from *corev1.Pod
	for _, p := range pods {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		k, r := p.Annotations[utils.BackendAnnotation], p.Annotations[utils.BackendConfigAnnotation]
		if from != nil && (k != kind || r != raw) {
			return nil, fmt.Errorf("pods %s/%s and %s/%s of the job disagree on the %s and %s annotations",
				from.Namespace, from.Name, p.Namespace, p.Name, utils.BackendAnnotation, utils.BackendConfigAnnotation)
		}
		kind, raw, from = k, r, p
	}
	switch kind {
	case "", string(backends.BackendCuda):
		return nil, nil //nolint:nilnil // nil config means the default (cuda-checkpoint) backend
	case BackendAnnotationAppChannel:
	default:
		return nil, fmt.Errorf("the %s annotation is %q; a donor is parked with cuda or %s",
			utils.BackendAnnotation, kind, BackendAnnotationAppChannel)
	}
	cfg := &pb.AppChannelConfig{}
	if raw != "" {
		if err := protojson.Unmarshal([]byte(raw), cfg); err != nil {
			return nil, fmt.Errorf("parse the %s annotation as an app_channel config: %w", utils.BackendConfigAnnotation, err)
		}
	}
	if cfg.GetMode() == pb.SuspendMode_SUSPEND_MODE_DISCARD {
		return nil, errors.New(
			"mode SUSPEND_MODE_DISCARD drops the trainer's state, and a donor must restore; use SUSPEND_MODE_OFFLOAD")
	}
	return &pb.BackendConfig{Backend: &pb.BackendConfig_AppChannel{AppChannel: cfg}}, nil
}

// donorConfig picks the backend config of a Snapshot or Restore: the
// request's own config when it has one, else in k8s mode the app_channel
// config of an application-aware donor. It reports whether the result is an
// application-aware donor's (which the server verifies).
func (s *Server) donorConfig(jobID string, req *pb.BackendConfig) (*pb.BackendConfig, bool, error) {
	if req != nil || s.deploymentMode != "k8s" || s.donorPods == nil {
		return req, false, nil
	}
	cfg, err := donorAppConfig(s.donorPods(jobID))
	if err != nil {
		return nil, false, err
	}
	return cfg, cfg != nil, nil
}

// donorBytes sums the VRAM NVML lists for the donor's processes on this
// node.
//
//nolint:gocritic // The project configuration bans named returns, conflicting with unnamedResult
func (s *Server) donorBytes(ctx context.Context, jobID string) (int64, []int, error) {
	pids, err := s.donorPIDs(ctx, jobID)
	if errors.Is(err, sm.ErrNoLiveProcesses) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("resolve the donor's processes: %w", err)
	}
	procs, err := s.donorGPU.Processes()
	if err != nil {
		return 0, pids, fmt.Errorf("list GPU processes: %w", err)
	}
	set := toSet(pids)
	var total int64
	for _, p := range procs {
		if !set[p.PID] || p.UsedBytes == 0 {
			continue
		}
		if p.UsedBytes == nvmlNotAvailable {
			return 0, pids, fmt.Errorf("donor pid %d holds VRAM, but NVML does not report how much", p.PID)
		}
		total += int64(p.UsedBytes) //nolint:gosec // VRAM sizes fit in int64
	}
	return total, pids, nil
}

// waitDonorChannel waits up to timeout for the donor's workload channel.
func (s *Server) waitDonorChannel(ctx context.Context, jobID string, timeout time.Duration) error {
	if s.channelRegistry == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		_, err := s.channelRegistry.Session(jobID)
		if err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w (waited %s)", err, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(donorChannelPoll):
		}
	}
}

// donorSnapshotFn parks an application-aware donor through its workload
// channel, then verifies its residual VRAM. A donor above the bound fails
// the Snapshot (VERIFY_FAILED, the job faults): the GPU is not lent.
func (s *Server) donorSnapshotFn(
	ctx context.Context, jobID string, backend backends.Backend, cfg *pb.BackendConfig,
) func() error {
	return func() error {
		start := time.Now()
		before, _, beforeErr := s.donorBytes(ctx, jobID)
		if beforeErr != nil {
			slog.WarnContext(ctx, "Snapshot: cannot read the donor's VRAM before the park", "error", beforeErr)
		}
		if err := s.waitDonorChannel(ctx, jobID, s.donorChannelWait); err != nil {
			return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, fmt.Errorf("donor %s: %w", jobID, err))
		}
		if err := backend.Snapshot(ctx, backends.Request{JobID: jobID, Config: cfg}); err != nil {
			return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, fmt.Errorf("park donor %s: %w", jobID, err))
		}
		parked := time.Since(start)
		residual, pids, err := s.donorBytes(ctx, jobID)
		if err != nil {
			return sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf("verify donor %s: %w", jobID, err))
		}
		if residual > donorResidualMaxBytes {
			return sm.NewOpError(pb.ErrorReason_VERIFY_FAILED, fmt.Errorf(
				"donor %s still holds %d bytes of VRAM after the park, more than the %d a parked application may keep",
				jobID, residual, donorResidualMaxBytes))
		}
		if len(pids) > 0 {
			s.state.UpdateJobPIDs(jobID, pids)
		}
		slog.InfoContext(ctx, "Snapshot: app donor parked",
			"jobID", jobID, "duration", time.Since(start), "parkDuration", parked,
			"deviceBytes", before, "residualBytes", residual, "pids", pids)
		return nil
	}
}

// donorRestoreFn brings an application-aware donor back through its
// workload channel.
func (s *Server) donorRestoreFn(
	ctx context.Context, jobID string, backend backends.Backend, cfg *pb.BackendConfig,
) func() error {
	return func() error {
		start := time.Now()
		if err := s.waitDonorChannel(ctx, jobID, s.donorChannelWait); err != nil {
			return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, fmt.Errorf("donor %s: %w", jobID, err))
		}
		if err := backend.Restore(ctx, backends.Request{JobID: jobID, Config: cfg}); err != nil {
			return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, fmt.Errorf("restore donor %s: %w", jobID, err))
		}
		restored := time.Since(start)
		device, _, err := s.donorBytes(ctx, jobID)
		if err != nil {
			slog.WarnContext(ctx, "Restore: cannot read the donor's VRAM after the restore", "error", err)
		}
		slog.InfoContext(ctx, "Restore: app donor restored",
			"jobID", jobID, "duration", time.Since(start), "restoreDuration", restored, "deviceBytes", device)
		return nil
	}
}

// defaultDonorPIDs resolves a donor's processes from its local pods.
func defaultDonorPIDs(ctx context.Context, jobID string) ([]int, error) {
	pids, _, err := resolvePIDs(ctx, jobID, nil)
	return pids, err
}
