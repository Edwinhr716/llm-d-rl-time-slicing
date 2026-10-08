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
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
)

// Application-aware guests. A guest pod annotated
// timeslice.io/backend: app_endpoint (copied onto its mirror by the guest
// kubelet) is suspended through the application's own API instead of
// cuda-checkpoint: the Suspend pipeline drains the guest's in-flight
// connections, drops its prefix cache, puts it to sleep (vLLM sleep level 1:
// weights offloaded to host memory, KV cache dropped) and confirms the sleep,
// then freezes and verifies it as for any guest. The process keeps its CUDA
// context and a small residual allocation, so verify accepts up to
// appResidualMaxBytes of guest VRAM where a checkpointed guest must hold
// none. Resume thaws, wakes the application and verifies. The backend is
// chosen per guest, so one agent serves cuda-checkpointed trainers and
// application-aware guests on the same node.
const (
	// appResidualMaxBytes is the most VRAM a sleeping application guest may
	// still hold at verify (its CUDA context and allocator residue).
	appResidualMaxBytes = int64(4) << 30
	// appDrainTimeout bounds the drain of a guest's in-flight connections
	// before the sleep; appDrainPoll is the pause between two aborts, and
	// appDrainSettle the pause after the last abort that reset anything, so
	// that the application has dropped the aborted requests before it
	// sleeps.
	appDrainTimeout = 2 * time.Second
	appDrainPoll    = 100 * time.Millisecond
	appDrainSettle  = 500 * time.Millisecond
	// appHTTPTimeout bounds one call to a guest's API (a sleep offloads the
	// weights, which takes seconds).
	appHTTPTimeout = 60 * time.Second
)

// appGuestBackend is the part of the app-endpoint backend the pipelines use.
type appGuestBackend interface {
	Snapshot(ctx context.Context, req backends.Request) error
	Restore(ctx context.Context, req backends.Request) error
	IsSuspended(ctx context.Context, config *pb.BackendConfig) (bool, error)
	ResetCache(ctx context.Context, config *pb.BackendConfig) error
}

// newAppGuestBackend returns the app-endpoint backend of the guest
// pipelines. Its client keeps no idle connection: the Suspend pipeline resets
// every connection into the guest's serving port after the freeze, and a
// reused connection would fail the next call.
func newAppGuestBackend() *backends.AppEndpointBackend {
	transport := &http.Transport{}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	}
	transport.DisableKeepAlives = true
	transport.Proxy = nil
	return backends.NewAppEndpointBackendWithClient(&http.Client{Timeout: appHTTPTimeout, Transport: transport})
}

// guestAppConfig returns the app-endpoint config of an application-aware
// guest from its mirror's annotations, or nil for a cuda-checkpointed guest.
// It fills the defaults (vLLM, OFFLOAD) and points the endpoints at the
// mirror's pod IP: the agent runs in the host network, where localhost is
// not the guest.
func (g *guestPipeline) guestAppConfig(ctx context.Context, mirror *corev1.Pod) (*pb.BackendConfig, error) {
	kind := mirror.Annotations[utils.BackendAnnotation]
	raw := mirror.Annotations[utils.BackendConfigAnnotation]
	switch kind {
	case "", string(backends.BackendCuda):
		return nil, nil //nolint:nilnil // nil config means a cuda-checkpointed guest
	case "app_endpoint":
	default:
		return nil, fmt.Errorf("the %s annotation is %q; a guest is suspended with cuda or app_endpoint",
			utils.BackendAnnotation, kind)
	}
	cfg := &pb.AppEndpointConfig{}
	if raw != "" {
		if err := protojson.Unmarshal([]byte(raw), cfg); err != nil {
			return nil, fmt.Errorf("parse the %s annotation as an app_endpoint config: %w", utils.BackendConfigAnnotation, err)
		}
	}
	if cfg.GetApp() == pb.App_APP_UNSPECIFIED {
		cfg.App = pb.App_APP_VLLM
	}
	if cfg.GetApp() != pb.App_APP_VLLM {
		// The pipeline confirms the sleep and makes Resume idempotent through
		// the application's sleep state, which only the vLLM profile reads.
		return nil, fmt.Errorf("app %s is not supported for guests (only APP_VLLM)", cfg.GetApp())
	}
	switch cfg.GetMode() {
	case pb.SuspendMode_SUSPEND_MODE_UNSPECIFIED:
		cfg.Mode = pb.SuspendMode_SUSPEND_MODE_OFFLOAD
	case pb.SuspendMode_SUSPEND_MODE_DISCARD:
		return nil, errors.New("mode SUSPEND_MODE_DISCARD drops the weights, and a guest must resume; use SUSPEND_MODE_OFFLOAD")
	}
	ip := mirror.Status.PodIP
	if len(cfg.GetEndpoints()) == 0 {
		ports := g.servingPorts(ctx, mirror)
		if len(ports) == 0 {
			return nil, fmt.Errorf("mirror %s/%s declares no serving port for the default endpoint; set endpoints in %s",
				mirror.Namespace, mirror.Name, utils.BackendConfigAnnotation)
		}
		if ip == "" {
			return nil, fmt.Errorf("mirror %s/%s has no pod IP yet", mirror.Namespace, mirror.Name)
		}
		cfg.Endpoints = []string{"http://" + net.JoinHostPort(ip, strconv.Itoa(ports[0]))}
	} else {
		for i, ep := range cfg.GetEndpoints() {
			rewritten, err := podLocalEndpoint(ep, ip)
			if err != nil {
				return nil, fmt.Errorf("mirror %s/%s: %w", mirror.Namespace, mirror.Name, err)
			}
			cfg.Endpoints[i] = rewritten
		}
	}
	return &pb.BackendConfig{Backend: &pb.BackendConfig_AppEndpoint{AppEndpoint: cfg}}, nil
}

// podLocalEndpoint points an endpoint whose host is local to the pod
// (empty, localhost, a loopback or an unspecified address) at the pod IP.
func podLocalEndpoint(ep, podIP string) (string, error) {
	parsed, err := url.Parse(ep)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q: %w", ep, err)
	}
	host := parsed.Hostname()
	local := host == "" || host == "localhost"
	if addr := net.ParseIP(host); addr != nil && (addr.IsLoopback() || addr.IsUnspecified()) {
		local = true
	}
	if !local {
		return ep, nil
	}
	if podIP == "" {
		return "", fmt.Errorf("endpoint %q is local to the pod, and the pod has no IP yet", ep)
	}
	if port := parsed.Port(); port != "" {
		parsed.Host = net.JoinHostPort(podIP, port)
	} else {
		parsed.Host = podIP
		if net.ParseIP(podIP).To4() == nil {
			parsed.Host = "[" + podIP + "]"
		}
	}
	return parsed.String(), nil
}

// appSuspend puts an application-aware guest to sleep: unless it already
// sleeps, drain its in-flight connections, check the budget, drop its prefix
// cache, sleep, and confirm. It returns the guest VRAM left after the sleep.
func (g *guestPipeline) appSuspend(
	ctx context.Context, jobID string, cfg *pb.BackendConfig, t *guestTarget, procs []int,
	deviceBytes int64, scrubs int, deadline time.Time,
) error {
	if g.app == nil {
		return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, errors.New("the app-endpoint backend is not configured on this agent"))
	}
	sleeping, err := g.app.IsSuspended(ctx, cfg)
	if err != nil {
		return backendError(ctx, fmt.Errorf("read the guest's sleep state: %w", err))
	}
	if sleeping {
		slog.InfoContext(ctx, "Suspend: app guest already sleeps", "jobID", jobID)
		return nil
	}
	g.drainInFlight(ctx, jobID, t, procs[0])
	if err := g.checkBudget(deadline, g.estimate(jobID, deviceBytes, false, scrubs)); err != nil {
		return err
	}
	// Nothing cached from before the suspend may be served after it.
	if err := g.app.ResetCache(ctx, cfg); err != nil {
		slog.WarnContext(ctx, "Suspend: resetting the app guest's cache failed; suspending anyway", "jobID", jobID, "error", err)
	}
	t0 := g.now()
	if err := g.app.Snapshot(ctx, backends.Request{JobID: jobID, Config: cfg}); err != nil {
		return backendError(ctx, fmt.Errorf("app sleep: %w", err))
	}
	took := g.now().Sub(t0)
	sleeping, err = g.app.IsSuspended(ctx, cfg)
	if err != nil {
		return backendError(ctx, fmt.Errorf("confirm the guest's sleep: %w", err))
	}
	if !sleeping {
		return sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, errors.New("the guest answered the sleep call but does not report sleeping"))
	}
	g.update(jobID, func(r *guestRecord) { r.lastCheckpoint = took })
	residual := int64(-1)
	if vram, err := g.deviceBytes(toSet(procs)); err == nil {
		residual = vram.bytes
	}
	appCfg := cfg.GetAppEndpoint()
	slog.InfoContext(ctx, "Suspend: app suspended", "jobID", jobID,
		"app", appCfg.GetApp().String(), "mode", appCfg.GetMode().String(),
		"duration", took, "deviceBytes", deviceBytes, "residualBytes", residual)
	return nil
}

// drainInFlight resets the guest's in-flight connections until an abort
// finds none or appDrainTimeout passes, then waits appDrainSettle if any was
// reset, so that the application has dropped the aborted requests before it
// sleeps: a vLLM engine put to sleep with a running request fails.
func (g *guestPipeline) drainInFlight(ctx context.Context, jobID string, t *guestTarget, pid int) {
	start := g.now()
	total := 0
	for {
		n := g.abortInFlight(ctx, jobID, "before-sleep", t, pid)
		if n <= 0 {
			break
		}
		total += n
		if g.now().Sub(start) >= appDrainTimeout {
			slog.WarnContext(ctx, "Suspend: connections still arriving at the drain timeout; sleeping anyway",
				"jobID", jobID, "connections", total)
			break
		}
		if !sleepCtx(ctx, appDrainPoll) {
			return
		}
	}
	if total > 0 {
		sleepCtx(ctx, appDrainSettle)
	}
}

// sleepCtx waits d, or less if ctx ends first; it reports whether d passed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// appResume wakes a thawed application-aware guest unless it is awake, and
// verifies that its processes hold VRAM again. It returns the guest's device
// bytes.
func (g *guestPipeline) appResume(ctx context.Context, jobID string, cfg *pb.BackendConfig, procs []int) (int64, error) {
	if g.app == nil {
		return 0, sm.NewOpError(pb.ErrorReason_BACKEND_ERROR, errors.New("the app-endpoint backend is not configured on this agent"))
	}
	sleeping, err := g.app.IsSuspended(ctx, cfg)
	if err != nil {
		return 0, backendError(ctx, fmt.Errorf("read the guest's sleep state: %w", err))
	}
	var took time.Duration
	if sleeping {
		t0 := g.now()
		if err := g.app.Restore(ctx, backends.Request{JobID: jobID, Config: cfg}); err != nil {
			return 0, backendError(ctx, fmt.Errorf("app wake up: %w", err))
		}
		took = g.now().Sub(t0)
	}
	deviceBytes, err := g.verifyResumed(toSet(procs))
	if err != nil {
		return 0, err
	}
	slog.InfoContext(ctx, "Resume: app woke", "jobID", jobID, "pids", procs, "wasSleeping", sleeping,
		"duration", took, "deviceBytes", deviceBytes)
	return deviceBytes, nil
}

// exclusiveGuestGPUs returns the GPUs NVML lists a guest process on and no
// other process: those a sleeping application guest's freed VRAM is on.
func (g *guestPipeline) exclusiveGuestGPUs(procSet map[int]bool) (map[string]bool, error) {
	procs, err := g.gpu.Processes()
	if err != nil {
		return nil, fmt.Errorf("query GPU processes: %w", err)
	}
	gpus, shared := map[string]bool{}, map[string]bool{}
	for _, p := range procs {
		if procSet[p.PID] {
			gpus[p.GPUUUID] = true
		} else {
			shared[p.GPUUUID] = true
		}
	}
	for u := range shared {
		delete(gpus, u)
	}
	return gpus, nil
}
