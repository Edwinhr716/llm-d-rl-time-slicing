package server

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
)

func donorPod(name string, ann map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Annotations: ann},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestDonorAppConfig(t *testing.T) {
	app := map[string]string{utils.BackendAnnotation: "app_channel"}
	tests := []struct {
		name    string
		pods    []*corev1.Pod
		wantApp bool
		mode    pb.SuspendMode
		wantErr string
	}{
		{name: "NoPodsIsDefault"},
		{name: "NoAnnotationIsDefault", pods: []*corev1.Pod{donorPod("a", nil)}},
		{name: "CudaIsDefault", pods: []*corev1.Pod{donorPod("a", map[string]string{utils.BackendAnnotation: "cuda"})}},
		{name: "AppChannel", pods: []*corev1.Pod{donorPod("a", app), donorPod("b", app)}, wantApp: true},
		{
			name: "AppChannelWithMode",
			pods: []*corev1.Pod{donorPod("a", map[string]string{
				utils.BackendAnnotation:       "app_channel",
				utils.BackendConfigAnnotation: `{"mode":"SUSPEND_MODE_OFFLOAD","tags":["weights"]}`,
			})},
			wantApp: true,
			mode:    pb.SuspendMode_SUSPEND_MODE_OFFLOAD,
		},
		{
			name: "DiscardRefused",
			pods: []*corev1.Pod{donorPod("a", map[string]string{
				utils.BackendAnnotation:       "app_channel",
				utils.BackendConfigAnnotation: `{"mode":"SUSPEND_MODE_DISCARD"}`,
			})},
			wantErr: "DISCARD",
		},
		{
			name: "BadConfigRefused",
			pods: []*corev1.Pod{donorPod("a", map[string]string{
				utils.BackendAnnotation:       "app_channel",
				utils.BackendConfigAnnotation: `{"mode":`,
			})},
			wantErr: "parse",
		},
		{
			name:    "UnknownBackendRefused",
			pods:    []*corev1.Pod{donorPod("a", map[string]string{utils.BackendAnnotation: "app_endpoint"})},
			wantErr: "app_endpoint",
		},
		{name: "PodsDisagree", pods: []*corev1.Pod{donorPod("a", app), donorPod("b", nil)}, wantErr: "disagree"},
		{
			name: "TerminatedPodSkipped",
			pods: func() []*corev1.Pod {
				old := donorPod("old", nil)
				old.Status.Phase = corev1.PodFailed
				return []*corev1.Pod{old, donorPod("a", app)}
			}(),
			wantApp: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := donorAppConfig(tt.pods)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("donorAppConfig() error = %v, want it to mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("donorAppConfig() error = %v", err)
			}
			if got := cfg.GetAppChannel() != nil; got != tt.wantApp {
				t.Fatalf("donorAppConfig() app_channel = %v, want %v (cfg %v)", got, tt.wantApp, cfg)
			}
			if !tt.wantApp && cfg != nil {
				t.Fatalf("donorAppConfig() = %v, want nil (default backend)", cfg)
			}
			if cfg.GetAppChannel().GetMode() != tt.mode {
				t.Fatalf("mode = %v, want %v", cfg.GetAppChannel().GetMode(), tt.mode)
			}
		})
	}
}

func TestDonorConfig_RequestConfigWinsAndStandaloneIgnoresAnnotations(t *testing.T) {
	app := []*corev1.Pod{donorPod("a", map[string]string{utils.BackendAnnotation: "app_channel"})}
	s := NewServer(nil, backends.BackendCuda, "k8s", nil, nil)
	s.donorPods = func(string) []*corev1.Pod { return app }

	explicit := &pb.BackendConfig{Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}}}
	if cfg, isApp, err := s.donorConfig("job", explicit); err != nil || isApp || cfg != explicit {
		t.Fatalf("donorConfig(explicit) = %v, %v, %v; want the request's config", cfg, isApp, err)
	}
	if cfg, isApp, err := s.donorConfig("job", nil); err != nil || !isApp || cfg.GetAppChannel() == nil {
		t.Fatalf("donorConfig(nil) = %v, %v, %v; want app_channel", cfg, isApp, err)
	}
	s.donorPods = nil
	if cfg, isApp, err := s.donorConfig("job", nil); err != nil || isApp || cfg != nil {
		t.Fatalf("donorConfig() without the pod cache = %v, %v, %v; want the default backend", cfg, isApp, err)
	}
	st := NewServer(nil, backends.BackendCuda, "standalone", nil, nil)
	st.donorPods = func(string) []*corev1.Pod { return app }
	if cfg, isApp, err := st.donorConfig("job", nil); err != nil || isApp || cfg != nil {
		t.Fatalf("donorConfig() in standalone mode = %v, %v, %v; want the default backend", cfg, isApp, err)
	}
}

// newDonorTestServer is newChannelTestServer with an application-aware donor
// "trainer" whose processes are PIDs 10 and 11 on a fake GPU.
func newDonorTestServer(t *testing.T, gpu *fakeGPU) (*Server, *backends.ChannelRegistry, pb.SnapshotAgentServiceClient) {
	t.Helper()
	srv, registry, client := newChannelTestServer(t)
	srv.donorPods = func(jobID string) []*corev1.Pod {
		if jobID != "trainer" {
			return nil
		}
		return []*corev1.Pod{donorPod("trainer-0", map[string]string{utils.BackendAnnotation: "app_channel"})}
	}
	srv.donorPIDs = func(context.Context, string) ([]int, error) { return []int{10, 11}, nil }
	srv.donorGPU = gpu
	srv.donorChannelWait = 2 * time.Second
	return srv, registry, client
}

func jobState(t *testing.T, srv *Server, jobID string) pb.JobState {
	t.Helper()
	for _, js := range srv.state.GetJobStatus() {
		if js.GetJobId() == jobID {
			return js.GetState()
		}
	}
	t.Fatalf("job %s not registered", jobID)
	return pb.JobState_JOB_STATE_UNSPECIFIED
}

func TestServer_AppDonor_SnapshotRestoreThroughChannel(t *testing.T) {
	gpu := &fakeGPU{procs: []gpuProcess{{PID: 10, UsedBytes: 1 << 30}, {PID: 99, UsedBytes: 20 << 30}}}
	srv, registry, client := newDonorTestServer(t, gpu)
	ctx := context.Background()
	registerWorkload(ctx, t, client, registry, "trainer", &pb.WorkloadCapabilities{
		SupportedModes: []pb.SuspendMode{pb.SuspendMode_SUSPEND_MODE_OFFLOAD},
	})
	if err := srv.state.TransitionToRunning("trainer", nil); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}

	// The orchestrator sends no backend config: the annotation selects app_channel.
	snap, err := client.Snapshot(ctx, &pb.SnapshotRequest{JobId: "trainer", Group: "g"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// PID 99 (another process, e.g. a guest) does not count against the donor.
	op := waitForOperation(ctx, t, client, snap.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		t.Fatalf("snapshot status = %v (%s), want COMPLETE", op.GetStatus(), op.GetError())
	}
	if got := jobState(t, srv, "trainer"); got != pb.JobState_JOB_STATE_SAVED {
		t.Fatalf("state after park = %v, want SAVED", got)
	}

	rest, err := client.Restore(ctx, &pb.RestoreRequest{JobId: "trainer", Group: "g"})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	op = waitForOperation(ctx, t, client, rest.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		t.Fatalf("restore status = %v (%s), want COMPLETE", op.GetStatus(), op.GetError())
	}
	if got := jobState(t, srv, "trainer"); got != pb.JobState_JOB_STATE_RUNNING {
		t.Fatalf("state after restore = %v, want RUNNING", got)
	}
}

func TestServer_AppDonor_ResidualAboveBoundFailsVerify(t *testing.T) {
	gpu := &fakeGPU{procs: []gpuProcess{{PID: 10, UsedBytes: 3 << 30}, {PID: 11, UsedBytes: 2 << 30}}}
	srv, registry, client := newDonorTestServer(t, gpu)
	ctx := context.Background()
	registerWorkload(ctx, t, client, registry, "trainer", nil)
	if err := srv.state.TransitionToRunning("trainer", nil); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	snap, err := client.Snapshot(ctx, &pb.SnapshotRequest{JobId: "trainer", Group: "g"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	op := waitForOperation(ctx, t, client, snap.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED || !strings.Contains(op.GetError(), "VRAM") {
		t.Fatalf("snapshot = %v (%q), want FAILED on the residual VRAM", op.GetStatus(), op.GetError())
	}
	if got := jobState(t, srv, "trainer"); got != pb.JobState_JOB_STATE_FAULTED {
		t.Fatalf("state = %v, want FAULTED (the GPU must not be lent)", got)
	}
}

func TestServer_AppDonor_WaitsForLateChannel(t *testing.T) {
	gpu := &fakeGPU{}
	srv, registry, client := newDonorTestServer(t, gpu)
	ctx := context.Background()
	srv.state.RegisterJob("trainer", "g") // the pod watcher registers the job before the trainer connects
	if err := srv.state.TransitionToRunning("trainer", nil); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	snap, err := client.Snapshot(ctx, &pb.SnapshotRequest{JobId: "trainer", Group: "g"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	registerWorkload(ctx, t, client, registry, "trainer", nil)
	op := waitForOperation(ctx, t, client, snap.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		t.Fatalf("snapshot status = %v (%s), want COMPLETE once the channel registers", op.GetStatus(), op.GetError())
	}
}

func TestServer_AppDonor_NoChannelFails(t *testing.T) {
	srv, _, client := newDonorTestServer(t, &fakeGPU{})
	srv.donorChannelWait = 200 * time.Millisecond
	ctx := context.Background()
	srv.state.RegisterJob("trainer", "g") // the pod watcher registers the job before the trainer connects
	if err := srv.state.TransitionToRunning("trainer", nil); err != nil {
		t.Fatalf("TransitionToRunning: %v", err)
	}
	snap, err := client.Snapshot(ctx, &pb.SnapshotRequest{JobId: "trainer", Group: "g"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	op := waitForOperation(ctx, t, client, snap.GetOperationId())
	if op.GetStatus() != pb.OperationStatus_OPERATION_STATUS_FAILED || !strings.Contains(op.GetError(), "no workload channel") {
		t.Fatalf("snapshot = %v (%q), want FAILED without a channel", op.GetStatus(), op.GetError())
	}
}

func TestServer_AppDonor_BadAnnotationRejected(t *testing.T) {
	srv, _, client := newChannelTestServer(t)
	srv.donorPods = func(string) []*corev1.Pod {
		return []*corev1.Pod{donorPod("trainer-0", map[string]string{utils.BackendAnnotation: "bogus"})}
	}
	if _, err := client.Snapshot(context.Background(), &pb.SnapshotRequest{JobId: "trainer"}); err == nil ||
		!strings.Contains(err.Error(), "bogus") {
		t.Fatalf("Snapshot error = %v, want InvalidArgument naming the annotation value", err)
	}
	if _, err := client.Restore(context.Background(), &pb.RestoreRequest{JobId: "trainer"}); err == nil ||
		!strings.Contains(err.Error(), "bogus") {
		t.Fatalf("Restore error = %v, want InvalidArgument naming the annotation value", err)
	}
}
