package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/features"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func directMemoryConfig() *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_DirectMemory{
			DirectMemory: &pb.DirectMemoryBackendConfig{
				ExplicitTarget: &pb.ProcessTarget{Pids: []int32{123}},
			},
		},
	}
}

func memoryRegionsGatedConfig() *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions: []*pb.MemoryRegion{{Pid: 123, Address: 0x7f0000000000, SizeBytes: 4096}},
			},
		},
	}
}

func TestServer_CheckFeatureGates(t *testing.T) {
	tests := []struct {
		name     string
		gates    features.Gates
		config   *pb.BackendConfig
		wantCode codes.Code
		// wantGate is the gate a FailedPrecondition error must name.
		wantGate features.Feature
	}{
		{
			name:     "nil config passes with nil gates",
			gates:    nil,
			config:   nil,
			wantCode: codes.OK,
		},
		{
			name:  "non-gated config passes with nil gates",
			gates: nil,
			config: &pb.BackendConfig{
				Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}},
			},
			wantCode: codes.OK,
		},
		{
			name:     "gated config rejected by default",
			gates:    nil,
			config:   directMemoryConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.DirectMemoryBackend,
		},
		{
			name:     "gated config rejected with explicit false",
			gates:    features.Gates{features.DirectMemoryBackend: false},
			config:   directMemoryConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.DirectMemoryBackend,
		},
		{
			name:     "memory-regions config rejected by default",
			gates:    nil,
			config:   memoryRegionsGatedConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.MemoryRegionsBackend,
		},
		{
			name:     "memory-regions config rejected with explicit false",
			gates:    features.Gates{features.MemoryRegionsBackend: false},
			config:   memoryRegionsGatedConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.MemoryRegionsBackend,
		},
		{
			name:     "memory-regions config passes with its gate enabled",
			gates:    features.Gates{features.MemoryRegionsBackend: true},
			config:   memoryRegionsGatedConfig(),
			wantCode: codes.OK,
		},
		{
			name:     "direct-memory gate does not enable memory-regions",
			gates:    features.Gates{features.DirectMemoryBackend: true},
			config:   memoryRegionsGatedConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.MemoryRegionsBackend,
		},
		{
			name:     "memory-regions gate does not enable direct-memory",
			gates:    features.Gates{features.MemoryRegionsBackend: true},
			config:   directMemoryConfig(),
			wantCode: codes.FailedPrecondition,
			wantGate: features.DirectMemoryBackend,
		},
		{
			name:     "gated config passes with its gate enabled",
			gates:    features.Gates{features.DirectMemoryBackend: true},
			config:   directMemoryConfig(),
			wantCode: codes.OK,
		},
		{
			name:  "enabling the gate does not affect non-gated configs",
			gates: features.Gates{features.DirectMemoryBackend: true},
			config: &pb.BackendConfig{
				Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}},
			},
			wantCode: codes.OK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(nil, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), tc.gates)

			err := srv.checkFeatureGates(tc.config)
			if status.Code(err) != tc.wantCode {
				t.Fatalf("checkFeatureGates() = %v, want code %v", err, tc.wantCode)
			}
			if tc.wantCode == codes.FailedPrecondition {
				if !strings.Contains(err.Error(), string(tc.wantGate)) {
					t.Errorf("Expected error to name gate %s, got: %v", tc.wantGate, err)
				}
				if !strings.Contains(err.Error(), "--feature-gates") {
					t.Errorf("Expected error to name the --feature-gates flag, got: %v", err)
				}
			}
		})
	}
}

// newFeatureGateTestServer starts a standalone-mode server whose default
// backend is noop, so a gated request that passes the gate check completes
// against the noop backend.
//
//nolint:nonamedreturns // Conflict between gocritic's unnamedResult and nonamedreturns
func newFeatureGateTestServer(
	t *testing.T,
	gates features.Gates,
) (client pb.SnapshotAgentServiceClient, srv *Server, cleanup func()) {
	t.Helper()

	noopBackend := backends.NewNoopBackend()
	backendsMap := map[backends.BackendType]backends.Backend{
		backends.BackendNoop: noopBackend,
	}

	lisLocal := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	srv = NewServer(backendsMap, backends.BackendNoop, "standalone", backends.NewChannelRegistry(), gates)
	pb.RegisterSnapshotAgentServiceServer(s, srv)
	go func() {
		if err := s.Serve(lisLocal); err != nil {
			return
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lisLocal.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}

	cleanup = func() {
		conn.Close()
		s.GracefulStop()
	}
	return pb.NewSnapshotAgentServiceClient(conn), srv, cleanup
}

// registerRunningJob puts a fresh job into the RUNNING state so a snapshot
// request against it is accepted by the state machine.
func registerRunningJob(t *testing.T, srv *Server, jobID string) {
	t.Helper()
	srv.state.RegisterJob(jobID, "")
	if err := srv.state.TransitionToRunning(jobID, []int{123}); err != nil {
		t.Fatalf("Failed to transition job to RUNNING: %v", err)
	}
}

// snapshotUntilSaved snapshots a RUNNING job with a direct_memory config
// and waits for the noop backend to complete so the job reaches SAVED and
// a subsequent Restore is accepted. Requires the DirectMemoryBackend gate
// to be enabled on the server.
func snapshotUntilSaved(
	t *testing.T,
	ctx context.Context,
	client pb.SnapshotAgentServiceClient,
	srv *Server,
	jobID string,
) {
	t.Helper()
	if _, err := client.Snapshot(ctx, &pb.SnapshotRequest{
		JobId:         jobID,
		BackendConfig: directMemoryConfig(),
	}); err != nil {
		t.Fatalf("Snapshot during setup: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, js := range srv.state.GetJobStatus() {
			if js.JobId == jobID && js.State == pb.JobState_JOB_STATE_SAVED {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for job %s to become SAVED", jobID)
}

func TestServer_FeatureGates_RPCEnforcement(t *testing.T) {
	enabled := features.Gates{features.DirectMemoryBackend: true}

	tests := []struct {
		name  string
		gates features.Gates
		// restore selects the RPC under test: Restore when true,
		// Snapshot otherwise.
		restore bool
		// jobState is the state the job is brought into before the RPC.
		// UNSPECIFIED means no setup: rejection happens before backend
		// routing and the state machine, so no job needs to exist.
		jobState pb.JobState
		wantCode codes.Code
	}{
		{
			name:     "Snapshot rejected by default",
			gates:    nil,
			restore:  false,
			jobState: pb.JobState_JOB_STATE_UNSPECIFIED,
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "Restore rejected by default",
			gates:    nil,
			restore:  true,
			jobState: pb.JobState_JOB_STATE_UNSPECIFIED,
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "Snapshot allowed when enabled",
			gates:    enabled,
			restore:  false,
			jobState: pb.JobState_JOB_STATE_RUNNING,
			wantCode: codes.OK,
		},
		{
			name:     "Restore allowed when enabled",
			gates:    enabled,
			restore:  true,
			jobState: pb.JobState_JOB_STATE_SAVED,
			wantCode: codes.OK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, srv, cleanup := newFeatureGateTestServer(t, tc.gates)
			defer cleanup()
			ctx := context.Background()
			jobID := "gated-job"

			switch tc.jobState {
			case pb.JobState_JOB_STATE_RUNNING:
				registerRunningJob(t, srv, jobID)
			case pb.JobState_JOB_STATE_SAVED:
				registerRunningJob(t, srv, jobID)
				snapshotUntilSaved(t, ctx, client, srv, jobID)
			}

			var err error
			if tc.restore {
				_, err = client.Restore(ctx, &pb.RestoreRequest{
					JobId:         jobID,
					BackendConfig: directMemoryConfig(),
				})
			} else {
				_, err = client.Snapshot(ctx, &pb.SnapshotRequest{
					JobId:         jobID,
					BackendConfig: directMemoryConfig(),
				})
			}
			if status.Code(err) != tc.wantCode {
				t.Errorf("Expected code %v, got: %v", tc.wantCode, err)
			}
		})
	}
}
