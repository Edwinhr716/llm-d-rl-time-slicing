package store_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeAgentServer struct {
	agentpb.UnimplementedSnapshotAgentServiceServer
	statusResponse *agentpb.StatusResponse
	statusErr      error
	callCount      int32

	mu              sync.Mutex
	lastSnapshotReq *agentpb.SnapshotRequest
	lastRestoreReq  *agentpb.RestoreRequest
}

func (s *fakeAgentServer) Snapshot(ctx context.Context, req *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSnapshotReq = req
	return &agentpb.SnapshotResponse{OperationId: "snap-op"}, nil
}

func (s *fakeAgentServer) Restore(ctx context.Context, req *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRestoreReq = req
	return &agentpb.RestoreResponse{OperationId: "restore-op"}, nil
}

func (s *fakeAgentServer) Status(ctx context.Context, req *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	atomic.AddInt32(&s.callCount, 1)
	if s.statusErr != nil {
		return nil, s.statusErr
	}
	return s.statusResponse, nil
}

func TestGRPCSnapshotAgentStore_GetStatus(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	addr := lis.Addr().String()

	fakeServer := &fakeAgentServer{
		statusResponse: &agentpb.StatusResponse{
			JobStatuses: []*agentpb.JobStatus{
				{JobId: "job-1", State: agentpb.JobState_JOB_STATE_RUNNING},
			},
			AcceleratorStatuses: []*agentpb.AcceleratorStatus{
				{Id: "acc-1", MemoryUsedBytes: 100, MemoryTotalBytes: 1000},
			},
		},
	}

	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, fakeServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Test with 5s TTL
	s := store.NewGRPCSnapshotAgentStore(5*time.Second, 9001)
	resp, err := s.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if len(resp.JobStatuses) != 1 || resp.JobStatuses[0].JobId != "job-1" {
		t.Errorf("Unexpected job statuses: %v", resp.JobStatuses)
	}
	if len(resp.AcceleratorStatuses) != 1 || resp.AcceleratorStatuses[0].Id != "acc-1" {
		t.Errorf("Unexpected accelerator statuses: %v", resp.AcceleratorStatuses)
	}

	fakeServer.statusErr = status.Error(codes.Internal, "internal error")
	// Should return cached response even if server returns error now, because TTL (5s) hasn't expired.
	respCached, err := s.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("Expected cached response, got error: %v", err)
	}
	if len(respCached.JobStatuses) != 1 || respCached.JobStatuses[0].JobId != "job-1" {
		t.Errorf("Unexpected cached job statuses: %v", respCached.JobStatuses)
	}
}

func TestGRPCSnapshotAgentStore_GetStatus_Caching(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	addr := lis.Addr().String()

	fakeServer := &fakeAgentServer{
		statusResponse: &agentpb.StatusResponse{
			JobStatuses: []*agentpb.JobStatus{
				{JobId: "job-1", State: agentpb.JobState_JOB_STATE_RUNNING},
			},
		},
	}

	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, fakeServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Create store with 100ms TTL
	agentStore := store.NewGRPCSnapshotAgentStore(100*time.Millisecond, 9001)

	// 1. First call - cache miss, calls server
	resp1, err := agentStore.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if atomic.LoadInt32(&fakeServer.callCount) != 1 {
		t.Errorf("Expected 1 call to server, got %d", fakeServer.callCount)
	}

	// 2. Second call (immediate) - cache hit
	resp2, err := agentStore.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if atomic.LoadInt32(&fakeServer.callCount) != 1 {
		t.Errorf("Expected still 1 call to server, got %d", fakeServer.callCount)
	}

	if resp1.JobStatuses[0].JobId != resp2.JobStatuses[0].JobId {
		t.Errorf("Expected cached response to match, got %v and %v", resp1, resp2)
	}

	// 3. Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// 4. Third call - cache miss, calls server again
	_, err = agentStore.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if atomic.LoadInt32(&fakeServer.callCount) != 2 {
		t.Errorf("Expected 2 calls to server, got %d", fakeServer.callCount)
	}
}

func TestGRPCSnapshotAgentStore_GetStatus_NoCaching(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	addr := lis.Addr().String()

	fakeServer := &fakeAgentServer{
		statusResponse: &agentpb.StatusResponse{
			JobStatuses: []*agentpb.JobStatus{
				{JobId: "job-1", State: agentpb.JobState_JOB_STATE_RUNNING},
			},
		},
	}

	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, fakeServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Create store with 0 TTL (no caching)
	s := store.NewGRPCSnapshotAgentStore(0, 9001)

	// 1. First call - calls server
	_, err = s.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if atomic.LoadInt32(&fakeServer.callCount) != 1 {
		t.Errorf("Expected 1 call to server, got %d", fakeServer.callCount)
	}

	// 2. Second call - should call server again because TTL is 0
	_, err = s.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if atomic.LoadInt32(&fakeServer.callCount) != 2 {
		t.Errorf("Expected 2 calls to server, got %d", fakeServer.callCount)
	}
}

func TestGRPCSnapshotAgentStore_CloseClient(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	addr := lis.Addr().String()

	fakeServer := &fakeAgentServer{
		statusResponse: &agentpb.StatusResponse{
			JobStatuses: []*agentpb.JobStatus{
				{JobId: "job-1", State: agentpb.JobState_JOB_STATE_RUNNING},
			},
		},
	}

	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, fakeServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()

	// 1. Create store with 5s TTL
	agentStore := store.NewGRPCSnapshotAgentStore(5*time.Second, 9001)

	// 2. First call - succeeds and caches
	_, err = agentStore.GetStatus(context.Background(), addr)
	if err != nil {
		t.Fatalf("First GetStatus failed: %v", err)
	}

	// 3. Stop the gRPC server
	grpcServer.GracefulStop()

	// 4. Second call - should still succeed because it is cached
	_, err = agentStore.GetStatus(context.Background(), addr)
	if err != nil {
		t.Errorf("Second GetStatus (cached) failed after server stop: %v", err)
	}

	// 5. CloseClient for this address (since address is same as nodeName in tests)
	err = agentStore.CloseClient(addr)
	if err != nil {
		t.Fatalf("CloseClient failed: %v", err)
	}

	// 6. Third call - should fail because cache is cleared and server is stopped
	_, err = agentStore.GetStatus(context.Background(), addr)
	if err == nil {
		t.Error("Expected GetStatus to fail after CloseClient and server stop, but it succeeded")
	}
}

// startFakeAgent serves fakeServer on a loopback listener and returns its address.
func startFakeAgent(t *testing.T, fakeServer *fakeAgentServer) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(grpcServer, fakeServer)
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

func TestGRPCSnapshotAgentStore_ForwardsBackendConfig(t *testing.T) {
	appChannel := &agentpb.BackendConfig{
		Backend: &agentpb.BackendConfig_AppChannel{AppChannel: &agentpb.AppChannelConfig{Tags: []string{"weights"}}},
	}
	tests := []struct {
		name string
		cfg  *agentpb.BackendConfig
	}{
		{name: "nil config stays nil on the wire", cfg: nil},
		{name: "app_channel config forwarded verbatim", cfg: appChannel},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeServer := &fakeAgentServer{}
			addr := startFakeAgent(t, fakeServer)
			agentStore := store.NewGRPCSnapshotAgentStore(0, 0)
			t.Cleanup(func() {
				if err := agentStore.CloseClient(addr); err != nil {
					t.Errorf("CloseClient failed: %v", err)
				}
			})
			ctx := context.Background()

			if _, err := agentStore.Snapshot(ctx, addr, "job-1", "group-1", tc.cfg); err != nil {
				t.Fatalf("Snapshot failed: %v", err)
			}
			if _, err := agentStore.Restore(ctx, addr, "job-1", "group-1", tc.cfg); err != nil {
				t.Fatalf("Restore failed: %v", err)
			}

			fakeServer.mu.Lock()
			defer fakeServer.mu.Unlock()
			if got := fakeServer.lastSnapshotReq; got.GetJobId() != "job-1" || got.GetGroup() != "group-1" ||
				!proto.Equal(got.GetBackendConfig(), tc.cfg) {
				t.Errorf("snapshot request = %v, want backend config %v", got, tc.cfg)
			}
			if got := fakeServer.lastRestoreReq; got.GetJobId() != "job-1" || got.GetGroup() != "group-1" ||
				!proto.Equal(got.GetBackendConfig(), tc.cfg) {
				t.Errorf("restore request = %v, want backend config %v", got, tc.cfg)
			}
		})
	}
}
