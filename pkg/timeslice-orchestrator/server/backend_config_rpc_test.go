package server_test

import (
	"context"
	"testing"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	cudaJobCfg       = &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_Cuda{Cuda: &pb.CudaBackend{}}}
	appChannelJobCfg = appChannelCfg(&pb.AppChannelBackend{})
	invalidJobCfg    = &pb.JobBackendConfig{}
)

// backendConfigStores builds a group whose lock is held by lockHolder, with job-1 present in the job
// store (when withJob) carrying storedCfg (nil for none).
func backendConfigStores(
	t *testing.T, ctx context.Context, lockHolder string, storedCfg *agentpb.BackendConfig, withJob bool,
) (server.GroupStore, *store.JobStore) {
	t.Helper()
	lockStore := store.NewMemLockStore()
	if lockHolder != "" {
		if err := lockStore.Lock(ctx, "group-1", lockHolder); err != nil {
			t.Fatalf("failed to lock: %v", err)
		}
	}
	gs := store.NewGroupStore(lockStore)
	if _, _, err := gs.GetOrCreate(ctx, "group-1"); err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	js := store.NewJobStore()
	if withJob {
		job := store.NewJob("group-1", "job-1")
		job.SetBackendConfig(storedCfg)
		if err := js.Put(ctx, job); err != nil {
			t.Fatalf("failed to put job: %v", err)
		}
	}
	return gs, js
}

// rpcHarness is a served orchestrator plus a client dialed to it over bufnet.
type rpcHarness struct {
	client pb.TimeSliceOrchestratorServiceClient
	queue  *server.MockWorkQueue
}

func dialServer(t *testing.T, gs server.GroupStore, js server.JobStore) *rpcHarness {
	t.Helper()
	_, mq, cleanup := server.InitGRPCServer(gs, js)
	t.Cleanup(cleanup)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(server.BufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close failed: %v", err)
		}
	})
	return &rpcHarness{client: pb.NewTimeSliceOrchestratorServiceClient(conn), queue: mq}
}

func storedConfig(t *testing.T, ctx context.Context, js *store.JobStore) *agentpb.BackendConfig {
	t.Helper()
	job, err := js.Get(ctx, "group-1", "job-1")
	if err != nil {
		t.Fatalf("job-1 missing from store: %v", err)
	}
	return job.BackendConfig()
}

func TestServer_Yield_BackendConfig(t *testing.T) {
	tests := []struct {
		name         string
		lockHolder   string
		stored       *agentpb.BackendConfig
		withJob      bool
		request      *pb.JobBackendConfig
		expectedCode codes.Code
		wantStored   *agentpb.BackendConfig
		wantEnqueue  bool
	}{
		{
			name:         "invalid config rejected, lock untouched",
			lockHolder:   "job-1",
			withJob:      true,
			request:      invalidJobCfg,
			expectedCode: codes.InvalidArgument,
		},
		{
			name:        "config stored on the job",
			lockHolder:  "job-1",
			withJob:     true,
			request:     appChannelJobCfg,
			wantStored:  server.ToAgentBackendConfig(appChannelJobCfg),
			wantEnqueue: true,
		},
		{
			name:        "omitted config keeps the stored one",
			lockHolder:  "job-1",
			withJob:     true,
			stored:      server.ToAgentBackendConfig(cudaJobCfg),
			wantStored:  server.ToAgentBackendConfig(cudaJobCfg),
			wantEnqueue: true,
		},
		{
			name:        "different config overwrites the stored one",
			lockHolder:  "job-1",
			withJob:     true,
			stored:      server.ToAgentBackendConfig(cudaJobCfg),
			request:     appChannelJobCfg,
			wantStored:  server.ToAgentBackendConfig(appChannelJobCfg),
			wantEnqueue: true,
		},
		{
			name:         "non-holder yield rejected and stored config rolled back",
			lockHolder:   "job-2",
			withJob:      true,
			stored:       server.ToAgentBackendConfig(appChannelJobCfg),
			request:      cudaJobCfg,
			expectedCode: codes.PermissionDenied,
			wantStored:   server.ToAgentBackendConfig(appChannelJobCfg),
		},
		{
			name:        "job without pods: config ignored, yield still succeeds",
			lockHolder:  "job-1",
			withJob:     false,
			request:     cudaJobCfg,
			wantEnqueue: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			gs, js := backendConfigStores(t, ctx, tc.lockHolder, tc.stored, tc.withJob)
			harness := dialServer(t, gs, js)

			resp, err := harness.client.Yield(ctx, &pb.YieldRequest{GroupId: "group-1", JobId: "job-1", BackendConfig: tc.request})

			if added := harness.queue.GetAdded(); tc.wantEnqueue != (len(added) == 1) {
				t.Errorf("enqueue: want %v, got %v", tc.wantEnqueue, added)
			}
			if tc.expectedCode != codes.OK {
				if st, ok := status.FromError(err); !ok || st.Code() != tc.expectedCode {
					t.Fatalf("expected code %v, got %v", tc.expectedCode, err)
				}
				g, gerr := gs.Get(ctx, "group-1")
				if gerr != nil || g.Spec().LockingJob() != tc.lockHolder {
					t.Errorf("lock holder changed on rejected yield: %v (err %v)", g.Spec().LockingJob(), gerr)
				}
			} else if err != nil || !resp.GetSuccess() {
				t.Fatalf("Yield failed: resp=%v err=%v", resp, err)
			}
			if tc.withJob {
				if got := storedConfig(t, ctx, js); !proto.Equal(got, tc.wantStored) {
					t.Errorf("stored config = %v, want %v", got, tc.wantStored)
				}
			}
		})
	}
}
