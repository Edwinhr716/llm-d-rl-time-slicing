package server

import (
	"context"
	"sync"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"gotest.tools/v3/assert"
)

// spyBackend records the requests it receives.
type spyBackend struct {
	mu           sync.Mutex
	snapshotReqs []backends.Request
	restoreReqs  []backends.Request
}

func (s *spyBackend) Snapshot(ctx context.Context, req backends.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotReqs = append(s.snapshotReqs, req)
	return nil
}

func (s *spyBackend) Restore(ctx context.Context, req backends.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restoreReqs = append(s.restoreReqs, req)
	return nil
}

func (s *spyBackend) HealthCheck(ctx context.Context) error { return nil }

func memoryRegionsTestConfig(snapshotName string) *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions: []*pb.MemoryRegion{
					{Pid: 123, Address: 0x7f00, SizeBytes: 1024},
					{Pid: 456, Address: 0x9f00, SizeBytes: 2048},
				},
				SnapshotName: snapshotName,
			},
		},
	}
}

func TestGetSnapshotBackendType(t *testing.T) {
	type testCase struct {
		name   string
		config *pb.BackendConfig
		want   backends.BackendType
	}

	srv := NewServer(nil, backends.BackendCuda, "k8s", backends.NewChannelRegistry())

	run := func(t *testing.T, tc testCase) {
		assert.Equal(t, srv.getSnapshotBackendType(tc.config), tc.want)
	}

	testCases := []testCase{
		{name: "nil config defaults", config: nil, want: backends.BackendCuda},
		{name: "empty config defaults", config: &pb.BackendConfig{}, want: backends.BackendCuda},
		{
			name:   "cuda",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}}},
			want:   backends.BackendCuda,
		},
		{
			name:   "app endpoint",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_AppEndpoint{AppEndpoint: &pb.AppEndpointConfig{}}},
			want:   backends.BackendAppEndpoint,
		},
		{
			name:   "app channel",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_AppChannel{AppChannel: &pb.AppChannelConfig{}}},
			want:   backends.BackendAppChannel,
		},
		{
			name:   "memory regions",
			config: memoryRegionsTestConfig("slot-a"),
			want:   backends.BackendMemoryRegions,
		},
		{
			// Documents the pre-existing upstream gap: direct_memory is not
			// routed and falls through to the default backend.
			name:   "direct memory unrouted",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_DirectMemory{DirectMemory: &pb.DirectMemoryBackendConfig{}}},
			want:   backends.BackendCuda,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestBuildSnapshotFnMemoryRegions(t *testing.T) {
	type testCase struct {
		name    string
		mode    string
		wantErr string
	}

	run := func(t *testing.T, tc testCase) {
		spy := &spyBackend{}
		srv := NewServer(
			map[backends.BackendType]backends.Backend{backends.BackendMemoryRegions: spy},
			backends.BackendCuda, tc.mode, backends.NewChannelRegistry(),
		)
		config := memoryRegionsTestConfig("slot-a")

		fn, err := srv.buildSnapshotFn(context.Background(), "job-1", backends.BackendMemoryRegions, spy, config)
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
			return
		}
		assert.NilError(t, err)

		// Job must exist for the PID cache update.
		srv.state.RegisterJob("job-1", "")
		assert.NilError(t, srv.state.TransitionToRunning("job-1", nil))

		assert.NilError(t, fn())

		assert.Equal(t, len(spy.snapshotReqs), 1)
		got := spy.snapshotReqs[0]
		assert.Equal(t, got.JobID, "job-1")
		// The config, including snapshot_name, must reach the backend intact.
		assert.Equal(t, got.Config.GetMemoryRegions().GetSnapshotName(), "slot-a")
		assert.Equal(t, len(got.Config.GetMemoryRegions().GetRegions()), 2)

		if tc.mode == "k8s" {
			// Region PIDs are cached for Status parity.
			pids, err := srv.state.GetJobPIDs("job-1")
			assert.NilError(t, err)
			assert.DeepEqual(t, pids, []int{123, 456})
		}
	}

	testCases := []testCase{
		{name: "k8s mode passes config through", mode: "k8s"},
		{name: "standalone mode passes config through", mode: "standalone"},
		{name: "unknown mode rejected", mode: "bogus", wantErr: "unknown deployment mode"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestBuildRestoreFnMemoryRegions(t *testing.T) {
	type testCase struct {
		name    string
		mode    string
		wantErr string
	}

	run := func(t *testing.T, tc testCase) {
		spy := &spyBackend{}
		srv := NewServer(
			map[backends.BackendType]backends.Backend{backends.BackendMemoryRegions: spy},
			backends.BackendCuda, tc.mode, backends.NewChannelRegistry(),
		)
		config := memoryRegionsTestConfig("slot-b")

		fn, err := srv.buildRestoreFn(context.Background(), "job-1", backends.BackendMemoryRegions, spy, config)
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
			return
		}
		assert.NilError(t, err)
		assert.NilError(t, fn())

		assert.Equal(t, len(spy.restoreReqs), 1)
		got := spy.restoreReqs[0]
		assert.Equal(t, got.JobID, "job-1")
		assert.Equal(t, got.Config.GetMemoryRegions().GetSnapshotName(), "slot-b")
		assert.Equal(t, len(got.Config.GetMemoryRegions().GetRegions()), 2)
	}

	testCases := []testCase{
		{name: "k8s mode passes config through", mode: "k8s"},
		{name: "standalone mode passes config through", mode: "standalone"},
		{name: "unknown mode rejected", mode: "bogus", wantErr: "unknown deployment mode"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestMemoryRegionsSlot(t *testing.T) {
	type testCase struct {
		name   string
		config *pb.BackendConfig
		want   string
	}

	run := func(t *testing.T, tc testCase) {
		assert.Equal(t, memoryRegionsSlot(tc.config, "job-1"), tc.want)
	}

	testCases := []testCase{
		{name: "nil config", config: nil, want: ""},
		{name: "non memory-regions config", config: &pb.BackendConfig{Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}}}, want: ""},
		{name: "explicit snapshot name", config: memoryRegionsTestConfig("slot-a"), want: "slot-a"},
		{name: "empty snapshot name falls back to job id", config: memoryRegionsTestConfig(""), want: "job-1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}
