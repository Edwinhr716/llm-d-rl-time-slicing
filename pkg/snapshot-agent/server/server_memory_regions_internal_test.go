package server

import (
	"reflect"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

func mrConfig(slot string, pids ...int32) *pb.BackendConfig {
	regions := make([]*pb.MemoryRegion, 0, len(pids))
	for _, pid := range pids {
		regions = append(regions, &pb.MemoryRegion{Pid: pid, Address: 0x7f00, SizeBytes: 1024})
	}
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions:      regions,
				SnapshotName: slot,
			},
		},
	}
}

func TestMemoryRegionsSlot(t *testing.T) {
	tests := []struct {
		name   string
		config *pb.BackendConfig
		jobID  string
		want   string
	}{
		{
			name:   "SnapshotNameWins",
			config: mrConfig("adapter-a", 123),
			jobID:  "job-1",
			want:   "adapter-a",
		},
		{
			name:   "EmptyNameFallsBackToJobID",
			config: mrConfig("", 123),
			jobID:  "job-1",
			want:   "job-1",
		},
		{
			name:   "NonMemoryRegionsConfigHasNoSlot",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}}},
			jobID:  "job-1",
			want:   "",
		},
		{
			name:   "NilConfigHasNoSlot",
			config: nil,
			jobID:  "job-1",
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := memoryRegionsSlot(tt.config, tt.jobID); got != tt.want {
				t.Errorf("memoryRegionsSlot() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPidsFromMemoryRegions(t *testing.T) {
	tests := []struct {
		name   string
		config *pb.BackendConfig
		want   []int
	}{
		{
			name:   "DeduplicatedFirstAppearanceOrder",
			config: mrConfig("adapter-a", 30, 10, 30, 20, 10),
			want:   []int{30, 10, 20},
		},
		{
			name:   "SinglePid",
			config: mrConfig("adapter-a", 123),
			want:   []int{123},
		},
		{
			name:   "NonMemoryRegionsConfig",
			config: &pb.BackendConfig{Backend: &pb.BackendConfig_Cuda{Cuda: &pb.CudaBackendConfig{}}},
			want:   nil,
		},
		{
			name:   "NilConfig",
			config: nil,
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pidsFromMemoryRegions(tt.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pidsFromMemoryRegions() = %v, want %v", got, tt.want)
			}
		})
	}
}
