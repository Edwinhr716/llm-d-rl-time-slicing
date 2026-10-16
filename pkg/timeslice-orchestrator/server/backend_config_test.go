package server_test

import (
	"testing"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"google.golang.org/protobuf/proto"
)

func TestValidateJobBackendConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *pb.JobBackendConfig
		wantErr bool
	}{
		{name: "nil is valid", cfg: nil},
		{name: "empty oneof rejected", cfg: &pb.JobBackendConfig{}, wantErr: true},
		{name: "cuda", cfg: &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_Cuda{Cuda: &pb.CudaBackend{}}}},
		{name: "tpu", cfg: &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_Tpu{Tpu: &pb.TpuBackend{}}}},
		{
			name: "app_endpoint ok",
			cfg: appEndpointCfg(&pb.AppEndpointBackend{
				App: pb.App_APP_VLLM, Endpoints: []string{"http://localhost:8000"},
				Mode: pb.SuspendMode_SUSPEND_MODE_DISCARD, Tags: []string{"weights"},
			}),
		},
		{
			name:    "app_endpoint missing app",
			cfg:     appEndpointCfg(&pb.AppEndpointBackend{Endpoints: []string{"http://localhost:8000"}}),
			wantErr: true,
		},
		{
			name:    "app_endpoint unknown app",
			cfg:     appEndpointCfg(&pb.AppEndpointBackend{App: pb.App(99), Endpoints: []string{"http://localhost:8000"}}),
			wantErr: true,
		},
		{
			name:    "app_endpoint no endpoints",
			cfg:     appEndpointCfg(&pb.AppEndpointBackend{App: pb.App_APP_SGLANG}),
			wantErr: true,
		},
		{
			name:    "app_endpoint empty endpoint",
			cfg:     appEndpointCfg(&pb.AppEndpointBackend{App: pb.App_APP_SGLANG, Endpoints: []string{"http://a", ""}}),
			wantErr: true,
		},
		{
			name: "app_endpoint unknown mode",
			cfg: appEndpointCfg(&pb.AppEndpointBackend{
				App: pb.App_APP_VLLM, Endpoints: []string{"http://a"}, Mode: pb.SuspendMode(7),
			}),
			wantErr: true,
		},
		{
			name: "app_endpoint empty tag",
			cfg: appEndpointCfg(&pb.AppEndpointBackend{
				App: pb.App_APP_VLLM, Endpoints: []string{"http://a"}, Tags: []string{"weights", ""},
			}),
			wantErr: true,
		},
		{name: "app_channel empty ok", cfg: appChannelCfg(&pb.AppChannelBackend{})},
		{
			name: "app_channel with mode and tags",
			cfg:  appChannelCfg(&pb.AppChannelBackend{Mode: pb.SuspendMode_SUSPEND_MODE_OFFLOAD, Tags: []string{"kv_cache"}}),
		},
		{name: "app_channel unknown mode", cfg: appChannelCfg(&pb.AppChannelBackend{Mode: pb.SuspendMode(3)}), wantErr: true},
		{name: "app_channel empty tag", cfg: appChannelCfg(&pb.AppChannelBackend{Tags: []string{""}}), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := server.ValidateJobBackendConfig(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateJobBackendConfig() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestToAgentBackendConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *pb.JobBackendConfig
		want *agentpb.BackendConfig
	}{
		{name: "nil", cfg: nil, want: nil},
		{
			name: "cuda has no process target",
			cfg:  &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_Cuda{Cuda: &pb.CudaBackend{}}},
			want: &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_Cuda{Cuda: &agentpb.CudaBackendConfig{}}},
		},
		{
			name: "tpu has no process target",
			cfg:  &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_Tpu{Tpu: &pb.TpuBackend{}}},
			want: &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_Tpu{Tpu: &agentpb.TpuBackendConfig{}}},
		},
		{
			name: "app_endpoint copies all fields",
			cfg: appEndpointCfg(&pb.AppEndpointBackend{
				App: pb.App_APP_SGLANG, Endpoints: []string{"http://a", "http://b"},
				Mode: pb.SuspendMode_SUSPEND_MODE_DISCARD, Tags: []string{"weights", "kv_cache"},
			}),
			want: &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_AppEndpoint{AppEndpoint: &agentpb.AppEndpointConfig{
				App: agentpb.App_APP_SGLANG, Endpoints: []string{"http://a", "http://b"},
				Mode: agentpb.SuspendMode_SUSPEND_MODE_DISCARD, Tags: []string{"weights", "kv_cache"},
			}}},
		},
		{
			name: "app_channel copies mode and tags",
			cfg:  appChannelCfg(&pb.AppChannelBackend{Mode: pb.SuspendMode_SUSPEND_MODE_OFFLOAD, Tags: []string{"weights"}}),
			want: &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_AppChannel{AppChannel: &agentpb.AppChannelConfig{
				Mode: agentpb.SuspendMode_SUSPEND_MODE_OFFLOAD, Tags: []string{"weights"},
			}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := server.ToAgentBackendConfig(tc.cfg)
			if !proto.Equal(got, tc.want) {
				t.Fatalf("ToAgentBackendConfig() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnumParityWithAgentProto guards the cast-based conversion: every App and SuspendMode value the
// orchestrator exposes must exist with the same name and number in the snapshot agent's proto.
func TestEnumParityWithAgentProto(t *testing.T) {
	for value, name := range pb.App_name {
		if agentName, ok := agentpb.App_name[value]; !ok || agentName != name {
			t.Errorf("App %d: orchestrator %q, agent %q (present=%v)", value, name, agentName, ok)
		}
	}
	for value, name := range pb.SuspendMode_name {
		if agentName, ok := agentpb.SuspendMode_name[value]; !ok || agentName != name {
			t.Errorf("SuspendMode %d: orchestrator %q, agent %q (present=%v)", value, name, agentName, ok)
		}
	}
}

func TestAgentBackendName(t *testing.T) {
	if got := server.AgentBackendName(nil); got != "default" {
		t.Errorf("AgentBackendName(nil) = %q, want default", got)
	}
	cfg := server.ToAgentBackendConfig(appChannelCfg(&pb.AppChannelBackend{}))
	if got := server.AgentBackendName(cfg); got != "app_channel" {
		t.Errorf("AgentBackendName(app_channel) = %q", got)
	}
}

func appEndpointCfg(cfg *pb.AppEndpointBackend) *pb.JobBackendConfig {
	return &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_AppEndpoint{AppEndpoint: cfg}}
}

func appChannelCfg(cfg *pb.AppChannelBackend) *pb.JobBackendConfig {
	return &pb.JobBackendConfig{Backend: &pb.JobBackendConfig_AppChannel{AppChannel: cfg}}
}
