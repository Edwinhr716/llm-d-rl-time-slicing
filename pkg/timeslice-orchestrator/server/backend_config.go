package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/protobuf/proto"
)

// ValidateJobBackendConfig checks a client-supplied backend config. A nil config is valid (it means
// "no preference"); a non-nil config must select exactly one backend and satisfy that backend's
// requirements. The returned error is suitable for an InvalidArgument status message.
func ValidateJobBackendConfig(cfg *pb.JobBackendConfig) error {
	if cfg == nil {
		return nil
	}
	switch backend := cfg.GetBackend().(type) {
	case nil:
		return errors.New("backend_config must select a backend (cuda, tpu, app_endpoint or app_channel)")
	case *pb.JobBackendConfig_Cuda, *pb.JobBackendConfig_Tpu:
		return nil
	case *pb.JobBackendConfig_AppEndpoint:
		return validateAppEndpoint(backend.AppEndpoint)
	case *pb.JobBackendConfig_AppChannel:
		return validateAppChannel(backend.AppChannel)
	default:
		return fmt.Errorf("backend_config selects an unsupported backend %T", backend)
	}
}

func validateAppEndpoint(cfg *pb.AppEndpointBackend) error {
	if _, known := pb.App_name[int32(cfg.GetApp())]; !known {
		return fmt.Errorf("app_endpoint.app has unknown value %d", cfg.GetApp())
	}
	if cfg.GetApp() == pb.App_APP_UNSPECIFIED {
		return errors.New("app_endpoint.app must be specified")
	}
	if len(cfg.GetEndpoints()) == 0 {
		return errors.New("app_endpoint.endpoints must contain at least one endpoint")
	}
	for i, endpoint := range cfg.GetEndpoints() {
		if endpoint == "" {
			return fmt.Errorf("app_endpoint.endpoints[%d] must not be empty", i)
		}
	}
	if err := validateSuspendMode(cfg.GetMode()); err != nil {
		return fmt.Errorf("app_endpoint.%w", err)
	}
	if err := validateTags(cfg.GetTags()); err != nil {
		return fmt.Errorf("app_endpoint.%w", err)
	}
	return nil
}

func validateAppChannel(cfg *pb.AppChannelBackend) error {
	if err := validateSuspendMode(cfg.GetMode()); err != nil {
		return fmt.Errorf("app_channel.%w", err)
	}
	if err := validateTags(cfg.GetTags()); err != nil {
		return fmt.Errorf("app_channel.%w", err)
	}
	return nil
}

func validateSuspendMode(mode pb.SuspendMode) error {
	if _, known := pb.SuspendMode_name[int32(mode)]; !known {
		return fmt.Errorf("mode has unknown value %d", mode)
	}
	return nil
}

func validateTags(tags []string) error {
	for i, tag := range tags {
		if tag == "" {
			return fmt.Errorf("tags[%d] must not be empty", i)
		}
	}
	return nil
}

// ToAgentBackendConfig converts a validated client config into the snapshot agent's BackendConfig.
// Process targets are never set: in k8s mode the agent discovers PIDs from the job's pods. A nil
// input yields nil (agent default backend).
func ToAgentBackendConfig(cfg *pb.JobBackendConfig) *agentpb.BackendConfig {
	if cfg == nil {
		return nil
	}
	switch backend := cfg.GetBackend().(type) {
	case *pb.JobBackendConfig_Cuda:
		return &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_Cuda{Cuda: &agentpb.CudaBackendConfig{}}}
	case *pb.JobBackendConfig_Tpu:
		return &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_Tpu{Tpu: &agentpb.TpuBackendConfig{}}}
	case *pb.JobBackendConfig_AppEndpoint:
		src := backend.AppEndpoint
		return &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_AppEndpoint{
			AppEndpoint: &agentpb.AppEndpointConfig{
				App:       agentpb.App(src.GetApp()),
				Endpoints: append([]string(nil), src.GetEndpoints()...),
				Mode:      agentpb.SuspendMode(src.GetMode()),
				Tags:      append([]string(nil), src.GetTags()...),
			},
		}}
	case *pb.JobBackendConfig_AppChannel:
		src := backend.AppChannel
		return &agentpb.BackendConfig{Backend: &agentpb.BackendConfig_AppChannel{
			AppChannel: &agentpb.AppChannelConfig{
				Mode: agentpb.SuspendMode(src.GetMode()),
				Tags: append([]string(nil), src.GetTags()...),
			},
		}}
	default:
		return nil
	}
}

// AgentBackendName returns a short name for the backend an agent config selects, for logging.
func AgentBackendName(cfg *agentpb.BackendConfig) string {
	switch cfg.GetBackend().(type) {
	case *agentpb.BackendConfig_Cuda:
		return "cuda"
	case *agentpb.BackendConfig_Tpu:
		return "tpu"
	case *agentpb.BackendConfig_AppEndpoint:
		return "app_endpoint"
	case *agentpb.BackendConfig_AppChannel:
		return "app_channel"
	case *agentpb.BackendConfig_DirectMemory:
		return "direct_memory"
	case *agentpb.BackendConfig_MemoryRegions:
		return "memory_regions"
	default:
		return "default"
	}
}

// appliedBackendConfig is the outcome of applyBackendConfig, kept so a caller can roll back.
type appliedBackendConfig struct {
	job      *store.Job             // nil when nothing was recorded
	previous *agentpb.BackendConfig // value stored before this request
}

// rollback restores the config stored before the request was applied.
func (a appliedBackendConfig) rollback() {
	if a.job != nil {
		a.job.SetBackendConfig(a.previous)
	}
}

// applyBackendConfig records the backend config a Yield supplied on the job. A nil requested config
// is a no-op: the job keeps whatever it had. When the job has no store entry (its pods do not exist,
// e.g. a lock acquired before deploy was yielded again) the config is dropped with a warning, since
// nothing can be snapshotted or restored for it anyway. A stored config that differs from the request
// is replaced with a warning.
func (s *Server) applyBackendConfig(
	ctx context.Context, groupID, jobID string, requested *pb.JobBackendConfig,
) (appliedBackendConfig, error) {
	if requested == nil {
		return appliedBackendConfig{}, nil
	}
	job, err := s.jobStore.Get(ctx, groupID, jobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.WarnContext(ctx, "Ignoring backend_config: job has no pods yet, nothing to snapshot or restore",
				"backend", AgentBackendName(ToAgentBackendConfig(requested)))
			return appliedBackendConfig{}, nil
		}
		return appliedBackendConfig{}, fmt.Errorf("failed to get job %s: %w", jobID, err)
	}
	previous := job.BackendConfig()
	next := ToAgentBackendConfig(requested)
	if previous != nil && !proto.Equal(previous, next) {
		// Backend names alone can be identical (e.g. only the suspend mode changed), so include the
		// full configs in compact text form.
		slog.WarnContext(ctx, "backend_config differs from the config stored for the job; replacing it",
			"stored", AgentBackendName(previous), "requested", AgentBackendName(next),
			"storedConfig", previous.String(), "requestedConfig", next.String())
	}
	job.SetBackendConfig(next)
	return appliedBackendConfig{job: job, previous: previous}, nil
}
