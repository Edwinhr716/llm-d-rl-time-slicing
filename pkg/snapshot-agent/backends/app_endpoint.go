package backends

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

const defaultHTTPTimeout = 30 * time.Second

// AppProfile defines the HTTP API dialect of an application-aware
// workload. Adding support for a new HTTP-capable application means adding a
// profile here and a value to the App enum — no new proto message.
type AppProfile struct {
	Name                 string
	BuildSnapshotRequest func(ctx context.Context, endpoint string, mode pb.SuspendMode, tags []string) (*http.Request, error)
	BuildRestoreRequest  func(ctx context.Context, endpoint string, tags []string) (*http.Request, error)
	// BuildSleepStateRequest, if set, asks whether the application is
	// suspended; the response is parsed by ParseSleepState.
	BuildSleepStateRequest func(ctx context.Context, endpoint string) (*http.Request, error)
	ParseSleepState        func(body []byte) (bool, error)
	// BuildResetCacheRequest, if set, drops the application's reusable
	// cache (for vLLM, the prefix cache), so that nothing computed before a
	// suspend is served after the resume.
	BuildResetCacheRequest func(ctx context.Context, endpoint string) (*http.Request, error)
}

// ErrUnsupported means the application's profile has no such operation.
var ErrUnsupported = errors.New("operation not supported by the application profile")

// vllmProfile speaks vLLM's dev-mode sleep API. SuspendMode maps to the
// per-call sleep level: OFFLOAD (default) -> level 1, DISCARD -> level 2.
var vllmProfile = &AppProfile{
	Name: "vllm",
	BuildSnapshotRequest: func(ctx context.Context, endpoint string, mode pb.SuspendMode, _ []string) (*http.Request, error) {
		u, err := appURL(endpoint, "sleep")
		if err != nil {
			return nil, err
		}
		level := "1"
		if mode == pb.SuspendMode_SUSPEND_MODE_DISCARD {
			level = "2"
		}
		q := u.Query()
		q.Set("level", level)
		u.RawQuery = q.Encode()
		return http.NewRequestWithContext(ctx, http.MethodPost, u.String(), http.NoBody)
	},
	BuildRestoreRequest: func(ctx context.Context, endpoint string, tags []string) (*http.Request, error) {
		u, err := appURL(endpoint, "wake_up")
		if err != nil {
			return nil, err
		}
		if len(tags) > 0 {
			q := u.Query()
			for _, tag := range tags {
				q.Add("tags", tag)
			}
			u.RawQuery = q.Encode()
		}
		return http.NewRequestWithContext(ctx, http.MethodPost, u.String(), http.NoBody)
	},
	BuildSleepStateRequest: func(ctx context.Context, endpoint string) (*http.Request, error) {
		u, err := appURL(endpoint, "is_sleeping")
		if err != nil {
			return nil, err
		}
		return http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	},
	ParseSleepState: func(body []byte) (bool, error) {
		var v struct {
			IsSleeping *bool `json:"is_sleeping"`
		}
		if err := json.Unmarshal(body, &v); err != nil {
			return false, fmt.Errorf("decode is_sleeping response: %w", err)
		}
		if v.IsSleeping == nil {
			return false, fmt.Errorf("is_sleeping response has no is_sleeping field: %s", string(body))
		}
		return *v.IsSleeping, nil
	},
	BuildResetCacheRequest: func(ctx context.Context, endpoint string) (*http.Request, error) {
		u, err := appURL(endpoint, "reset_prefix_cache")
		if err != nil {
			return nil, err
		}
		return http.NewRequestWithContext(ctx, http.MethodPost, u.String(), http.NoBody)
	},
}

// sglangProfile speaks SGLang's memory occupation API. SuspendMode is
// advisory here: whether released weights are preserved is fixed by the
// server's launch flags (--enable-weights-cpu-backup), not per call.
var sglangProfile = &AppProfile{
	Name: "sglang",
	BuildSnapshotRequest: func(ctx context.Context, endpoint string, _ pb.SuspendMode, tags []string) (*http.Request, error) {
		return buildSGLangRequest(ctx, endpoint, "release_memory_occupation", tags)
	},
	BuildRestoreRequest: func(ctx context.Context, endpoint string, tags []string) (*http.Request, error) {
		return buildSGLangRequest(ctx, endpoint, "resume_memory_occupation", tags)
	},
}

// appURL parses the base endpoint once and joins the operation path onto it,
// preserving any base path and query while normalizing slashes.
func appURL(endpoint, op string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	return u.JoinPath(op), nil
}

// appProfiles maps the App enum to its HTTP dialect.
var appProfiles = map[pb.App]*AppProfile{
	pb.App_APP_VLLM:   vllmProfile,
	pb.App_APP_SGLANG: sglangProfile,
}

// buildSGLangRequest builds a JSON POST request. SGLang requires a JSON body
// on these endpoints even when no tags are given, so an empty object is sent
// rather than an empty body.
func buildSGLangRequest(ctx context.Context, endpoint, op string, tags []string) (*http.Request, error) {
	u, err := appURL(endpoint, op)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{}
	if len(tags) > 0 {
		payload["tags"] = tags
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// AppEndpointBackend implements Backend for application-aware workloads
// reached through their HTTP APIs (the app_endpoint transport). The dialect
// is selected per request from the config's App field.
type AppEndpointBackend struct {
	client *http.Client
}

// NewAppEndpointBackend creates the app-endpoint backend.
func NewAppEndpointBackend() *AppEndpointBackend {
	return &AppEndpointBackend{
		client: &http.Client{Timeout: defaultHTTPTimeout},
	}
}

// NewAppEndpointBackendWithClient creates the app-endpoint backend with the
// given HTTP client, for example one without keep-alive connections for a
// caller that resets the application's connections between two calls.
func NewAppEndpointBackendWithClient(client *http.Client) *AppEndpointBackend {
	return &AppEndpointBackend{client: client}
}

// resolve validates the config and returns the dialect profile and config.
func (b *AppEndpointBackend) resolve(config *pb.BackendConfig) (*AppProfile, *pb.AppEndpointConfig, error) {
	appCfg := config.GetAppEndpoint()
	if appCfg == nil {
		return nil, nil, fmt.Errorf("app_endpoint config is required")
	}
	profile, ok := appProfiles[appCfg.GetApp()]
	if !ok {
		return nil, nil, fmt.Errorf("unknown or unspecified app %q", appCfg.GetApp())
	}
	if len(appCfg.GetEndpoints()) == 0 {
		return nil, nil, fmt.Errorf("at least one endpoint is required for %s", profile.Name)
	}
	return profile, appCfg, nil
}

// Snapshot suspends the application on all configured endpoints.
func (b *AppEndpointBackend) Snapshot(ctx context.Context, req Request) error {
	profile, appCfg, err := b.resolve(req.Config)
	if err != nil {
		return err
	}
	for _, ep := range appCfg.GetEndpoints() {
		req, err := profile.BuildSnapshotRequest(ctx, ep, appCfg.GetMode(), appCfg.GetTags())
		if err != nil {
			return fmt.Errorf("failed to build snapshot request for %s: %w", ep, err)
		}
		slog.InfoContext(ctx, "Sending app snapshot request",
			"app", profile.Name, "endpoint", ep, "url", req.URL.Redacted())
		start := time.Now()
		if err := b.doRequest(req); err != nil {
			return fmt.Errorf("snapshot request to %s failed: %w", ep, err)
		}
		slog.InfoContext(ctx, "App snapshot request completed",
			"app", profile.Name, "endpoint", ep, "duration", time.Since(start))
	}
	return nil
}

// Restore resumes the application on all configured endpoints.
func (b *AppEndpointBackend) Restore(ctx context.Context, req Request) error {
	profile, appCfg, err := b.resolve(req.Config)
	if err != nil {
		return err
	}
	for _, ep := range appCfg.GetEndpoints() {
		req, err := profile.BuildRestoreRequest(ctx, ep, appCfg.GetTags())
		if err != nil {
			return fmt.Errorf("failed to build restore request for %s: %w", ep, err)
		}
		slog.InfoContext(ctx, "Sending app restore request",
			"app", profile.Name, "endpoint", ep, "url", req.URL.Redacted())
		start := time.Now()
		if err := b.doRequest(req); err != nil {
			return fmt.Errorf("restore request to %s failed: %w", ep, err)
		}
		slog.InfoContext(ctx, "App restore request completed",
			"app", profile.Name, "endpoint", ep, "duration", time.Since(start))
	}
	return nil
}

// HealthCheck returns nil — endpoints are supplied per-request in the
// BackendConfig, so there is nothing to probe at the backend level.
func (b *AppEndpointBackend) HealthCheck(_ context.Context) error {
	return nil
}

// IsSuspended asks every configured endpoint whether the application is
// suspended, and reports true only if all are. It returns ErrUnsupported
// when the application's profile cannot tell.
func (b *AppEndpointBackend) IsSuspended(ctx context.Context, config *pb.BackendConfig) (bool, error) {
	profile, appCfg, err := b.resolve(config)
	if err != nil {
		return false, err
	}
	if profile.BuildSleepStateRequest == nil || profile.ParseSleepState == nil {
		return false, fmt.Errorf("%s sleep state: %w", profile.Name, ErrUnsupported)
	}
	all := true
	for _, ep := range appCfg.GetEndpoints() {
		req, err := profile.BuildSleepStateRequest(ctx, ep)
		if err != nil {
			return false, fmt.Errorf("failed to build sleep state request for %s: %w", ep, err)
		}
		body, err := b.doRequestBody(req)
		if err != nil {
			return false, fmt.Errorf("sleep state request to %s failed: %w", ep, err)
		}
		sleeping, err := profile.ParseSleepState(body)
		if err != nil {
			return false, fmt.Errorf("sleep state of %s: %w", ep, err)
		}
		all = all && sleeping
	}
	return all, nil
}

// ResetCache drops the application's reusable cache on every configured
// endpoint. It returns ErrUnsupported when the profile has no such call.
func (b *AppEndpointBackend) ResetCache(ctx context.Context, config *pb.BackendConfig) error {
	profile, appCfg, err := b.resolve(config)
	if err != nil {
		return err
	}
	if profile.BuildResetCacheRequest == nil {
		return fmt.Errorf("%s cache reset: %w", profile.Name, ErrUnsupported)
	}
	for _, ep := range appCfg.GetEndpoints() {
		req, err := profile.BuildResetCacheRequest(ctx, ep)
		if err != nil {
			return fmt.Errorf("failed to build cache reset request for %s: %w", ep, err)
		}
		if err := b.doRequest(req); err != nil {
			return fmt.Errorf("cache reset request to %s failed: %w", ep, err)
		}
	}
	return nil
}

func (b *AppEndpointBackend) doRequest(req *http.Request) error {
	_, err := b.doRequestBody(req)
	return err
}

// doRequestBody sends req and returns the first 64 KiB of a 2xx response's
// body.
func (b *AppEndpointBackend) doRequestBody(req *http.Request) ([]byte, error) {
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return nil, fmt.Errorf("unexpected status %d (failed to read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	return body, nil
}
