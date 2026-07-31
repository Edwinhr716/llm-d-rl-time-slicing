# Plan: Integrating Direct Memory (Process-Level) Support in Snapshot Agent

This document outlines the plan to integrate support for the `direct_memory` (process-level) backend into the `snapshot-agent`. This plan is based on the proposal in [api_extension_proposal.md](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/api_extension_proposal.md) and the reference implementation in the `gcr-backend-memory-allocation` branch.

As per requirements, **we will not integrate the memory address variant** (`GPU_CR_MEMORY_ADDRESSES`). We will only implement the process-level `direct_memory` backend.

---

## 1. API Changes (Proto)

We need to extend the gRPC API to support the new `direct_memory` backend.

### Modifications to [snapshot_agent.proto](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1/snapshot_agent.proto):

1.  **Define `DirectMemoryBackendConfig`**:
    This configuration targets specific processes for checkpoint/restore, identical to how `CudaBackendConfig` works.

    ```proto
    // Configuration for the Direct Memory (process-level) backend.
    message DirectMemoryBackendConfig {
      // The target processes to checkpoint/restore.
      ProcessTarget explicit_target = 1;
    }
    ```

2.  **Extend `BackendConfig`**:
    Add `direct_memory` to the `oneof` field in `BackendConfig`.

    ```proto
    message BackendConfig {
      oneof backend {
        CudaBackendConfig cuda = 1;
        AppEndpointConfig app_endpoint = 2;
        AppChannelConfig app_channel = 3;
        DirectMemoryBackendConfig direct_memory = 4; // New backend
      }
    }
    ```

3.  **Regenerate Protos**:
    Run `go generate ./...` in the `pkg/snapshot-agent/api/v1alpha1` directory to update the Go proto bindings.

---

## 2. Go Backend Implementation

We will implement the `direct_memory` backend as a new struct that implements the `backends.Backend` interface.

### Steps:

1.  **Add Backend Type Constant** in [pkg/snapshot-agent/backends/checkpoint.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/backends/checkpoint.go):
    ```go
    const (
        // ...
        BackendDirectMemory BackendType = "direct-memory"
    )
    ```

2.  **Create [pkg/snapshot-agent/backends/direct_memory.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/backends/direct_memory.go)**:
    Implement the `DirectMemory` struct. It will invoke the `cr_client` binary.

    *   **CLI Commands**:
        *   **Checkpoint**: `cr_client -c -p <pid>`
        *   **Restore**: `cr_client -r -p <pid>`
    *   **Interface Implementation**:
        *   `Snapshot(ctx, req)`: Extracts PIDs from `req.Config.GetDirectMemory()`, validates they are present, and runs `cr_client -c -p <pid>` for each.
        *   `Restore(ctx, req)`: Extracts PIDs, validates, and runs `cr_client -r -p <pid>` for each.
        *   `HealthCheck(ctx)`: Verifies `cr_client` executable is available in `PATH` (or fallback `/bin/cr_client`).

3.  **Update Server Logic** in [pkg/snapshot-agent/server/server.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/server/server.go):
    *   Update `getSnapshotBackendType` to recognize `direct_memory`:
        ```go
        if config.GetDirectMemory() != nil {
            return backends.BackendDirectMemory
        }
        ```
    *   Update `buildSnapshotFn` and `buildRestoreFn` to support `backends.BackendDirectMemory` in `k8s` mode. It must perform PID discovery (similar to `BackendCuda`) if no PIDs are explicitly provided.
    *   Add helper `BuildDirectMemoryConfig(pids []string)` in `backends` package to wrap resolved PIDs back into `DirectMemoryBackendConfig`.

4.  **Register Backend** in [cmd/snapshot-agent/main.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/cmd/snapshot-agent/main.go):
    ```go
    registeredBackends := map[backends.BackendType]backends.Backend{
        backends.BackendCuda:        backends.NewCudaCheckpoint(),
        backends.BackendNoop:        backends.NewNoopBackend(),
        backends.BackendAppEndpoint: backends.NewAppEndpointBackend(),
        backends.BackendDirectMemory: backends.NewDirectMemory(), // Register Direct Memory
    }
    ```

---

## 3. Python Client Updates

The Python client library will be updated to support the new backend.

### Steps:

1.  **Regenerate Python Protos**:
    Regenerate `snapshot_agent_pb2.py` and `snapshot_agent_pb2_grpc.py` in `pkg/client/python/timeslice/snapshot_agent/` using `protoc` with the updated `snapshot_agent.proto`.

To maintain consistency with other backends, **no convenience helper methods will be added** to the client class. Workloads will construct the `DirectMemoryBackendConfig` directly using the generated protobuf classes.

---

## 4. Dockerfile & Helm Chart Changes

We will modify the Dockerfile and Helm chart to support deploying `snapshot-agent` with Direct Memory capabilities. Instead of installing `cr_client` in an init container at deploy time, we will download the pre-built `cr_client` binary directly into the image in the Dockerfile.

### Modifications to [docker/snapshot-agent/Dockerfile](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/docker/snapshot-agent/Dockerfile):
In the builder stage, download the pre-built `cr_client` binary from the GPU-CR v0.1.0 GitHub release into `/workspace/bin/cr_client`:

```dockerfile
# Download pre-built cr_client binary from GPU-CR GitHub release
RUN mkdir -p /workspace/bin && \
    curl -fsSL -o /workspace/bin/cr_client https://github.com/Edwinhr716/GPU-CR/releases/download/v0.1.0/cr_client && \
    chmod 755 /workspace/bin/cr_client
```

This binary is copied to `./bin` in the runtime distroless container alongside `cuda-checkpoint`, making `cr_client` available in `/bin/cr_client`.

### Modifications to [deploy/snapshot-agent/values.yaml](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/deploy/snapshot-agent/values.yaml):
Add configuration flags for Direct Memory (`repository` and `branch` are no longer required since `cr_client` is bundled in the container image):

```yaml
directMemory:
  enabled: false
  # Path on the host for hugepages/VRAM staging
  stagingHostPath: /var/tmp/huge-ckpt
  stagingMountPath: /mnt/huge-ckpt
```

### Modifications to [deploy/snapshot-agent/templates/daemonset.yaml](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/deploy/snapshot-agent/templates/daemonset.yaml):
1.  **Conditional `hostIPC`**: Set `hostIPC: true` if `directMemory.enabled` is true.
2.  **Volume Mounts**: Mount the staging path (`stagingHostPath` -> `stagingMountPath`, for hugepages/VRAM staging) into the main container if `directMemory.enabled` is true.

---

## 5. Testing Plan

### Unit Tests:

1.  **Backend Tests ([pkg/snapshot-agent/backends/direct_memory_test.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/backends/direct_memory_test.go))**:
    *   Test `Snapshot` and `Restore` with mocked command execution. Verify that `cr_client -c -p <pid>` and `cr_client -r -p <pid>` are called.
    *   Test validation (fail if no PIDs provided, fail if config is missing).
    *   Test `HealthCheck` (succeed if `cr_client` exists in path, fail otherwise).
2.  **Server Tests ([pkg/snapshot-agent/server/server_internal_test.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/pkg/snapshot-agent/server/server_internal_test.go))**:
    *   Test that `getSnapshotBackendType` resolves `direct_memory` backend type.
    *   Test `buildSnapshotFn` and `buildRestoreFn` in `standalone` mode (passes config through).
    *   Test `buildSnapshotFn` and `buildRestoreFn` in `k8s` mode (performs PID discovery and wraps them in `DirectMemoryBackendConfig`).

### Integration Tests:

1.  **Harness Update ([tests/integration/snapshot-agent/engines.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/tests/integration/snapshot-agent/engines.go))**:
    *   Modify the `agentPod` function in the integration test harness to install `cr_client` in the test agent pod by downloading the pre-built binary from `https://github.com/Edwinhr716/GPU-CR/releases/download/v0.1.0/cr_client`.
2.  **Test Cases**:
    *   Add `DirectMemoryCheckpointRestore` in [standalone_test.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/tests/integration/snapshot-agent/standalone_test.go): Verify process-level checkpoint and restore using `direct_memory` backend on a running engine (e.g. vLLM).
    *   Add `DirectMemoryWatcherDiscoveredPIDs` in [k8s_test.go](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/tests/integration/snapshot-agent/k8s_test.go): Verify automatic PID discovery works with `direct_memory` backend in K8s mode.
3.  **Python Helper Update ([tests/integration/snapshot-agent/agentctl.py](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/tests/integration/snapshot-agent/agentctl.py))**:
    *   Add `direct_memory` to `--backend` choices.
    *   Add logic in `build_config` to construct `DirectMemoryBackendConfig`.

---

## 6. Documentation

Update [guides/snapshot-agent/README.md](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/guides/snapshot-agent/README.md):

1.  **Add `Direct Memory` to Backends Table**:
    Add a row for `Direct Memory` detailing its mechanism (`cr_client`), expected VRAM freed (100%), and approximate resume time.
2.  **Add `Direct Memory` Detailed Section**:
    Provide a python code example showing how to construct `DirectMemoryBackendConfig` and call `snapshot_and_wait`/`restore_and_wait` using the Python client.
3.  **Update K8s Mode Section**:
    Document that in K8s mode, the `direct_memory` backend can also be used without explicit PIDs (relying on watcher discovery).

---

## 7. Recommended PR Breakdown

To ensure easy reviews and that each PR passes the CI/CD pipeline (linter, unit tests), we propose splitting the implementation into 4 independent PRs:

### PR 1: API Schema Definition & Python Client Update
*   **Changes**:
    *   `snapshot_agent.proto` (API extension)
    *   Regenerated Go protos (`snapshot_agent.pb.go`, etc.)
    *   Regenerated Python protos (`snapshot_agent_pb2.py`, etc.)
*   **Validation**:
    *   Go build/test (ensures regenerated protos compile).
    *   Python linter (ruff) check.
*   **Merge State**: Safe to merge, doesn't impact runtime yet since backend is not registered.

### PR 2: Go Backend Implementation & Unit Tests
*   **Changes**:
    *   `backends/checkpoint.go` (BackendType constant)
    *   `backends/direct_memory.go` (DirectMemory implementation)
    *   `backends/direct_memory_test.go` (Unit tests for DirectMemory backend)
    *   `server/server.go` (Server integration and PID discovery for Direct Memory)
    *   `server/server_internal_test.go` (Unit tests for server integration)
    *   `cmd/snapshot-agent/main.go` (Registering DirectMemory backend)
*   **Validation**:
    *   `make build` (Go compile check)
    *   `make test` (Runs all unit tests, including new `direct_memory_test.go` and updated `server_internal_test.go`).
*   **Merge State**: Safe to merge, backend is fully functional in unit tests but not exposed in Helm or integration tests yet.

### PR 3: Dockerfile & Helm Chart Updates
*   **Changes**:
    *   `docker/snapshot-agent/Dockerfile` (download pre-built `cr_client` binary from GPU-CR GitHub release v0.1.0 into `/workspace/bin`)
    *   `deploy/snapshot-agent/values.yaml` (adding `directMemory` section with staging paths)
    *   `deploy/snapshot-agent/templates/daemonset.yaml` (conditional `hostIPC` and staging volume mount)
*   **Validation**:
    *   `docker build -t snapshot-agent-test -f docker/snapshot-agent/Dockerfile .` (ensures image builds with `cr_client`).
    *   `helm lint deploy/snapshot-agent` (ensures Helm syntax is correct).
*   **Merge State**: Safe to merge, updates deployment and container image configurations.

### PR 4: Integration Tests & Documentation
*   **Changes**:
    *   `tests/integration/snapshot-agent/engines.go` (install pre-built `cr_client` binary in test agent pod)
    *   `tests/integration/snapshot-agent/agentctl.py` (CLI update)
    *   `tests/integration/snapshot-agent/standalone_test.go` (Direct Memory test cases)
    *   `tests/integration/snapshot-agent/k8s_test.go` (Direct Memory test cases)
    *   `guides/snapshot-agent/README.md` (Documentation update)
*   **Validation**:
    *   Manual integration test execution (using the gate mechanism: `needs-integration-test` label will be added, developer runs tests and removes it).
*   **Merge State**: Feature complete.
