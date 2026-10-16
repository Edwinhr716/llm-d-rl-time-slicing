# TimeSlice Orchestrator Python Client

The `TimeSliceOrchestratorClient` coordinates access to shared accelerators (GPUs/TPUs) among multiple jobs in a time-slice group. It ensures that only one job has exclusive access to the accelerator at any given time, driving the snapshot/restore cycle automatically behind the scenes.

## Usage

### Explicit Acquire & Release

You can explicitly control accelerator access using `acquire()` and `release()`. It is highly recommended to use a `try...finally` block to ensure the access is always released.

```python
from timeslice import TimeSliceOrchestratorClient
from timeslice.orchestrator import OrchestratorError

# Initialize client for a specific job in a shared GPU group
client = TimeSliceOrchestratorClient(
    target="orchestrator-service:50051",
    job_id="my-job-123",
    group_id="shared-gpu-group"
)

try:
    print("Waiting to acquire GPU access...")
    # Blocks until access is granted
    result = client.acquire(timeout_sec=60.0)
    print(f"Acquired! Waited {result.waited_ms}ms.")

    # --- Perform GPU intensive work here ---
    
finally:
    print("Releasing GPU access...")
    client.release()
    client.close()
```

### Context Manager (`on_accelerators`)

A cleaner and safer way to manage accelerator access is using the `on_accelerators()` context manager, which automatically handles acquisition and release. It yields an `AcquireResult` that you can inspect inside the block.

```python
from timeslice import TimeSliceOrchestratorClient

client = TimeSliceOrchestratorClient("orchestrator-service:50051", "my-job", "shared-gpu")

# Automatically acquires on enter, releases on exit (even if exceptions occur)
with client.on_accelerators(timeout_sec=30.0) as result:
    print(f"GPU access secured! Waited {result.waited_ms}ms.")
    # Run GPU kernels...
```

### Decorator Usage

The `on_accelerators()` method also supports decorator usage out of the box. Exclusive accelerator access is acquired when the decorated function is called and released when it returns.

```python
from timeslice import TimeSliceOrchestratorClient

client = TimeSliceOrchestratorClient("orchestrator-service:50051", "my-job", "shared-gpu")

@client.on_accelerators(timeout_sec=10.0)
def run_gpu_kernel():
    print("Running GPU kernel on accelerators...")
    # GPU work...

# Call the function; accelerator access is managed automatically
run_gpu_kernel()
```

### Optional Initialization & Dynamic Overrides

The `job_id` and `group_id` are optional on construction. If omitted, they **must** be provided dynamically during the method calls. You can also override the constructor-configured values on a per-call basis.

```python
from timeslice import TimeSliceOrchestratorClient

# Initialize without job_id and group_id
client = TimeSliceOrchestratorClient(target="orchestrator-service:50051")

# Pass them dynamically during acquire/release
client.acquire(job_id="job-A", group_id="group-A")
client.release(job_id="job-A", group_id="group-A")

# You can also use the context manager with overrides
with client.on_accelerators(job_id="job-B", group_id="group-B") as result:
    # Runs with job-B on group-B
    print(f"Acquired group-B! Waited {result.waited_ms}ms.")
```

---

### Backend Configuration

A job may choose which snapshot-agent backend parks and restores its context by passing `backend_config` to the constructor (sent on every `release`) or to an individual `release` call (overrides the constructor value for that call). `acquire` takes no backend config: a parked job is restored with the backend it was parked with. Builders live in `timeslice.orchestrator.configs` and are re-exported from `timeslice.orchestrator`:

| Builder | Backend | Notes |
| --- | --- | --- |
| `cuda_config()` or `"cuda"` | `cuda` | PIDs discovered by the agent from the job's pods |
| `tpu_config()` or `"tpu"` | `tpu` | PIDs discovered by the agent from the job's pods |
| `vllm_config(endpoints, mode=None, tags=None)` | `app_endpoint` | vLLM sleep/wake API |
| `sglang_config(endpoints, mode=None, tags=None)` | `app_endpoint` | SGLang memory-occupation API |
| `app_endpoint_config(app, endpoints, mode=None, tags=None)` | `app_endpoint` | Generic form of the two above |
| `app_channel_config(mode=None, tags=None)` or `"app_channel"` | `app_channel` | Workload registered a channel with the agent |

`mode` is `"offload"`, `"discard"` or `None` (application/workload default); `tags` selects memory regions such as `["weights", "kv_cache"]`.

```python
from timeslice import TimeSliceOrchestratorClient
from timeslice.orchestrator import app_channel_config, vllm_config

client = TimeSliceOrchestratorClient(
    "orchestrator-service:50051", "my-job", "shared-gpu",
    backend_config=app_channel_config(),
)

with client.on_accelerators():          # the release on exit carries app_channel
    ...

# Per-call override; the orchestrator stores the latest value it was given.
client.release(backend_config=vllm_config(["http://localhost:8000"], mode="discard"))
```

The orchestrator stores one config per job and forwards it on the job's snapshot and restore; a release without `backend_config` keeps the stored value, and a job that never passes one is served by the agent's default backend. Known issue: the stored config is not persisted across orchestrator restarts, so a job parked with a non-default backend when the orchestrator restarts is restored with the agent default. Invalid configs (unknown app, no endpoints, empty tags) are rejected client-side with `ValueError`, or by the orchestrator with `INVALID_ARGUMENT`.

## Comprehensive Example: Reinforcement Learning (RL) Job (Decorator Pattern)

In a typical Reinforcement Learning setup, a single training job might need to coordinate access across two different GPU resource pools: one dedicated to **Sampling** (generating experience rollouts) and another dedicated to **Training** (updating model weights).

Using `TimeSliceOrchestratorClient` as a **decorator** (`@client.on_accelerators()`), we can cleanly bind GPU access management directly to the execution functions. This keeps the orchestration loop in `main()` extremely clean and readable.

```python
import time
from timeslice import TimeSliceOrchestratorClient

# Configuration
ORCHESTRATOR_TARGET = "orchestrator-service:50051"

# A single Job ID representing this specific RL run
RL_JOB_ID = "rl-run-123"

# Two different time-slice groups representing different GPU resource pools
SAMPLING_GROUP = "sampling-gpu-pool"
TRAINING_GROUP = "training-gpu-pool"

# Initialize clients at the module level so they can be used as decorators
sampler_client = TimeSliceOrchestratorClient(ORCHESTRATOR_TARGET, job_id=RL_JOB_ID, group_id=SAMPLING_GROUP)
trainer_client = TimeSliceOrchestratorClient(ORCHESTRATOR_TARGET, job_id=RL_JOB_ID, group_id=TRAINING_GROUP)


# Decorate functions to automatically manage GPU access when they are invoked

@sampler_client.on_accelerators()
def deploy_sampler_pods():
    print(f"[Deployer] Launching sampler pods in group '{SAMPLING_GROUP}'...")
    time.sleep(1)

@trainer_client.on_accelerators()
def deploy_trainer_pods():
    print(f"[Deployer] Launching trainer pods in group '{TRAINING_GROUP}'...")
    time.sleep(1)

@sampler_client.on_accelerators()
def run_sampling_phase():
    print(f"[Samplers] Generating rollouts on GPUs in group '{SAMPLING_GROUP}'...")
    time.sleep(2)  # Simulating GPU load

@trainer_client.on_accelerators()
def run_training_phase():
    print(f"[Trainer] Updating policy weights on GPUs in group '{TRAINING_GROUP}'...")
    time.sleep(3)  # Simulating GPU load


def main():
    # 1. Deploy Sampler Pods
    # Calling this function automatically acquires the sampling group access
    print("\n=== Phase 1: Deploying Samplers ===")
    deploy_sampler_pods()
    print("[System] Samplers deployed and initialized.")

    # 2. Deploy Trainer Pods
    # Calling this function automatically acquires the training group access
    print("\n=== Phase 2: Deploying Trainers ===")
    deploy_trainer_pods()
    print("[System] Trainers deployed and initialized.")

    # 3. RL Orchestration Loop
    # Alternate GPU access between the sampling group and the training group
    print("\n=== Phase 3: Starting RL Training Loop ===")
    try:
        for iteration in range(1, 4):
            print(f"\n--- Iteration {iteration} ---")
            
            # --- Step A: Sampling ---
            # Calling this automatically locks the sampling group, runs, and releases
            print(f"[Orchestrator] Triggering Sampling on '{SAMPLING_GROUP}'...")
            run_sampling_phase()
            print(f"[Orchestrator] Sampling completed.")

            # --- Step B: Training ---
            # Calling this automatically locks the training group, runs, and releases
            print(f"[Orchestrator] Triggering Training on '{TRAINING_GROUP}'...")
            run_training_phase()
            print(f"[Orchestrator] Training completed.")
            
    finally:
        # Clean up client channels
        sampler_client.close()
        trainer_client.close()
        print("\n[System] RL Job completed and clients closed.")

if __name__ == "__main__":
    main()
```

---

## Development

To regenerate gRPC stubs for the TimeSlice Orchestrator, first install the pinned
codegen toolchain (generated code sets the package's minimum supported
grpcio/protobuf — regenerating with a newer toolchain silently raises the floors,
see `[dev]` in `pyproject.toml`):

```bash
pip install "grpcio-tools==1.81.0"
```

Then:

```bash
# Run from pkg/client/python directory
python3 -m grpc_tools.protoc \
    -Itimeslice/orchestrator/_generated=../../timeslice-orchestrator/api/v1alpha1 \
    --python_out=. \
    --grpc_python_out=. \
    timeslice/orchestrator/_generated/timeslice_orchestrator.proto
```

