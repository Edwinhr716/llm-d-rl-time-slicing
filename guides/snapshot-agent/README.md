# Snapshot Agent

The Snapshot Agent provides GPU checkpoint/restore primitives to enable efficient resource sharing for GPU-bound workloads. By allowing processes to save and reload their entire GPU state, it enables scenarios where multiple high-memory workloads can share the same physical GPU hardware.

## Use Cases

1.  **Multi-tenant GPU Time-slicing:** Enable multiple tenants to share GPU nodes by time-slicing their workloads (e.g., RL trainers and samplers). The Snapshot Agent provides the primitives to preempt and resume these jobs transparently.
2.  **Multi-model vLLM Server Proxy:** Serve multiple models (e.g., same family such as Gemma4 or Qwen3) on the same GPU by taking turns for inference requests. A proxy can snapshot the GPU state of one model and restore another to handle incoming requests for different models without requiring them to fit in GPU memory simultaneously.

## Overview

The Snapshot Agent is a gRPC service that runs as a DaemonSet on each GPU node. It provides a local interface for application pods to:
- **Snapshot:** Save the current state of a GPU job.
- **Restore:** Reload a previously saved state to the GPU.
- **Status:** Monitor the current state of jobs and accelerators.

By using the Snapshot Agent, you can implement context switching without modifying the core engine code, simply by wrapping your execution loop with the `timeslice.SnapshotAgentClient` client library.

## Architecture

1.  **Snapshot Agent (DaemonSet):** Runs on every node with GPUs. It has privileged access to the GPU devices and host paths required for snapshotting.
2.  **Workload:** A GPU-bound process (e.g., a sampler, trainer, or vLLM instance) running in a pod.
3.  **Snapshot Agent Client:** A Python library used by the application pod to communicate with the local Snapshot Agent via gRPC (port 9001).

---

## 1. Deploying the Snapshot Agent

The Snapshot Agent must be deployed as a DaemonSet to ensure it is available on every node.

### Using Helm
Follow the instructions in [helm-snapshot-agent.md](../../deploy/snapshot-agent/README.md) to deploy the Snapshot Agent.

### Key Configuration (`values.yaml`)
- `port`: The gRPC port (default: `9001`).
- `securityContext.privileged`: Must be `true` to access GPU registers.
- `nvidia.driver.hostPath`: Path to NVIDIA driver binaries on the host (e.g., `/home/kubernetes/bin/nvidia`).

See [helm-snapshot-agent.md](../../deploy/snapshot-agent/README.md) for more details on the Helm chart.

---

## 2. Integrating with Workloads

To enable a pod for snapshotting, you need to add specific labels and environment variables to your Deployment.

### Required Labels
The Snapshot Agent identifies pods that should be managed via labels:
- `timeslice.io/job-id: "<unique-job-id>"`: A unique identifier for the job.

### Environment Variables
The pod needs to know the IP of the node it is running on to connect to the local Snapshot Agent.

```yaml
env:
  - name: NODE_IP
    valueFrom:
      fieldRef:
        fieldPath: status.hostIP
  - name: AGENT_ENDPOINT
    value: "$(NODE_IP):9001"
```

---

## 3. Using the Python Client

The `timeslice.SnapshotAgentClient` library provides a high-level Python API for interacting with the Snapshot Agent.

### Installation
From the local repository:
```bash
cd ../../pkg/client/python
pip install .
```

Or install from the remote repository:
```bash
pip install "git+https://github.com/llm-d-incubation/llm-d-rl-time-slicing.git#subdirectory=pkg/client/python"
```

### Choosing a Backend
The `backend` parameter controls how GPU state is checkpointed. It is an optional argument in the following `SnapshotAgentClient` methods:

*   `snapshot(job_id, backend=...)`
*   `restore(job_id, backend=...)`
*   `snapshot_and_wait(job_id, backend=...)`
*   `restore_and_wait(job_id, backend=...)`

Available backends:
*   **BACKEND_CUDA (default):** Full process-level GPU checkpoint via `cuda-checkpoint`. Suitable for any GPU-bound component (trainer, sampler, LLM engine) running full model weights.
*   Additional backends for lighter-weight workloads (e.g. LoRA adapters) are planned for a future release.

### Basic Workflow
The most common usage is to trigger a snapshot at the end of a "slice" or request batch and a restore at the beginning of the next one.

```python
import os
from timeslice.snapshot_agent import SnapshotAgentClient

# AGENT_ENDPOINT is typically $(NODE_IP):9001
endpoint = os.getenv("AGENT_ENDPOINT", "localhost:9001")
job_id = "test-job"

with SnapshotAgentClient(endpoint) as client:
    # 1. Trigger Snapshot and wait for completion
    print("Snapshotting...")
    result = client.snapshot_and_wait(job_id)
    if result.status == "OPERATION_STATUS_COMPLETE":
        print(f"Snapshot success! Completed in {result.elapsed_ms} ms")

    # ... wait for your turn or next request (orchestrated externally) ...

    # 2. Trigger Restore and wait for completion
    print("Restoring...")
    result = client.restore_and_wait(job_id)
    if result.status == "OPERATION_STATUS_COMPLETE":
        print(f"Restore success! Completed in {result.elapsed_ms} ms")
```

### Advanced Usage
For more granular control, you can use asynchronous methods and poll for status manually.

```python
# Start snapshot
response = client.snapshot(job_id)
op_id = response.operation_id

# Do other work...

# Wait for completion
result = client.wait_for_operation(op_id)
```

---

## 4. Monitoring and Troubleshooting

### Checking Agent Status
You can query the agent for the status of all managed jobs:
```python
status = client.status()
for job in status.job_statuses:
    print(f"Job {job.job_id}: {job.state}")
```

### Direct gRPC Access
For debugging, you can use `grpcurl`:
```bash
# Get node IP first
NODE_IP=$(kubectl get pod <agent-pod> -o jsonpath='{.status.hostIP}')

grpcurl -plaintext -d '{"job_id": "test-job"}' \
  $NODE_IP:9001 snapshot_agent.v1alpha1.SnapshotAgentService/Snapshot
```

### Common Issues
- **Permission Denied:** Ensure the Snapshot Agent pod is running as `privileged: true`.
- **Connection Refused:** Verify the `AGENT_ENDPOINT` environment variable correctly points to `$(NODE_IP):9001`.
- **GPU Not Found:** Check that the `nvidia.driver.hostPath` in the agent's configuration matches your node's setup.
