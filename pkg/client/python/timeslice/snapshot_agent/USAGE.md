# SnapshotAgentClient Usage Guide

The `SnapshotAgentClient` is a high-level Python wrapper for the `SnapshotAgentService` gRPC API. It provides an idiomatic interface, handles resource management, and maps gRPC response messages to dedicated Python dataclasses.

## Installation

Ensure you have the package installed in your environment:

```bash
pip install .
```

## Basic Usage

### Context Manager (Recommended)
The client supports the context manager pattern to ensure the gRPC channel is automatically closed.

```python
from timeslice.snapshot_agent import SnapshotAgentClient

# Connect to the local Snapshot Agent
with SnapshotAgentClient(endpoint="localhost:9001") as client:
    status_resp = client.status()
    print(f"Number of jobs: {len(status_resp.job_statuses)}")
```

### Manual Management
If you prefer to manage the lifecycle manually:

```python
client = SnapshotAgentClient(endpoint="localhost:9001")
try:
    # Use the client
    pass
finally:
    client.close()
```

## Core Operations

### Snapshotting a Job

You can trigger a snapshot by providing the `job_id`. The `backend` defaults to `BACKEND_UNSPECIFIED` but can be explicitly set.

```python
# Returns a SnapshotResponse object immediately (asynchronous)
response = client.snapshot(job_id="my-job", backend="CUDA")
print(f"Started snapshot with operation_id: {response.operation_id}")
```

### Restoring a Job

```python
# Returns a RestoreResponse object immediately (asynchronous)
response = client.restore(job_id="my-job", backend="CUDA")
print(f"Started restoration with operation_id: {response.operation_id}")
```

### Polling for Completion

Use `wait_for_operation` to block until an operation finishes. It returns a `GetOperationResponse` object.

```python
result = client.wait_for_operation(response.operation_id, poll_interval_sec=0.5)
if result.status == "OPERATION_STATUS_COMPLETE":
    print(f"Success! Storage used: {result.storage_bytes} bytes")
else:
    print(f"Failed: {result.error}")
```

### Combined Helper Methods

For convenience, use the `_and_wait` methods. These return the final `GetOperationResponse` object.

```python
# Triggers snapshot and blocks until it finishes or fails
result = client.snapshot_and_wait(job_id="my-job", backend="CUDA")
print(f"Snapshot status: {result.status}")
```

## Monitoring Status

The `status()` method returns a `StatusResponse` object containing lists of `JobStatus` and `AcceleratorStatus`.

```python
status_resp = client.status()

for job in status_resp.job_statuses:
    print(f"Job {job.job_id} is in state {job.state}")

for acc in status_resp.accelerator_statuses:
    print(f"Accelerator {acc.id}: {acc.memory_used_bytes}/{acc.memory_total_bytes} bytes")
```

## Health Checks

The client supports the standard gRPC Health Checking Protocol. You can check the overall health or the health of a specific backend.

```python
# Check default health
health_resp = client.check_health()
print(f"Agent health: {health_resp.status}") # e.g., 'SERVING'

# Check specific backend health
health_resp = client.check_health(service="cuda")
print(f"CUDA backend health: {health_resp.status}")
```

## Error Handling

The client raises `SnapshotAgentError` for gRPC failures or unexpected errors.

```python
from timeslice.snapshot_agent import SnapshotAgentClient, SnapshotAgentError

try:
    with SnapshotAgentClient(endpoint="invalid-host:9001") as client:
        client.status()
except SnapshotAgentError as e:
    print(f"Caught error: {e}")
    if e.code:
        print(f"gRPC Status Code: {e.code}")
```
