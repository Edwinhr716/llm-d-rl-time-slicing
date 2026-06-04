# Timeslice Python SDK

This is the Python library for Timeslice, including the Snapshot Agent client.

## Installation

```bash
pip install .
```

Or from source:

```bash
pip install timeslice
```

## Usage

```python
from timeslice import SnapshotAgentClient

with SnapshotAgentClient("localhost:9001") as client:
    # Trigger a snapshot
    resp = client.snapshot(job_id="my-job", group="default")
```

## Development

To generate gRPC stubs:

```bash
pip install grpcio-tools
python -m grpc_tools.protoc -I../../snapshot-agent/api/v1alpha1 --python_out=timeslice/snapshot_agent --grpc_python_out=timeslice/snapshot_agent ../../snapshot-agent/api/v1alpha1/snapshot_agent.proto
```

You will need to fix the imports in the generated files (e.g., `import snapshot_agent_pb2 as snapshot__agent__pb2` -> `from . import snapshot_agent_pb2 as snapshot__agent__pb2`).
