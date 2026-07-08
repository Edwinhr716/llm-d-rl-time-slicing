import os
import sys
import time
import torch
import grpc

# Add path to client library
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../llm-d-rl-time-slicing/pkg/client/python')))

from timeslice.snapshot_agent.client import SnapshotAgentClient
from timeslice.snapshot_agent import snapshot_agent_pb2

def main():
    # 1. Initialize PyTorch and allocate memory
    if not torch.cuda.is_available():
        print("CUDA not available. This script requires a GPU to run.")
        sys.exit(1)

    device = torch.device("cuda")
    print(f"Using device: {device}")

    # Allocate a tensor
    size_elements = 1024 * 1024 # 1M floats = 4MB
    tensor = torch.arange(size_elements, dtype=torch.float32, device=device)
    
    addr = tensor.data_ptr()
    size_bytes = tensor.element_size() * tensor.nelement()
    pid = os.getpid()

    print(f"Allocated tensor at address: {hex(addr)}, size: {size_bytes} bytes, PID: {pid}")

    # Verify initial data
    original_data = tensor.clone()
    print("Original data (first 5):", tensor[:5].cpu().tolist())

    # 2. Connect to Snapshot Agent
    # Assume Snapshot Agent is running on localhost:9001
    agent_endpoint = "localhost:9001"
    print(f"Connecting to Snapshot Agent at {agent_endpoint}...")
    
    try:
        client = SnapshotAgentClient(endpoint=agent_endpoint)
        client.check_health()
        print("Connected to Snapshot Agent successfully.")
    except Exception as e:
        print(f"Failed to connect to Snapshot Agent: {e}")
        print("Please make sure the Snapshot Agent server is running.")
        sys.exit(1)

    # Prepare target string: "pid:addr:size"
    target = f"{pid}:{hex(addr)}:{size_bytes}"
    print(f"Target spec: {target}")

    job_id = "test-job"
    group = "test-group"

    # 3. Trigger Snapshot
    print("Triggering snapshot...")
    backend = snapshot_agent_pb2.BACKEND_GPU_CR_MEMORY_ADDRESSES
    
    try:
        resp = client.snapshot_and_wait(job_id=job_id, group=group, backend=backend, memory_addresses=[target])
        print("Snapshot response:", resp)
        if resp.status != "OPERATION_STATUS_COMPLETE":
            print(f"Snapshot failed: {resp.error}")
            sys.exit(1)
    except Exception as e:
        print(f"Snapshot call failed: {e}")
        sys.exit(1)

    # 4. Modify memory content
    print("Modifying tensor content in GPU memory...")
    tensor.fill_(0.0)
    print("Modified data (first 5):", tensor[:5].cpu().tolist())

    # 5. Trigger Restore
    print("Triggering restore...")
    try:
        resp = client.restore_and_wait(job_id=job_id, group=group, backend=backend, memory_addresses=[target])
        print("Restore response:", resp)
        if resp.status != "OPERATION_STATUS_COMPLETE":
            print(f"Restore failed: {resp.error}")
            sys.exit(1)
    except Exception as e:
        print(f"Restore call failed: {e}")
        sys.exit(1)

    # 6. Validate data
    print("Validating restored data...")
    if torch.equal(tensor, original_data):
        print("SUCCESS: Data restored correctly!")
    else:
        print("FAILURE: Restored data does not match original data.")
        print("Restored data (first 5):", tensor[:5].cpu().tolist())
        sys.exit(1)

if __name__ == "__main__":
    main()
