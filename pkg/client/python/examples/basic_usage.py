import logging
import sys
import os

# Add the parent directory to sys.path so we can import the package without installing it
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from timeslice import SnapshotAgentClient

logging.basicConfig(level=logging.INFO)

def main():
    endpoint = os.getenv("AGENT_ENDPOINT", "localhost:9001")
    job_id = "test-job"
    group = "default"

    print(f"Connecting to {endpoint}...")
    with SnapshotAgentClient(endpoint) as client:
        # 1. Snapshot
        print("Triggering snapshot...")
        resp = client.snapshot(job_id, group)
        if not resp:
            print("Failed to trigger snapshot")
            return

        operation_id = resp.operation_id
        print(f"Snapshot operation ID: {operation_id}")

        # 2. Get Operation (Poll)
        print("Polling operation status...")
        op = client.get_operation(operation_id)
        if op:
            print(f"Operation status: {op.status}")
            print(f"Elapsed time: {op.elapsed_ms}ms")

        # 3. Restore
        print("Triggering restore...")
        resp = client.restore(job_id, group)
        if resp:
            print(f"Restore operation ID: {resp.operation_id}")

if __name__ == "__main__":
    main()
