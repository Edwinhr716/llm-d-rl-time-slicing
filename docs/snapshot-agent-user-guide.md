# User Guide

This guide provides step-by-step instructions on how to set up a GKE cluster, configure HugePages, build your container with the GPU-CR preloader, deploy the Snapshot Agent, and orchestrate selective memory swaps in your Python workload.

## Step 1: Create a GKE Cluster and Nodepool with HugePages

GPU-CR requires HugePages to be pre-allocated on the host nodes to act as a staging buffer for GPU memory dumps.

1. **Create a HugePage Config File (`hugepages-config.yaml`)**: Create a file named `hugepages-config.yaml` with the following content to allocate 60 GiB of 2Mi HugePages:

```
linuxConfig:
  hugepageConfig:
    hugepage_size2m: 30720 # 30720 * 2MB = 60 GiB
```

2. **Create a GKE Standard Cluster**: *Autopilot clusters are not supported due to hostPath and custom Linux system configuration restrictions.*

```shell
gcloud container clusters create timeslice-cluster \
    --project=<your-project-id> \
    --zone=asia-southeast1-a \
    --enable-dataplane-v2 \
    --num-nodes=1
```

3. **Create the GPU Node Pool with HugePages**: Create a GPU-enabled node pool (e.g., L4 GPUs) applying the `hugepages-config.yaml` system configuration:

```shell
gcloud beta container node-pools create gpu-hugepages-pool \
    --cluster=timeslice-cluster \
    --zone=asia-southeast1-a \
    --machine-type=g2-standard-24 \
    --accelerator=type=nvidia-l4,count=1 \
    --num-nodes=1 \
    --system-config-from-file=hugepages-config.yaml
```

## Step 2: Build the Workload Container Image

Your workload container must include the C++ `vGPU-NVIDIA.so` preloader library and the Snapshot Agent Python client.

Add the following multi-stage build block to your workload's **Dockerfile**:

```
# --- Stage 1: Build GPU-CR Preloader ---
FROM nvidia/cuda:13.0.0-devel-ubuntu22.04 AS gpu-cr-builder
RUN apt-get update && apt-get install -y git cmake build-essential

# Clone the GPU-CR repository (memory-allocation branch)
RUN git clone -b memory-allocation https://github.com/Edwinhr716/GPU-CR.git /tmp/GPU-CR

# Apply signal patches and compile the library
RUN cd /tmp/GPU-CR && \
    sed -i 's/#define CR_CKPT_SIGNAL     SIGUSR1/#define CR_CKPT_SIGNAL     (SIGRTMAX - 8)/' src/common.h && \
    sed -i 's/#define CR_RESTORE_SIGNAL  SIGUSR2/#define CR_RESTORE_SIGNAL  (SIGRTMAX - 7)/' src/common.h && \
    sed -i 's/int fd_control = open(control_name, O_CREAT | O_RDWR, 0755);/int fd_control = open(control_name, O_CREAT | O_RDWR, 0777); fchmod(fd_control, 0777);/' src/comm/share_mem.cpp && \
    mkdir build && \
    cd build && \
    cmake -DGPU_VENDOR=NVIDIA .. && \
    make -j$(nproc)

# --- Stage 2: Main Workload (e.g., vLLM) ---
FROM vllm/vllm-openai:v0.22.0
USER root

# 1. Copy the compiled preloader library to a standard search path
COPY --from=gpu-cr-builder /tmp/GPU-CR/build/vGPU-NVIDIA.so /usr/local/lib/

# 2. Install the Snapshot Agent Python Client library directly from GitHub
RUN pip install --no-cache-dir --upgrade protobuf grpcio grpcio-tools && \
    pip install --no-cache-dir \
    "git+https://github.com/Edwinhr716/llm-d-rl-time-slicing.git@gcr-backend-memory-allocation#subdirectory=pkg/client/python"

# 3. Copy your application scripts (e.g. lora_swap_script.py)
# COPY lora_swap_script.py /app/
```

Build and push this image to your container registry:

```shell
IMAGE_URI="gcr.io/<project-id>/vllm-gpu-cr:latest"
docker build -t $IMAGE_URI .
docker push $IMAGE_URI
```

## Step 3: Deploy the Snapshot Agent (DaemonSet)

1. **Clone the Snapshot Agent repository**:

```shell
git clone -b gcr-backend-memory-allocation https://github.com/Edwinhr716/llm-d-rl-time-slicing.git
cd llm-d-rl-time-slicing/deploy/snapshot-agent
```

2. **Configure values.yaml**: Open `values.yaml` and update the `resources` block to request HugePages. This allows the agent container's cgroup to write snapshot copies to the HugePage filesystem:

```
resources:
  requests:
    hugepages-2Mi: "2Gi"
    memory: "512Mi"
  limits:
    hugepages-2Mi: "2Gi"
    memory: "512Mi"
```

3. **Deploy using Helm**:

```shell
helm install snapshot-agent . -n timeslice-system --create-namespace
```

## Step 4: Deploy your Workload Pod

Your workload deployment YAML must be configured to mount the HugePage volume and enable host PID/IPC namespaces so the Snapshot Agent can communicate with it.

Add the following sections to your **workload deployment YAML**:

```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vllm-lora-swap
spec:
  template:
    spec:
      hostIPC: true   # REQUIRED: Share host IPC for GPU-CR communication channel
      hostPID: true   # REQUIRED: Share host PID namespace so Agent can resolve PIDs
      containers:
      - name: vllm-container
        image: gcr.io/<project-id>/vllm-gpu-cr:latest # Your built image
        env:
        - name: LD_PRELOAD
          value: "/usr/local/lib/vGPU-NVIDIA.so"      # REQUIRED: Preload vGPU library
        - name: EXPORT_FILE_PATH
          value: "/mnt/huge-ckpt"
 - name: NODE_IP
   valueFrom:
    fieldRef:
      fieldPath: status.hostIP
  - name: AGENT_ENDPOINT
    value: "$(NODE_IP):9001"
        # ... other environment variables ...
        volumeMounts:
        - name: huge-ckpt
          mountPath: /mnt/huge-ckpt                   # REQUIRED: Mount target path
        resources:
          requests:
            hugepages-2Mi: "40Gi"                     # REQUIRED: Reserve HugePages for the staging buffer
            memory: "4Gi"
          limits:
            hugepages-2Mi: "40Gi"
            memory: "4Gi"
      volumes:
      - name: huge-ckpt
        hostPath:
          path: /var/tmp/huge-ckpt                    # Must match the host mount path
          type: DirectoryOrCreate
```

Deploy the workload:

```shell
kubectl apply -f workload-deployment.yaml
```

## Step 5: Orchestrate Swaps in Python

Inside your Python application (running in the workload container), use the `SnapshotAgentClient` to snapshot and restore specific memory locations (e.g. weights in your GPU LoRA slots).

### Example Swapping Logic:

```py
import os
from timeslice.snapshot_agent.client import SnapshotAgentClient
from timeslice.snapshot_agent import snapshot_agent_pb2

# Connect to the local Snapshot Agent running on the node
agent_endpoint = os.environ.get("AGENT_ENDPOINT", "localhost:9001")
client = SnapshotAgentClient(endpoint=agent_endpoint)

# Define the backend (using the current enum in the branch)
backend = snapshot_agent_pb2.BACKEND_GPU_CR_MEMORY_ADDRESSES

job_id = "my-lora-job"
# The group field is used as the snapshot storage folder for this PoC #(/mnt/huge-ckpt/snapshots/<group>/)
group_id_a = "adapter_a"

# --- 1. Snapshot Adapter A ---
# List of target strings in format: "pid:address:size"
# The address must be a hex string (e.g. 0x7f...), size in bytes
pid = os.getpid()
lora_a_targets = [
    f"{pid}:0x7f9a12000000:1048576",
    f"{pid}:0x7f9a13000000:2097152",
]

# Trigger snapshot and block until complete
client.snapshot_and_wait(
    job_id=job_id, 
    group=group_id_a, 
    backend=backend, 
    memory_addresses=lora_a_targets
)


# --- 2. Restore Adapter A ---
# To restore, we call restore_and_wait. In the current implementation, we pass 
# the same target list containing the PIDs and memory addresses.
client.restore_and_wait(
    job_id=job_id, 
    group=group_id_a, 
    backend=backend, 
    memory_addresses=lora_a_targets
)
```

