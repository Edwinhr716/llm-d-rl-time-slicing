# GKE User Guide: Memory-Address Snapshots with the Snapshot Agent

This guide walks through integrating the Snapshot Agent's memory-address
backend (`BACKEND_GPU_CR_MEMORY_ADDRESSES`) with your own GPU workloads on
GKE. The backend snapshots and restores individual GPU memory regions of a
live process — the process keeps running, the bytes come back bit-for-bit at
the same addresses, and the physical VRAM behind parked regions is freed in
between. Typical uses: parking inactive tenants' adapter + optimizer state in
a multi-tenant trainer, and swapping adapters through a fixed slot in an
inference server.

What you need from where:

| Source | Provides |
|---|---|
| [`GPU-CR`](https://github.com/Edwinhr716/GPU-CR) **release v0.3.0** | `vGPU-NVIDIA.so` (workload preload) + `cr_client` (agent CLI) |
| `llm-d-rl-time-slicing` @ `gcr-backend-memory-allocation` (this repo) | Snapshot Agent, example manifests, Python client |

## 1. Prerequisites

- `gcloud`, `kubectl`, `docker`, `git`.
- A GCP project with GKE enabled and an Artifact Registry repo you can push to
  (`gcloud auth configure-docker <REGION>-docker.pkg.dev`).
- GKE **Standard** (not Autopilot — the agent needs hostPath, hostPID,
  privileged pods, and node system config).

Set these once; the rest of the guide uses them:

```shell
export PROJECT=<your-project>  REGION=us-central1  ZONE=us-central1-a
export CLUSTER=<your-cluster>
export REGISTRY=<REGION>-docker.pkg.dev/$PROJECT/<your-repo>
```

## 2. Create the cluster

Any Standard cluster works; a small CPU pool covers non-GPU components:

```shell
gcloud container clusters create $CLUSTER \
  --project=$PROJECT --location=$REGION --node-locations=$ZONE \
  --num-nodes=1 --machine-type=e2-standard-8
gcloud container clusters get-credentials $CLUSTER --location=$REGION
```

## 3. Create the GPU node pool (with hugepages)

GPU-CR stages snapshots in 2Mi hugepages, and its staging buffer is sized at
**build time**. The standard build uses a 25GiB buffer — sized for
whole-model checkpoints — which makes every GPU process reserve ~27Gi of
hugepages at startup and forces a 60Gi+ carve-out per node. Region snapshots
of adapter-scale state need far less, so this guide builds the library with
an **8GiB buffer** (step 4): each process then reserves ~10Gi (8Gi buffer +
2×1Gi staging areas), a 24Gi pool fits two GPU workloads per node, and the
~38Gi difference stays available to your workloads as regular RAM.

```shell
cat > hugepages-config.yaml <<'EOF'
linuxConfig:
  hugepageConfig:
    hugepage_size2m: 12288   # 24Gi of 2Mi pages
EOF

# g2-standard-24 requires exactly 2x L4. Size the disk for your images
# (large inference images can hit DiskPressure on small disks).
gcloud container node-pools create <your-gpu-pool> \
  --project=$PROJECT --cluster=$CLUSTER --location=$REGION --node-locations=$ZONE \
  --machine-type=g2-standard-24 \
  --accelerator=type=nvidia-l4,count=2,gpu-driver-version=latest \
  --num-nodes=1 --disk-size=300 \
  --system-config-from-file=hugepages-config.yaml
```

If you keep the standard 25GiB build instead, scale the pool accordingly
(~30Gi of hugepages per concurrent GPU-CR process).

## 4. Build the GPU-CR binaries from release v0.3.0

Build both binaries from the
[v0.3.0 release](https://github.com/Edwinhr716/GPU-CR/releases/tag/v0.3.0)
source using the release's own `Dockerfile.build`. It applies the two
deployment patches this integration needs (checkpoint signals remapped to
SIGRTMAX-8/-7 so they don't collide with the workload's own signal use, and a
world-writable control file) and takes the staging-buffer size as a build
argument:

```shell
git clone --branch v0.3.0 --depth 1 https://github.com/Edwinhr716/GPU-CR
mkdir -p gpu-cr-bin && cd GPU-CR
docker build -f Dockerfile.build --build-arg SHM_SIZE_GB=8 --target builder -t gpu-cr-build .
docker create --name gpu-cr-tmp gpu-cr-build
docker cp gpu-cr-tmp:/tmp/GPU-CR/build/vGPU-NVIDIA.so ../gpu-cr-bin/
docker cp gpu-cr-tmp:/tmp/GPU-CR/build/cr_client     ../gpu-cr-bin/
docker rm gpu-cr-tmp && cd ..
```

> The release page also attaches prebuilt `vGPU-NVIDIA.so` / `cr_client`
> assets. Those are stock builds (default signals, 25GiB buffer, no
> deployment patches) — don't mix them with the patched builds; the two
> binaries must always come from the same build so their signal numbers match.

## 5. Build and push the Snapshot Agent image

From this repo (branch `gcr-backend-memory-allocation`), with your
`cr_client` dropped into `bin/`:

```shell
cd llm-d-rl-time-slicing
cp ../gpu-cr-bin/cr_client bin/cr_client
docker build -f docker/snapshot-agent/Dockerfile -t $REGISTRY/snapshot-agent:v0.3.0 .
docker push $REGISTRY/snapshot-agent:v0.3.0
```

## 6. Deploy the Snapshot Agent

An example DaemonSet is included at
`deploy/examples/snapshot-agent-memory-addresses.yaml`. Fill in the two
placeholders (agent image, GPU pool selector), then:

```shell
kubectl create namespace timeslice-system
# ServiceAccount + RBAC come from the helm chart in deploy/snapshot-agent;
# the example DaemonSet replaces the chart's daemonset template.
kubectl apply -f deploy/examples/snapshot-agent-memory-addresses.yaml
```

The manifest encodes the load-bearing details:

- An init container that **mounts hugetlbfs at `/var/tmp/huge-ckpt` on the
  host** — GPU-CR requires the filesystem itself to be hugetlbfs; without it,
  dumps silently degrade to boot-disk page cache and the hugepage pool goes
  unused.
- `mountPropagation: HostToContainer` so the agent sees that mount.
- `SNAPSHOT_DIR` on tmpfs, not hugetlbfs (hugetlbfs has no `write(2)`; tmpfs
  keeps snapshot copies at RAM speed).
- An agent memory limit sized for parked snapshots — tmpfs writes are charged
  to the **agent's** cgroup, roughly the size of each parked group.

## 7. Integrate your workload

### 7.1 Your workload image

Add two things to your existing Dockerfile: the preload library built in
step 4, and the Python client for the agent's gRPC API.

```dockerfile
# --- Snapshot Agent integration ---
# GPU-CR preload library (from the v0.3.0 release build, step 4).
# Place vGPU-NVIDIA.so in the build context first.
COPY vGPU-NVIDIA.so /usr/local/lib/vGPU-NVIDIA.so
RUN chmod 755 /usr/local/lib/vGPU-NVIDIA.so

# Snapshot Agent Python client.
RUN pip install --no-cache-dir \
    "git+https://github.com/Edwinhr716/llm-d-rl-time-slicing.git@gcr-backend-memory-allocation#subdirectory=pkg/client/python"
```

Nothing activates inside the image itself: the library only engages when the
pod sets `LD_PRELOAD`, so the same image runs unchanged with the integration
disabled.

### 7.2 Pod spec

```yaml
metadata:
  labels:
    snapshot-agent: "true"           # agent discovers pods by this label
    timeslice.io/job-id: "my-job"    # must match the JOB_ID env below
spec:
  hostIPC: true                      # GPU-CR control channel
  hostPID: true                      # agent signals the workload PID
  # Schedule onto the hugepages GPU pool created in step 3:
  nodeSelector: {cloud.google.com/gke-nodepool: <your-gpu-pool>}
  containers:
  - name: workload
    image: <your image from 7.1>
    securityContext: {runAsUser: 0}
    env:
    - name: NODE_IP
      valueFrom: {fieldRef: {fieldPath: status.hostIP}}
    - {name: AGENT_ENDPOINT, value: "$(NODE_IP):9001"}      # node-local agent
    - {name: JOB_ID,        value: "my-job"}
    - {name: GPU_VENDOR,    value: "NVIDIA"}
    - {name: LD_PRELOAD,    value: "/usr/local/lib/vGPU-NVIDIA.so"}
    - {name: LD_LIBRARY_PATH, value: "/usr/local/nvidia/lib64:/usr/local/cuda/lib64"}
    - {name: PYTORCH_NO_CUDA_MEMORY_CACHING, value: "1"}    # required: 1 tensor = 1 block
    - {name: CUDA_LAUNCH_BLOCKING, value: "1"}              # required by GPU-CR
    volumeMounts:
    - {name: huge-ckpt, mountPath: /mnt/huge-ckpt, mountPropagation: HostToContainer}
    resources:
      requests: {nvidia.com/gpu: "1", hugepages-2Mi: "11Gi", memory: "10Gi"}
      limits:   {nvidia.com/gpu: "1", hugepages-2Mi: "11Gi", memory: "10Gi"}
  volumes:
  - {name: huge-ckpt, hostPath: {path: /var/tmp/huge-ckpt, type: DirectoryOrCreate}}
```

vLLM workloads additionally need
`{name: VLLM_ENABLE_V1_MULTIPROCESSING, value: "0"}` (addresses must live in
the process the agent targets).

### 7.3 Calling the agent from Python

Regions are strings of the form `"<pid>:<hex address>:<bytes>"`, taken from
live tensors. Snapshots are grouped per tenant/session with `group=`.

```python
import os
from timeslice.snapshot_agent.client import SnapshotAgentClient

client = SnapshotAgentClient(endpoint=os.environ["AGENT_ENDPOINT"])
JOB_ID, BACKEND = os.environ["JOB_ID"], "BACKEND_GPU_CR_MEMORY_ADDRESSES"
```

### 7.4 Training workload example

Park an inactive tenant (adapter weights + optimizer state → VRAM freed) and
revive it when its next batch arrives:

```python
import torch

def regions_for(params, optimizer):
    """Adapter weights + AdamW moments -> 'pid:hexaddr:size' strings."""
    pid, tensors = os.getpid(), []
    for p in params:                                  # trainable adapter params
        tensors.append(p.data)
        state = optimizer.state.get(p, {})
        for key in ("exp_avg", "exp_avg_sq"):         # only exists AFTER the
            if key in state:                          # first optimizer step
                tensors.append(state[key])
    return [f"{pid}:{hex(t.data_ptr())}:{t.element_size() * t.nelement()}"
            for t in tensors if t.nelement() > 0]

def switch_tenant(incoming, outgoing):
    # Order matters: restore the incoming tenant BEFORE snapshotting the
    # outgoing one.
    if incoming.parked:
        client.restore_and_wait(job_id=JOB_ID, group=incoming.name,
                                backend=BACKEND, memory_addresses=incoming.regions)
        incoming.parked = False
    if outgoing is not None:
        outgoing.regions = regions_for(outgoing.params, outgoing.optimizer)
        client.snapshot_and_wait(job_id=JOB_ID, group=outgoing.name,
                                 backend=BACKEND, memory_addresses=outgoing.regions)
        outgoing.parked = True                        # physical VRAM now freed
    torch.cuda.synchronize()
```

Rules that keep this correct:

- Only swap a tenant **after its first optimizer step** (AdamW state is lazy).
- **Restore incoming before snapshotting outgoing** at every switch.
- Re-collect regions after any model reload — addresses change.

### 7.5 Sampling / inference workload example

vLLM with a single LoRA slot (`max_loras=1`): the slot's stacked buffers have
stable device addresses, so switching adapters = snapshot the resident
adapter's slot bytes, restore the incoming adapter's bytes into the same
slot. No reload from storage. (A complete standalone version of this test
lives in this repo at `testing-artifacts/test_lora_swap_max1.py`.)

```python
def slot_regions(model):
    """Runs inside the vLLM worker (llm.apply_model(slot_regions)):
    'pid:hexaddr:size' for slot 0 of every stacked LoRA buffer."""
    import os
    from vllm.lora.layers import BaseLayerWithLoRA
    pid, targets = os.getpid(), []
    for _, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            for stacked in (module.lora_a_stacked, module.lora_b_stacked):
                for t in (stacked if isinstance(stacked, tuple) else (stacked,)):
                    size = t.element_size() * t.nelement()
                    if size > 0:
                        targets.append(f"{pid}:{hex(t.data_ptr())}:{size}")
    return targets

REGIONS = llm.apply_model(slot_regions)      # discover once; addresses are stable

def switch_adapter(resident, incoming):
    """Swap which adapter occupies the vLLM LoRA slot."""
    if resident is not None:                 # save the resident adapter's bytes
        client.snapshot_and_wait(job_id=JOB_ID, group=resident,
                                 backend=BACKEND, memory_addresses=REGIONS)
    client.restore_and_wait(job_id=JOB_ID, group=incoming,   # overwrite the slot
                            backend=BACKEND, memory_addresses=REGIONS)
```

The incoming adapter must have been loaded (and snapshotted) through the slot
once before — after that, it never touches storage again.

## 8. Operations

**Clean slate between workload restarts.** GPU-CR dump files pin their full
hugepage reservations even after the owning process dies (reservations attach
to the file inode). Before restarting GPU workloads:

```shell
# Scale your GPU workloads to zero, then in a privileged pod (or node
# shell) on the GPU node:
#   rm -rf /var/tmp/huge-ckpt/* /dev/shm/gcr-snapshots/*
#   grep HugePages_Free /proc/meminfo   # must equal HugePages_Total
# then scale the workloads back up.
```

The agent also garbage-collects stale dump files and snapshots for dead PIDs
automatically, on a delay.

**Verify it's working:**

- **Swaps are real**: `kubectl logs -n timeslice-system -l app.kubernetes.io/name=snapshot-agent | grep took`
  shows per-operation timings, and `torch.cuda.mem_get_info()` before/after a
  snapshot shows the freed VRAM.
- **Hugepages are used**: on the GPU node, `grep Huge /proc/meminfo` —
  `HugePages_Free` must drop below `HugePages_Total` while workloads run.
- **Correctness**: restore a group and compare tensors against copies saved
  before the snapshot (`torch.equal`), or check that deterministic
  (temperature-0) outputs are identical before and after a park/revive cycle.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `mmap with hugepages failed: Cannot allocate memory` at startup | Stale dump files pin reservations — run the clean-slate step in §8. |
| Agent reports job `FAULTED` | A cr_client op hit a dead PID; the agent auto-recovers after its op timeout. Re-check pod health, then retry. |
| `libcuda.so.1` not found in workload | `LD_LIBRARY_PATH` must include `/usr/local/nvidia/lib64`. |
| Snapshots "work" but `HugePages_Free` never moves | hugetlbfs isn't actually mounted on the host — check the agent's init container ran on that node. |
| Xid 31 / illegal memory access | A snapshot was left un-restored on regions the workload then touched — always pair snapshot with a restore of the same regions (see the slot-swap pattern); also verify `CUDA_LAUNCH_BLOCKING=1`. |
