# Running the Memory-Address LoRA Swap Demo on GKE

This guide walks through running the multi-tenant LoRA swap demo end to end on
GKE: the Snapshot Agent (backend `BACKEND_GPU_CR_MEMORY_ADDRESSES`) snapshots
and restores individual GPU memory regions of live processes, so many LoRA
tenants can share one GPU — on both the trainer and the sampler side of an RL
loop.

Repositories involved:

| Repo / branch | Provides |
|---|---|
| [`GPU-CR`](https://github.com/Edwinhr716/GPU-CR) **release v0.3.0** | `vGPU-NVIDIA.so` (workload preload) + `cr_client` (agent CLI) |
| `llm-d-rl-time-slicing` @ `gcr-backend-memory-allocation` (this repo) | Snapshot Agent + Python client |
| [`open-rl`](https://github.com/Edwinhr716/open-rl) @ `timeslice-lora-swap` | Demo workloads (trainer, vLLM sampler, gateway, driver) + manifests |

## 1. Prerequisites

- `gcloud`, `kubectl`, `docker`, `git`.
- A GCP project with GKE enabled and an Artifact Registry repo you can push to
  (`gcloud auth configure-docker <REGION>-docker.pkg.dev`).
- GKE **Standard** (not Autopilot — the demo needs hostPath, hostPID,
  privileged pods, and node system config).

Set these once; the rest of the guide uses them:

```shell
export PROJECT=<your-project>  REGION=us-central1  ZONE=us-central1-a
export CLUSTER=timeslice-demo
export REGISTRY=<REGION>-docker.pkg.dev/$PROJECT/<your-repo>
```

## 2. Create the cluster

A small CPU pool is enough for redis, the gateway, and the demo driver:

```shell
gcloud container clusters create $CLUSTER \
  --project=$PROJECT --location=$REGION --node-locations=$ZONE \
  --num-nodes=1 --machine-type=e2-standard-8
gcloud container clusters get-credentials $CLUSTER --location=$REGION
```

## 3. Create the GPU node pool (with hugepages)

GPU-CR stages snapshots in 2Mi hugepages. With the 8GiB staging buffer built
in step 4, each GPU process reserves ~10Gi of hugepages at startup, so a 24Gi
pool comfortably fits the two GPU workloads (trainer + sampler):

```shell
cat > hugepages-config.yaml <<'EOF'
linuxConfig:
  hugepageConfig:
    hugepage_size2m: 12288   # 24Gi of 2Mi pages
EOF

# g2-standard-24 requires exactly 2x L4. 300GB disk: the vLLM image is ~20GB
# unpacked; smaller disks hit DiskPressure.
gcloud container node-pools create rl-hugepages-l4 \
  --project=$PROJECT --cluster=$CLUSTER --location=$REGION --node-locations=$ZONE \
  --machine-type=g2-standard-24 \
  --accelerator=type=nvidia-l4,count=2,gpu-driver-version=latest \
  --num-nodes=1 --disk-size=300 \
  --system-config-from-file=hugepages-config.yaml
```

## 4. Build the GPU-CR binaries from release v0.3.0

Build both binaries from the
[v0.3.0 release](https://github.com/Edwinhr716/GPU-CR/releases/tag/v0.3.0)
source using the release's own `Dockerfile.build`. It applies the two
deployment patches the demo needs (checkpoint signals remapped to
SIGRTMAX-8/-7 so they don't collide with the workload's own signal use, and a
world-writable control file) and lets you right-size the staging buffer:

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

## 5. Build and push the two images

**Snapshot Agent** (this repo, branch `gcr-backend-memory-allocation`) — bakes
in your `cr_client`:

```shell
cd llm-d-rl-time-slicing
cp ../gpu-cr-bin/cr_client bin/cr_client
docker build -f docker/snapshot-agent/Dockerfile -t $REGISTRY/snapshot-agent:v0.3.0 .
docker push $REGISTRY/snapshot-agent:v0.3.0
```

**Demo workload image** (open-rl, branch `timeslice-lora-swap`) — one image
serves the sampler, trainer, gateway, and driver, and bakes in your
`vGPU-NVIDIA.so`. Point the `.so` line in `src/server/Dockerfile.timeslice`
at your build:

```shell
cd open-rl
cp ../gpu-cr-bin/vGPU-NVIDIA.so .
# In src/server/Dockerfile.timeslice, replace the ADD line for vGPU-NVIDIA.so with:
#   COPY vGPU-NVIDIA.so /usr/local/lib/vGPU-NVIDIA.so
docker build -f src/server/Dockerfile.timeslice -t $REGISTRY/timeslice-demo:v0.3.0 .
docker push $REGISTRY/timeslice-demo:v0.3.0
```

## 6. Deploy the Snapshot Agent

The manifest lives in open-rl at `k8s/deploy/timeslice-demo/snapshot-agent.yaml`.
Edit its image to `$REGISTRY/snapshot-agent:v0.3.0` and its `nodeSelector` to
your pool, then:

```shell
kubectl create namespace timeslice-system
kubectl apply -f k8s/deploy/timeslice-demo/snapshot-agent.yaml
```

The manifest already encodes the load-bearing details: an init container that
mounts hugetlbfs at `/var/tmp/huge-ckpt` on the host (GPU-CR needs a real
hugetlbfs mount), `mountPropagation: HostToContainer`, a tmpfs-backed
`SNAPSHOT_DIR` for the per-tenant snapshot copies, and a memory limit sized
for parked tenants (tmpfs writes are charged to the agent's cgroup).

## 7. Integrate your workloads

This is what any workload — training or inference — needs to be swappable.

### 7.1 Pod spec (both kinds of workload)

```yaml
metadata:
  labels:
    snapshot-agent: "true"           # agent discovers pods by this label
    timeslice.io/job-id: "my-job"    # must match the JOB_ID env below
spec:
  hostIPC: true                      # GPU-CR control channel
  hostPID: true                      # agent signals the workload PID
  nodeSelector: {cloud.google.com/gke-nodepool: rl-hugepages-l4}
  containers:
  - name: workload
    image: <your image with vGPU-NVIDIA.so baked in>
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

### 7.2 Calling the agent from Python

Install the client (already baked into the demo image):

```shell
pip install "git+https://github.com/Edwinhr716/llm-d-rl-time-slicing.git@gcr-backend-memory-allocation#subdirectory=pkg/client/python"
```

Regions are strings of the form `"<pid>:<hex address>:<bytes>"`, taken from
live tensors. Snapshots are grouped per tenant with `group=`.

### 7.3 Training workload example

Park an inactive tenant (adapter weights + AdamW state → VRAM freed) and
revive it when its next batch arrives. Full implementation:
`src/training/timeslice_tenant.py` in open-rl.

```python
import os, torch
from timeslice.snapshot_agent.client import SnapshotAgentClient

client = SnapshotAgentClient(endpoint=os.environ["AGENT_ENDPOINT"])
JOB_ID, BACKEND = os.environ["JOB_ID"], "BACKEND_GPU_CR_MEMORY_ADDRESSES"

def regions_for(params, optimizer):
    """Adapter weights + AdamW moments -> 'pid:hexaddr:size' strings."""
    pid, tensors = os.getpid(), []
    for p in params:                                  # trainable LoRA params
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

### 7.4 Sampling / inference workload example

vLLM with a single LoRA slot (`max_loras=1`): the slot's stacked buffers have
stable device addresses, so switching adapters = snapshot the resident
adapter's slot bytes, restore the incoming adapter's bytes into the same
slot. No reload from storage. Full implementation: `src/server/timeslice_lora.py`
and `src/server/timeslice_vllm_sampler.py` in open-rl.

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

## 8. Deploy and run the demo

The prewired manifests in open-rl `k8s/deploy/timeslice-demo/` apply exactly
the integration above. Edit the images to `$REGISTRY/timeslice-demo:v0.3.0`,
then:

```shell
kubectl apply -f k8s/deploy/timeslice-demo/stack.yaml     # redis, gateway, sampler, trainer
kubectl get pods -n openrl-demo -w                        # wait for Ready
kubectl apply -f k8s/deploy/timeslice-demo/driver-job.yaml
kubectl logs -f job/timeslice-demo-driver -n openrl-demo | grep '\[driver\]'
```

The driver alternates two tenants through full RL rounds
(forward_backward → optim_step → save weights → sample) and ends with
determinism tripwires; look for `PASSED` at the end.

**Before every re-run**, clear stale GPU-CR dump files — they pin their full
hugepage reservations even after the process dies:

```shell
kubectl scale deploy/timeslice-sampler deploy/trainer-worker -n openrl-demo --replicas=0
sleep 25
# In a privileged pod (or node shell) on the GPU node:
#   rm -rf /var/tmp/huge-ckpt/* /dev/shm/gcr-snapshots/* /var/tmp/open-rl/*
#   grep HugePages_Free /proc/meminfo   # must equal HugePages_Total
kubectl scale deploy/timeslice-sampler deploy/trainer-worker -n openrl-demo --replicas=1
```

## 9. Verify it's working

- **Swaps are real**: `kubectl logs -n timeslice-system -l app.kubernetes.io/name=snapshot-agent | grep took`
  shows per-operation timings; the trainer's metrics
  (`/mnt/open-rl/metrics/trainer.jsonl`) record `vram_freed_mb` per swap-out.
- **Hugepages are used**: on the GPU node, `grep Huge /proc/meminfo` —
  `HugePages_Free` must drop below `HugePages_Total` while workloads run.
- **Correctness**: the driver's tripwires verify the same tenant restored
  twice samples identically and different tenants sample differently.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `mmap with hugepages failed: Cannot allocate memory` at startup | Stale dump files pin reservations — run the clean-slate step in §8. |
| Agent reports job `FAULTED` | A cr_client op hit a dead PID; the agent auto-recovers after its op timeout. Re-check pod health, then retry. |
| `libcuda.so.1` not found in workload | `LD_LIBRARY_PATH` must include `/usr/local/nvidia/lib64`. |
| Snapshots "work" but `HugePages_Free` never moves | hugetlbfs isn't actually mounted on the host — check the agent's init container ran on that node. |
| Xid 31 / illegal memory access on the sampler | Restore must immediately follow snapshot on the same regions (the demo code handles this); also verify `CUDA_LAUNCH_BLOCKING=1`. |
