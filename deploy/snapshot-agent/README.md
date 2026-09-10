# Deploying Snapshot Agent

This directory contains the Helm chart for deploying the Snapshot Agent DaemonSet in a Kubernetes cluster.

Public images are published to `ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/*` by CI: `latest` on every merge to main; versioned tags via a manual workflow run.

## Prerequisites

*   A Kubernetes cluster with GPU nodes (NVIDIA).
*   `kubectl` configured to connect to your cluster.
*   `helm` (v3+) installed.

## Deployment with Helm

> [!IMPORTANT]
> The Snapshot Agent is hardcoded to be deployed in the `timeslice-system` namespace. Consequently, the Helm chart creates resources specifically in the `timeslice-system` namespace.

To deploy the agent independently using the local Helm chart:

1.  **Install the chart**:
    From the `deploy` directory, install the chart into the `timeslice-system` namespace (creating it if it doesn't exist):
    ```bash
    helm install snapshot-agent ./snapshot-agent \
      --namespace timeslice-system \
      --create-namespace
    ```
    This will deploy the agent as a `DaemonSet` and set up the required RBAC permissions:
    *   Creating a `ServiceAccount` for the agent.
    *   Creating a `ClusterRole` and `ClusterRoleBinding` granting permissions to `get`, `list`, and `watch` pods and nodes, and `get` on `nodes/proxy`.
    *   Configuring the agent pods to use this `ServiceAccount`.

2.  **Verify the deployment**:
    ```bash
    kubectl get pods -n timeslice-system -l app.kubernetes.io/name=snapshot-agent
    ```

3.  **Uninstall the chart**:
    ```bash
    helm uninstall snapshot-agent --namespace timeslice-system
    ```

## Deployment on GKE GPU Clusters

### 1. Requirements

*   A GKE cluster with at least one GPU node pool.
*   The NVIDIA GPU device driver must be installed on the nodes (e.g., using the [GKE GPU driver installer](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#installing_drivers)).

### 2. Default Configuration for GKE

*   `nvidia.driver.hostPath`: `/home/kubernetes/bin/nvidia` (Standard path for GPU drivers on GKE COS).
*   `nvidia.devices.hostPath`: `/dev` (Standard path for device access).
*   `tolerations`: Includes `nvidia.com/gpu` to allow the agent to run on GPU-tainted nodes.

### 3. Installation on GKE

To install the chart on GKE, ensuring it only targets nodes with GPUs:

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set-string "nodeSelector.cloud\.google\.com/gke-gpu=true"
```

### 4. Customizing for Ubuntu Nodes on GKE

If your GKE nodes are using Ubuntu instead of COS, you may need to override the driver path:

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set-string "nodeSelector.cloud\.google\.com/gke-gpu=true" \
  --set nvidia.driver.hostPath=/usr/lib/nvidia
```

### 5. Deploying on Non-GKE GPU Clusters

If you are deploying the snapshot agent to a non-GKE cluster (e.g., EKS, AKS, or bare-metal), you will likely need to adjust the node selector and driver paths because they differ from GKE defaults.

#### A. Override GPU Node Selector
Non-GKE clusters typically use different labels to identify GPU nodes. For example, standard NVIDIA GPU nodes often use `nvidia.com/gpu=true` or `hardware=gpu`. 

You can override the GKE-default node selector during installation:

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set-string "nodeSelector.nvidia\.com/gpu=true"
```

*Note: You may need to escape the dots in the label key as shown above (`nodeSelector.nvidia\.com/gpu=true`).*

#### B. Override NVIDIA Driver Host Path
On non-GKE clusters, the NVIDIA driver libraries might be installed in different locations on the host. Common paths include:
*   `/usr/lib/nvidia`
*   `/usr/local/nvidia`
*   `/usr/lib/x86_64-linux-gnu`

You can override the host path using:

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set nvidia.driver.hostPath=/usr/lib/nvidia
```

#### C. Override Tolerations
If your GPU nodes have different taints than the default `nvidia.com/gpu=present:NoSchedule`, you must override the tolerations. For example, if your nodes are tainted with `sku=gpu:NoSchedule`:

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set tolerations[0].key=sku \
  --set tolerations[0].operator=Equal \
  --set tolerations[0].value=gpu \
  --set tolerations[0].effect=NoSchedule
```

## GPU-CR Backends (`directMemory.*` / `memoryRegions.*`)

The `direct_memory` (full-process park/resume) and `memory_regions`
(selective checkpoint/restore of explicit device-memory ranges) backends are
both driven by GPU-CR's `cr_client` and need node-level machinery that the
CUDA/app backends do not. They share one machinery block, configured under
`directMemory.*` and deployed when **either** backend is enabled; each
backend's `enabled` flag also implies its feature gate (`DirectMemoryBackend`
/ `MemoryRegionsBackend`). Both are **disabled by default** — with both flags
off the rendered chart is identical to a plain CUDA/app deployment.

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set directMemory.enabled=true    # and/or memoryRegions.enabled=true
```

Since GPU-CR GEP-0001 (destination-path checkpoints) + GEP-0006 (ctl tmpfs),
the agent moves **no dump bytes**: dumps are written by the workload's own
preloader into the shared store and the control files live on a nested
tmpfs. The agent therefore requests **no hugepages-2Mi** at all, which is
what lets it schedule on fresh nodes before hugepage capacity exists and
absorb the hugepage bootstrap as an init container.

Enabling either backend adds:

*   A `PriorityClass` (`priorityClass.*`, with the hugepage bootstrap) so
    the agent wins node placement over GPU workloads.
*   Privileged init containers that `nsenter` the host mount namespace,
    idempotent per node boot:
    *   `provision-hugepages` (`directMemory.hugetlbfs.bootstrap.*`) —
        writes `vm.nr_hugepages` (`pages2Mi`, default 12288 = 24 Gi) and
        restarts the kubelet so the node publishes `hugepages-2Mi`
        capacity for WORKLOAD pods.
    *   `mount-hugetlbfs` — mounts hugetlbfs at
        `directMemory.hostCtlPath` (default `/var/tmp/huge-ckpt`,
        `pagesize=2M,mode=0777`); without it every dump silently degrades
        to boot-disk page cache.
    *   `mount-ctl-tmpfs` — mounts the GEP-0006 control-plane tmpfs nested
        at `<hostCtlPath>/ctl`; both sides discover it through the store
        mount they already share, so neither needs any configuration.
*   Env for the agent: `EXPORT_FILE_PATH` (= `directMemory.ctlDir`), plus
    the per-invocation deadlines `DIRECT_MEMORY_OP_TIMEOUT_SEC`
    (`directMemory.opTimeoutSec`) and `GPU_CR_OP_TIMEOUT_SEC`
    (`memoryRegions.opTimeoutSec`) for whichever backends are enabled.
    Setting `EXPORT_FILE_PATH` also switches on the agent's GPU-CR
    artifact GC and the 0777 chmod of the checkpoint dir at startup.
*   Volumes: `huge-ckpt` at `directMemory.ctlDir`
    (`mountPropagation: HostToContainer` so the init containers' mounts
    are visible).

Node prerequisites:

*   Workload pods need the GPU-CR preloader (`LD_PRELOAD=vGPU-NVIDIA.so`),
    the `huge-ckpt` hostPath mounted at the same in-container path (with
    `mountPropagation: HostToContainer`), and `hugepages-2Mi` resources
    sized for their dump buffers plus destination-group headroom.

### Where the GPU-CR binaries come from

CI publishes `cr_client` and `vGPU-NVIDIA.so` as the released `gpu-cr`
package (`ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/gpu-cr`), built as
a version-locked pair from `third_party/gpu-cr` — the two share compiled-in
constants and the control-channel protocol, and a mismatch makes
checkpoints silently no-op, so always take both from the **same tag**, and
keep that tag in lock-step with `image.tag` (releases publish all packages
together under one tag).

There is no `cr_client` install step: the agent image copies it from the
package to `/usr/local/bin/cr_client`, so the agent and `cr_client` versions
always roll together. `grpc.health.v1.Health/Check` with
`service: "direct-memory"` or `service: "memory-regions"` reports
`NOT_SERVING` if the binary is missing.

Workload images get the preloader the same way:

```dockerfile
COPY --from=ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/gpu-cr:latest \
    /opt/gpu-cr/vGPU-NVIDIA.so /opt/gpu-cr/vGPU-NVIDIA.so
ENV LD_PRELOAD=/opt/gpu-cr/vGPU-NVIDIA.so
```

or, for pods whose image can't be rebuilt, run the package image as an init
container and copy the `.so` into a shared volume:

```yaml
initContainers:
  - name: fetch-gpu-cr
    image: ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/gpu-cr:latest
    command: ["cp", "/opt/gpu-cr/vGPU-NVIDIA.so", "/gpu-cr/vGPU-NVIDIA.so"]
    volumeMounts:
      - name: gpu-cr
        mountPath: /gpu-cr
```

## Development Workflow: Custom Images

During development, you will need to build your own container image containing your changes and push it to a custom registry.

### 1. Build and Push the Image

We use the provided `Makefile` targets to build and push the container image.

1.  Define your custom registry and version (tag) by setting them as environment variables:
    ```bash
    export REGISTRY=your-custom-registry.com/your-project
    export VERSION=dev-$(git rev-parse --short HEAD)
    ```
2.  Run the following make target from the repository root to build and push the image:
    ```bash
    make snapshot-agent-image-push
    ```
    This will build the image and push it to `your-custom-registry.com/your-project/llm-d-rl-time-slicing/snapshot-agent:dev-<hash>` (the repo name comes from `PROJECT_NAME`, also overridable).

### 2. Deploy with your Custom Image

Once your image is pushed, you can instruct Helm to use it.

#### Option A: Via Command Line Flags (Recommended for Development)

```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace \
  --set image.repository=your-custom-registry.com/your-project/snapshot-agent \
  --set image.tag=dev
```

#### Option B: Via `values.yaml`

Edit `deploy/snapshot-agent/values.yaml` directly:

```yaml
image:
  repository: your-custom-registry.com/your-project/snapshot-agent
  pullPolicy: IfNotPresent
  tag: "dev"
```

And then run:
```bash
helm install snapshot-agent ./snapshot-agent \
  --namespace timeslice-system \
  --create-namespace
```
