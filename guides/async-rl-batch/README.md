# Async RL and batch inference: lend an idle RL trainer GPU to batch inference

Asynchronous RL jobs (for example verl's fully-async mode) keep the rollout GPUs
busy all the time, but the trainer GPU often sits idle while it waits for the
next batch of samples. On long chain-of-thought math RL the trainer can be idle
for minutes in every step.

This platform lends that idle trainer GPU to your batch inference servers and
takes it back the moment the trainer needs it. The trainer always wins:

1. When the trainer starts waiting for samples, its GPU state is checkpointed to
   host memory and the GPU is lent to a batch server (a "guest").
2. When the trainer is about to train again, the guest is frozen in place and
   the trainer's GPU state is restored. Requests still running on the guest are
   cut off just before the freeze, so the caller retries them on another server
   instead of waiting for the next lend.

You do not change your training code, your model server, or your batch
pipeline. The whole setup is:

| Who | What |
|---|---|
| Cluster admin | `helm install timeslice` |
| RL team | the prebuilt verl image, one label and three settings in the RayJob |
| Batch team | one label on the model server pods |

The RL team runs **the prebuilt verl image**
(`ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl`): a stock verl image
plus the verl fork with the lifecycle hooks and the timeslice packages.

Sections 1 to 4 are all you need to run. [Section 7](#7-optional-settings)
lists optional settings.

## Contents

1. [Requirements](#1-requirements)
2. [Install the platform (cluster admin)](#2-install-the-platform-cluster-admin)
3. [Change the RayJob (RL team)](#3-change-the-rayjob-rl-team)
4. [Label the model server (batch team)](#4-label-the-model-server-batch-team)
5. [What the platform adds to your pods](#5-what-the-platform-adds-to-your-pods)
6. [Verify and observe](#6-verify-and-observe)
7. [Optional settings](#7-optional-settings)
8. [Troubleshooting](#8-troubleshooting)
9. [Limits](#9-limits)

## 1. Requirements

- A Kubernetes cluster with NVIDIA GPU nodes and the NVIDIA device plugin (a
  normal GKE GPU node pool is enough). No DRA driver is needed.
- KubeRay, if your RL jobs are RayJobs.
- One GPU pod per trainer node: the platform lends a trainer node's GPUs only
  while the trainer pod is the only pod holding `nvidia.com/gpu` there. Keep
  rollout workers and other GPU pods on other nodes.
- Host memory: the trainer's GPU memory is checkpointed into its pod's memory.
  The platform raises the trainer pod's memory limit by one GPU's memory per
  GPU (requests are unchanged), so the node needs that much free memory on top
  of the trainer's normal use.
- The RL image: the prebuilt verl image
  (`ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl`, about 11 GB to
  pull). To keep your own verl image instead, it needs Python 3.10 or later
  with `grpcio` 1.66 or later and `protobuf` 6 or later (stock `verlai/verl`
  images have them); see [Keep your own verl image](#keep-your-own-verl-image).
- The batch server: anything that serves over HTTP and tolerates pauses of a
  few minutes. Put a queue in front of it (for example llm-d-async) so requests
  wait instead of failing.

## 2. Install the platform (cluster admin)

```bash
git clone --branch demo/batch https://github.com/llm-d-incubation/llm-d-rl-time-slicing.git
cd llm-d-rl-time-slicing/deploy
helm dependency update .
helm install timeslice . -n timeslice-system --create-namespace
```

That is all. CI publishes every image the chart uses to
`ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/<component>`:
`timesliceorchestrator`, `snapshot-agent`, `guest-kubelet`,
`gpu-shadow-plugin`, `timeslice-webhook`, `donor-controller`,
`rl-integration`, and the prebuilt `verl` image. Each merge to `demo/batch`
gets an immutable `demo-<short sha>` tag and the moving tag `demo-batch`. The
chart on this branch defaults `global.imageTag` (the tag of every platform
image) to `demo-batch`, so you don't set a tag.

The defaults:

- run the node agents only on GPU nodes;
- detect the GPU model and its memory on each node;
- watch every namespace except `kube-system` and `timeslice-system`;
- include a DCGM exporter with 1 s samples (`gpu-metrics`), so you can see the
  trainer GPU before and after.

Optional settings (pinning the image tag, limiting namespaces, keeping your
own verl image) are in [section 7](#7-optional-settings).

## 3. Change the RayJob (RL team)

Start from your normal RayJob. The trainer worker group and the rollout worker
group must be separate worker groups, with the trainer group on its own GPU
nodes. Switch the image to the prebuilt verl image
(`ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl:demo-batch`, which has
verl and its fully-async dependencies, so drop any `pip install` of verl at
start), then make three changes:

1. **Label the trainer worker group** `timeslice.io/donor: "true"`. This is the
   group whose GPUs are lent.
2. **Pin Ray placement** with one `resources` line per worker group, so verl's
   trainer and rollout placement groups land on the right pods.
3. **Two verl settings**: `async_training.trainer_name=timeslice` (the
   time-slicing trainer, a subclass of verl's fully-async trainer) and
   `ray_pg_extra_resources=...` (the custom resources from change 2).

The diff on a typical fully-async RayJob (1 trainer GPU + 1 rollout GPU; your
group names, resources and node pools will differ):

```diff
@@ entrypoint @@
     async_training.staleness_threshold=8
     async_training.trigger_parameter_sync_step=1
+    async_training.trainer_name=timeslice
+    'ray_pg_extra_resources={trainer_pool:{trainer_node:1},rollout_pool:{rollout_node:1}}'
@@ headGroupSpec @@
           containers:
             - name: ray-head
-              image: verlai/verl:vllm020.dev2
+              image: ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl:demo-batch
@@ workerGroupSpecs: trainer @@
       - groupName: trainer
         replicas: 1
         rayStartParams:
           num-cpus: "20"
           num-gpus: "1"
+          resources: "'{\"trainer_node\": 100}'"
         template:
+          metadata:
+            labels: {timeslice.io/donor: "true"}
           spec:
             nodeSelector: {cloud.google.com/gke-nodepool: h100-trainer-pool}   # your trainer pool
             containers:
               - name: ray-worker
-                image: verlai/verl:vllm020.dev2
+                image: ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl:demo-batch
@@ workerGroupSpecs: rollout @@
       - groupName: rollout
         replicas: 1
         rayStartParams:
           num-cpus: "20"
           num-gpus: "1"
+          resources: "'{\"rollout_node\": 100}'"
         template:
           spec:
             nodeSelector: {cloud.google.com/gke-nodepool: h100-rollout-pool}   # your rollout pool
             containers:
               - name: ray-worker
-                image: verlai/verl:vllm020.dev2
+                image: ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/verl:demo-batch
```

Remove any `pip install` of verl, `TransferQueue` or `cupy-cuda12x` from the
pods' start-up commands. Nothing else changes: same command, same data, same
resources. Apply the RayJob as usual. To pin the images to the commit you
installed the platform from, use the same `demo-<short sha>` tag as
`global.imageTag`.

The prebuilt image is built by CI from
[`docker/verl/Dockerfile`](../../docker/verl/Dockerfile): `verlai/verl:vllm020.dev2`
plus the verl fork with the lifecycle hooks (pinned in
`docker/verl/verl-pin.env`), the timeslice client and verl plugin from the
same commit as the platform, and `TransferQueue` and `cupy-cuda12x`, so the
pods install nothing from PyPI at start. It is large (about 24 GB unpacked,
11 GB to pull), like the stock image it is built on. Do not `pip install` verl
at start: it would replace the fork.

### Keep your own verl image

This is optional and off by default. If the RL team must keep its own verl
image (its own dependencies, a different CUDA or vLLM version, a private base
image), the webhook can inject the verl fork and the timeslice packages into
the Ray pods instead.

1. Install the platform with the overlay that turns the injection on:

   ```bash
   helm install timeslice . -n timeslice-system --create-namespace \
     -f values-inject-verl.yaml
   ```

2. Keep your image (and any start-up `pip install`) and make only the three
   changes above.

With the overlay, the webhook adds an init container to every pod of a Ray
cluster that has a donor group (head, trainer and rollout workers). The init
container copies these packages into a read-only volume, and `PYTHONPATH`
points at it:

- the timeslice client and the verl plugin (`trainer_name=timeslice`);
- **a verl fork**: verl at commit `983cb0f2` plus two commits that add
  lifecycle hooks to the fully-async trainer (source:
  `github.com/aishukamal/verl`, branch `feat/fully-async-lifecycle-hooks`,
  commit `976b5f57`), the same pinned commit as in the prebuilt image. It
  ships with its Apache-2.0 LICENSE and replaces the image's own verl in those
  pods only. verl's dependencies still come from your image. If your image
  already has a verl with these hooks, add
  `--set timeslice-webhook.rlIntegration.injectVerl=false` to inject only the
  timeslice packages.

If your RL pods are not created by KubeRay, label every non-trainer pod of the
job `timeslice.io/rl-integration: "true"` so they get the same packages (for
KubeRay the webhook does this for you).

## 4. Label the model server (batch team)

Add `timeslice.io/guest: "true"` to the model server's pod labels. Keep asking
for `nvidia.com/gpu` as usual.

```diff
   template:
     metadata:
       labels:
         app: guest-vllm
+        timeslice.io/guest: "true"
     spec:
       containers:
         - name: vllm
           image: vllm/vllm-openai:v0.9.2
```

Guests run only on lent GPUs. While no trainer GPU is lent, a guest pod stays
`Pending` or frozen; that is expected.

### If you use llm-d-async and the llm-d router

Install llm-d-async with its own Helm chart, unchanged; it needs no time-slicing
settings. Label only the model server pods, not the async processor.

When a guest is reclaimed, the requests running on it are cut off before it is
frozen: the snapshot agent resets their connections. The router answers a
request cut off before its response started with `503` (Envoy's
`upstream_reset_before_response_started`), which the async processor retries.
The async processor sends non-streamed requests, so this covers it. A streamed
response that had already started ends early instead, still with status `200`
but without the final `data: [DONE]` line: a client that streams must treat a
stream without `[DONE]` as failed and retry it. No request waits for a frozen
guest, so neither timeout depends on how long a trainer update takes. Check
both against your longest single generation instead:

- **Router route timeout**: Envoy's default route timeout is 15 s, shorter
  than many generations. Set the route `timeout` above your longest single
  generation (or `0s`, no limit). The route `idle_timeout` can stay at its
  default (5 minutes) unless one non-streamed generation takes longer.
- **Async processor request timeout**: the default (5 minutes) is enough as
  long as it stays above your longest single generation.

## 5. What the platform adds to your pods

The admission webhook changes only pods and Ray clusters that opted in with the
labels above. It adds:

- **Into every pod of a Ray cluster that has a donor group** (head, trainer and
  rollout workers): `TIMESLICE_FULLY_ASYNC=1` and the job's identity and
  orchestrator address (`TIMESLICE_JOB_ID`, `TIMESLICE_GROUP`,
  `TIMESLICE_ORCH_ADDR`), and `NCCL_CUMEM_ENABLE=0` / `NCCL_NVLS_ENABLE=0` (NCCL
  then allocates no buffers that the GPU checkpoint cannot handle). Values you
  set yourself are kept.
- **Into the trainer pods**: a higher memory limit (one GPU's memory per GPU,
  for the checkpoint) and the annotation `timeslice.io/donor-memory-raised`.
- **Into the guest pods**: scheduling onto the virtual nodes that represent
  lent GPUs.

The verl plugin also defaults one verl setting the lend cycle needs
(`rebuild_group=True` for the NCCL weight sync, so the sync group is rebuilt
after each restore). An explicit setting of yours wins.

As soon as a guest is admitted, the platform starts pulling its images onto the
trainer node, so the first lend usually finds them there. A large image on a
node that has never pulled it can still delay the first lend (100 s for the
vLLM image in our H100 runs; 2 s once cached).

## 6. Verify and observe

**Platform up:**

```bash
kubectl -n timeslice-system get pods -o wide
```

You should see the orchestrator, the webhook (2 replicas), the donor
controller, and on GPU nodes the snapshot agent, the shadow device plugin and
the DCGM exporter. The guest kubelet starts on a trainer node once a trainer
pod runs there.

**RL job wired:**

```bash
kubectl get pod -l ray.io/group=trainer -o jsonpath='{.items[0].metadata.annotations}' | tr , '\n' | grep timeslice
kubectl get nodes -l type=virtual-kubelet
```

The trainer pod carries `timeslice.io/donor-memory-raised`, and a virtual node
appears for the trainer node. The job's log shows the time-slicing trainer:

```bash
kubectl logs <rayjob submitter pod> | grep -i timeslice | head
```

**Lending in action:**

```bash
kubectl get pods -l timeslice.io/guest=true -o wide -w
kubectl -n timeslice-system logs ds/timeslice-guest-kubelet -f | grep 'host command'
```

Each trainer step shows one lend (the guest is resumed or starts, then becomes
Ready) and one reclaim (the guest is suspended, the trainer is restored).

**GPU utilization:** the `gpu-metrics` exporter serves DCGM metrics on port
9400 of each GPU node (`DCGM_FI_PROF_SM_ACTIVE`, `DCGM_FI_DEV_GPU_UTIL`, memory
and power, 1 s samples). Point your Prometheus at it, or scrape it directly.

## 7. Optional settings

None of these is needed to run. Use them to pin versions, mirror images or
limit the platform's scope.

| Setting | Who | Use it to |
|---|---|---|
| `global.imageTag` (Helm value) | Cluster admin | Pin the images to the commit you cloned (`demo-<short sha>`) or a version. The default is `demo-batch`, the newest release of the `demo/batch` branch. |
| Image references (`<component>.image.repository`, and `timeslice-webhook.rlIntegration.image.repository`) | Cluster admin | Pull the images from your own registry or from images you built yourself. |
| `timeslice-webhook.namespaceSelector` (Helm value) | Cluster admin | Limit the platform to some namespaces. By default it watches every namespace except `kube-system` and `timeslice-system`. |
| `-f values-inject-verl.yaml` | Cluster admin | Keep the RL team's own verl image instead of the prebuilt one; see [Keep your own verl image](#keep-your-own-verl-image). |

## 8. Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| Platform pods in `ImagePullBackOff` or `ErrImagePull` | No image with that tag: CI has not published it yet (the first release of `demo/batch`, or a pinned commit whose release run has not finished or failed), or the image is not public yet | Wait for the release workflow run (Actions, "CI - Release"); if you pinned `global.imageTag`, remove the pin to use `demo-batch` |
| The RayJob's pods are not created; an admission error names the webhook | The webhook is not ready yet | Wait until both webhook replicas are Ready, then re-apply |
| No virtual node appears | More than one pod holds `nvidia.com/gpu` on the trainer node, or the trainer pod lacks the donor label | Keep only the trainer pod on that node; check the label on the pod template |
| The job fails at start with an import error for `timeslice_verl` | The pod was created before the platform was installed, or outside a labelled Ray cluster | Re-create the RayJob after installing the platform |
| Guest stays `Pending` | No trainer GPU is lent yet (the trainer is busy or still starting) | Wait for the first trainer wait; check the virtual node exists |
| Trainer pod OOM-killed | The node lacks host memory for the checkpoint | Use nodes with more memory, or fewer GPUs per trainer pod |
| Batch requests fail with 504 or time out | The router's route timeout is shorter than a generation | Set the route timeout as in section 4 |
| The router logs `503` with `upstream_reset_before_response_started` around each reclaim | Requests running on a guest were cut off before its freeze | Expected: the async processor retries them; the snapshot agent logs `aborted in-flight connections` with the count |
| Steps get slower than without time-slicing | The reclaim is slow (large guest or checkpoint) | See the guest kubelet log for `agent call done` durations |
| The job fails at start with `The grpc package installed is at version ...` | The RL image's `grpcio` is older than 1.66 | Use an image with `grpcio` 1.66 or later |
| Leftover `NotReady` virtual nodes (`kubectl get nodes -l type=virtual-kubelet`) after RL jobs end | A virtual node is deleted about a minute after its era ends (the donor controller's era TTL, 15 minutes by default, after the trainer pod ends). One that stays was kept for a restart: its guest kubelet stopped while it could not read its DaemonSet (an older chart, or the RBAC was removed first during `helm uninstall`) | When no RL job runs on that trainer node: `kubectl delete node <name>`, then `kubectl patch node <name> --type=merge -p '{"metadata":{"finalizers":null}}'` |
| A guest pod stays `Terminating` on a `NotReady` virtual node | Its guest kubelet went away without releasing the node (a crash, or the case above). When it releases a node it force-deletes the guests left there | `kubectl delete pod <name> --grace-period=0 --force` |
| The first lend after a guest is admitted takes about a minute longer | The guest image is still being pulled onto the trainer node | Expected once per node; later lends find the image cached |
| A guest stays `Pending` on a virtual node whose trainer pod has ended | The guest kubelet cordons a virtual node as soon as its trainer pod ends, and 10 s later deletes the `Pending` guests there that have a controller, which re-creates them on another node. A bare pod is not deleted; it gets a `StrandedNoDonor` event | Delete the bare pod, or run guests from a Deployment |

## 9. Limits

- One trainer pod per trainer node, and that node runs no other GPU pods.
- While a GPU is lent, its guests have it to themselves (the trainer's state is
  in host memory), so a guest's GPU memory must fit in that GPU.
- Requests running on a guest when it is reclaimed are cut off and retried
  from the start; the tokens they had generated are lost. Long generations
  waste the most. A client that does not retry `503`, or a streaming client
  that accepts a stream without `[DONE]`, loses those requests.
- The verl plugin targets verl's fully-async mode.
- Guests run only on lent time. When no RL job is lending (between jobs, or
  after the last one ends), guests have no GPU and queued requests wait until
  their deadline and then fail. Keep a small dedicated pool for batch work that
  must always make progress, or pause the queue between RL jobs.
- A guest that is reclaimed before it has ever served is deleted and
  re-created, not frozen, so its next lend is a cold start.
