# Time-slicing quickstart: lend an idle RL trainer GPU to batch inference

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
| Cluster admin | `helm install timeslice` (on GPUs other than the L4, one more value) |
| RL team | one label and three settings in the RayJob |
| Batch team | one label on the model server pods |

The RL team keeps its stock verl image: **the platform's admission webhook
injects the verl fork with the lifecycle hooks and the timeslice packages into
the Ray pods** (see [section 5](#5-what-the-platform-injects)).

A few settings beyond the labels remain; they are listed honestly in
[section 6](#6-settings-beyond-the-labels).

## Contents

1. [Requirements](#1-requirements)
2. [Install the platform (cluster admin)](#2-install-the-platform-cluster-admin)
3. [Change the RayJob (RL team)](#3-change-the-rayjob-rl-team)
4. [Label the model server (batch team)](#4-label-the-model-server-batch-team)
5. [What the platform injects](#5-what-the-platform-injects)
6. [Settings beyond the labels](#6-settings-beyond-the-labels)
7. [Verify and observe](#7-verify-and-observe)
8. [Troubleshooting](#8-troubleshooting)
9. [Limits](#9-limits)
10. [Appendix: full before/after diffs](#10-appendix-full-beforeafter-diffs)

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
- The RL image: Python 3.10 or later with `grpcio` 1.66 or later and `protobuf` 6 or later
  (stock `verlai/verl` images have them). The platform brings the verl version
  and plugin it needs (see [section 5](#5-what-the-platform-injects)).
- The batch server: anything that serves over HTTP and tolerates pauses of a
  few minutes. Put a queue in front of it (for example llm-d-async) so requests
  wait instead of failing.

## 2. Install the platform (cluster admin)

```bash
git clone https://github.com/llm-d-incubation/llm-d-rl-time-slicing.git
cd llm-d-rl-time-slicing/deploy
helm dependency update .
helm install timeslice . -n timeslice-system --create-namespace
```

That is all. The defaults:

- run the node agents only on GPU nodes;
- detect the GPU model and its memory on each node;
- watch every namespace except `kube-system` and `timeslice-system`;
- include a DCGM exporter with 1 s samples (`gpu-metrics`), so you can see the
  trainer GPU before and after.

On GPUs other than the L4 (for example H100), also set
`--set snapshot-agent.scrubPolicy=flag`; see
[Troubleshooting](#8-troubleshooting).

To pin a release, clone at the release tag and set the image tags (each
component's `image.tag`). To limit the platform to some namespaces, set
`timeslice-webhook.namespaceSelector`.

## 3. Change the RayJob (RL team)

Start from your normal RayJob. The trainer worker group and the rollout worker
group must be separate worker groups, with the trainer group on its own GPU
nodes. Make three changes:

1. **Label the trainer worker group** `timeslice.io/donor: "true"`. This is the
   group whose GPUs are lent.
2. **Pin Ray placement** with one `resources` line per worker group, so verl's
   trainer and rollout placement groups land on the right pods.
3. **Two verl settings**: `async_training.trainer_name=timeslice` (the
   time-slicing trainer, a subclass of verl's fully-async trainer) and
   `ray_pg_extra_resources=...` (the custom resources from change 2).

Full diff of the example in this guide (long chain-of-thought math RL,
DeepSeek-R1-Distill-Qwen-1.5B, 16K responses, 1 trainer GPU + 1 rollout GPU):

```diff
@@ entrypoint @@
     async_training.staleness_threshold=8
     async_training.trigger_parameter_sync_step=1
+    async_training.trainer_name=timeslice
+    'ray_pg_extra_resources={trainer_pool:{trainer_node:1},rollout_pool:{rollout_node:1}}'
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
```

The complete before and after manifests are in [examples/](examples/):
`rayjob-before.yaml` and `rayjob-after.yaml`. Both mount
`data-prep-configmap.yaml` (the script that samples the DAPO-Math-17k subsets)
and use the node pool names `cpu-pool`, `h100-trainer-pool` and
`h100-rollout-pool`; change those to your own pools.

Nothing else changes: same image, same command, same resources. Apply it with
`kubectl apply -f rayjob-after.yaml`.

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

The full manifests are `examples/guest-before.yaml` and
`examples/guest-after.yaml`.

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

## 5. What the platform injects

The admission webhook changes only pods and Ray clusters that opted in with the
labels above. It adds:

- **Into every pod of a Ray cluster that has a donor group** (head, trainer and
  rollout workers): an init container that copies the timeslice Python
  packages into a read-only volume, and `PYTHONPATH` pointing at it. The
  packages are:
  - the timeslice client and the verl plugin (`trainer_name=timeslice`);
  - **a verl fork**: verl at commit `983cb0f2` plus two commits that add
    lifecycle hooks to the fully-async trainer (source:
    `github.com/aishukamal/verl`, branch `feat/fully-async-lifecycle-hooks`,
    commit `976b5f57`). It is pinned, ships with its Apache-2.0 LICENSE, and
    replaces the image's own verl in those pods only. verl's dependencies still
    come from your image. Turn it off with
    `timeslice-webhook.rlIntegration.injectVerl=false` if your image already
    has a verl with these hooks.
- **Into the same pods**: `TIMESLICE_FULLY_ASYNC=1` and the job's identity and
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

## 6. Settings beyond the labels

The labels are the only change to your workloads. These settings are the rest,
so you can plan for them:

| Setting | Who | When it is needed |
|---|---|---|
| `snapshot-agent.scrubPolicy=flag` (Helm value) | Cluster admin | On every GPU other than the qualified NVIDIA L4 with driver 580, for example H100. Without it the agent refuses to freeze guests there and kills them at each reclaim instead. With it, the agent zeroes freed GPU memory itself: about 0.4 s for 84 GB on an H100. |
| Image references (`<component>.image.repository` / `.tag`, and `timeslice-webhook.rlIntegration.image`) | Cluster admin | Only when you install from a source checkout instead of a published release, or mirror images to your own registry. |
| One `resources` line per Ray worker group | RL team | Always (Ray placement pinning, so verl's trainer and rollout placement groups land on the right pods). |
| `async_training.trainer_name=timeslice` and `ray_pg_extra_resources=...` | RL team | Always (two verl command-line settings). |
| `timeslice.io/rl-integration: "true"` pod label | RL team | Not for KubeRay: the webhook adds it to the head and rollout groups of a Ray cluster that has a donor group. Only if your RL pods are not created by KubeRay, label every non-trainer pod of the job yourself so they get the same packages. |
| `timeslice-webhook.rlIntegration.injectVerl=false` | Cluster admin | Only if your RL image already has a verl with the lifecycle hooks. |
| Router route `timeout` above your longest single generation (or `0s`) | Batch team | If requests go through the llm-d router and a generation can take more than 15 s (Envoy's default route timeout). |
| A separate node for the trainer pod | RL team | Always: the trainer must be the only GPU pod on its node. |

## 7. Verify and observe

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

## 8. Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| The RayJob's pods are not created; an admission error names the webhook | The webhook is not ready yet | Wait until both webhook replicas are Ready, then re-apply |
| No virtual node appears | More than one pod holds `nvidia.com/gpu` on the trainer node, or the trainer pod lacks the donor label | Keep only the trainer pod on that node; check the label on the pod template |
| The job fails at start with an import error for `timeslice_verl` | The pod was created before the platform was installed, or outside a labelled Ray cluster | Re-create the RayJob after installing the platform |
| Guest stays `Pending` | No trainer GPU is lent yet (the trainer is busy or still starting) | Wait for the first trainer wait; check the virtual node exists |
| Trainer pod OOM-killed | The node lacks host memory for the checkpoint | Use nodes with more memory, or fewer GPUs per trainer pod |
| Batch requests fail with 504 or time out | The router's route timeout is shorter than a generation | Set the route timeout as in section 4 |
| The router logs `503` with `upstream_reset_before_response_started` around each reclaim | Requests running on a guest were cut off before its freeze | Expected: the async processor retries them; the snapshot agent logs `aborted in-flight connections` with the count |
| Steps get slower than without time-slicing | The reclaim is slow (large guest or checkpoint) | See the guest kubelet log for `agent call done` durations |
| The guest is killed at every reclaim instead of frozen; the snapshot agent logs `VRAM zeroing qualification` with `qualified: false` | The GPU model and driver are not on the qualified list (default `NVIDIA L4:580`). Under the default scrub policy `keep` the agent will not freeze a guest whose freed GPU memory it cannot trust to be zeroed | Set `snapshot-agent.scrubPolicy=flag`: the agent then scrubs freed GPU memory itself on unqualified GPUs (on an H100 80GB, about 0.4 s per freeze) |
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

## 10. Appendix: full before/after diffs

These are the complete differences between the example manifests in
[examples/](examples/) (`diff -u`). Lines marked `timeslice:` are the only
changes.

RayJob:

```diff
--- rayjob-before.yaml
+++ rayjob-after.yaml
@@ -1,6 +1,6 @@
 # Long-CoT math RL (DeepSeek-R1-Distill-Qwen-1.5B, DAPO-Math-17k 512/64, 16K responses, staleness 8),
 # verl fully-async, 1 trainer H100 + 1 rollout H100: the benchmark's single 2-GPU Job as a KubeRay RayJob.
-# This is the RL team's normal manifest, with no time-slicing changes.
+# Time-sliced: the RL team's normal manifest plus three changes (marked "timeslice:").
 apiVersion: ray.io/v1
 kind: RayJob
 metadata:
@@ -75,6 +75,8 @@
     rollout.total_rollout_steps=65536
     async_training.staleness_threshold=8
     async_training.trigger_parameter_sync_step=1
+    async_training.trainer_name=timeslice
+    'ray_pg_extra_resources={trainer_pool:{trainer_node:1},rollout_pool:{rollout_node:1}}'
   runtimeEnvYAML: |
     env_vars:
       PYTHONUNBUFFERED: "1"
@@ -100,7 +102,10 @@
         rayStartParams:
           num-cpus: "20"
           num-gpus: "1"
+          resources: "'{\"trainer_node\": 100}'"   # timeslice: Ray placement pinning
         template:
+          metadata:
+            labels: {timeslice.io/donor: "true"}   # timeslice: lend this group's GPU
           spec:
             nodeSelector: {cloud.google.com/gke-nodepool: h100-trainer-pool}   # your trainer pool
             tolerations: [{key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}]
@@ -135,6 +140,7 @@
         rayStartParams:
           num-cpus: "20"
           num-gpus: "1"
+          resources: "'{\"rollout_node\": 100}'"   # timeslice: Ray placement pinning
         template:
           spec:
             nodeSelector: {cloud.google.com/gke-nodepool: h100-rollout-pool}   # your rollout pool
```

Model server:

```diff
--- guest-before.yaml
+++ guest-after.yaml
@@ -1,4 +1,4 @@
-# The batch team's normal vLLM model server (llm-d-async sends it work through the llm-d router).
+# The batch team's vLLM model server with the one time-slicing change (marked "timeslice:").
 apiVersion: apps/v1
 kind: Deployment
 metadata:
@@ -12,6 +12,7 @@
     metadata:
       labels:
         app: guest-vllm
+        timeslice.io/guest: "true"   # timeslice: run on lent GPU time
     spec:
       terminationGracePeriodSeconds: 10
       containers:
```
