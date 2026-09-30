# timeslice-demo chart

The workloads of the time-slicing demo, deployed next to the three
timeslice charts. Every knob a demo run turns is a chart value: a
harness writes one values file per release and runs `helm install`.
No hand-written manifests are needed.

## Install order

1. GPU driver (only for a pool created with
   `gpu-driver-version=disabled`): this chart with only
   `driverInstaller.enabled=true`, in its own release.
2. `deploy/timesliceorchestrator`: set orchestrator flags under
   `flags`, one entry per flag.
3. `deploy/snapshot-agent`: select the group hosts.
4. `deploy/guest-kubelet`: select the group hosts. Set the guest
   kubelet flags under `flags`.
5. The upstream CRDs the serving path uses (InferencePool from the
   Gateway API Inference Extension) and, for `donor.kind=rayjob`, a
   KubeRay operator.
6. This chart, in the namespace of the donor and the guests.
7. llm-d-async from its own chart, with `igwBaseURL` pointing at
   `http://q1-router.<appNamespace>:8081`.

Use `fullnameOverride` on the three timeslice charts when two
releases share a cluster; their ClusterRoles are cluster-scoped.

## What this chart deploys

- `donor`: the RL job that owns the GPUs. `rayjob` is a KubeRay
  RayJob that runs verl fully-async with one trainer per group host.
  `pod` is one stand-in GPU pod per host.
- `guests`: one vLLM Deployment per group host, pinned to that
  host's virtual Node.
- `serving`: Redis, the InferencePool, the EPP, the llm-d-router
  Envoy and a load pod.
- `dcgm`: NVIDIA dcgm-exporter on every GPU node, which reports SM
  utilization.
- `tools` and `fault`: a CPU pod with the rlts client, and a pod on
  one host that can SIGSTOP and SIGCONT the snapshot agent there.

## Settings the TODAY stack needs

The binaries' defaults do not lend a GPU. A demo run sets these
values:

- orchestrator `flags`: `background-role: "true"`, so guests are
  adopted as background work.
- orchestrator `flags`: `min-bubble: 30s`, the smallest bubble that
  is lent.
- guest-kubelet `flags`: `orchestrator-addr`, where the guest kubelet
  registers.
- guest-kubelet `flags`: `freezer: agent` and `agent-addr`, so guests
  are frozen through the snapshot agent.
- guest-kubelet `flags`: `gpu-claim-mode: donor`, so guests reuse the
  donor's GPU claim.
- this chart: `donor.rayjob.expectedIdle: "45"`, the bubble hint that
  verl sends.

With `expectedIdle: "off"` (the default), verl sends no idle hint.
Under the orchestrator's default `--lend-policy=hint`, nothing is
lent.

`gpu-claim-mode: donor` finds the donor pod with the guest kubelet's
default donor selector:
`timeslice.io/job-id,timeslice.io/role!=background`.
The donor pod must be in the guest's namespace.

## RL pacing

The verl pacing values are stated here, not taken from verl's
defaults:

- `donor.rayjob.staleness`, default `0`: the
  `async_training.staleness_threshold`. At 0, the trainer waits for
  fresh rollouts, so each step leaves a bubble on the trainer GPUs.
- `donor.rayjob.miniBatch`, default `16`: the `ppo_mini_batch_size`.
- `donor.rayjob.totalEpochs` and `donor.rayjob.timeoutS`.

## GPU metrics

`dcgm.access` selects how the exporter reaches the GPUs:

- `dra-admin`: a DRA ResourceClaimTemplate with `adminAccess: true`
  and `allocationMode: All`. The exporter gets monitoring access to
  GPUs that are allocated to other pods. The exporter namespace must
  carry the label `resource.kubernetes.io/admin-access=true`.
- `hostpath`: mounts the host driver directory and `/dev` into a
  privileged container.

`DCGM_FI_DEV_GPU_UTIL` is reported on every GPU. The
`DCGM_FI_PROF_*` fields need profiling support. If the GPU does not
support them, the exporter skips them.

## SYS_PTRACE on the sampler

The sampler pod runs several processes that share CUDA tensors for
verl's bucketed weight transfer. How PyTorch shares them depends on
its allocator:

- With `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`, the
  receiving process takes the sender's file descriptors with
  `pidfd_getfd`. Without `CAP_SYS_PTRACE` that call fails with
  "Operation not permitted".
- Without expandable segments, PyTorch uses cudaIpc memory handles,
  and no added capability is needed.

A two-process probe on an L4 node, run with the demo's RL image,
showed this:

| Allocator | No added capability | `SYS_PTRACE` |
| --- | --- | --- |
| default | ok | ok |
| expandable segments | `pidfd_getfd` refused | ok |

So the chart's default leaves expandable segments off on the
sampler and adds no capability. To use expandable segments on the
sampler, set both `donor.rayjob.sampler.expandableSegments` and
`donor.rayjob.sampler.sysPtrace` to `true`. The trainer pods keep
expandable segments and never need `SYS_PTRACE`.
