# guest-kubelet

A *guest kubelet*: a [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) (v1.14.0)
that registers a virtual Node (`vk-<host>`) next to a real GPU node (the donor host) and acts as
the kubelet for the guest pods scheduled onto it. Each guest runs as a mirror pod on the host, on
the GPUs the donor (the trainer pod) holds, and only while the orchestrator lends the host.

Deploy it with the Helm chart in `../deploy` (subcharts `guest-kubelet` and `gpu-shadow`, on by
default in the parent chart); see `../deploy/README.md` and `../guides/timeslice-quickstart`.

## What it does

- Registers Node `vk-<host suffix>` with labels `type=virtual-kubelet` and
  `timeslice.io/virtual-node=true`, taint `timeslice.io/guest=true:NoSchedule`, capacity
  `cpu`/`memory`/`pods`/`nvidia.com/gpu`, and the host's IP as `InternalIP`. No
  `kubernetes.io/os` label, which keeps system DaemonSets off it.
- A guest is a pod bound to the node that tolerates `timeslice.io/guest` (`--guest-marker`). For
  each one the VK creates a mirror pod `<guest>-m` pinned to the host (`internal/backend/mirror`):
  - scheduling fields, probes, readiness gates and guest labels are removed;
  - labels `timeslice.io/mirror-of=<guest UID>`, `timeslice.io/mirror-node=<vnode>` and
    `timeslice.io/group` are added;
  - requests are capped at `--mirror-cpu-headroom` / `--mirror-memory-headroom`
    (`--guest-budget=computed` sizes the virtual Node from what the host has free);
  - `nvidia.com/gpu` is replaced by the same quantity of `timeslice.io/gpu-shadow`, the pooled
    resource of the GPU shadow device plugin (below).
- The mirror's status is copied back to the guest: phase, IPs, start time, conditions and
  container states. Deleting the guest deletes the mirror with the guest's grace period.
- The VK runs the guest's readinessProbe itself against the mirror's IP, with kubelet semantics
  (`internal/probe`); `--guest-probe-policy` picks which probes a guest may carry.
- Admission refuses a guest before it gets a mirror (a probe the policy does not allow, a GPU
  resource other than `nvidia.com/gpu`, a host GPU model not in `--gpu-allowlist`, a privileged
  GPU container). The guest gets a `GuestRejected` Warning event and goes `Failed`.
- The real node's labels `timeslice.io/donor=true` and `timeslice.io/group=<group>` name its
  group (`--group-source`). With no single group, no mirror is created and the virtual Node gets
  a `GroupUnresolved` event.

## GPUs: the pooled shadow resource (`--gpu-mode=pooled`)

The donor books plain `nvidia.com/gpu` through the cluster's normal device plugin. The GPU shadow
device plugin (`cmd/gpu-shadow-plugin`, `internal/gpushadow`) advertises the donor's GPUs a second
time on one resource, `timeslice.io/gpu-shadow`: its devices are the GPUs the node's single
`nvidia.com/gpu` holder holds, read from the kubelet's pod-resources API, and zero when more than
one pod holds GPUs (fail closed). Its Allocate hands out the same device nodes and driver mount
the normal plugin does. The real kubelet picks the devices for each mirror and sets its device
cgroup, so the mirror needs no privilege and no hostPath. No DRA driver is needed.

The virtual Node's GPU capacity follows the host's `timeslice.io/gpu-shadow` allocatable. The VK
watches the donor pods on its host (`--gpu-donor-selector`, default `timeslice.io/donor=true`):
when the donor holding a guest's GPUs is gone, it taints the host `timeslice.io/gpu-fence` and
stops the mirror.

## Host commands: lend and reclaim

With `--host-command-port`, the VK serves the orchestrator's `HostCommandService`
(`pkg/timeslice-orchestrator/api/hostcommand/v1alpha1` in the root module) on
`--host-ip:<port>`. No mirror is created before the first Resume.

- Resume creates the missing mirrors, resumes suspended ones and releases each guest's Ready once
  its engine serves.
- Vacate holds each guest NotReady, confirms it, suspends it by the command's deadline and acks
  VACATED only when every guest is suspended or its mirror is gone; a guest that cannot be
  suspended is killed (agent Kill, then a normal delete).
- `--freezer=agent` suspends and resumes through the node's snapshot agent
  (`pkg/snapshot-agent/api/v1alpha1`), which checkpoints the guest's GPU state; the VK never
  touches cgroups.
- Epochs fence the commands. A journal on the virtual Node (`timeslice.io/host-command`) and the
  per-mirror suspend state let a restarted VK re-derive what it held (`provider.Recover`).
- `--cordon-while-held` cordons the virtual Node while any guest is suspended.
- `--cordon-without-donor` (default true) also cordons it while the host has no donor pod, and
  uncordons it when one is back. After `--requeue-stranded-after` (default 10s) without a donor,
  Pending guests that have a controller are deleted so the controller re-creates them on another
  node; a bare pod gets a `StrandedNoDonor` event and is left alone.

## Outage guard: Node finalizer

The VK Node carries the finalizer `timeslice.io/virtual-node-protection` and an ownerReference to
the real Node, so a VK outage never garbage-collects its guests. Only the VK and the donor
controller remove the finalizer:

- the donor controller, when the real Node is gone (`--release-dead-hosts`);
- the VK when it is stopped and its controller no longer wants a VK on the host (a DaemonSet
  that stops selecting the host at era end, or an uninstall; `--stop-cleanup`): it deletes the
  Node, removes the finalizer, deletes the Node's mirrors and force-deletes the pods left bound
  to it. This needs `get` on its DaemonSet (the chart grants it); when the read fails the VK
  assumes a restart and keeps the Node cordoned and NotReady;
- the VK when it deregisters: stop the serving VK, then run
  `guest-kubelet --deregister --node-name=<node>`;
- the VK when it returns and finds its Node Terminating on a live host
  (`--reclaim-terminating-node`, default true): it registers the Node again before the kubelet
  role starts.

## Layout

```
cmd/guest-kubelet/                   flags, leader election, nodeutil wiring, host command server
cmd/gpu-shadow-plugin/               the GPU shadow device plugin
internal/provider/                   Node spec, pod provider, guest marker, admission, finalizer,
                                     cordon, recovery after a restart
internal/group/                      the host node's group
internal/backend/mirror/             guest -> mirror (builder), status copy, mirror lifecycle,
                                     pooled GPU attachment, donor fence, suspend and kill
internal/hostcmd/                    Vacate/Resume server, epochs, journal, snapshot-agent client
internal/freeze/                     the freezer interface the VK uses
internal/probe/                      readiness, startup, exec and gRPC probes
internal/gpushadow/                  the shadow plugin: GPU discovery, pod-resources, pooled plugin
internal/prepull/                    image prepull on the host
```

## Build

Nothing runs locally. From the repository root (the module imports the root module's gRPC APIs
through a `replace` directive):

```
gcloud builds submit --project=YOUR_PROJECT --config guest-kubelet/cloudbuild.yaml \
  --substitutions=_REGION=YOUR_REGION,_TAG=$(git rev-parse --short HEAD) .
gcloud builds submit --project=YOUR_PROJECT --config guest-kubelet/cloudbuild-gpu-shadow.yaml \
  --substitutions=_REGISTRY=YOUR_REGISTRY,_TAG=$(git rev-parse --short HEAD) .
```

`make build` and `make build-gpu-shadow` run the same commands.
