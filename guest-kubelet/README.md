# guest-kubelet

A throwaway prototype of a *guest kubelet*: a [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet)
(v1.14.0) that registers a virtual Node (`vk-<host>`) next to a real GPU node and acts as the
kubelet for the guest pods scheduled onto it.

## Status: M2 (readiness) and M3 (suspend and resume, below)

- Registers Node `vk-<host suffix>` (for example `vk-abcd`) with labels `type=virtual-kubelet`,
  `timeslice.io/virtual-node=true`, taint `timeslice.io/guest=true:NoSchedule`, capacity
  `cpu`/`memory`/`pods`/`nvidia.com/gpu: 1`, and the host's IP as `InternalIP`. No
  `kubernetes.io/os` label, which keeps GKE's system DaemonSets off it.
- A guest is a pod bound to the node that tolerates `timeslice.io/guest` by key. For each one the
  VK creates a mirror pod `<guest>-m` pinned to the host (`internal/backend/mirror`):
  - scheduling fields, probes, readiness gates and guest labels are removed;
  - labels `timeslice.io/mirror-of=<guest UID>` and `timeslice.io/mirror-node=<vnode>` are added;
  - requests are capped at `--mirror-cpu-headroom` / `--mirror-memory-headroom`;
  - `nvidia.com/gpu` is replaced by the ResourceClaim `--gpu-claim`, which must already be
    allocated on the host (by the trainer).
- A label-filtered informer copies the mirror's status to the guest: phase, IPs, start time,
  conditions and container states.
- Deleting the guest deletes the mirror with the guest's grace period. The guest is removed as
  soon as the mirror has stopped.
- Leader election (`--leader-elect`, Lease `guest-kubelet-<vnode>`) runs 2 replicas. On restart
  or failover, existing mirrors are found again and nothing is re-created.
- `--mirror-owner-ref=false` lets mirrors outlive guests that were force-deleted after a Node
  deletion. A guest re-created with the same name and the same containers re-adopts the mirror
  (same UID and IP). Orphans are deleted after `--orphan-grace`.
- The real node's labels `timeslice.io/donor=true` and `timeslice.io/group=<group>` name its group
  (`--group-source=ns`, default; `either` also accepts `group.timeslice.io/<group>=true`). Mirrors
  carry `timeslice.io/group`. With no single group (missing, half-written or two groups) no mirror
  is created and the virtual Node gets a `GroupUnresolved` event.
- M2: the VK runs the guest's readinessProbe itself (`internal/probe`)
  against the mirror's IP, with kubelet semantics: period, timeout and
  thresholds with the kubelet's defaults; a verdict that starts not ready
  and starts over when the container restarts; HTTP 200-399 without
  keep-alive or redirects; named ports. The guest's ContainersReady and
  Ready follow the verdicts (reason `ContainersNotReady`). httpGet and
  tcpSocket only: a container with an exec or gRPC readiness probe stays
  not ready, with one `ReadinessProbeUnsupported` Warning event.
  `--readiness-probes=false` restores M1 (Ready follows the mirror).
- `--debug-addr` (off by default, loopback only) serves
  `POST /debug/readiness?pod=ns/name&ready=true|false|clear` (an override
  that wins over the probes) and `GET /debug/ready-edges?pod=ns/name`
  (when the VK sent each Ready change). Only the Q5 measurement uses it.
  The same address serves the M3 suspend and resume hooks (below).
- Host commands (`--host-command-port`, off by default): the VK serves the
  orchestrator's `HostCommandService` (`api/hostcommand/v1alpha1`) on
  `--host-ip:<port>` and never reads the orchestrator's lock. No mirror is
  created before the first Resume. Resume creates the missing mirrors, resumes
  suspended ones and releases each guest's Ready once its engine serves;
  guests that arrive while the node is lent get a mirror at once. Vacate holds
  each guest NotReady, confirms it, then suspends it by the command's deadline
  and acks VACATED only when every guest is suspended or its mirror is gone; a
  guest that cannot be suspended is killed (agent Kill, then a normal delete).
  Epochs fence the commands. A command still running just before the caller's
  RPC deadline is answered `OUTCOME_UNSPECIFIED` (not finished); calling again
  joins it. Mirrors then carry `timeslice.io/job-id`,
  `timeslice.io/role=background`, `restartPolicy: Never` and
  `timeslice.io/guest-epoch`. `--freezer`: `agent` (snapshot-agent
  SuspendAll/ResumeAll with the command's epoch, one call per command),
  `delete` (default: delete the mirror, re-create it on the next Resume) or
  `fake` (test freezer that only annotates the mirror). `--host-command-allow`
  limits the callers.
- Admission (VK-A7) refuses a guest before it gets a mirror: a liveness
  or startup probe, an exec or grpc readiness probe, readiness gates (an
  httpGet or tcpSocket readinessProbe is allowed), a GPU resource other
  than `nvidia.com/gpu`, or a GPU guest on a host whose model label is
  not in `--gpu-allowlist` (default `nvidia-l4`; no label fails closed).
  The guest gets a Warning event `GuestRejected` naming the rule and goes
  `Failed` with reason `GuestRejected`.
- A GPU mirror container's memory limit is its limit (or request) plus
  the device reserve, `ceil(--gpu-memory x --mirror-memory-factor)`
  (defaults `23034Mi` x `1.1`, about 24.7 GiB).
- Outage (VK-A7 on this branch): the Node finalizer below holds the
  virtual Node through a guest-kubelet outage; no keeper process.
- Not yet: logs/exec (use `kubectl logs <guest>-m`), stats. Liveness and
  startup probes are refused by admission (above), so an exec or gRPC
  readinessProbe never reaches the prober.

## Layout

```
cmd/guest-kubelet/main.go            flags; leader election; nodeutil.NewNode wiring; own event recorder
internal/provider/node.go            the Node spec (labels, taint, capacity, conditions); NodeProvider
internal/provider/provider.go        the pod provider: guest filter, hands guests to the backend
internal/provider/marker.go          --guest-marker: what makes a pod a guest
internal/provider/admission.go       admission: probes, gates, GPU allowlist
internal/provider/events.go          drops events about non-guest pods
internal/group/                      the host node's group (timeslice.io/donor + timeslice.io/group), host watch
internal/provider/finalizer.go       Node finalizer, ownerRef to host, release
internal/donorstandin/               donor stand-in: releases Node on host death
cmd/donor-standin/main.go            its command (same image as the VK)
internal/backend/mirror/builder.go   guest -> mirror pod (pure function)
internal/backend/mirror/status.go    mirror status -> guest status
internal/backend/mirror/backend.go   create/adopt/delete mirrors, mirror informer, orphan GC
internal/backend/mirror/claim.go     optional reservedFor write (kube-controller-manager also does it)
internal/hostcmd/                    Vacate/Resume server, epochs, freezers
internal/backend/mirror/orchestrated.go  Ready hold, epoch CAS, vacate deletes
api/                                 copied protos, generated code
internal/probe/probe.go              readiness prober (httpGet, tcpSocket)
internal/probe/debug.go              debug endpoint: override, Ready edges
cmd/q5-measure/main.go               Q5 timings; runs in a pod
deploy/opt-b/host-labels.yaml        the donor/group node labels as a kubectl merge patch
internal/backend/mirror/suspend.go   M3 suspend/resume, state, status overlay
internal/backend/mirror/readiness.go MarkNotReady; wait for NotReady
internal/backend/mirror/readycheck.go resume check: probe until Ready
internal/freeze/                     cgroup v2 freezer (freeze.Backend)
internal/backend/mirror/hold.go      Hold: any guest suspended (cordon)
internal/provider/cordon.go          --cordon-while-held (D-NS-8)
cmd/guest-kubelet/debug.go           --debug-addr: M2 hooks, suspend, resume
deploy/                              namespace + SA, RBAC, Deployment, CPU test guest + Service
deploy/guests/                       guest manifests: today, ns
deploy/admission/                    W9 policies, one per guest marker
deploy/m1/                           claim + trainer stand-in, vLLM guest, StatefulSet guest,
                                     rollout-test DaemonSet, curl client, driver installer, VAP test
deploy/opt-c/                        donor stand-in Deployment + RBAC
deploy/m2/                           probed guests, pool, router, Q5 pods
cloudbuild.yaml                      tidy check, vet, test, build, image push (nothing runs locally)
```

## M3: suspend and resume by cgroup freeze

Interim step: the VK freezes the mirror's pod cgroup itself. It sits behind
`freeze.Backend` (`internal/freeze`), so M4 swaps it for the snapshot agent's
Suspend/Resume and the VK stops touching cgroups.

- Suspend (`Backend.Suspend`), in this order: raise `timeslice.io/guest-epoch`
  and set `timeslice.io/suspend-state=Suspending` on the mirror
  (compare-and-swap on its resourceVersion); the guest turns NotReady
  (`MarkNotReady`, the same function the prober uses for a failed probe, with
  reason `Suspending`); wait until the API shows the guest NotReady
  (`--suspend-notready-timeout`); write `1` to `cgroup.freeze` on the
  pod-level cgroup and wait for `frozen 1` in `cgroup.events`
  (`--freeze-timeout`); record `Suspended`. No drain. If the wait or the
  freeze fails, the pod is thawed and put back to Running.
- While frozen the guest shows phase Running, Ready=False, condition
  `timeslice.io/suspended=True`, and its containers Waiting with reason
  `Suspended`, so `kubectl get pods` prints `0/1 Suspended`. Restart counts
  do not change.
- Resume (`Backend.Resume`): raise the epoch, record `Resuming` (still
  NotReady), thaw, run the guest's httpGet/tcpSocket readinessProbe against
  the mirror (one attempt at a time with the prober's HTTP and TCP semantics)
  until it passes (`--resume-ready-timeout`), then clear the state; from then
  on the prober's verdict decides Ready, as for any guest. Any failure leaves
  it `Resuming`, so it is never Ready on a process that did not come back.
- The suspend state is applied after the prober's verdict, so a probe that
  still passes (or a debug override) never shows a suspended guest Ready.
- One suspend or resume per guest at a time. The state lives on the mirror,
  so a restarted VK or a new leader derives the same guest status. Deleting a
  suspended guest thaws the mirror first, so it can act on SIGTERM.
- Deployment: the VK container is privileged, runs as root and mounts the
  host's `/sys/fs/cgroup` read-write at `/host/cgroup` (`--cgroup-root`;
  empty disables suspend). The cgroup is found under both kubelet cgroup
  drivers (systemd `kubepods.slice/...` and cgroupfs `kubepods/...`). Mirror
  pods stay unprivileged.
- Until the orchestrator loop (VK-A6) drives it, suspend and resume are
  triggered by hand through `--debug-addr` (loopback only, for example
  `127.0.0.1:10261`, reached with `kubectl port-forward` to the leader pod;
  it works with `--readiness-probes=false` too):
  `POST /debug/suspend?namespace=<ns>&name=<guest>` and
  `POST /debug/resume?...`. The reply is the step timings as JSON. It has no
  authentication: enable it only in test deployments.
- Cordon while held (`--cordon-while-held`, pending decision D-NS-8; default
  true = option ns-cordon, false = option skip): while at least one guest of
  the virtual Node is `Suspending` or `Suspended` (the donor has the GPU),
  the VK sets `spec.unschedulable` on its Node, so `kubectl get nodes` shows
  `SchedulingDisabled` and no new guest binds to it. `Resuming` is not held.
  The hold is read from the mirrors (so a restarted VK decides the same),
  re-checked on every suspend-state change and every 0.5 s, and the Node is
  re-read every 5 s to repair a lost cordon. The VK patches only
  `spec.unschedulable` and its own annotation
  `timeslice.io/cordoned-by=guest-kubelet`, and only removes a cordon
  carrying that annotation, so an admin `kubectl cordon` is left alone.
  Guests already bound stay bound and resident. The orchestrator loop
  (VK-A6) can feed the same cordoner from the group state.

## M5: restart and relist

Nothing in memory survives a guest-kubelet restart. Before the pod
controller and the host command server start, `provider.Recover`
(`internal/provider/recover.go`) lists the pods bound to the virtual node
and their mirrors from the API and re-derives what the process held:

- Per-guest suspend state lives on each mirror (`timeslice.io/suspend-state`
  plus `timeslice.io/guest-epoch`), written before and after every freeze and
  thaw in both the M3 and the host command paths. The cordon while held
  counts these mirrors, so it comes back on its own.
- Host command mode keeps a journal on the virtual Node, annotation
  `timeslice.io/host-command` (last accepted command, epoch, deadline, phase,
  outcome, mirror attempt counters). It is written before a command acts and
  when it is done. After a restart:
  - the epoch fence is the journal's epoch (older commands are stale);
  - a command that succeeded is answered from the journal on a retry with
    the same epoch, without acting again; a failed or aborted one runs again;
  - an unfinished command runs again with its epoch once the pod controller
    is ready, and the orchestrator's retry joins it; each guest step is
    idempotent on the mirror's suspend state, and a freezer that can report
    its state (`Frozen`) wins over a half-written record;
  - after a Resume the node is lent again; a running, Ready guest stays Ready;
  - no journal (first start, or the Node was deleted): fail closed, held
    until the first command.
- M3 path: `ReconcileSuspend` makes each mirror's recorded state agree with
  the cgroup freezer (the host wins).
- A mirror deleted to vacate is marked `timeslice.io/vacated`, so a delete
  seen only after the restart reports the guest vacated, not failed.
- Attempt counters are restored from the mirrors and the journal, so no job
  id repeats.
- Commands wait until the pod controller is ready (`Config.Ready`).

Limits: journal and suspend-state writes are best effort (a failed write is
logged; the restart after it knows less and fails closed). The readiness
probe verdict is re-learned after a restart.

## Build and deploy

```
make build        # Cloud Build: tidy check, go vet, go test -race, image -> Artifact Registry
make deploy       # namespace, RBAC, Deployment on the test cluster (HOST=<real node>)
make test-guest   # CPU guest + Service
make gpu-guest    # claim + trainer stand-in, then the vLLM guest
make m2-deploy    # probed guests, pool, EPP, router, RBAC
make q5-force     # Q5 run, readiness forced via the debug endpoint
make q5-toggle    # Q5 run, a real probe flipped by the guest
make status
make undeploy
```

## Outage guard: Node finalizer (option c of an open decision)

The VK Node carries the finalizer `timeslice.io/virtual-node-protection`
and an ownerReference to the real Node. When anyone else deletes the VK
Node during a VK outage (for example the cloud node lifecycle
controller), the finalizer holds it (Terminating), so its guests are not
garbage-collected. Only the VK and the donor controller remove the
finalizer:

- the donor controller, when the real Node is gone (`deploy/opt-c/`, a
  stand-in until the real controller exists; the VK cannot act on its
  own host's death);
- the VK itself when it deregisters at the end of an era: stop the
  serving VK, then run `guest-kubelet --deregister --node-name=<node>`;
- the VK itself when it returns and finds its Node Terminating on a live
  host (`--reclaim-terminating-node`, default true). A Terminating Node
  cannot be un-deleted, so the VK removes its finalizer, waits for the
  old object to go and registers the Node again (same name, new UID)
  before the kubelet role starts. Guests are bound by Node name and pod
  GC waits 40 s before it treats a Node as missing, so guests and mirrors
  are untouched. It reclaims only a Node whose only finalizer is ours and
  whose ownerReference names the host this VK runs on; a Node from a
  recreated host stays held for the donor controller.

With `--reclaim-terminating-node=false` a held Node keeps serving but
stays Terminating until the donor controller or a deregistration
releases it. A `kubectl delete node` made while the VK is down is also
undone by the next start; use `--deregister` to remove the Node.

The plan and the notes explaining this code are kept outside this repository
(prototype plan, milestones M0 to M2).

## M2 results (the test cluster, 2026-09-28)

Q5 spans in seconds, p50 / p90 / max, 25 edges each way per run
(`make q5-force`, `make q5-toggle`):

- notify: trigger -> the VK calls NotifyPods;
- e1: notify -> pod status seen in a watch;
- e2: pod Ready -> EndpointSlice endpoint ready;
- e3: EndpointSlice -> first ClusterIP request with the new outcome;
- pool: pod Ready -> first router (InferencePool) request with the new
  outcome.

Force mode (readiness forced through the debug endpoint):

| span | rising | falling |
| --- | --- | --- |
| notify | 0.000 / 0.000 / 0.001 | 0.000 / 0.000 / 0.000 |
| e1 | 0.036 / 0.042 / 0.247 | 0.036 / 0.044 / 0.045 |
| e2 | 0.028 / 0.048 / 0.061 | 0.028 / 0.035 / 0.039 |
| e3 | 7.042 / 7.352 / 7.445 | 5.848 / 6.422 / 6.592 |
| pool | 0.005 / 0.016 / 0.025 | 0.007 / 0.017 / 0.019 |

Probe mode (a real readinessProbe, period 1 s, flipped by the guest):

| span | rising | falling |
| --- | --- | --- |
| notify | 0.457 / 0.959 / 0.977 | 0.432 / 0.884 / 0.995 |
| e1 | 0.035 / 0.040 / 0.045 | 0.036 / 0.039 / 0.042 |
| e2 | 0.026 / 0.030 / 0.030 | 0.025 / 0.029 / 0.031 |
| e3 | 6.458 / 6.981 / 8.228 | 5.396 / 6.142 / 6.269 |

- The guest's Ready follows the VK's probe: the inference simulator's
  guest turned Ready about 9 s after creation, when `/metrics` first
  answered, not when its container started.
- notify in probe mode is the probe period (1 s) plus the probe itself.
- e3 is the node's kube-proxy, which syncs rules at most once every 10 s
  on the test cluster (`--iptables-min-sync-period=10s`). These runs flip
  readiness every 4 to 10 s, so most changes wait for the next allowed
  sync. A third force run with 12 s between edges (20 each way,
  `--settle=12s`) gave e3 p50 0.517 s rising (bounded by the 0.5 s
  request timeout) and 0.027 s falling. The router does not go through
  the ClusterIP: its endpoint picker watches the pods, so a Ready change
  reaches it in tens of milliseconds. Router requests are timed from when
  they were sent, so a request sent just before the watch saw the change
  can give a slightly negative span.

## Guest steering options (pending decisions D-NS-2, D-VK-4, D-NS-3)

Three pending lead decisions choose how a guest finds the virtual Node and
what makes a pod a guest. Every option ships; flags and manifests pick one.
The defaults are today's behaviour.

- D-NS-2, labels of the virtual Node: `--guest-node-label`.
  - `false` (default): `timeslice.io/virtual-node=true` only.
  - `true`: also `timeslice.io/guest=true`.
- D-VK-4, what makes a pod a guest: `--guest-marker`.
  - `toleration` (default): the pod tolerates `timeslice.io/guest` by key.
  - `label`: the pod carries the label `timeslice.io/guest=true`.
  - `both`: either one.
- D-NS-3, how a guest steers: the manifest in `deploy/guests/`.
  - `guest-today.yaml`: the toleration and the nodeSelector
    `timeslice.io/virtual-node: "true"`. It waits Pending when no
    virtual Node exists or has room.
  - `guest-ns.yaml`: the label `timeslice.io/guest=true`, the toleration and
    a preferred node affinity (weight 100) on `timeslice.io/guest=true`.
    It falls back to real nodes.
- W9 for each marker: `deploy/admission/w9-<variant>.yaml`, a
  ValidatingAdmissionPolicy and binding. It rejects `timeslice.io/group`,
  `timeslice.io/job-id` and `timeslice.io/role` on guests and allows
  `timeslice.io/guest`. The variants are `toleration`, `label`, `both`
  (one per marker) and `either` (toleration or label, whatever the marker).

Combinations that work together:

- today: `--guest-node-label=false --guest-marker=toleration`,
  `guest-today.yaml`, `w9-toleration.yaml`.
- north star: `--guest-node-label=true --guest-marker=label`,
  `guest-ns.yaml`, `w9-label.yaml`.
- both forms (the D-NS-3 flag option): `--guest-node-label=true
  --guest-marker=both`, either manifest, `w9-both.yaml`.

`guest-ns.yaml` without `--guest-node-label=true` is admitted and runs,
but has nothing to prefer: the scheduler puts it on any node that fits.
The two flags need no new RBAC: the Node label is set when the Node is
created. A cluster admin applies the W9 policies, one copy per namespace:

```sh
sed 's/__NS__/<namespace>/g' deploy/admission/w9-label.yaml | kubectl apply -f -
```

The `isGuest` CEL variable of each policy is the guest predicate of the
matching `--guest-marker`. `internal/provider/guests_internal_test.go`
checks that they agree and that both manifests steer as described.

## M1 results (the test cluster, 2026-09-25)

- Echo guest Running with the mirror's IP; `curl <podIP>:8080` and the Service work from a
  default-pool node.
- vLLM guest (approach-7 args) sees the L4 through the claim and served a completion.
- Deleting a guest: mirror and guest gone in 1.6 s (quick exit); 10.5 s for a container that
  ignores SIGTERM, with grace 10.
- A 7.5-CPU guest with the cap off: kubelet rejects the mirror (`OutOfcpu`) and the guest shows
  the same. With the cap on, the mirror requests 1 CPU and runs.
- kube-controller-manager adds a mirror to the claim's `reservedFor` by itself (~1 s).
- Leader failover ~2 s; several VK restarts: same mirror UID and IP, no duplicate.
- providerID: denied by GKE's `validate-node-providerid` admission policy.
- VK stopped 3 min: Node deleted at +60 s. With ownerRef, guests and mirrors are gone at +103 s.
  Without ownerRef, mirrors keep serving (vLLM answered throughout). The StatefulSet pod and
  re-applied bare pods re-adopt their mirrors with the same IP.
- A DaemonSet that lands on the node stalls its rolling update (maxUnavailable 1). A policy that
  denies the pod does not help.

## M0 results (the test cluster, 2026-09-25)

- Node registered in ~3 s; Lease renewed every 10 s; Ready for a 30-minute soak with no foreign
  taints or conditions.
- Test guest Running with 198.18.0.1 within a second; the EndpointSlice listed it (removed within
  2 s of delete; the object itself goes after the 30 s grace period).
- Five system DaemonSets land on the node and stay Pending (guest filter). The library's env
  resolution failed on three of them until `SkipDownwardAPIResolution` was set (commit 5e68be8).
- Outage: with the VK stopped, the node went NotReady at +51 s and the cloud node lifecycle
  controller deleted it at +54 s ("does not exist in the cloud provider"); its pods were
  garbage-collected ~40 s later. The VK re-creates the Node on restart.
- metrics-server scrapes InternalIP:10250, i.e. the real kubelet, so `kubectl top node vk-abcd`
  shows the real host.
