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
internal/provider/finalizer.go       Node finalizer, ownerRef to host, release
internal/donorstandin/               donor stand-in: releases Node on host death
cmd/donor-standin/main.go            its command (same image as the VK)
internal/backend/mirror/builder.go   guest -> mirror pod (pure function)
internal/backend/mirror/status.go    mirror status -> guest status
internal/backend/mirror/backend.go   create/adopt/delete mirrors, mirror informer, orphan GC
internal/backend/mirror/claim.go     optional reservedFor write (kube-controller-manager also does it)
internal/probe/probe.go              readiness prober (httpGet, tcpSocket)
internal/probe/debug.go              debug endpoint: override, Ready edges
cmd/q5-measure/main.go               Q5 timings; runs in a pod
internal/backend/mirror/suspend.go   M4 suspend/resume via the agent
internal/backend/mirror/kill.go      kill sequence (Q6)
internal/backend/mirror/host.go      host-level SuspendAll/ResumeAll
internal/backend/mirror/admit.go     admission: restore + checkpoints fit N-K
internal/backend/mirror/readiness.go MarkNotReady; wait for NotReady
internal/backend/mirror/readycheck.go resume check: probe until Ready
internal/freeze/                     freeze.Agent: the agent API the VK uses
internal/hostcmd/                    snapshot-agent gRPC client, fault injector
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

## M4: suspend, resume and kill through the snapshot agent

M3 froze the mirror's pod cgroup from the VK. M4 hands that to the node's
snapshot agent (Q6 API, `internal/hostcmd` behind the `freeze.Agent`
interface): the VK never touches cgroups, runs unprivileged and mounts
nothing from the host. `--agent-addr` (the Deployment passes
`$(HOST_IP):9101`; empty disables suspend) names the agent.

- Each mirror carries `timeslice.io/job-id=<guest UID>-<attempt>` (a
  recreated mirror gets attempt+1), `timeslice.io/role=background` and
  `restartPolicy: Never`.
- Suspend, in this order: raise `timeslice.io/guest-epoch` and set
  `timeslice.io/suspend-state=Suspending` (compare-and-swap on the mirror's
  resourceVersion; one agent call in flight per guest); the guest turns
  NotReady and the VK waits until the API shows it
  (`--suspend-notready-timeout`; if not, it goes back to Running with no
  agent call); read the agent's Status; a job the agent does not list has
  no accelerator context and is deleted instead; otherwise agent Suspend
  with the epoch and an absolute deadline (the caller's, or now + N - K);
  record `Suspended`.
- Resume: one at a time; raise the epoch, record `Resuming` (still
  NotReady), agent Resume, run the guest's readinessProbe until it passes
  (`--resume-ready-timeout`), then clear the state.
- Kill sequence, on any agent failure, deadline miss or `Unimplemented`,
  or a failed ready check: agent Kill with deadline now + K
  (`--kill-budget`), then a normal-grace delete of the mirror with a UID
  precondition, then the guest is reported Failed (reason
  `SnapshotAgentKilled`, or `NoAcceleratorContext` for a job the agent did
  not list). An unconfirmed Kill still deletes. Deleting a suspended guest
  kills it through the agent first.
- Host level (pending decision D-NS-5, option ns-host):
  `SuspendAll`/`ResumeAll` on the agent for every background job of the
  node, in one operation, with one host epoch; each target that does not
  reach the wanted state gets the kill sequence. A STALE_EPOCH answer is
  retried once with the agent's last epoch + 1.
- Admission: a new guest is admitted only while one restore plus the sum
  of checkpoints fits N - K (`--restore-estimate`, `--checkpoint-estimate`,
  `--notice-window`; 0 disables).
- Agent calls: poll GetOperation every `--agent-poll` (100 ms), each RPC
  times out after `--agent-rpc-timeout` (5 s), a lost call is sent again
  with the same epoch after `--agent-retry-initial` (1 s) doubling to
  `--agent-retry-max` (30 s); an operation the agent lost (restart) is
  started again. The VK waits for an answer until the agent's deadline plus
  1 s.
- While suspended the guest shows phase Running, Ready=False, condition
  `timeslice.io/suspended=True`, containers Waiting with reason
  `Suspended` (`0/1 Suspended`). The suspend state is applied after the
  prober's verdict, so a suspended guest is never Ready.
- Every `--agent-status-poll` (2 s) the VK reads the agent's Status. A job
  the agent reports SUSPENDED while its mirror shows Running is recorded
  Suspended (a VK that stopped between the agent's answer and its own
  write), which keeps the guest NotReady.
- Test hooks on `--debug-addr` (loopback only, no authentication):
  `POST /debug/suspend?namespace=<ns>&name=<guest>[&within=<duration>]`,
  `POST /debug/resume?...`, `POST /debug/suspend-all[?within=]`,
  `POST /debug/resume-all`, `GET /debug/agent`. With
  `--agent-fault-injection`, `POST /debug/fault?rpc=<RPC>&kind=<kind>&count=<n>`
  arms a fault on an agent RPC (kinds: hang, crash, drop-ack, pending,
  unimplemented, refuse; count -1 = until cleared), `GET` lists them with
  their hits and `DELETE` clears them.
- Cordon while held (`--cordon-while-held`, pending decision D-NS-8; default
  true = option ns-cordon, false = option skip): while at least one guest of
  the virtual Node is `Suspending` or `Suspended` on its mirror, or its job
  is SUSPENDED in the agent's last Status (a killed mirror counts until it
  is gone), the VK sets `spec.unschedulable` on its Node, so `kubectl get
  nodes` shows `SchedulingDisabled` and no new guest binds to it.
  The hold is re-checked on every suspend-state change, every agent Status
  change and every 0.5 s, and the Node is re-read every 5 s to repair a
  lost cordon. The VK patches only `spec.unschedulable` and its own
  annotation `timeslice.io/cordoned-by=guest-kubelet`, and only removes a
  cordon carrying that annotation, so an admin `kubectl cordon` is left
  alone. Guests already bound stay bound and resident.

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
