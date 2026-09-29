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
- Which probes a guest may carry is lead decision D-VK-5, selected with
  `--guest-probe-policy` (default `a`, the rule above). `b` refuses every
  probe and readiness gates. `c` refuses none: liveness probes are dropped,
  the VK runs readiness and startup probes of any kind (exec through
  `pods/exec` on the mirror, gRPC health), holds Ready false until the
  startup probe passes and until every readiness gate is true, and its
  status writes keep the gate conditions (`internal/backend/mirror/gates.go`).
  The mirror carries no probes under every policy.
- A GPU mirror container's memory limit is its limit (or request) plus
  the device reserve, `ceil(--gpu-memory x --mirror-memory-factor)`
  (defaults `23034Mi` x `1.1`, about 24.7 GiB).
- Outage guard (VK-A7): `deploy/guard/node-keeper.yaml` runs the same
  binary with `--node-keeper` (1 replica, Recreate, pinned to the host).
  While the host is Ready and the guest kubelet was seen within
  `--keeper-outage-grace` (15m), it renews the virtual Node's Lease
  whenever the guest kubelet has not for `--keeper-stale-after` (15s).
  The Node stays Ready through a guest-kubelet outage, so it is not
  deleted and its guests and mirrors keep running. Past the grace it
  stops and the Node goes NotReady as before.
- Not yet: logs/exec (use `kubectl logs <guest>-m`), stats. Liveness and
  startup probes are refused by admission (above), so an exec or gRPC
  readinessProbe never reaches the prober.

## Layout

```text
cmd/guest-kubelet/main.go            flags; leader election; nodeutil.NewNode wiring; own event recorder
internal/provider/node.go            the Node spec (labels, taint, capacity, conditions); NodeProvider
internal/provider/provider.go        the pod provider: guest filter, hands guests to the backend
internal/provider/admission.go       admission: probes, gates, GPU allowlist
internal/provider/marker.go          --guest-marker: what makes a pod a guest
internal/provider/probepolicy.go     --guest-probe-policy (D-VK-5 a/b/c)
internal/provider/events.go          drops events about non-guest pods
internal/keeper/keeper.go            outage guard (keeps the Node Lease fresh)
internal/backend/mirror/builder.go   guest -> mirror pod (pure function)
internal/backend/mirror/status.go    mirror status -> guest status
internal/backend/mirror/backend.go   create/adopt/delete mirrors, mirror informer, orphan GC
internal/backend/mirror/claim.go     optional reservedFor write (kube-controller-manager also does it)
internal/probe/probe.go              readiness prober (httpGet, tcpSocket)
internal/probe/startup.go            startup gate (D-VK-5 c)
internal/probe/handlers.go           exec and gRPC probes (D-VK-5 c)
internal/backend/mirror/gates.go     readiness gates kept (D-VK-5 c)
internal/probe/debug.go              debug endpoint: override, Ready edges
cmd/q5-measure/main.go               Q5 timings; runs in a pod
internal/backend/mirror/suspend.go   suspend/resume/kill, state on the mirror
internal/backend/mirror/readiness.go MarkNotReady, the one NotReady signal
internal/backend/mirror/readycheck.go readinessProbe after a resume
internal/freeze/                     freeze.Backend: Suspend, Resume, Kill
internal/handshake/                  freeze.Backend over the agent's gRPC API
api/snapshot_agent/v1alpha1/         agent proto copy; Go code from Cloud Build
cmd/guest-kubelet/debug.go           the --debug-addr mux: M2 hooks,
                                     POST /debug/suspend, /debug/resume,
                                     GET /debug/freeze-state
cmd/guest-kubelet/orchestrator.go    --orchestrator-addr: the orchestrator
                                     loop, its freezer, the M5 relist call
internal/orchestrator/               the orchestrator loop (Acquire,
                                     heartbeat, serve, vacate, Yield)
internal/backend/mirror/loopfreezer.go
                                     the loop's agent freezer; relist
                                     helpers (ListMirrors, GuestNow, Adopt)
internal/provider/recover.go         M5: relist mirrors after a restart and
                                     rebuild the lost state
deploy/                              namespace + SA, RBAC, Deployment, CPU test guest + Service
deploy/guard/                        node keeper Deployment (outage guard)
deploy/guests/                       guest manifests: today, ns
deploy/admission/                    W9 policies, one per guest marker
deploy/m1/                           claim + trainer stand-in, vLLM guest, StatefulSet guest,
                                     rollout-test DaemonSet, curl client, driver installer, VAP test
deploy/m2/                           probed guests, pool, router, Q5 pods
cloudbuild.yaml                      tidy check, vet, test, build, image push (nothing runs locally)
```

## M3/M4: suspend and resume through the snapshot agent

M3 froze the mirror's pod cgroup from the VK. Since M4 the node's snapshot
agent does it (checkpoint, freeze, verify), behind `freeze.Backend`
(`internal/freeze`), implemented by `internal/handshake`: the VK never
touches cgroups or the GPU. `--agent-addr` (host:port of the agent on the
node; empty disables suspend) turns it on.

- Mirrors are agent jobs: label `timeslice.io/job-id` =
  `<guest UID>-<attempt>` (an adopted mirror keeps its id),
  `restartPolicy: Never` (a kubelet restart of a suspended or killed process
  would run behind the agent's back), and the guest as owner
  (`--mirror-owner-ref`, required: the agent reads the guest's Ready through
  it). The memory limit of each GPU container grows by the device reserve
  (VK-A7, below the admission rules): the agent checkpoints device memory
  into the pod's memory cgroup and refuses a pod without room.
- Suspend (`Backend.Suspend`), in this order: raise
  `timeslice.io/guest-epoch` and set `timeslice.io/suspend-state=Suspending`
  on the mirror (compare-and-swap on its resourceVersion); the guest turns
  NotReady (`MarkNotReady`, with reason `Suspending`); wait until the API
  shows the guest NotReady (`--suspend-notready-timeout`); require the job
  in the agent's `Status`; agent `Suspend(job_id, epoch, deadline)` with
  deadline `--agent-suspend-timeout` (or the caller's earlier one), polled
  through `GetOperation`; record `Suspended`. No drain. If the NotReady wait
  fails, the agent was never called and the guest goes back to Running.
- While suspended the guest shows phase Running, Ready=False, condition
  `timeslice.io/suspended=True`, and its containers Waiting with reason
  `Suspended`, so `kubectl get pods` prints `0/1 Suspended`. Restart counts
  do not change.
- Resume (`Backend.Resume`): raise the epoch, record `Resuming` (still
  NotReady), agent `Resume` (`--agent-resume-timeout`; outcome RESUMED or
  none), run the guest's httpGet/tcpSocket readinessProbe against the mirror
  until it passes (`--resume-ready-timeout`), then clear the state; from then
  on the prober's verdict decides Ready.
- Kill sequence (Q6): any failure of the agent's Suspend or Resume (a
  refusal, a FAILED operation, a missed deadline, `Unimplemented`, a job the
  agent does not list or reports FAULTED, an outcome such as RELEASED) and a
  failed ready check after a Resume: record `Killing` (NotReady), agent
  `Kill` (`--agent-kill-timeout`), delete the mirror with its normal grace
  period (never a force delete), record `Killed` on the terminating mirror
  and report the guest Failed with reason `SnapshotAgentKilled`. Each step
  runs even if the one before failed, so a hung or crashed agent still ends
  with the mirror gone and the guest Failed. A later suspend or resume of a
  guest left `Killing` finishes the sequence.
- Agent calls follow the Q13 defaults: every RPC times out after
  `--agent-rpc-timeout` (5s), pending operations are polled every
  `--agent-poll` (100ms), and a call that does not reach the agent
  (Unavailable, a timed-out RPC: a hung agent) or whose operation the agent
  lost (GetOperation NotFound after a restart) is re-issued with the same
  epoch, backing off from `--agent-retry-initial` (1s) to
  `--agent-retry-max` (30s), never past the deadline. The agent is told a
  deadline `--agent-deadline-grace` (1s) earlier than the VK's own, so its
  verdict is still read. The agent's refusal reason (the `ErrorReason` name
  that prefixes its message) is logged and recorded in the event.
- The suspend state is applied after the prober's verdict, so a probe that
  still passes (or a debug override) never shows a suspended guest Ready.
- One suspend or resume per guest at a time. The state lives on the mirror,
  so a restarted VK or a new leader derives the same guest status. Deleting
  a guest that is not Running has the agent kill the mirror first, so it
  does not sit out the grace period frozen.
- Deployment: the VK is unprivileged (no cgroup mount) and reaches the agent
  over the host network (`--agent-addr=127.0.0.1:9001` in
  `deploy/deployment.yaml`).
- With `--orchestrator-addr` and `--freezer=agent` the orchestrator loop
  (VK-A6) suspends, resumes and kills through the same agent client
  (`LoopFreezer`); a failed loop suspend or resume ends in the loop's kill
  sequence (agent Kill, mirror delete, guest Failed). Otherwise suspend and
  resume are triggered by hand through `--debug-addr` (loopback only, e.g.
  `127.0.0.1:10261`, reached with `kubectl port-forward` to the leader pod;
  it works with `--readiness-probes=false` too):
  `POST /debug/suspend?namespace=<ns>&name=<guest>` and
  `POST /debug/resume?...`. The reply is the step timings as JSON; a call
  that ended in the kill sequence answers 410 with the error. It has no
  authentication: enable it only in test deployments.

## M5: restart and relist

A restart (crash, rollout or leader failover) loses what the VK holds in
memory: which guests are suspended, which are released to Ready, and each
guest's mirror attempt counter. The leader rebuilds it before the pod
controller and the orchestrator loop start (`internal/provider/recover.go`):

- It lists this node's mirrors and their guests from the API (the library's
  informer is not synced yet). Terminal or deleting mirrors are skipped; a
  mirror whose guest is gone is left to the orphan collector.
- The host wins over the annotation. With `--freezer=cgroup` (or M3 without
  the loop) it reads `cgroup.events` of each mirror: frozen means `Suspended`
  whatever `timeslice.io/suspend-state` says (a crash between the freeze and
  the write leaves `Suspending`), thawed means running (a crash mid-thaw
  leaves `Resuming`), and the annotation is rewritten to match. No pod cgroup
  counts as thawed; an unreadable one keeps the annotation. With
  `--freezer=fake` the fake freezer's annotation is the fact; with
  `--freezer=delete` there is none.
- A frozen guest is adopted suspended (held NotReady, reason
  `GuestSuspended`) and resumed at the next grant, never re-created. A mirror
  that was running all along, whose guest is Ready, stays released, so a
  restart causes no Ready flap. One caught mid-suspend or mid-resume and
  found thawed is held NotReady until the loop's engine check releases it.
  The attempt counter continues from the mirror's job id.
- Nothing is created, deleted, frozen or thawed during recovery. An error
  stops the VK (it restarts and tries again) rather than serve a guest whose
  state it does not know.
- `GET /debug/freeze-state?namespace=<ns>&name=<guest>` on `--debug-addr`
  reports the mirror, its recorded state and epoch, and what the host says
  now (frozen, process count).
- Limit: if a guest's mirror is gone when the VK starts, its attempt counter
  starts again at 0, so a later mirror may reuse an earlier job id.

## Build and deploy

```text
make build        # Cloud Build: tidy check, go vet, go test -race, image -> Artifact Registry
make deploy       # namespace, RBAC, Deployment, node keeper (HOST=<real node>)
make test-guest   # CPU guest + Service
make gpu-guest    # claim + trainer stand-in, then the vLLM guest
make m2-deploy    # probed guests, pool, EPP, router, RBAC
make q5-force     # Q5 run, readiness forced via the debug endpoint
make q5-toggle    # Q5 run, a real probe flipped by the guest
make status
make undeploy
```

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
