# guest-kubelet

A throwaway prototype of a *guest kubelet*: a [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet)
(v1.14.0) that registers a virtual Node (`vk-<host>`) next to a real GPU node and acts as the
kubelet for the guest pods scheduled onto it.

## Status: M2 (readiness)

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
- Not yet: liveness and startup probes, logs/exec
  (use `kubectl logs <guest>-m`), stats.

## Layout

```
cmd/guest-kubelet/main.go            flags; leader election; nodeutil.NewNode wiring; own event recorder
internal/provider/node.go            the Node spec (labels, taint, capacity, conditions); NodeProvider
internal/provider/provider.go        the pod provider: guest filter, hands guests to the backend
internal/provider/events.go          drops events about non-guest pods
internal/backend/mirror/builder.go   guest -> mirror pod (pure function)
internal/backend/mirror/status.go    mirror status -> guest status
internal/backend/mirror/backend.go   create/adopt/delete mirrors, mirror informer, orphan GC
internal/backend/mirror/claim.go     optional reservedFor write (kube-controller-manager also does it)
internal/probe/probe.go              readiness prober (httpGet, tcpSocket)
internal/probe/debug.go              debug endpoint: override, Ready edges
cmd/q5-measure/main.go               Q5 timings; runs in a pod
deploy/                              namespace + SA, RBAC, Deployment, CPU test guest + Service
deploy/m1/                           claim + trainer stand-in, vLLM guest, StatefulSet guest,
                                     rollout-test DaemonSet, curl client, driver installer, VAP test
deploy/m2/                           probed guests, pool, router, Q5 pods
cloudbuild.yaml                      tidy check, vet, test, build, image push (nothing runs locally)
```

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
