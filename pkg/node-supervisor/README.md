# Node Supervisor

The time-slicing tenant supervisor, moved out of the tenant pod and up to one
DaemonSet pod per time-slicing node.

`node_supervisor.py` is the in-pod supervisor's lock / drain / sleep / wake loop
with exactly one behaviour removed: it does not launch vLLM. It keeps the 0.5 s
waiter poll, the drain-before-yield, the `LOCAL_SLEEP` escape hatch and the
workload-channel registration. Two consequences are the whole reason the
component exists:

1. **The tenant runs a stock image.** No rebuilt vLLM, no baked supervisor, no
   `/tmp/serving`, no exec probe. A time-sliced batch tenant should not need a
   custom image to be a polite citizen. Readiness is driven from outside the pod
   by patching a pod condition.
2. **The RBAC argument.** Patching `pods/status` is a real privilege — with it
   you can make any pod in the cluster an endpoint, or not an endpoint. The
   DaemonSet form grants it **once**, to a platform-owned ServiceAccount, on a
   workload the tenant does not control. The alternative — a supervisor sidecar
   in the tenant pod — would hand the same verb to a pod whose spec the tenant
   author writes, in every tenant namespace, forever. Same capability, very
   different blast radius.

## Hard precondition

The orchestrator **must** be installed with

```yaml
timesliceorchestrator:
  dispatchBudget:
    externalRisingEdge: true   # flag: --dispatch-budget-external-rising-edge
```

That is what makes the orchestrator write only `"0"` and never `"1"`. Without
it, the orchestrator keeps publishing its own rising edge from `ServingSince()`
on a 1 Hz idempotent refresh, which lands 0.74–1.84 s before the route exists
and overwrites — or races — the `"1"` this supervisor writes after actually
observing the route.

The failure mode is not that the component is useless; it is that the component
is **invisible**. The gate opens early exactly as before and the supervisor's
own logs still say `clusterip_probe_ok`. Before believing a run, check that
`timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total` is climbing
during trainer bursts.

## Split-writer gate ownership

The llm-d-async dispatch budget key has exactly two writers, moving in opposite
monotone directions, with no overlap:

| Writer | Value | When |
| --- | --- | --- |
| Orchestrator | `"0"` only | synchronously inside `GetGroupStatus`, before returning the yield signal and ahead of the drain |
| Node supervisor | `"1"` only | after a ClusterIP self-probe of the tenant Service returns HTTP 200 |

The supervisor never writes `"0"`; the orchestrator never writes `"1"`. Closing
the gate must happen *before* the tenant stops serving, so it is synchronous on
the orchestrator's yield path. Opening the gate must happen *after* the route is
actually programmed, which only an observer on the node can know — hence the
self-probe.

`DISPATCH_BUDGET_KEY` must name the identical key as the async pipeline's
`gate_params.budget_key` and the orchestrator's `--dispatch-budget-key`.

### Why probe the ClusterIP, not the pod

`TIMESLICE_SERVICE_PROBE_URL` is the URL a batch dispatch actually uses. The
engine answering `/health` on its pod IP proves the CUDA context is restored; it
does not prove kube-proxy has programmed the Service route. The measured gap
between those two events is 0.74–1.84 s, and it is what destroyed 59 of 140
requests when the orchestrator published the rising edge itself.

Because the pod is `hostNetwork`, the probe is DNAT'd by the same host
iptables/IPVS chains a dispatch arriving at this node would hit, so a failure is
unambiguously "the route is not programmed" rather than "the CNI is not ready".

## Readiness gate

The tenant declares:

```yaml
spec:
  readinessGates:
    - conditionType: timeslice.io/serving
```

and the supervisor PATCHes that condition on `pods/status`. Pod Ready =
ContainersReady AND every readiness gate, so the tenant is a Service endpoint
only when vLLM answers `/health` **and** the platform says it holds the GPU.

A gate whose condition is **absent counts as False**. A supervisor that has not
started yet, or crashed before ever patching, simply leaves the tenant out of
the Service: it fails closed, which is the correct direction for the failure
that matters (dispatching into a frozen engine). The corollary is an ordering
requirement — deploy this DaemonSet **before** the tenant, or the tenant is safe
but never serves.

Only `patch` is granted, never `update`: a strategic merge patch keyed on
`conditions[].type` is the right mechanism and never needs to send a whole
`PodStatus`, so `update` would only widen the grant to "can overwrite any field
of any pod's status" for no gain.

## Pod-level requirements

* **`hostNetwork: true` and `dnsPolicy: ClusterFirstWithHostNet` are mandatory
  *together*.** `hostNetwork` is what makes the self-probe meaningful and makes
  `<hostIP>:9001` (the snapshot agent, itself hostNetwork) a literal node-local
  address. Without the paired `dnsPolicy` the pod inherits the node's
  `resolv.conf`, the tenant Service's cluster DNS name does not resolve, the
  self-probe silently becomes a permanent timeout, and the budget never opens.
* **`hostPID` is deliberately NOT set.** The in-pod supervisor needed PID 1
  because it fork/exec'd `vllm serve` and reaped it. This one launches nothing
  and signals nothing: every engine interaction is HTTP to the pod IP, and every
  CUDA-state interaction goes through the snapshot agent. A node-wide process
  view would be privilege for its own sake.
* **No GPU.** No `claims:` block, no `nvidia.com/gpu` request. This pod shares a
  node with the tenants fighting over a single GPU, holds no CUDA context, and
  is never snapshotted.
* **No `timeslice.io/group` or `timeslice.io/job-id` label** on the DaemonSet
  pod, so it is never selected for snapshot and never discovers itself as a
  tenant.

## Environment variables

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `NODE_NAME` | yes | — | fieldRef `spec.nodeName`. Field selector for tenant discovery on this node. |
| `TIMESLICE_ORCH_ADDR` | yes | — | gRPC address of the timeslice orchestrator. |
| `TIMESLICE_AGENT_ADDR` | yes | — | Node-local snapshot agent, `$(NODE_IP):9001`, with `NODE_IP` a fieldRef on `status.hostIP`. |
| `TENANT_NAMESPACE` | no | `default` | Namespace the supervised tenant pods live in. |
| `TIMESLICE_GROUP_LABEL` | no | `timeslice.io/group` | Existence selector: any pod on this node carrying it is ours to supervise. |
| `TIMESLICE_JOB_ID_LABEL` | no | `timeslice.io/job-id` | Supplies the lock identity. |
| `DISCOVERY_POLL_SECONDS` | no | `1.0` | Tenant discovery poll interval. |
| `VLLM_PORT` | no | `8000` | Port the tenant's engine listens on, probed on the pod IP. |
| `WAITER_POLL_SECONDS` | no | `0.5` | Waiter poll interval for the lock loop. |
| `LOCAL_SLEEP` | no | `0` | `1` sleeps the engine before releasing the lock. Leave `0` unless the agent resolves this job to the app-channel backend — vLLM sleep and cuda-checkpoint on the same process conflict, and the engine dies during the agent's RESTORE of an already-slept process. |
| `READINESS_GATE_CONDITION` | no | `timeslice.io/serving` | Must equal the tenant's `spec.readinessGates[].conditionType`. |
| `NOTREADY_TIMEOUT_SECONDS` | no | `10.0` | Bound on waiting for the API server to reflect `Ready=False` after patching the gate to False. |
| `TIMESLICE_SERVICE_PROBE_URL` | no | `http://shadow-vllm.default.svc.cluster.local:8000/health` | The ClusterIP URL a dispatch actually uses; a 200 here is the precondition for writing `"1"`. |
| `TIMESLICE_SERVICE_PROBE_TIMEOUT_SECONDS` | no | `30.0` | Deadline for the self-probe. |
| `DISPATCH_BUDGET_REDIS_ADDR` | no | `redis.default.svc.cluster.local:6379` | Redis holding the dispatch budget key. |
| `DISPATCH_BUDGET_KEY` | no | `dispatch-gate-budget` | Budget key. Must match AP's `gate_params.budget_key` and the orchestrator's `--dispatch-budget-key`. |

## Logging is an interface

Two layers, both load-bearing:

* **Structured.** One flushed line per state transition,
  `[node-supervisor] <RFC3339 UTC ts> job=<id> event=<name> k=v ...`, matched by
  `^\[node-supervisor\] (\S+) job=(\S+) event=(\S+)(.*)$`. Events:
  `condition_patched`, `endpoint_notready`, `drain_complete`, `lock_released`,
  `lock_reacquired`, `vllm_healthy_local`, `clusterip_probe_ok`, `budget_set`.
* **Verbatim legacy.** Four phrases are reproduced character for character so
  existing, unmodified analysis scripts parse a run of this component:
  `lock acquired`, `lock reacquired`, `trainer is waiting - yielding GPU`,
  `vLLM is up`, plus `workload registered with agent ...`.

## Deploying

The component and its precondition must be switched on in the **same**
invocation of the umbrella chart:

```bash
helm upgrade --install timeslice ./deploy \
  --namespace timeslice-system --create-namespace \
  --set node-supervisor.enabled=true \
  --set timesliceorchestrator.dispatchBudget.externalRisingEdge=true
```

Enabling either one without the other is the silent-failure case, and neither
mistake produces an error message. With the supervisor on but
`externalRisingEdge` off, the orchestrator keeps publishing its own rising edge
0.74–1.84 s early and races the supervisor's `"1"`, while the supervisor's logs
still report `clusterip_probe_ok` — the gate opens into a hole exactly as it did
before the component existed. With `externalRisingEdge` on but no supervisor
running, nobody ever writes `"1"` and the gate is wedged closed rather than
merely late.

`timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total` is the
two-sided diagnostic: it climbing means the orchestrator is correctly declining
to publish, but if it is climbing **and** the tenant is still getting no
traffic, nothing is publishing the `"1"`.

`dispatchBudget.openDelay` is ignored in this mode (it only ever delays a `"1"`,
and none is written); the ClusterIP self-probe replaces it with an observation
of the route rather than a guess at how long it takes to appear.

Because the subchart is gated on `node-supervisor.enabled`, which defaults to
`false`, an umbrella upgrade that forgets the flag removes the DaemonSet — and
every tenant then fails closed (never an endpoint) rather than serving into a
frozen engine.

Build the image with `make node-supervisor-image-push` (see the `Makefile`).
See [`deploy/node-supervisor/`](../../deploy/node-supervisor/) for the Helm
chart and its full values table, and
[`docker/node-supervisor/Dockerfile`](../../docker/node-supervisor/Dockerfile)
for the image (built from the repository root as context).
