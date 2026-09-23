# Deploying Node Supervisor

This directory contains the Helm chart for deploying the Node Supervisor
DaemonSet — one pod per time-slicing node, running the tenant supervisor's
lock/drain/wake loop out-of-pod. See
[`pkg/node-supervisor/README.md`](../../pkg/node-supervisor/README.md) for what
the component does and why.

Public images are published to `ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/*` by CI: `latest` on every merge to main; versioned tags via a manual workflow run.

## Prerequisites

> [!IMPORTANT]
> **Hard precondition.** The orchestrator must be installed with
> `timesliceorchestrator.dispatchBudget.externalRisingEdge: true` (flag
> `--dispatch-budget-external-rising-edge`). Without it the orchestrator keeps
> publishing its own rising edge 0.74–1.84 s before the route exists and
> overwrites the `"1"` this DaemonSet writes — and it does so *silently*, with
> this component's logs still reporting `clusterip_probe_ok`. Verify
> `timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total` is climbing
> during trainer bursts before believing a run.

*   A Kubernetes cluster with GPU nodes labelled for time slicing
    (`group.timeslice.io/trainers=true` by default).
*   The snapshot agent DaemonSet running on the same nodes (hostNetwork, `:9001`).
*   Tenant pods that declare
    `spec.readinessGates: [{conditionType: timeslice.io/serving}]`.
*   `kubectl` configured to connect to your cluster.
*   `helm` (v3+) installed.

## Namespace

> [!IMPORTANT]
> Unlike the Snapshot Agent (which is pinned to `timeslice-system`), this chart
> installs into the **tenant** namespace — `tenantNamespace`, default `default`.
> That is where the pods it supervises live and where its ServiceAccount has to
> be for the cluster-scoped binding to be meaningful in the audit log. The same
> value is passed to the container as `TENANT_NAMESPACE`.

## Ordering

Deploy this DaemonSet **before** the tenant workload. A readiness gate whose
condition is absent counts as False, so a tenant that starts with no supervisor
running is simply never an endpoint — safe, but it also never serves.

## Deployment with Helm

1.  **Install the chart**:
    From the `deploy` directory:
    ```bash
    helm install node-supervisor ./node-supervisor \
      --namespace default
    ```
    This will deploy the supervisor as a `DaemonSet` and set up the required
    RBAC permissions:
    *   Creating a `ServiceAccount` in the tenant namespace.
    *   Creating a `ClusterRole` and `ClusterRoleBinding` granting `get`,
        `list` and `watch` on pods, and `patch` on `pods/status` — nothing
        wider. `patch` alone, never `update`.
    *   Configuring the supervisor pods to use this `ServiceAccount`.

2.  **Verify the deployment**:
    ```bash
    kubectl get pods -n default -l app.kubernetes.io/name=node-supervisor
    ```

3.  **Uninstall the chart**:
    ```bash
    helm uninstall node-supervisor --namespace default
    ```

## Deployment as part of the umbrella chart

The subchart is wired into `deploy/Chart.yaml` and gated on
`node-supervisor.enabled`, which defaults to `false` in `deploy/values.yaml`
because of the orchestrator precondition above.

Turn the component and its precondition on in the **same** invocation:

```bash
helm upgrade --install timeslice ./deploy \
  --namespace timeslice-system --create-namespace \
  --set node-supervisor.enabled=true \
  --set timesliceorchestrator.dispatchBudget.externalRisingEdge=true
```

> [!WARNING]
> Enabling one without the other is the silent-failure case, and neither
> mistake produces an error message.
>
> * **Supervisor on, `externalRisingEdge` off** — the orchestrator keeps
>   publishing its own rising edge 0.74–1.84 s early and races the supervisor's
>   `"1"`, while this component's logs still report `clusterip_probe_ok`. The
>   gate opens into a hole exactly as it did before the component existed.
> * **`externalRisingEdge` on, no supervisor** — nobody writes `"1"` and the
>   gate is wedged closed rather than merely late.
>
> Read `timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total` as a
> two-sided check, not a success signal: it climbing means the orchestrator is
> correctly declining to publish. If it is climbing **and** the batch tenant is
> still getting no traffic, nothing is publishing the `"1"` — that is the
> second failure above.

Setting `timesliceorchestrator.dispatchBudget.openDelay` alongside
`externalRisingEdge: true` is incoherent and ignored: the hold-down only ever
delays a `"1"`, and in this mode the orchestrator writes no `"1"` at all. The
orchestrator logs a warning if both are set. The supervisor's ClusterIP probe is
what replaces the fixed delay — an observation of the route instead of a guess
at how long it takes to appear.

Note that because the condition defaults to `false`, an umbrella upgrade that
forgets `--set node-supervisor.enabled=true` will *remove* the DaemonSet. Tenants
then fail closed — never an endpoint — rather than serving into a frozen engine.

Rendering either half alone is a useful pre-flight check:

```bash
helm template timeslice ./deploy \
  --set node-supervisor.enabled=true \
  --set timesliceorchestrator.dispatchBudget.externalRisingEdge=true \
  | grep -A2 'externalRisingEdge\|dispatch-budget-external-rising-edge'
```

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `image.repository` | `ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/node-supervisor` | Image repository. |
| `image.tag` | `latest` | Image tag. |
| `image.pullPolicy` | `Always` | Image pull policy. |
| `tenantNamespace` | `default` | Namespace for all objects and `TENANT_NAMESPACE`. |
| `orchestrator.address` | `timeslice-timesliceorchestrator.timeslice-system.svc:50051` | `TIMESLICE_ORCH_ADDR`. |
| `agent.port` | `9001` | Snapshot agent port; `TIMESLICE_AGENT_ADDR` becomes `$(NODE_IP):<port>`. |
| `dispatchBudget.redisAddr` | `redis.default.svc.cluster.local:6379` | `DISPATCH_BUDGET_REDIS_ADDR`. |
| `dispatchBudget.key` | `dispatch-gate-budget` | `DISPATCH_BUDGET_KEY`. Must match AP's `gate_params.budget_key` and the orchestrator's `--dispatch-budget-key`. |
| `serviceProbe.url` | `http://shadow-vllm.default.svc.cluster.local:8000/health` | `TIMESLICE_SERVICE_PROBE_URL`; the ClusterIP a dispatch actually uses. |
| `serviceProbe.timeoutSeconds` | `"30.0"` | `TIMESLICE_SERVICE_PROBE_TIMEOUT_SECONDS`. |
| `readinessGate.conditionType` | `timeslice.io/serving` | `READINESS_GATE_CONDITION`; must equal the tenant's gate. |
| `tenant.vllmPort` | `8000` | `VLLM_PORT`. |
| `supervisor.waiterPollSeconds` | `"0.5"` | `WAITER_POLL_SECONDS`. |
| `supervisor.discoveryPollSeconds` | `"1.0"` | `DISCOVERY_POLL_SECONDS`. |
| `supervisor.localSleep` | `"0"` | `LOCAL_SLEEP`. Leave at `0` unless the agent resolves this job to the app-channel backend. |
| `extraEnv` | `[]` | Extra env vars, e.g. `TIMESLICE_GROUP_LABEL`, `TIMESLICE_JOB_ID_LABEL`, `NOTREADY_TIMEOUT_SECONDS`. |
| `hostNetwork` | `true` | Required for a meaningful ClusterIP self-probe. |
| `dnsPolicy` | `ClusterFirstWithHostNet` | Mandatory alongside `hostNetwork`. |
| `nodeSelector` | `group.timeslice.io/trainers: "true"` | Same selector the tenant uses. |
| `tolerations` | `nvidia.com/gpu` Exists, `timeslice.io/shared=true` | Tolerate the time-slicing node taints. |
| `resources` | 100m/128Mi requests, 256Mi limit | No `claims:` and no `nvidia.com/gpu`, on purpose. |
| `rbac.create` | `true` | Create the ClusterRole/ClusterRoleBinding. |
| `serviceAccount.create` | `true` | Create the ServiceAccount. |

`hostPID` is not a value and is never set — see the component README.

## Non-default tenants

If your tenant Service, namespace or engine port differ from the shadow-vLLM
defaults, the three values that must move together are `tenantNamespace`,
`serviceProbe.url` and `tenant.vllmPort`:

```bash
helm install node-supervisor ./node-supervisor \
  --namespace my-tenants \
  --set tenantNamespace=my-tenants \
  --set serviceProbe.url=http://my-vllm.my-tenants.svc.cluster.local:8000/health
```

`serviceProbe.url` must be the ClusterIP URL the batch pipeline dispatches to.
Pointing it at a pod IP, a localhost address, or a different Service defeats the
entire purpose of the component: the probe would return 200 before kube-proxy
has programmed the route, and the budget would open into a hole exactly as it
did before.

## Development Workflow: Custom Images

### 1. Build and Push the Image

The image builds from the repository root as context (the client library is
installed from `pkg/client/python` in this tree, not from a VCS requirement):

```bash
export REGISTRY=your-custom-registry.com/your-project
export VERSION=dev-$(git rev-parse --short HEAD)
docker buildx build \
  --platform linux/amd64 \
  --tag $REGISTRY/llm-d-rl-time-slicing/node-supervisor:$VERSION \
  -f docker/node-supervisor/Dockerfile \
  .
docker push $REGISTRY/llm-d-rl-time-slicing/node-supervisor:$VERSION
```

### 2. Deploy with your Custom Image

```bash
helm install node-supervisor ./node-supervisor \
  --namespace default \
  --set image.repository=your-custom-registry.com/your-project/node-supervisor \
  --set image.tag=dev
```
