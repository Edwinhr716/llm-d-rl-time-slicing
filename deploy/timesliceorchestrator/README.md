# Deploying TimeSlice Orchestrator

This directory contains the Helm chart for deploying the TimeSlice Orchestrator in a Kubernetes cluster.

Public images are published to `ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/*` by CI: `latest` on every merge to main; versioned tags via a manual workflow run.

## Prerequisites

*   A Kubernetes cluster.
*   `kubectl` configured to connect to your cluster.
*   `helm` (v3+) installed.

## Deployment with Helm

> [!IMPORTANT]
> By default the orchestrator keeps its locks (a ConfigMap named
> `timeslice-orchestrator-locks`) in the `timeslice-system` namespace, and the
> Helm chart creates the namespace-scoped RBAC resources (`Role` and
> `RoleBinding`) there. See
> [Scoping and second installs](#scoping-and-second-installs) to change this.
>
> It is highly recommended to deploy the orchestrator itself into the `timeslice-system` namespace.

To deploy the orchestrator using the local Helm chart:

1.  **Install the chart**:
    From the `deploy` directory, install or upgrade the chart into the `timeslice-system` namespace (creating it if it doesn't exist):
    ```bash
    helm upgrade --install timesliceorchestrator ./timesliceorchestrator \
      --namespace timeslice-system \
      --create-namespace
    ```
    This will deploy the orchestrator and set up the required RBAC permissions:
    *   Creating a `ServiceAccount` for the orchestrator in the release namespace.
    *   Creating a `ClusterRole` and `ClusterRoleBinding` granting the service account cluster-wide read-only permissions (`get`, `list`, `watch`) for `pods` and `nodes`.
    *   Creating a `Role` and `RoleBinding` **specifically in the `timeslice-system` namespace** granting the service account read-write permissions (`get`, `list`, `watch`, `create`, `update`, `patch`, `delete`) for `configmaps` in that namespace.
    *   Configuring the orchestrator pod to use this `ServiceAccount`.

2.  **Verify the deployment**:
    ```bash
    kubectl get pods -n timeslice-system -l app.kubernetes.io/name=timesliceorchestrator
    ```

3.  **Uninstall the chart**:
    ```bash
    helm uninstall timesliceorchestrator --namespace timeslice-system
    ```

## Development Workflow: Custom Images

During development, you will need to build your own container image containing your changes and push it to a custom registry (e.g., Google Container Registry, Artifact Registry, Docker Hub, or a local registry like Kind/Minikube).

### 1. Build and Push the Image

We use the provided `Makefile` targets to build and push the container image. The Makefile uses `docker buildx` under the hood to build multi-arch images (amd64/arm64) and push them to your registry.

1.  Define your custom registry and version (tag) by setting them as environment variables:
    ```bash
    export REGISTRY=your-custom-registry.com/your-project
    export VERSION=dev-$(git rev-parse --short HEAD)
    ```
2.  Run the following make target from the repository root to build and push the image:
    ```bash
    make image-push-orchestrator
    ```
    This will build the image using `docker/timesliceorchestrator/Dockerfile` and push it to `your-custom-registry.com/your-project/timesliceorchestrator:dev-<hash>`.

### 2. Deploy with your Custom Image

Once your image is pushed, you can instruct Helm to use it.

#### Option A: Via Command Line Flags (Recommended for Development)

This avoids modifying files in your git tree:

```bash
helm upgrade --install timesliceorchestrator ./timesliceorchestrator \
  --namespace timeslice-system \
  --create-namespace \
  --set image.repository=your-custom-registry.com/your-project/timesliceorchestrator \
  --set image.tag=dev
```

#### Option B: Via `values.yaml`

Edit `deploy/timesliceorchestrator/values.yaml` directly:

```yaml
image:
  repository: your-custom-registry.com/your-project/timesliceorchestrator
  pullPolicy: IfNotPresent
  tag: "dev"
```

And then run:
```bash
helm upgrade --install timesliceorchestrator ./timesliceorchestrator \
  --namespace timeslice-system \
  --create-namespace
```

## Scoping and second installs

By default the orchestrator watches pods in every namespace and every node,
and keeps its locks in `timeslice-system/timeslice-orchestrator-locks`. These
chart values (and the flags they set) narrow that down:

* `namespace` (default `timeslice-system`): namespace for the Deployment,
  Service, ServiceAccount and bindings.
* `lock.namespace`, flag `--lock-namespace` (default `""`, the chart
  namespace): namespace of the lock ConfigMap. The chart's `Role` is created
  there.
* `lock.configMap`, flag `--lock-configmap` (default `""`, which means
  `timeslice-orchestrator-locks`): name of the lock ConfigMap.
* `scope.watchNamespaces`, flag `--watch-namespaces` (default `[]`, all
  namespaces): pods are watched only in these namespaces, one informer each.
  Pods elsewhere join no group.
* `scope.nodeSelector`, flag `--node-selector` (default `""`, all nodes):
  label selector limiting the nodes the orchestrator sees. Nodes outside it
  contribute to no group, and pods bound to them are ignored. Group
  membership still comes from the `group.timeslice.io/<group>` node label.
* `strategy` (default `type: Recreate`): the old pod stops before the new one
  starts, so two replicas never act on the lock ConfigMap at once.

Flags are only passed when they differ from the defaults, so an image without
them keeps working with the default values.

Two orchestrators in one cluster must use different lock ConfigMaps and
should watch disjoint namespaces and nodes. For example, a second install
that manages only the `rl-demo` namespace and the nodes labelled
`timeslice.io/pool=demo`:

```bash
helm upgrade --install demo-orchestrator ./timesliceorchestrator \
  --namespace rl-demo-system --create-namespace \
  --set namespace=rl-demo-system \
  --set lock.configMap=demo-orchestrator-locks \
  --set 'scope.watchNamespaces={rl-demo}' \
  --set scope.nodeSelector=timeslice.io/pool=demo
```

The chart still grants read access to pods and nodes cluster-wide through a
`ClusterRole`.
