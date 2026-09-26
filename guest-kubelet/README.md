# guest-kubelet

A throwaway prototype of a *guest kubelet*: a [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet)
(v1.14.0) that registers a virtual Node (`vk-<host>`) next to a real GPU node and acts as the
kubelet for the guest pods scheduled onto it.

## Status: M0 (fake node)

- Registers Node `vk-abcd` with labels `type=virtual-kubelet`, `timeslice.io/virtual-node=true`,
  taint `timeslice.io/guest=true:NoSchedule`, capacity `cpu`/`memory`/`pods`/`nvidia.com/gpu: 1`,
  and the host's IP as `InternalIP`.
- Keeps the Lease `kube-node-lease/vk-abcd` renewed (every 10 s) and the Node Ready.
- Pods bound to the node that tolerate `timeslice.io/guest` by key are kept in memory and
  reported Running with a fake IP from `198.18.0.0/15`, so Services list them.
  Other pods (for example DaemonSets with a blanket toleration) are ignored and stay Pending.
- Nothing runs. No HTTP server (logs/exec) yet.

## Layout

```
cmd/guest-kubelet/main.go       flags; nodeutil.NewNode wiring; re-register if the Node is deleted
internal/provider/node.go       the Node spec (labels, taint, capacity, conditions); NodeProvider
internal/provider/provider.go   the pod provider: in-memory pods, fake Running status
deploy/                         namespace + SA, RBAC, Deployment (pinned to the host), test guest + Service
cloudbuild.yaml                 tidy check, vet, test, build, image push (nothing runs locally)
```

## Build and deploy

```
make build        # Cloud Build: tidy check, go vet, go test, image -> Artifact Registry
make deploy       # namespace, RBAC, Deployment on the test cluster
make test-guest   # guest pod + Service
make status
make undeploy     # teardown in the plan's order
```

The plan and the notes explaining this code are kept outside this repository
(prototype plan, milestone M0).

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
