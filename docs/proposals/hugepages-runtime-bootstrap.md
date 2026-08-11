# Proposal: Runtime Hugepage Provisioning via a Bootstrap DaemonSet

Status: validated on GKE Standard (test report:
`testing-artifacts/hugepages-runtime-bootstrap-results.md`; all hard
gates PASS, 2026-08-10/11).

## What this enables

The Snapshot Agent stack currently requires a GPU nodepool created with
`--system-config-from-file` (`hugepageConfig.hugepage_size2m`), which means
a **dedicated nodepool and node recreation** just to get a 2Mi hugepage
pool. This proposal replaces that requirement with a privileged bootstrap
DaemonSet (`deploy/examples/hugepages-bootstrap-daemonset.yaml`) that, on
each node of an existing **vanilla** GPU pool:

1. writes the pool size to `/proc/sys/vm/nr_hugepages`, and
2. restarts the kubelet **once per node boot** so the node publishes
   `hugepages-2Mi` capacity (upstream kubelets only discover hugepage
   capacity at startup).

No new nodepool, no node config file, no node recreation. Deploy the
bootstrap, wait for the nodes to report capacity (seconds), then deploy
the agent and workloads exactly as in the user guide.

## Measured behavior backing the design

All numbers from the test report (GKE 1.35.6-gke.1250000, COS,
kernel 6.12.85+, g2-standard-24, cgroup v2).

- **Allocation is instant on fresh and mildly-churned nodes**: 12288
  pages (24Gi) in 0.045 s on a fresh node; identical result 0.046 s after
  48G of anonymous churn plus 20G of page-cache fill (T1, T9).
- **The kubelet restart is required**: >3.5 min after allocation the node
  still reported `hugepages-2Mi: 0` (T2). After restart, capacity AND the
  matching ~24Gi drop in allocatable memory appear within 2–4 s (T3) —
  the scheduler cannot overcommit.
- **The restart is invisible to workloads** (T4): across three kubelet
  restarts on two GPU nodes — zero container restarts (system DaemonSets
  incl. GPU device plugin, dcgm-exporter, and test pods), zero non-Ready
  samples at 2 s polling (301/301 Ready=True through a restart), no
  heartbeat gap >3 s in a 1 Hz canary, no NodeNotReady events, no GKE
  auto-repair then or during a 60+ min soak (T10).
- **Scheduling gates correctly** (T5): pods requesting `hugepages-2Mi`
  stay Pending (`Insufficient hugepages-2Mi`) until the node publishes
  capacity, then schedule unaided (observed 2 s after publication). Safe
  to deploy workloads and bootstrap simultaneously.
- **cgroup enforcement is real and pod-scoped** (T6): kubelet sets
  `hugetlb.2MB.max` on the pod slice — `0` for pods that don't request
  `hugepages-2Mi` (they SIGBUS on first hugetlbfs fault, exit 135), the
  byte-exact request value for pods that do. The user guide's resource
  requests are mandatory; conversely the pool cannot be consumed by
  non-requesting pods.
- **Idempotent and reboot-safe** (T8a/T8b): pod restarts hit the
  "already provisioned, nothing to do" path (no restart storm); a node
  reboot (nr_hugepages resets to 0) self-heals — reallocation + kubelet
  restart + node reporting full capacity again ~2.5 min after reboot,
  zero operator action. Node UID is preserved (no recreation).
- **Pool resizing rolls out like any env change**: raising
  `HUGEPAGES_2M` 12288→30720 on a live node re-ran the script and
  published 60Gi with exactly one additional kubelet restart.
- **End-to-end parity** (T7): the real stack (Cloud-Built agent image +
  GPU-CR LoRA slot-swap validation) passed with identical outcomes to the
  hugepage-nodepool baseline on a runtime-bootstrapped vanilla node —
  hugepage-backed dumps (`HugePages_Free` 30720→29549 during swaps),
  normal per-op timings (warm checkpoint ~54 ms, restore 51–60 ms),
  temperature-0 outputs identical across park/revive.

## Pros

- Works on **existing** GPU nodepools; adoption is `kubectl apply`.
- Reversible: `echo 0 > nr_hugepages` + one kubelet restart returns the
  memory (or just delete the DS and let node upgrades recreate nodes).
- Sizing is a DaemonSet env var, not immutable node config; growing the
  pool costs one kubelet restart per node.
- Same enforcement/scheduling semantics as node-config hugepages — the
  kubelet does not distinguish how the pool got there.

## Cons / residual risks

- **Privileged pod + host sysctl + kubelet restart** — a heavier security
  posture than declarative node config. Must be restricted (nodeSelector)
  to the GPU pools that need it.
- **Fragmentation on long-lived, heavily-churned nodes is not fully
  bounded.** T9's churn profile (48G anon + 20G page cache, kernel 6.12)
  allocated instantly, but an arbitrarily fragmented node can still fall
  short. The script's guard: on shortfall after
  drop_caches/compact_memory it **exits 1 without restarting the
  kubelet**, so failure is visible in the DS pod (CrashLoopBackOff), not
  as workload SIGBUS. Prefer deploying the bootstrap before heavy
  workloads land.
- **One kubelet restart per node boot** is inherent (upstream k8s
  behavior). Measured blast radius is nil (T4), and GKE's
  node-problem-detector `FrequentKubeletRestart` threshold is nowhere
  near one restart, but operators should still expect the
  `Starting kubelet` event.
- If GKE ever reconciles `vm.nr_hugepages` on vanilla pools this approach
  breaks; 60+ min soak and the operations log showed no such reconciler
  today. The DS re-converges on any reset that empties the pool
  (as it does after reboot) — but a reset while GPU-CR mappings are live
  would still disrupt workloads (same failure mode as losing node config).
- Anything the scheduler places on the pool needs the
  `nvidia.com/gpu` toleration *and* (for hugepage consumers) explicit
  `hugepages-2Mi` requests — same as with node-config pools.

## Rollout recommendation

1. Apply `deploy/examples/hugepages-bootstrap-daemonset.yaml` with
   `HUGEPAGES_2M` sized per the user guide (12288 for the SHM_SIZE_GB=8
   GPU-CR build; 30720 for the stock 25GiB build with two workloads).
2. Wait for nodes to report `hugepages-2Mi` allocatable (seconds).
3. Deploy the agent and workloads per the user guide, unchanged.
