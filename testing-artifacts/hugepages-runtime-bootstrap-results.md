# Results: Runtime Hugepage Provisioning by the Snapshot Agent

**Status: EXECUTED 2026-08-10/11 (UTC). This is the filled-in copy of
`hugepages-runtime-bootstrap-test-plan.md`. All hard gates PASS.**

Raw command transcripts: `campaign-logs/taskc/` on the test workstation
(T0-baseline.log, T1.log … T10.log, ready-watch-a.log,
T7-hugepages-sampler.log). Manifests used:
`testing-artifacts/hugepages-bootstrap/`.

## Environment (T0 baseline)

- Cluster `dra-testing`, asia-southeast1, GKE v1.35.6-gke.1250000.
- Dedicated **vanilla** nodepool `hpboot-l4-c`: g2-standard-24 (2×L4,
  96Gi RAM), COS, kernel 6.12.85+, containerd 2.1.7, **no
  `--system-config-from-file`, no hugepageConfig** (verified in the
  nodepool describe output: `config.linuxNodeConfig` absent). autoRepair
  and autoUpgrade ON (deliberately — that is what T4/T10 stress).
- The plan's CPU-pool option was not used; all tracks ran on the GPU pool
  shape that production actually targets (three nodes, tracks A/B/C).
- cgroup v2 (`cgroup2fs`), `hugetlb` present in `cgroup.controllers` on all
  nodes. Baseline `hugepages-2Mi: 0` in node capacity, `HugePages_Total: 0`
  on the hosts.

| Track | Node | UID | bootID at T0 |
|---|---|---|---|
| A | gke-dra-testing-hpboot-l4-c-4b3a7927-l9fv | 4d4bb736-7d70-41b0-bdb4-64b8b9309e29 | 6a937abd-… |
| B | gke-dra-testing-hpboot-l4-c-4b3a7927-hsvt | 18708707-6db5-446a-ba65-7c837c8fc248 | 3e5df667-… |
| C | gke-dra-testing-hpboot-l4-c-4b3a7927-290w | 5111ab5e-91db-4a43-b59a-575f7e5e2350 | d60cebc1-… |

Namespace `taskc-hpboot`; every workload pinned with nodeSelector
`cloud.google.com/gke-nodepool: hpboot-l4-c` (+ `hp-test/track`). One
plan deviation forced by the GPU pool: all probe/consumer pods need a
`nvidia.com/gpu Exists NoSchedule` toleration or they never reach
resource-based scheduling (first hp-consumer attempt failed on the taint,
not on hugepages).

## Go / no-go summary

| Test | Gate | Result |
|---|---|---|
| T1 fresh-node allocation | **hard** | **PASS** — 12288/12288 pages in 0.045 s |
| T2 restart-required confirmation | informational | **CONFIRMED** — still `0 / 0` >3.5 min after allocation; restart is genuinely required |
| T3 capacity after restart | **hard** | **PASS** — `24Gi / 24Gi` ≤2 s after restart; allocatable memory 91054940Ki → 65889116Ki (−24Gi exactly) |
| T4 zero disruption | **hard** | **PASS** — 0 container restarts on 3 kubelet restarts across 2 nodes; 0 non-True Ready samples in 301; no repair events |
| T5 scheduling gate | **hard** | **PASS** — Pending `Insufficient hugepages-2Mi` before; auto-scheduled 2 s after capacity, `wrote OK`, Succeeded |
| T6 variant B (requesting pod can fault pages) | **hard** | **PASS** — `wrote OK`, exit 0 |
| T6 variant A (enforcement mode) | informational | **ENFORCED** — exit 135 (SIGBUS); pod-slice `hugetlb.2MB.max=0` without a request, `=2147483648` with a 2Gi request |
| T8a idempotency | **hard** | **PASS** — "already provisioned (12288/12288), nothing to do"; kubelet ExecMainStartTimestamp unchanged |
| T8b reboot recovery | **hard** | **PASS** — same UID, new bootID; full re-provision (0→30720) + kubelet restart unattended; node back at 60Gi ~2.5 min after `reboot` |
| T9 churned-node allocation | informational | **CLEAN** — full 12288 without remediation in 0.046 s after 48G anon churn + 20G page-cache fill (kernel 6.12; mild profile, see notes) |
| T10 stability soak | **hard** | **PASS** — T3+28 min and T3+64 min: same UID/bootID, `nr_hugepages` 12288, allocatable 24Gi, zero REPAIR/AUTO_REPAIR operations |
| T7 end-to-end | **hard** (Phase 2) | **PASS** — full LoRA slot-swap validation SUCCESS on a bootstrap-provisioned node; hugepage-backed dumps confirmed |

## Per-test evidence

### T1 — Runtime allocation on a fresh node (node A)

`echo 12288 > /proc/sys/vm/nr_hugepages` took **0.045 s**; readback 12288,
`HugePages_Total: 12288`, `HugePages_Free: 12288`. (`T1.log`)

### T2 — No capacity without kubelet restart

3 m 40 s after T1 (several status cycles): node capacity/allocatable still
`0 / 0` while the host held 12288 pages. Kubelet only discovers hugepage
capacity at startup — the restart step is load-bearing. (`T2.log`)

### T3 — Kubelet restart publishes capacity

Restart at 23:30:26Z; first poll at 23:30:28Z already showed
`24Gi / 24Gi`, allocatable memory 91054940Ki → 65889116Ki, a drop of
exactly 25165824Ki = 24Gi (no scheduler overcommit). (`T3.log`)

### T4 — Restart was non-disruptive (the core claim)

Three kubelet restarts observed in total (node A manual; node B via the
DaemonSet twice — initial 12288 and later grow-to-30720):

- Heartbeat canary on node A: `restartCount 0`, same `startedAt`
  (23:27:08Z), **zero gaps >3 s** in 216 one-second heartbeats spanning
  the restart.
- Ready watcher (2 s sampling, 15 min window covering the restart):
  **301/301 samples `True`** — not even a single-sample blip.
- Node A events: only `Starting kubelet` / `KubeletStart` +
  `NodeHasSufficient*`; no `NodeNotReady`, no repair events.
- Every pod on node A (12 system DaemonSet pods incl. dcgm-exporter,
  gpu-device-plugin, gke-metadata-server) and node B: restartCount 0,
  original container start times, across all restarts. Node B `Ready`
  condition `lastTransitionTime` remained its node-creation value
  (23:19:02Z) through both restarts; node-problem-detector's
  `FrequentKubeletRestart` stayed `False`.
- GKE operations list: no REPAIR_CLUSTER / AUTO_REPAIR_NODES ever
  appeared (checked through T10). (`T4.log`, `ready-watch-a.log`, `T10.log`)

### T5 — Scheduling gate

Pre (before any provisioning): `FailedScheduling … 1 Insufficient
hugepages-2Mi` (with the GPU taint tolerated — see deviation note).
Post: the same pending pod was scheduled at 23:30:28Z — **2 s after the
node published capacity** — ran, printed `wrote OK`, exit 0, Succeeded.
No race window needing operator intervention. (`T5-pre.log`, `T5-post.log`)

### Track B — the draft bootstrap DaemonSet (Appendix B) end-to-end

Applied at 23:26:24Z; init container logged the allocate path
(`reserving 12288 … (currently 0)` → `pool ready; restarting kubelet`),
node B showed `24Gi / 24Gi` and allocatable memory
91054948Ki → 65889124Ki within ~20 s of apply. Pause container Running.
Later, raising `HUGEPAGES_2M` to 30720 re-ran the script
(`reserving 30720 … (currently 12288)`) and published `60Gi / 60Gi`
within 4 s of the kubelet restart — pool growth on a live node works and
costs exactly one more restart. (`trackB-bootstrap.log`, `T7-prep-60Gi.log`)

### T6 — hugetlb cgroup enforcement mode (node B)

- Variant B (requests `hugepages-2Mi: 2Gi`): **`wrote OK`, exit 0**.
- Variant A (no request): **exit 135 = SIGBUS. Enforcement is REAL.**
- Ground truth: kubelet sets the **pod-level** cgroup —
  `kubepods-burstable-pod<uid>.slice/hugetlb.2MB.max = 0` for
  non-requesting pods, `= 2147483648` for the 2Gi-requesting pod
  (container scopes are `max`; the limit lives on the pod slice).

Consequence: the `hugepages-2Mi` requests in the user guide are
**mandatory, not advisory** — a workload that fails to request the
resource SIGBUSes on first hugetlbfs fault; conversely the pool cannot be
stolen by non-requesting pods (no overcommit risk). (`T6.log`)

### T8a — Bootstrap idempotency

Deleted the DS pod; replacement logged
`hugepage pool already provisioned (12288/12288 pages), nothing to do`;
kubelet `ExecMainStartTimestamp` identical before/after
(`Mon 2026-08-10 23:26:28 UTC`). No restart storm. (`T8a.log`)

### T8b — Node reboot recovery (node B, run last)

`reboot` at 23:59:12Z. Node UID unchanged
(18708707-6db5-446a-ba65-7c837c8fc248), bootID changed
(3e5df667-… → bf2f0914-…): a reboot, not a recreation. `nr_hugepages`
reset to 0 as expected; the bootstrap re-ran the full path
(`reserving 30720 … (currently 0)` → kubelet restart) and the node
reported `60Gi / 60Gi` at 00:01:52Z — **~2.5 min after the reboot
command, zero human intervention**. hugetlbfs was remounted by the
snapshot-agent's init container; dump files were gone (reboot clears
/var/tmp/huge-ckpt), `HugePages_Free` back to 30720; agent Running and
serving. Ready flap during reboot: False 23:59:35–23:59:51, True again
by 00:00:07. (`T8b.log`)

### T9 — Allocation on a memory-churned node (node C, informational)

Churn: `stress --vm 8 --vm-bytes 6G` (48G anon, 5 min) + 20 GiB dd
page-cache fill + sync. Then:

- Step 2 (no help): `echo 12288 > nr_hugepages` → **12288, 0.046 s**.
- Step 3 (remediation path): still 12288, 2.17 s (drop_caches dominated).

On kernel 6.12.85+ this churn profile left MemFree ≈ 86G at attempt time
(anon churn exits, page cache is instantly reclaimable), so this is a
**mild** profile as the plan anticipated. It does not prove allocation
succeeds on an arbitrarily fragmented long-lived node; the bootstrap's
fail-without-kubelet-restart path remains the guard for that case.
"Install the bootstrap before heavy workloads" stays the recommendation,
but is not a hard constraint at this profile. (`T9.log`)

### T10 — Stability soak (node A)

- T3+28 min (23:58:34Z) and T3+64 min (00:34:48Z): UID
  4d4bb736-7d70-41b0-bdb4-64b8b9309e29 and bootID 6a937abd-… unchanged,
  allocatable `24Gi`, host `nr_hugepages` 12288, canary still
  restartCount 0 / startedAt 23:27:08Z.
- `gcloud container operations list` filtered for REPAIR_CLUSTER /
  AUTO_REPAIR_NODES: **empty** both times. No GKE reconciler resets the
  sysctl and no auto-repair reacts to the restart. (`T10.log`)

### T7 — End-to-end integration (bootstrap-provisioned GPU node)

Setup deviation (deliberate): the current demo stack
(`lora-swap-client-max1:latest`, stock 25GiB-staging vGPU-NVIDIA.so build)
requests **28Gi** of hugepages per workload, and the production baseline
nodepool (`rl-hugepages-l4-b`) is configured with `hugepageSize2m: 30720`
(60Gi) — not the plan's 12288. To keep "identical outcome to the
hugepage-nodepool baseline" meaningful, the bootstrap target on the T7
node was raised to **30720 (60Gi)**, i.e. the same pool size the node
config would have provisioned, but provisioned **at runtime on a vanilla
node**. (The 12288 figure used by tracks A/B matches the SHM_SIZE_GB=8
build described in the user guide.)

- Agent image `snapshot-agent:taskc-1a0245b` built via Cloud Build from
  this branch and deployed as DaemonSet `snapshot-agent-taskc`
  (timeslice-system, pinned to the vanilla pool; manifest =
  `deploy/examples/snapshot-agent-memory-addresses.yaml` with the two
  placeholders filled).
- LoRA slot-swap validation (`test_lora_swap_max1.py` via
  `job-lora-swap-max1.yaml`, pinned to the node): **all SUCCESS lines** —
  slot addresses identical for A and B, hijacked request returned A's
  output after restoring A over B, B restored correctly, temperature-0
  outputs bit-identical across park/revive. Exit 0.
- Agent per-op timings normal: selective checkpoint 506 ms (first) /
  54 ms (warm), selective restore 51–60 ms, snapshot-copy 217–235 ms.
- Hugepage backing real: `HugePages_Free` 30720 → **29549** (with
  `HugePages_Rsvd` 12655) while the workload ran — dumps landed on
  hugetlbfs, not page cache — returning to 30718 after the agent's GC.
- No SIGBUS, no `mmap with hugepages failed`, no dump degradation.
  (`T7-build.log`, `T7-agent.log`, `T7-job.log`, `T7-hugepages-sampler.log`,
  `T7-prep-60Gi.log`)

## Notable findings beyond the pass/fail gates

1. **Kubelet restart blast radius is nil on this node shape** — three
   restarts across two GPU nodes: zero container restarts, zero Ready
   flaps at 2 s sampling, no auto-repair. The capacity flip is visible
   API-side within ~2–4 s of the restart.
2. **hugetlb cgroup v2 enforcement is active on GKE COS** at the pod
   slice. Requests are load-bearing; pods without them SIGBUS on
   hugetlbfs faults. This also means the pool is protected from
   non-requesting tenants.
3. **Pool growth on a live node works** (12288→30720 + one kubelet
   restart), so a target-size change rolls out like any DS env change.
4. **Node reboot is fully self-healing**: reallocation + kubelet restart
   + hugetlbfs remount + agent restart, ~2.5 min, no operator action.
5. **GPU-taint toleration is required** on anything scheduled to the pool
   by the scheduler (the plan's Appendix B/C manifests lacked it; fixed
   copies live in `testing-artifacts/hugepages-bootstrap/`).
6. The bootstrap DS priority class + "fail WITHOUT restarting kubelet on
   shortfall" behaved as designed; the shortfall path was never triggered
   (allocations were instant even post-churn on kernel 6.12).

## Cleanup performed

Namespace `taskc-hpboot`, DaemonSet `snapshot-agent-taskc`, PriorityClass
`timeslice-hugepages-bootstrap` deleted; nodepool `hpboot-l4-c` deleted.
