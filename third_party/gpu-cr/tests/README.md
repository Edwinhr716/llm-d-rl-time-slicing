# GPU-CR test suites

Three tiers, from GPU-free unit tests to on-node performance regression.
The unit tier runs automatically inside every `Dockerfile.build` image build
(`RUN_TESTS=1`, the default) — a red suite fails the build.

## 1. Unit tests — `tests/unit/` (GoogleTest, no GPU, Linux)

Function-level coverage of the upstream-baseline (v0.2.1) functions: the
2MB rounding macro, signal numbers and wire structs
(`common_baseline_test`), the ShareMemComm control channel
(`share_mem_comm_test`), the ShareMem dump/staging buffer mapping via the
file backend (`mmap_backend_test`), and the UDS SCM_RIGHTS fd exchange
(`ipc_fd_exchange_test`). `createGPU()` and the CUDA/HIP hook layers need
a driver link, so they stay covered by the e2e tier.

```sh
cmake -DGPU_VENDOR=NVIDIA -DGPU_CR_BUILD_TESTS=ON .. && make && ctest -R unit
```

## 2. End-to-end — `tests/e2e/run_e2e.sh` (GPU node)

A real CUDA workload (`pattern_workload`) under `LD_PRELOAD=vGPU-NVIDIA.so`
goes through full checkpoint/restore, gating on **byte-identical GPU memory
after every restore**.

## 3. Performance regression — `tests/e2e/perf_regression.sh` (GPU node)

Verifies a build has not regressed the full checkpoint/restore data plane
that upstream v0.2.1 (`e9bbb52`) delivers: same workload, same node,
baseline .so vs candidate .so, median-of-N compared against a threshold
(default 15%).

Build the baseline once with `tests/e2e/build_baseline_so.sh`, then run
both GPU tiers as a one-shot pod: `tests/e2e/e2e-pod.yaml` (exits 0 only if
every e2e gate and the perf gate pass).
