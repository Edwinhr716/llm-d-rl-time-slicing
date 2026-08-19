# GPU-CR test suites

Three tiers, from GPU-free unit tests to on-node performance regression.
Throughout, "upstream" means the original GPU-CR project this tree builds
on — <https://github.com/gpu-os/GPU-CR/tree/main> — pinned at commit
`e9bbb52`. The unit tier runs automatically inside every
`Dockerfile.build` image build (`RUN_TESTS=1`, the default) — a red suite
fails the build.

## How the unit tier works

The unit tier uses three standard C++ tools. If you have not worked with
a C++ test suite before, this is the map:

- **CMake** is the build system: it reads `CMakeLists.txt` and generates
  the actual build scripts. The tests sit behind the
  `-DGPU_CR_BUILD_TESTS=ON` switch (off by default, so ordinary builds
  never pay for them). When the switch is on, CMake downloads
  **GoogleTest** at configure time (`FetchContent`) — there is nothing to
  install by hand.
- **GoogleTest** is the test framework. Unlike interpreted languages,
  where a runner discovers test files at run time, C++ tests are
  *compiled into a program*: every `tests/unit/*_test.cpp` file, plus the
  GPU-free production sources under test, build into one executable,
  `gpu_cr_unit_tests`. Running that program runs every test case, prints
  an `[  OK  ]` or `[  FAILED  ]` line per case, and exits nonzero if
  anything failed.
- **ctest** is the test runner that ships with CMake. The build registers
  the executable under the name `unit` (the `add_test` line in
  `CMakeLists.txt`), and `ctest -R unit` runs every registered test whose
  name matches `unit`, reporting pass/fail from the exit code.

Inside a `*_test.cpp` file, each case is a `TEST` block:

```cpp
// TEST(<suite name>, <case name>). It registers itself; no main() is
// needed — the GTest::gtest_main library supplies one.
TEST(RoundUp2MBTest, ZeroStaysZero) {
  EXPECT_EQ(ROUND_UP_2MB(0UL), 0UL);  // check, record failure, continue
}
```

`EXPECT_*` assertions record a failure and keep going, so a single run
reports every broken expectation. The `ASSERT_*` variants abort the
current test case on failure — used when the lines after them would
crash on the bad value (e.g. dereferencing the result of a failed
`mmap`).

Useful invocations:

```sh
# Configure, build, and run the whole unit tier:
cmake -DGPU_VENDOR=NVIDIA -DGPU_CR_BUILD_TESTS=ON .. && make && ctest -R unit

# Show the failing assertions, not just the red summary line:
ctest -R unit --output-on-failure

# Run the test program directly, filtered to one suite:
./gpu_cr_unit_tests --gtest_filter='RoundUp2MBTest.*'
```

To add a test, add a `TEST(...)` block to the matching `*_test.cpp` file
and rebuild — registration is automatic. A brand-new test file must also
be added to the `gpu_cr_unit_tests` source list in the top-level
`CMakeLists.txt`, or it will not be compiled in.

The two GPU tiers below are plain bash scripts, not GoogleTest: each
prints a `PASS`/`FAIL` line per gate and exits nonzero if any gate
failed.

## 1. Unit tests — `tests/unit/` (GoogleTest, no GPU, Linux)

Function-level coverage of the upstream-baseline functions: the
2MB rounding macro, signal numbers and wire structs
(`common_baseline_test`), the ShareMemComm control channel
(`share_mem_comm_test`), the ShareMem dump/staging buffer mapping via the
file backend (`mmap_backend_test`), and the UDS SCM_RIGHTS fd exchange
(`ipc_fd_exchange_test`). `createGPU()` and the CUDA/HIP hook layers need
a driver link, so they stay covered by the e2e tier.

## 2. End-to-end — `tests/e2e/run_e2e.sh` (GPU node)

A real CUDA workload (`pattern_workload`) under `LD_PRELOAD=vGPU-NVIDIA.so`
goes through full checkpoint/restore, gating on **byte-identical GPU memory
after every restore**.

## 3. Performance regression — `tests/e2e/perf_regression.sh` (GPU node)

Verifies a build has not regressed the full checkpoint/restore data plane
that upstream (`e9bbb52`) delivers: same workload, same node,
baseline .so vs candidate .so, median-of-N compared against a threshold
(default 15%).

Build the baseline once with `tests/e2e/build_baseline_so.sh`, then run
both GPU tiers as a one-shot pod: `tests/e2e/e2e-pod.yaml` (exits 0 only if
every e2e gate and the perf gate pass).
