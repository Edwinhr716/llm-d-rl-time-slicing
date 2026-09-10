# GPU-CR artifact package: vGPU-NVIDIA.so (workload preloader) + cr_client
# (agent-side coordinator), built from the vendored third_party/gpu-cr tree
# and published to ghcr.io by ci-release.yaml alongside the snapshot-agent
# and timesliceorchestrator images.
#
# The two binaries are version-locked through the GPU-CR control-channel
# protocol and share compiled-in constants, so they must always be consumed
# as a pair FROM THE SAME IMAGE TAG: the snapshot-agent image copies
# cr_client from this package, and workload images copy vGPU-NVIDIA.so from
# the same tag. A mismatch makes checkpoints silently no-op.
#
# Build context is the repo root (like the other docker/ images). CPU-only:
# compiles against CUDA headers, no GPU needed.

# --- Build stage ---
FROM nvidia/cuda:13.0.0-devel-ubuntu22.04 AS builder
ARG SHM_SIZE_GB=25
ARG RUN_TESTS=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    git cmake build-essential && rm -rf /var/lib/apt/lists/*

COPY third_party/gpu-cr /tmp/GPU-CR
# Build-time patches, kept byte-in-sync with third_party/gpu-cr/Dockerfile.build
# and docker/snapshot-agent/Dockerfile: remap checkpoint/restore signals off
# SIGUSR1/2 (taken by NCCL/vLLM). control-file 0777 is source truth in
# share_mem.cpp; the grep asserts it stays that way.
#
# RUN_TESTS=1 (the default, and what CI publishes with) also compiles and
# runs the GPU-free test suites — a red suite fails the release build.
RUN cd /tmp/GPU-CR && \
    sed -i 's/#define CR_CKPT_SIGNAL     SIGUSR1/#define CR_CKPT_SIGNAL     (SIGRTMAX - 8)/' src/common.h && \
    sed -i 's/#define CR_RESTORE_SIGNAL  SIGUSR2/#define CR_RESTORE_SIGNAL  (SIGRTMAX - 7)/' src/common.h && \
    grep -q "SIGRTMAX - 8" src/common.h && \
    grep -q "SIGRTMAX - 7" src/common.h && \
    grep -q "0777" src/comm/share_mem.cpp && \
    mkdir build && cd build && \
    if [ "$RUN_TESTS" = "1" ]; then \
        cmake -DGPU_VENDOR=NVIDIA -DSHM_SIZE_GB=${SHM_SIZE_GB} \
              -DGPU_CR_BUILD_TESTS=ON .. && \
        make -j"$(nproc)" && \
        ctest --output-on-failure; \
    else \
        cmake -DGPU_VENDOR=NVIDIA -DSHM_SIZE_GB=${SHM_SIZE_GB} .. && \
        make -j"$(nproc)" vGPU cr_client; \
    fi

# --- Artifact carrier ---
# busybox (not scratch) on purpose: consumers that can't COPY --from at
# image-build time can run this image as an init container and `cp` the
# binaries into a shared volume.
FROM busybox:stable
COPY --from=builder /tmp/GPU-CR/LICENSE /opt/gpu-cr/LICENSE
COPY --from=builder /tmp/GPU-CR/build/vGPU-NVIDIA.so /opt/gpu-cr/vGPU-NVIDIA.so
COPY --from=builder /tmp/GPU-CR/build/cr_client /opt/gpu-cr/cr_client
