# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire project to resolve internal dependencies
COPY . .

# Recreate go.work to ensure clean multi-module build
RUN rm -f go.work && \
    go work init . && \
    go work use ./pkg/timeslice-orchestrator && \
    go work use ./pkg/snapshot-agent

# Build the snapshot-agent
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /workspace/snapshot-agent ./cmd/snapshot-agent

# Fetch cuda-checkpoint (pinned to a specific NVIDIA/cuda-checkpoint commit) so the
# image is self-contained and builds from a fresh clone without a pre-populated bin/
ADD https://github.com/NVIDIA/cuda-checkpoint/raw/00d5cce84c628088d6caa203fc4af40c1538b6f7/bin/x86_64_Linux/cuda-checkpoint /workspace/bin/cuda-checkpoint
RUN chmod +x /workspace/bin/cuda-checkpoint

# --- GPU-CR cr_client build stage (direct_memory backend) ---
# CPU-only: compiles against CUDA headers, no GPU needed. Pinned to the same
# gpu-os/GPU-CR commit the project vendors, so agent and cr_client always
# roll together and the version is pinned by the image digest.
FROM nvidia/cuda:13.0.0-devel-ubuntu22.04 AS gpucr-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates cmake build-essential && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/gpu-os/GPU-CR /tmp/GPU-CR && \
    git -C /tmp/GPU-CR checkout e9bbb52e1f52986587fc631217c0f2b50b46245a
# Build-time patches, kept byte-in-sync with the deployed vGPU-NVIDIA.so
# preloader (cr_client and the .so share the signal constants in
# src/common.h; a mismatch makes checkpoints silently no-op):
#   - remap checkpoint/restore signals off SIGUSR1/2 (taken by NCCL/vLLM)
#   - create the shared-memory control file world-writable so unprivileged
#     workloads can open it
RUN cd /tmp/GPU-CR && \
    sed -i 's/#define CR_CKPT_SIGNAL     SIGUSR1/#define CR_CKPT_SIGNAL     (SIGRTMAX - 8)/' src/common.h && \
    sed -i 's/#define CR_RESTORE_SIGNAL  SIGUSR2/#define CR_RESTORE_SIGNAL  (SIGRTMAX - 7)/' src/common.h && \
    sed -i 's/int fd_control = open(control_name, O_CREAT | O_RDWR, 0755);/int fd_control = open(control_name, O_CREAT | O_RDWR, 0777); fchmod(fd_control, 0777);/' src/comm/share_mem.cpp && \
    grep -q "SIGRTMAX - 8" src/common.h && \
    grep -q "0777" src/comm/share_mem.cpp && \
    mkdir build && cd build && \
    cmake -DGPU_VENDOR=NVIDIA .. && \
    make -j"$(nproc)" cr_client

# --- Runtime stage ---
# distroless/cc (base + libgcc/libstdc++): cr_client is a dynamically linked
# C++ binary.
FROM gcr.io/distroless/cc

WORKDIR /
COPY --from=builder /workspace/snapshot-agent .
# Copy bundled binaries (cuda-checkpoint) from the builder's bin directory
COPY --from=builder /workspace/bin ./bin
COPY --from=gpucr-builder /tmp/GPU-CR/build/cr_client /usr/local/bin/cr_client


ENTRYPOINT ["/snapshot-agent"]