# cr_client and vGPU-NVIDIA.so are version-locked through the GPU-CR v2/v3
# control protocol (GEP-0001/GEP-0006), so the client is taken from the SAME
# GPU-CR build image that supplies the workload's .so — never from a stale
# checked-in binary.
ARG GPUCR_IMAGE=asia-southeast1-docker.pkg.dev/edwinhernandez-gke-dev/time-slicing/gpucr-so:gep0006-6bdce96
FROM ${GPUCR_IMAGE} AS gpucr

# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire project to resolve internal dependencies
COPY . .

# Drop any local go.work; the root go.mod resolves all packages
RUN rm -f go.work

# Unit tests run here because the dev workstation may not execute
# cgo-linked (nvml) test binaries; the Linux builder always can.
RUN CGO_ENABLED=1 go vet ./pkg/... ./cmd/... && CGO_ENABLED=1 go test ./pkg/...

# Build the snapshot-agent
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /workspace/snapshot-agent ./cmd/snapshot-agent

# Fetch cuda-checkpoint (pinned to a specific NVIDIA/cuda-checkpoint commit) so the
# image is self-contained and builds from a fresh clone without a pre-populated bin/
ADD https://github.com/NVIDIA/cuda-checkpoint/raw/00d5cce84c628088d6caa203fc4af40c1538b6f7/bin/x86_64_Linux/cuda-checkpoint /workspace/bin-dist/cuda-checkpoint
RUN chmod +x /workspace/bin-dist/cuda-checkpoint

# --- Runtime stage ---
# distroless/cc (base + libgcc/libstdc++): cr_client is a dynamically linked
# C++ binary.
FROM gcr.io/distroless/cc

WORKDIR /
COPY --from=builder /workspace/snapshot-agent .
# Copy bundled binaries (cuda-checkpoint) from the builder's bin directory
COPY --from=builder /workspace/bin-dist ./bin
# cr_client at the memory-regions backend's default fallback path (also on PATH).
COPY --from=gpucr /cr_client /usr/local/bin/cr_client
COPY --from=gpucr /cr_client /bin/cr_client

ENTRYPOINT ["/snapshot-agent"]
