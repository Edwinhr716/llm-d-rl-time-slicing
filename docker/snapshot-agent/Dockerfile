# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire project to resolve internal dependencies
COPY . .

# Drop any local go.work; the root go.mod resolves all packages
RUN rm -f go.work

# Build the snapshot-agent
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /workspace/snapshot-agent ./cmd/snapshot-agent

# Fetch cuda-checkpoint (pinned to a specific NVIDIA/cuda-checkpoint commit) so the
# image is self-contained and builds from a fresh clone without a pre-populated bin/
ADD https://github.com/NVIDIA/cuda-checkpoint/raw/00d5cce84c628088d6caa203fc4af40c1538b6f7/bin/x86_64_Linux/cuda-checkpoint /workspace/bin-dist/cuda-checkpoint
RUN chmod +x /workspace/bin-dist/cuda-checkpoint

# The GPU-CR cr_client CLI (memory-regions backend) is taken from the
# checkout's bin/ directory, so agent and cr_client always roll together and
# the version is pinned by the image digest. To use a specific GPU-CR
# version, replace bin/cr_client before building.
RUN chmod 755 /workspace/bin/cr_client

# --- Runtime stage ---
# distroless/cc (base + libgcc/libstdc++): cr_client is a dynamically linked
# C++ binary.
FROM gcr.io/distroless/cc

WORKDIR /
COPY --from=builder /workspace/snapshot-agent .
# Copy bundled binaries (cuda-checkpoint) from the builder's bin directory
COPY --from=builder /workspace/bin-dist ./bin
# cr_client at the memory-regions backend's default fallback path (also on PATH).
COPY --from=builder /workspace/bin/cr_client /usr/local/bin/cr_client


ENTRYPOINT ["/snapshot-agent"]
