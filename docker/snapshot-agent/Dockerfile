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

# --- Runtime stage ---
FROM gcr.io/distroless/cc

COPY --from=builder /workspace/snapshot-agent .
COPY --from=gpucr /cr_client /usr/local/bin/cr_client
COPY --from=gpucr /cr_client /bin/cr_client

ENTRYPOINT ["/snapshot-agent"]
