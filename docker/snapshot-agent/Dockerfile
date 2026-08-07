# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire project to resolve internal dependencies
COPY . .

# Drop any local go.work; the root go.mod resolves all packages
RUN rm -f go.work

# The GPU-CR cr_client CLI is taken from the checkout's bin/ directory.
# To use a specific GPU-CR version, replace bin/cr_client before building
# (see docs/gke-memory-address-demo.md for building it from a GPU-CR release).
RUN chmod 755 /workspace/bin/cr_client

# Build the snapshot-agent
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /workspace/snapshot-agent ./cmd/snapshot-agent

# --- Runtime stage ---
FROM gcr.io/distroless/cc

COPY --from=builder /workspace/snapshot-agent .
COPY --from=builder /workspace/bin/cr_client /usr/local/bin/cr_client
COPY --from=builder /workspace/bin/cr_client /bin/cr_client

ENTRYPOINT ["/snapshot-agent"]