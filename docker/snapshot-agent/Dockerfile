# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire project to resolve internal dependencies
COPY . .

# Drop any local go.work; the root go.mod resolves all packages
RUN rm -f go.work

# Download pre-built cr_client binary from GitHub repository
RUN mkdir -p /workspace/bin && \
    curl -fsSL -o /workspace/bin/cr_client https://raw.githubusercontent.com/Edwinhr716/llm-d-rl-time-slicing/gcr-backend-memory-allocation/bin/cr_client && \
    chmod 755 /workspace/bin/cr_client

# Build the snapshot-agent
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /workspace/snapshot-agent ./cmd/snapshot-agent

# --- Runtime stage ---
FROM gcr.io/distroless/cc

COPY --from=builder /workspace/snapshot-agent .
COPY --from=builder /workspace/bin/cr_client /usr/local/bin/cr_client
COPY --from=builder /workspace/bin/cr_client /bin/cr_client

ENTRYPOINT ["/snapshot-agent"]