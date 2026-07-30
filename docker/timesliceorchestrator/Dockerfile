# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

# Copy the entire repository
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /workspace/timesliceorchestrator ./cmd/timesliceorchestrator

# --- Runtime stage ---
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/timesliceorchestrator .

USER 65532:65532

# Expose gRPC port
EXPOSE 50051

ENTRYPOINT ["/timesliceorchestrator"]