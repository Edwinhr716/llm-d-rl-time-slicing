# Timeslice admission webhook (cmd/timeslice-webhook). No client, no RBAC: it only answers the
# API server over HTTPS with the certificate mounted from a Secret.
#
# Build context is the repository root, same as the other images in docker/:
#   docker buildx build -f docker/timeslice-webhook/Dockerfile .

# --- Build stage ---
FROM golang:1.26.2 AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/timeslice-webhook cmd/timeslice-webhook
COPY pkg/webhook pkg/webhook
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /workspace/timeslice-webhook ./cmd/timeslice-webhook

# --- Runtime stage ---
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/timeslice-webhook .

USER 65532:65532

EXPOSE 8443

ENTRYPOINT ["/timeslice-webhook"]
