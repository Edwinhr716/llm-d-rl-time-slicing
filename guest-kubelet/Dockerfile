# Build from the repository root (the module imports the root module's gRPC APIs):
#   docker build -f guest-kubelet/Dockerfile .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY guest-kubelet/go.mod guest-kubelet/go.sum ./guest-kubelet/
WORKDIR /src/guest-kubelet
RUN go mod download
WORKDIR /src
COPY pkg/timeslice-orchestrator/api/ ./pkg/timeslice-orchestrator/api/
COPY pkg/snapshot-agent/api/ ./pkg/snapshot-agent/api/
COPY guest-kubelet/ ./guest-kubelet/
WORKDIR /src/guest-kubelet
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /guest-kubelet ./cmd/guest-kubelet

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /guest-kubelet /guest-kubelet
USER 65532:65532
ENTRYPOINT ["/guest-kubelet"]
