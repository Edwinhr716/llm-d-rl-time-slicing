# Donor controller image. Build from the repository root:
#   docker build -f deploy/donor-controller/Dockerfile .
FROM golang:1.26.2 AS builder

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /workspace/donor-controller ./cmd/donor-controller

FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/donor-controller /donor-controller
USER 65532:65532

ENTRYPOINT ["/donor-controller"]
