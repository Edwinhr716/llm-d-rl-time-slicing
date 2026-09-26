FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /guest-kubelet ./cmd/guest-kubelet

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /guest-kubelet /guest-kubelet
USER 65532:65532
ENTRYPOINT ["/guest-kubelet"]
