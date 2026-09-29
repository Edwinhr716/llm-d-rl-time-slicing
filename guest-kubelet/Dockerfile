FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /guest-kubelet ./cmd/guest-kubelet \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /donor-standin ./cmd/donor-standin \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /q5-measure ./cmd/q5-measure

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /guest-kubelet /guest-kubelet
COPY --from=build /donor-standin /donor-standin
# M2 test tool (Q5 timings); the Deployment runs /guest-kubelet only.
COPY --from=build /q5-measure /q5-measure
USER 65532:65532
ENTRYPOINT ["/guest-kubelet"]
