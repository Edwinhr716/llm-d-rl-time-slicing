// Package v1alpha1 holds the host command API. The orchestrator tells each
// host of a group to vacate its guests before a deadline and to resume them;
// the host answers each command with an ack. The per-host component (the
// virtual kubelet) serves it and the orchestrator is its only caller.
package v1alpha1

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative host_command.proto
