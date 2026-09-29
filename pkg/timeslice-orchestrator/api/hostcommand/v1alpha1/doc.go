// Package v1alpha1 holds the host command API: the orchestrator commands each
// host of a group to vacate its guests within a deadline and to resume them,
// and the host acks each command. The per-host component (the virtual
// kubelet) serves it; the orchestrator is the only caller.
package v1alpha1

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative host_command.proto
