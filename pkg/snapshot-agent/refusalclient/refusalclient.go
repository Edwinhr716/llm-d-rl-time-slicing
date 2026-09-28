// Package refusalclient reads the ErrorReason of a refused snapshot-agent
// Suspend, Resume or Kill call.
//
// The agent puts the reason's enum name at the start of the gRPC status
// message, followed by ": " (for example "STALE_EPOCH: epoch 3 < 5"). A
// refusal without a reason has no prefix.
//
// The package imports nothing from this module, so callers in other Go
// modules can copy it as is. For the same reason Reason returns the enum
// name as a string rather than an ErrorReason value.
package refusalclient

import (
	"errors"
	"strings"

	"google.golang.org/grpc/status"
)

// reasons are the ErrorReason names the agent can put in the prefix. They
// mirror the ErrorReason enum of the snapshot-agent API, without
// ERROR_REASON_UNSPECIFIED, which the agent never writes.
var reasons = []string{
	"DEADLINE_INFEASIBLE",
	"DEADLINE_EXCEEDED",
	"STALE_EPOCH",
	"PRECONDITION_READINESS",
	"PRECONDITION_PROBES",
	"PRECONDITION_MEMORY",
	"PRECONDITION_NODE",
	"BACKEND_ERROR",
	"VERIFY_FAILED",
	"KILL_UNCONFIRMED",
}

// Reason returns the ErrorReason name carried by err (for example
// "STALE_EPOCH"), or "" when err carries none.
//
// err may be the error a gRPC client returns, that error wrapped with %w,
// or the agent's own refusal error. Reason reads the message of the first
// error in the chain that has a gRPC status, so text added by wrapping does
// not hide the prefix.
func Reason(err error) string {
	var withStatus interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &withStatus) {
		return ""
	}
	st := withStatus.GRPCStatus()
	if st == nil {
		return ""
	}
	msg := st.Message()
	for _, name := range reasons {
		if strings.HasPrefix(msg, name+": ") {
			return name
		}
	}
	return ""
}
