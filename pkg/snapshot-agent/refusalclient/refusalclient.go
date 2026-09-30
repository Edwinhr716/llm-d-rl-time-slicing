// Package refusalclient reads the ErrorReason of a refused snapshot-agent
// Suspend, Resume or Kill call.
//
// The agent puts the reason in the gRPC status details, as one
// google.rpc.ErrorInfo whose Reason is the ErrorReason enum name (for example
// "STALE_EPOCH") and whose Domain is Domain. A refusal without a reason has
// no ErrorInfo. The status message is free text.
//
// The package imports nothing from this module, so callers in other Go
// modules can copy it as is. For the same reason Reason returns the enum
// name as a string rather than an ErrorReason value.
package refusalclient

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// Domain is the ErrorInfo domain of snapshot-agent refusals.
const Domain = "snapshot-agent.llm-d-rl-time-slicing"

// Reason returns the ErrorReason name carried by err (for example
// "STALE_EPOCH"), or "" when err carries none.
//
// err may be the error a gRPC client returns, that error wrapped with %w,
// or the agent's own refusal error. Wrapping changes the status message but
// keeps its details.
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok || st == nil {
		return ""
	}
	for _, detail := range st.Details() {
		info, isInfo := detail.(*errdetails.ErrorInfo)
		if isInfo && info.GetDomain() == Domain {
			return info.GetReason()
		}
	}
	return ""
}
