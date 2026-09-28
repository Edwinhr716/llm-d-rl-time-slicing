package statemachine_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/refusalclient"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRefusalWire_Prefix_EveryReason checks that refusalclient, which cannot
// import the API package, reads back every ErrorReason the agent can refuse
// with, from the agent's own error, from the status a client sees, and from
// both wrapped with %w.
func TestRefusalWire_Prefix_EveryReason(t *testing.T) {
	reasons := make([]pb.ErrorReason, 0, len(pb.ErrorReason_name))
	for v := range pb.ErrorReason_name {
		reasons = append(reasons, pb.ErrorReason(v))
	}
	slices.Sort(reasons)
	for _, reason := range reasons {
		want := reason.String()
		if reason == pb.ErrorReason_ERROR_REASON_UNSPECIFIED {
			want = ""
		}
		t.Run(reason.String(), func(t *testing.T) {
			refusal := &statemachine.RefusalError{Code: codes.FailedPrecondition, Reason: reason, Msg: "a: b"}
			onWire := status.Convert(refusal).Err()
			views := []struct {
				name string
				err  error
			}{
				{"agent error", refusal},
				{"agent error wrapped", fmt.Errorf("suspend: %w", refusal)},
				{"status", onWire},
				{"status wrapped", fmt.Errorf("suspend: %w", onWire)},
			}
			for _, view := range views {
				if got := refusalclient.Reason(view.err); got != want {
					t.Errorf("%s: got %q, want %q", view.name, got, want)
				}
			}
			if got := status.Code(onWire); got != codes.FailedPrecondition {
				t.Errorf("code: got %s, want %s", got, codes.FailedPrecondition)
			}
			if got := statemachine.ErrorReasonOf(refusal); got != reason {
				t.Errorf("ErrorReasonOf: got %s, want %s", got, reason)
			}
			if want != "" && !strings.HasPrefix(status.Convert(refusal).Message(), want+": ") {
				t.Errorf("status message %q does not start with %s", status.Convert(refusal).Message(), want)
			}
		})
	}
}

// TestRefusalWire_Prefix_UnspecifiedHasNoPrefix checks that a refusal
// without a reason carries its message as is.
func TestRefusalWire_Prefix_UnspecifiedHasNoPrefix(t *testing.T) {
	refusal := &statemachine.RefusalError{
		Code: codes.InvalidArgument, Reason: pb.ErrorReason_ERROR_REASON_UNSPECIFIED, Msg: "job_id is required",
	}
	if got := status.Convert(refusal).Message(); got != "job_id is required" {
		t.Errorf("status message: got %q, want %q", got, "job_id is required")
	}
	if got := refusalclient.Reason(refusal); got != "" {
		t.Errorf("Reason: got %q, want \"\"", got)
	}
}
