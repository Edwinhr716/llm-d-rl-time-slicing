package statemachine_test

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/refusalclient"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRefusalWire_ErrorInfo_EveryReason checks the status of a refusal for
// every ErrorReason: plain message, and exactly one ErrorInfo with the
// reason's name unless the reason is UNSPECIFIED. It also checks that
// refusalclient, which cannot import the API package, reads the reason back
// from the agent's own error, from the status a client sees, and from both
// wrapped with %w.
func TestRefusalWire_ErrorInfo_EveryReason(t *testing.T) {
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
			st := status.Convert(refusal)
			if st.Code() != codes.FailedPrecondition {
				t.Errorf("code: got %s, want %s", st.Code(), codes.FailedPrecondition)
			}
			if st.Message() != "a: b" {
				t.Errorf("message: got %q, want %q", st.Message(), "a: b")
			}
			var infos []*errdetails.ErrorInfo
			for _, detail := range st.Details() {
				info, ok := detail.(*errdetails.ErrorInfo)
				if !ok {
					t.Errorf("unexpected detail %T", detail)
					continue
				}
				infos = append(infos, info)
			}
			switch {
			case want == "" && len(infos) != 0:
				t.Errorf("got %d ErrorInfo details, want none", len(infos))
			case want != "" && len(infos) != 1:
				t.Errorf("got %d ErrorInfo details, want 1", len(infos))
			case want != "":
				if infos[0].GetReason() != want || infos[0].GetDomain() != statemachine.ErrorInfoDomain {
					t.Errorf("ErrorInfo: got %q/%q, want %q/%q",
						infos[0].GetReason(), infos[0].GetDomain(), want, statemachine.ErrorInfoDomain)
				}
			}

			onWire := st.Err()
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
			if got := statemachine.ErrorReasonOf(refusal); got != reason {
				t.Errorf("ErrorReasonOf: got %s, want %s", got, reason)
			}
		})
	}
}

// TestRefusalWire_ErrorInfo_DomainMatchesClient checks that the agent and the
// client parser agree on the ErrorInfo domain.
func TestRefusalWire_ErrorInfo_DomainMatchesClient(t *testing.T) {
	if statemachine.ErrorInfoDomain != refusalclient.Domain {
		t.Errorf("agent domain %q, client domain %q", statemachine.ErrorInfoDomain, refusalclient.Domain)
	}
}
