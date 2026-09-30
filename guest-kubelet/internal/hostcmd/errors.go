package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sapb "github.com/edwinhr716/guest-kubelet/api/snapshotagent/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// ErrorInfoDomain is the domain of the google.rpc.ErrorInfo detail the snapshot agent puts on a
// refusal (lead decision D-AGENT-2, option errorinfo).
const ErrorInfoDomain = "snapshot-agent.llm-d-rl-time-slicing"

// AgentError is a refusal or a failed operation from the snapshot agent. Reason is the
// ErrorReason name, read from the ErrorInfo detail in ErrorInfoDomain on the gRPC status
// (D-AGENT-2, option errorinfo), or "" when the status has no such detail. Msg is the status
// message as sent; it is never parsed for a reason.
type AgentError struct {
	Code   codes.Code
	Reason string
	Msg    string
}

func (e *AgentError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("snapshot agent: %s: %s", e.Code, e.Msg)
	}
	return fmt.Sprintf("snapshot agent: %s: %s: %s", e.Code, e.Reason, e.Msg)
}

// Unwrap lets errors.Is(err, freeze.ErrUnimplemented) find an Unimplemented answer.
func (e *AgentError) Unwrap() error {
	if e.Code == codes.Unimplemented {
		return freeze.ErrUnimplemented
	}
	return nil
}

// ReasonOf returns the ErrorReason name carried by err, or "".
func ReasonOf(err error) string {
	var ae *AgentError
	if errors.As(err, &ae) {
		return ae.Reason
	}
	return ""
}

// decodeError turns a gRPC status error into an *AgentError. Other errors are returned as
// they are.
func decodeError(err error) error {
	if err == nil {
		return nil
	}
	var ae *AgentError
	if errors.As(err, &ae) {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	return &AgentError{Code: st.Code(), Reason: statusReason(st), Msg: st.Message()}
}

// statusReason returns the Reason of the first ErrorInfo detail in ErrorInfoDomain on st, or
// "" when there is none or it names ERROR_REASON_UNSPECIFIED.
func statusReason(st *status.Status) string {
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != ErrorInfoDomain {
			continue
		}
		if v, known := sapb.ErrorReason_value[info.GetReason()]; known && v == 0 {
			return ""
		}
		return info.GetReason()
	}
	return ""
}

// refusal builds a refusal status error the way the snapshot agent sends it: message msg and,
// when reason is set, one ErrorInfo detail in ErrorInfoDomain.
func refusal(code codes.Code, reason, msg string) error {
	st := status.New(code, msg)
	if reason == "" {
		return st.Err()
	}
	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorInfoDomain})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}

// retryable: the call may not have reached the agent, or its answer was lost. Sending it
// again with the same epoch is safe. The caller's own deadline is not retryable.
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var ae *AgentError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.Code {
	case codes.Unavailable, codes.DeadlineExceeded:
		return ae.Reason == "" // a DEADLINE_EXCEEDED reason is the agent's answer
	default:
		return false
	}
}

var lastEpochRE = regexp.MustCompile(`last epoch (\d+)`)

// StaleEpoch returns the agent's last epoch from a STALE_EPOCH refusal ("... is lower than the
// last epoch N"), and whether err is one that says so.
func StaleEpoch(err error) (int64, bool) {
	var ae *AgentError
	if !errors.As(err, &ae) || ae.Reason != sapb.ErrorReason_STALE_EPOCH.String() {
		return 0, false
	}
	m := lastEpochRE.FindStringSubmatch(ae.Msg)
	if m == nil {
		return 0, false
	}
	n, perr := strconv.ParseInt(m[1], 10, 64)
	return n, perr == nil
}

func trimEnum(name, prefix string) string {
	if strings.HasSuffix(name, "UNSPECIFIED") {
		return ""
	}
	return strings.TrimPrefix(name, prefix)
}

// LastEpoch is StaleEpoch as a method, so freeze.LastEpoch finds it without importing hostcmd.
func (e *AgentError) LastEpoch() (int64, bool) { return StaleEpoch(e) }
