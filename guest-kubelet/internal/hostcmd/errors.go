package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sapb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// AgentError is a refusal or a failed operation from the snapshot agent. Reason is the
// ErrorReason name, read from the start of the gRPC status message ("STALE_EPOCH: ...", pending
// lead decision D-AGENT-2, option prefix), or "" when the message has no known prefix.
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
	out := &AgentError{Code: st.Code(), Msg: st.Message()}
	if name, rest, found := strings.Cut(st.Message(), ": "); found {
		if v, known := sapb.ErrorReason_value[name]; known && v != 0 {
			out.Reason, out.Msg = name, rest
		}
	}
	return out
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
