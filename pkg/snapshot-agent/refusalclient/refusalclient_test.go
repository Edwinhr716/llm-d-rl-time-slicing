package refusalclient_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/refusalclient"
)

// refusingHealth answers every Check with the status in err.
type refusingHealth struct {
	grpc_health_v1.UnimplementedHealthServer
	err error
}

func (h *refusingHealth) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return nil, h.err
}

// callRefused returns the error a real gRPC client receives when the server
// returns serverErr.
func callRefused(t *testing.T, serverErr error) error {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, &refusingHealth{err: serverErr})
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	defer func() {
		srv.Stop()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected an error from the server, got nil")
	}
	return err
}

// localRefusalError stands in for the agent's own refusal error, which this
// package cannot import.
type localRefusalError struct{ st *status.Status }

func (e *localRefusalError) Error() string { return e.st.Message() }

func (e *localRefusalError) GRPCStatus() *status.Status { return e.st }

// withDetails returns a status with code FailedPrecondition, message msg and
// the given details.
func withDetails(t *testing.T, msg string, details ...protoadapt.MessageV1) *status.Status {
	t.Helper()
	st := status.New(codes.FailedPrecondition, msg)
	if len(details) == 0 {
		return st
	}
	withInfo, err := st.WithDetails(details...)
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return withInfo
}

func info(reason, domain string) *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{Reason: reason, Domain: domain}
}

func errorInfoCases(t *testing.T) []struct {
	name string
	st   *status.Status
	want string
} {
	t.Helper()
	return []struct {
		name string
		st   *status.Status
		want string
	}{
		{"stale epoch", withDetails(t, "epoch 3 is lower than 5", info("STALE_EPOCH", refusalclient.Domain)), "STALE_EPOCH"},
		{"kill unconfirmed", withDetails(t, "a: b: c", info("KILL_UNCONFIRMED", refusalclient.Domain)), "KILL_UNCONFIRMED"},
		{"empty message", withDetails(t, "", info("VERIFY_FAILED", refusalclient.Domain)), "VERIFY_FAILED"},
		{
			"message names another reason",
			withDetails(t, "STALE_EPOCH: x", info("BACKEND_ERROR", refusalclient.Domain)),
			"BACKEND_ERROR",
		},
		{
			"after another detail",
			withDetails(t, "x", &errdetails.RetryInfo{}, info("DEADLINE_INFEASIBLE", refusalclient.Domain)),
			"DEADLINE_INFEASIBLE",
		},
		{"no detail", withDetails(t, "job_id is required"), ""},
		{"no detail, message starts with a name", withDetails(t, "STALE_EPOCH: x"), ""},
		{"other domain", withDetails(t, "x", info("STALE_EPOCH", "example.com")), ""},
	}
}

func TestRefusalWire_ErrorInfo_OverGRPC(t *testing.T) {
	for _, tc := range errorInfoCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			err := callRefused(t, tc.st.Err())
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("code: got %s, want %s", got, codes.FailedPrecondition)
			}
			if got := refusalclient.Reason(err); got != tc.want {
				t.Errorf("direct: got %q, want %q (%v)", got, tc.want, err)
			}
			wrapped := fmt.Errorf("suspend guest-1: %w", err)
			if got := refusalclient.Reason(wrapped); got != tc.want {
				t.Errorf("wrapped: got %q, want %q (%v)", got, tc.want, wrapped)
			}
			twice := fmt.Errorf("vacate: %w", wrapped)
			if got := refusalclient.Reason(twice); got != tc.want {
				t.Errorf("wrapped twice: got %q, want %q (%v)", got, tc.want, twice)
			}
		})
	}
}

func TestRefusalWire_ErrorInfo_LocalError(t *testing.T) {
	for _, tc := range errorInfoCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			err := &localRefusalError{st: tc.st}
			if got := refusalclient.Reason(err); got != tc.want {
				t.Errorf("direct: got %q, want %q", got, tc.want)
			}
			if got := refusalclient.Reason(fmt.Errorf("kill: %w", err)); got != tc.want {
				t.Errorf("wrapped: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRefusalWire_ErrorInfo_NoStatus(t *testing.T) {
	stale := withDetails(t, "y", info("STALE_EPOCH", refusalclient.Domain))
	cases := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"plain error", errors.New("STALE_EPOCH: not a gRPC error")},
		{"wrapped plain", fmt.Errorf("x: %w", errors.New("STALE_EPOCH: y"))},
		{"nil status", &localRefusalError{st: nil}},
		// Flattened on purpose: %v drops the gRPC status from the chain.
		{"flattened with %v", fmt.Errorf("x: %v", stale.Err())}, //nolint:errorlint // see above
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := refusalclient.Reason(tc.err); got != "" {
				t.Errorf("got %q, want \"\"", got)
			}
		})
	}
}
