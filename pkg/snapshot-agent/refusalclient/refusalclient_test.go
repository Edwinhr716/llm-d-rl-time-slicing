package refusalclient_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

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

var prefixCases = []struct {
	name string
	msg  string
	want string
}{
	{"stale epoch", "STALE_EPOCH: epoch 3 is lower than 5", "STALE_EPOCH"},
	{"deadline infeasible", "DEADLINE_INFEASIBLE: deadline has passed", "DEADLINE_INFEASIBLE"},
	{"deadline exceeded", "DEADLINE_EXCEEDED: x", "DEADLINE_EXCEEDED"},
	{"kill unconfirmed", "KILL_UNCONFIRMED: a: b: c", "KILL_UNCONFIRMED"},
	{"empty msg after prefix", "VERIFY_FAILED: ", "VERIFY_FAILED"},
	{"no reason", "job_id is required", ""},
	{"empty message", "", ""},
	{"lower case name", "stale_epoch: x", ""},
	{"name without separator", "STALE_EPOCH", ""},
	{"unknown name", "SOMETHING_ELSE: x", ""},
	{"name not at start", "call refused: STALE_EPOCH: x", ""},
}

func TestRefusalWire_Prefix_OverGRPC(t *testing.T) {
	for _, tc := range prefixCases {
		t.Run(tc.name, func(t *testing.T) {
			err := callRefused(t, status.Error(codes.FailedPrecondition, tc.msg))
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

func TestRefusalWire_Prefix_LocalError(t *testing.T) {
	for _, tc := range prefixCases {
		t.Run(tc.name, func(t *testing.T) {
			err := &localRefusalError{st: status.New(codes.Aborted, tc.msg)}
			if got := refusalclient.Reason(err); got != tc.want {
				t.Errorf("direct: got %q, want %q", got, tc.want)
			}
			if got := refusalclient.Reason(fmt.Errorf("kill: %w", err)); got != tc.want {
				t.Errorf("wrapped: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRefusalWire_Prefix_NoStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"plain error", errors.New("STALE_EPOCH: not a gRPC error")},
		{"wrapped plain", fmt.Errorf("x: %w", errors.New("STALE_EPOCH: y"))},
		{"nil status", &localRefusalError{st: nil}},
		// Flattened on purpose: %v drops the gRPC status from the chain.
		{"flattened with %v", fmt.Errorf("x: %v", status.Error(codes.Aborted, "STALE_EPOCH: y"))}, //nolint:errorlint // see above
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := refusalclient.Reason(tc.err); got != "" {
				t.Errorf("got %q, want \"\"", got)
			}
		})
	}
}
