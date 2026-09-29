package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	sapb "github.com/edwinhr716/guest-kubelet/api/snapshotagent/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// HostAgent is the snapshot-agent's host-level API (decision D-NS-5, option ns-host). The
// agent finds the node's background jobs itself when the call arrives, so the VK hands it one
// command per host instead of a guest list that can go stale. The VK passes the host command
// epoch straight through.
type HostAgent interface {
	// SuspendAll suspends every background job on the node and returns once the agent's
	// operation is finished (or ctx ends).
	SuspendAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error)
	// ResumeAll resumes them the same way.
	ResumeAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error)
	// Kill kills one job and returns once the agent confirmed it (or ctx ends).
	Kill(ctx context.Context, jobID, reason string) error
}

// AgentResult is a finished host-level operation.
type AgentResult struct {
	// Complete: every target reached the wanted state.
	Complete bool
	Error    string
	// Targets by job id.
	Targets map[string]TargetResult
}

// TargetResult is one job of a host-level operation.
type TargetResult struct {
	// Done: the job reached the wanted state (SUSPENDED or RELEASED for SuspendAll, RESUMED
	// for ResumeAll).
	Done    bool
	Outcome string
	Error   string
}

// AgentClient is the gRPC HostAgent.
type AgentClient struct {
	conn *grpc.ClientConn
	api  sapb.SnapshotAgentServiceClient
	// Poll is the GetOperation period (100 ms).
	Poll time.Duration
	// RPCTimeout bounds each unary call (5 s).
	RPCTimeout time.Duration
}

// DialAgent returns a client of the snapshot-agent at addr (host:port).
func DialAgent(addr string) (*AgentClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial snapshot-agent %s: %w", addr, err)
	}
	return NewAgentClient(conn), nil
}

// NewAgentClient returns a client on conn. Close closes conn.
func NewAgentClient(conn *grpc.ClientConn) *AgentClient {
	return &AgentClient{
		conn: conn, api: sapb.NewSnapshotAgentServiceClient(conn),
		Poll: 100 * time.Millisecond, RPCTimeout: 5 * time.Second,
	}
}

// Close closes the connection.
func (c *AgentClient) Close() error { return c.conn.Close() }

// SuspendAll implements HostAgent.
func (c *AgentClient) SuspendAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error) {
	id, err := c.start(ctx, func(cctx context.Context) (string, error) {
		resp, err := c.api.SuspendAll(cctx, &sapb.SuspendAllRequest{
			Role: mirror.RoleBackground, Epoch: epoch, Deadline: timestamppb.New(deadline),
		})
		return resp.GetOperationId(), err
	})
	if err != nil {
		return nil, fmt.Errorf("SuspendAll: %w", err)
	}
	return c.wait(ctx, id, func(t *sapb.TargetResult) bool {
		return t.GetOutcome() == sapb.Outcome_OUTCOME_SUSPENDED || t.GetOutcome() == sapb.Outcome_OUTCOME_RELEASED
	})
}

// ResumeAll implements HostAgent.
func (c *AgentClient) ResumeAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error) {
	id, err := c.start(ctx, func(cctx context.Context) (string, error) {
		resp, err := c.api.ResumeAll(cctx, &sapb.ResumeAllRequest{
			Role: mirror.RoleBackground, Epoch: epoch, Deadline: timestamppb.New(deadline),
		})
		return resp.GetOperationId(), err
	})
	if err != nil {
		return nil, fmt.Errorf("ResumeAll: %w", err)
	}
	return c.wait(ctx, id, func(t *sapb.TargetResult) bool {
		return t.GetOutcome() == sapb.Outcome_OUTCOME_RESUMED
	})
}

// Kill implements HostAgent. The agent's deadline is ctx's, or 30 s from now.
func (c *AgentClient) Kill(ctx context.Context, jobID, reason string) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	id, err := c.start(ctx, func(cctx context.Context) (string, error) {
		resp, err := c.api.Kill(cctx, &sapb.KillRequest{JobId: jobID, Deadline: timestamppb.New(deadline), Reason: reason})
		return resp.GetOperationId(), err
	})
	if err != nil {
		return fmt.Errorf("kill %s: %w", jobID, err)
	}
	res, err := c.wait(ctx, id, nil)
	if err != nil {
		return fmt.Errorf("kill %s: %w", jobID, err)
	}
	if !res.Complete {
		return fmt.Errorf("kill %s: %s", jobID, res.Error)
	}
	return nil
}

func (c *AgentClient) start(ctx context.Context, call func(context.Context) (string, error)) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
	defer cancel()
	id, err := call(cctx)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", errors.New("agent returned no operation id")
	}
	return id, nil
}

// wait polls GetOperation until the operation is no longer PENDING. done says whether a
// target reached the wanted state; nil means only the operation's status counts.
func (c *AgentClient) wait(ctx context.Context, id string, done func(*sapb.TargetResult) bool) (*AgentResult, error) {
	tick := time.NewTicker(c.Poll)
	defer tick.Stop()
	for {
		cctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		op, err := c.api.GetOperation(cctx, &sapb.GetOperationRequest{OperationId: id})
		cancel()
		if err == nil && op.GetStatus() != sapb.OperationStatus_OPERATION_STATUS_PENDING {
			return toResult(op, done), nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return nil, fmt.Errorf("operation %s: %w (last poll: %w)", id, ctx.Err(), err)
			}
			return nil, fmt.Errorf("operation %s still pending: %w", id, ctx.Err())
		case <-tick.C:
		}
	}
}

func toResult(op *sapb.GetOperationResponse, done func(*sapb.TargetResult) bool) *AgentResult {
	res := &AgentResult{
		Complete: op.GetStatus() == sapb.OperationStatus_OPERATION_STATUS_COMPLETE,
		Error:    op.GetError(),
		Targets:  map[string]TargetResult{},
	}
	if res.Error == "" && !res.Complete {
		res.Error = fmt.Sprintf("operation %s (%s)", op.GetStatus(), op.GetErrorReason())
	}
	for _, t := range op.GetTargets() {
		ok := t.GetStatus() == sapb.OperationStatus_OPERATION_STATUS_COMPLETE && (done == nil || done(t))
		msg := t.GetError()
		if !ok && msg == "" {
			msg = fmt.Sprintf("%s %s %s", t.GetStatus(), t.GetOutcome(), t.GetErrorReason())
		}
		res.Targets[t.GetJobId()] = TargetResult{Done: ok, Outcome: t.GetOutcome().String(), Error: msg}
	}
	return res
}

// jobID is the mirror's agent job id.
func jobID(m *corev1.Pod) string { return m.Labels[mirror.LabelJobID] }
