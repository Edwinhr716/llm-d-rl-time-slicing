package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

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

// AgentResult is a finished operation.
type AgentResult struct {
	// Complete: every target reached the wanted state (a per-job operation: it is COMPLETE).
	Complete bool
	Error    string
	// Outcome and Reason are the operation's own outcome (COMPLETE) and error reason (FAILED),
	// without their enum prefixes.
	Outcome string
	Reason  string
	// Targets by job id (host-level operations only).
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

// AgentClient is the gRPC client of the node's snapshot-agent. Timings are the Q13 defaults
// (pending lead decision): poll 100 ms, every RPC 5 s, retries from 1 s doubling to 30 s.
//
// A call that is lost (Unavailable, or no answer within RPCTimeout) is sent again with the
// same epoch until ctx ends: the agent answers a repeat with the same operation. An operation
// the agent no longer knows (GetOperation NotFound, the agent restarted) is started again the
// same way. A refusal is returned as an *AgentError and is never retried.
type AgentClient struct {
	conn *grpc.ClientConn
	api  sapb.SnapshotAgentServiceClient
	// Poll is the GetOperation period (100 ms).
	Poll time.Duration
	// RPCTimeout bounds each unary call (5 s).
	RPCTimeout time.Duration
	// RetryInitial and RetryMax are the first and the largest delay between two sends of a
	// lost call (1 s, 30 s).
	RetryInitial, RetryMax time.Duration
}

// DialAgent returns a client of the snapshot-agent at addr (host:port). opts are added to the
// dial options (the fault injector's interceptor).
func DialAgent(addr string, opts ...grpc.DialOption) (*AgentClient, error) {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(addr, opts...)
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
		RetryInitial: time.Second, RetryMax: 30 * time.Second,
	}
}

// Close closes the connection.
func (c *AgentClient) Close() error { return c.conn.Close() }

// SuspendAll implements HostAgent.
func (c *AgentClient) SuspendAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error) {
	res, err := c.run(ctx, "SuspendAll", func(cctx context.Context) (string, error) {
		resp, err := c.api.SuspendAll(cctx, &sapb.SuspendAllRequest{
			Role: mirror.RoleBackground, Epoch: epoch, Deadline: timestamppb.New(deadline),
		})
		return resp.GetOperationId(), err
	}, func(t *sapb.TargetResult) bool {
		return t.GetOutcome() == sapb.Outcome_OUTCOME_SUSPENDED || t.GetOutcome() == sapb.Outcome_OUTCOME_RELEASED
	})
	if err != nil {
		return nil, fmt.Errorf("SuspendAll: %w", err)
	}
	return res, nil
}

// ResumeAll implements HostAgent.
func (c *AgentClient) ResumeAll(ctx context.Context, epoch int64, deadline time.Time) (*AgentResult, error) {
	res, err := c.run(ctx, "ResumeAll", func(cctx context.Context) (string, error) {
		resp, err := c.api.ResumeAll(cctx, &sapb.ResumeAllRequest{
			Role: mirror.RoleBackground, Epoch: epoch, Deadline: timestamppb.New(deadline),
		})
		return resp.GetOperationId(), err
	}, func(t *sapb.TargetResult) bool {
		return t.GetOutcome() == sapb.Outcome_OUTCOME_RESUMED
	})
	if err != nil {
		return nil, fmt.Errorf("ResumeAll: %w", err)
	}
	return res, nil
}

// Kill implements HostAgent. The agent's deadline is ctx's, or 30 s from now.
func (c *AgentClient) Kill(ctx context.Context, jobID, reason string) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	return c.KillJob(ctx, jobID, deadline, reason)
}

// KillJob kills one job with an explicit agent deadline and returns once the agent confirmed
// it (outcome KILLED).
func (c *AgentClient) KillJob(ctx context.Context, jobID string, deadline time.Time, reason string) error {
	res, err := c.run(ctx, "Kill", func(cctx context.Context) (string, error) {
		resp, err := c.api.Kill(cctx, &sapb.KillRequest{JobId: jobID, Deadline: timestamppb.New(deadline), Reason: reason})
		return resp.GetOperationId(), err
	}, nil)
	if err != nil {
		return fmt.Errorf("kill %s: %w", jobID, err)
	}
	if !res.Complete {
		return fmt.Errorf("kill %s: %s", jobID, res.Error)
	}
	return nil
}

// SuspendJob suspends one job and returns the finished operation. A FAILED operation is an
// error.
func (c *AgentClient) SuspendJob(ctx context.Context, jobID string, epoch int64, deadline time.Time) (*AgentResult, error) {
	return c.jobOp(ctx, "Suspend", jobID, func(cctx context.Context) (string, error) {
		resp, err := c.api.Suspend(cctx, &sapb.SuspendRequest{JobId: jobID, Epoch: epoch, Deadline: timestamppb.New(deadline)})
		return resp.GetOperationId(), err
	})
}

// ResumeJob resumes one job the same way.
func (c *AgentClient) ResumeJob(ctx context.Context, jobID string, epoch int64, deadline time.Time) (*AgentResult, error) {
	return c.jobOp(ctx, "Resume", jobID, func(cctx context.Context) (string, error) {
		resp, err := c.api.Resume(cctx, &sapb.ResumeRequest{JobId: jobID, Epoch: epoch, Deadline: timestamppb.New(deadline)})
		return resp.GetOperationId(), err
	})
}

func (c *AgentClient) jobOp(
	ctx context.Context, call, jobID string, send func(context.Context) (string, error),
) (*AgentResult, error) {
	res, err := c.run(ctx, call, send, nil)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", call, jobID, err)
	}
	if !res.Complete {
		return res, fmt.Errorf("%s %s: %w", call, jobID, &AgentError{Code: codes.Unknown, Reason: res.Reason, Msg: res.Error})
	}
	return res, nil
}

// Status returns the agent's jobs. A lost call is retried until ctx ends.
func (c *AgentClient) Status(ctx context.Context) ([]*sapb.JobStatus, error) {
	var out []*sapb.JobStatus
	err := c.retry(ctx, "Status", func(cctx context.Context) error {
		resp, err := c.api.Status(cctx, &sapb.StatusRequest{})
		out = resp.GetJobStatuses()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("Status: %w", err)
	}
	return out, nil
}

// errOperationLost: GetOperation answered NotFound, so the agent lost the operation (it
// restarted). The call is sent again with the same epoch.
var errOperationLost = errors.New("agent does not know the operation")

// run starts an operation and waits for its end, starting it again while the agent loses it.
func (c *AgentClient) run(
	ctx context.Context, call string, send func(context.Context) (string, error), done func(*sapb.TargetResult) bool,
) (*AgentResult, error) {
	for {
		id, err := c.start(ctx, call, send)
		if err != nil {
			return nil, err
		}
		res, err := c.wait(ctx, id, done)
		if errors.Is(err, errOperationLost) {
			log.G(ctx).WithField("call", call).WithField("operation", id).
				Warn("snapshot agent lost the operation (restart?); sending the call again with the same epoch")
			continue
		}
		return res, err
	}
}

func (c *AgentClient) start(ctx context.Context, call string, send func(context.Context) (string, error)) (string, error) {
	var id string
	err := c.retry(ctx, call, func(cctx context.Context) error {
		var err error
		id, err = send(cctx)
		return err
	})
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", errors.New("agent returned no operation id")
	}
	return id, nil
}

// retry calls rpc until it succeeds, fails with a non-retryable error, or ctx ends. Each try
// gets RPCTimeout.
func (c *AgentClient) retry(ctx context.Context, call string, rpc func(context.Context) error) error {
	delay := c.RetryInitial
	for {
		cctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		err := rpc(cctx)
		cancel()
		if err == nil {
			return nil
		}
		err = decodeError(err)
		if !retryable(err) {
			return err
		}
		log.G(ctx).WithField("call", call).WithField("retryIn", delay.String()).WithError(err).
			Warn("snapshot agent call lost; sending it again")
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(2*delay, c.RetryMax)
	}
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
		if status.Code(err) == codes.NotFound {
			return nil, errOperationLost
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
		Outcome:  trimEnum(op.GetOutcome().String(), "OUTCOME_"),
		Targets:  map[string]TargetResult{},
	}
	if op.GetErrorReason() != sapb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		res.Reason = op.GetErrorReason().String()
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
