// Package handshake is the guest kubelet's side of the Q6 contract with the snapshot agent
// (M4). It implements freeze.Backend with the agent's Suspend, Resume and Kill RPCs: every call
// carries an absolute deadline, is polled through GetOperation, and is re-issued with the same
// epoch after an agent restart. Anything else the agent answers (a refusal, a failed operation,
// a missed deadline, Unimplemented) is returned as an *Error, and the caller runs the kill
// sequence. The guest kubelet itself never touches cgroups or the GPU.
package handshake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/edwinhr716/guest-kubelet/api/snapshot_agent/v1alpha1"
)

// LabelJobID is the mirror label that holds the agent's job id (mirror.LabelJobID; repeated
// here so this package does not import the backend).
const LabelJobID = "timeslice.io/job-id"

// Options are the Q13 timings. The zero value of each field takes its default.
type Options struct {
	// Poll is the GetOperation interval while an operation is pending. Default 100 ms.
	Poll time.Duration
	// RetryInitial and RetryMax bound the exponential backoff between retries of a call that
	// did not reach the agent (Unavailable, a timed-out RPC) or whose operation the agent
	// lost in a restart. Defaults 1 s and 30 s. No retry outlives the call's deadline.
	RetryInitial time.Duration
	RetryMax     time.Duration
	// RPCTimeout bounds every single RPC. Default 5 s.
	RPCTimeout time.Duration
	// Grace is kept between the deadline sent to the agent and the caller's own deadline,
	// so that the agent's verdict (it fails any operation that finishes after its deadline)
	// is still read before the caller gives up. Default 1 s.
	Grace time.Duration
}

func (o Options) withDefaults() Options {
	if o.Poll <= 0 {
		o.Poll = 100 * time.Millisecond
	}
	if o.RetryInitial <= 0 {
		o.RetryInitial = time.Second
	}
	if o.RetryMax <= 0 {
		o.RetryMax = 30 * time.Second
	}
	if o.RPCTimeout <= 0 {
		o.RPCTimeout = 5 * time.Second
	}
	if o.Grace < 0 {
		o.Grace = 0
	} else if o.Grace == 0 {
		o.Grace = time.Second
	}
	return o
}

// Client calls one node's snapshot agent. It implements freeze.Backend.
type Client struct {
	api  pb.SnapshotAgentServiceClient
	opts Options
}

// New returns a client over an existing agent connection.
func New(api pb.SnapshotAgentServiceClient, opts Options) *Client {
	return &Client{api: api, opts: opts.withDefaults()}
}

// Dial connects to the agent at addr (host:port; the agent serves plaintext gRPC on the node).
// The connection is lazy: an agent that is down fails the calls, not Dial.
//
// Reconnects back off at most RPCTimeout apart (gRPC's default cap is 120 s), so that calls
// reach an agent that has just restarted instead of failing Unavailable until their deadline.
func Dial(addr string, opts Options) (*Client, *grpc.ClientConn, error) {
	opts = opts.withDefaults()
	bo := backoff.DefaultConfig
	bo.MaxDelay = opts.RPCTimeout
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bo, MinConnectTimeout: opts.RPCTimeout}))
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot agent %s: %w", addr, err)
	}
	return New(pb.NewSnapshotAgentServiceClient(conn), opts), conn, nil
}

// Kind classifies an *Error.
type Kind string

// Error kinds. All of them mean the same to the caller (run the kill sequence); they differ in
// what is logged and recorded.
const (
	// KindRefused: the agent refused the call (a gRPC error status).
	KindRefused Kind = "Refused"
	// KindFailed: the operation ended FAILED.
	KindFailed Kind = "Failed"
	// KindOutcome: the operation completed with an outcome the call does not accept, for
	// example RELEASED for a Suspend (the job had no process left).
	KindOutcome Kind = "UnexpectedOutcome"
	// KindDeadline: no verdict before the caller's deadline (the agent hangs, is down, or
	// the operation is still pending).
	KindDeadline Kind = "DeadlineMissed"
	// KindUnimplemented: the agent does not implement the call.
	KindUnimplemented Kind = "Unimplemented"
	// KindNotListed: the agent's Status does not list the job, so it is not suspended.
	KindNotListed Kind = "NotListed"
	// KindFaulted: the agent's Status reports the job FAULTED; only Kill is left.
	KindFaulted Kind = "Faulted"
	// KindInvalid: the call could not be made (no deadline, no job id).
	KindInvalid Kind = "Invalid"
)

// Error is every failure the client returns.
type Error struct {
	Op    string // Suspend, Resume, Kill or Status
	JobID string
	Kind  Kind
	// Reason is the agent's ErrorReason name when it gave one: from a refusal's
	// google.rpc.ErrorInfo detail (D-AGENT-2 errorinfo) or from the failed operation.
	Reason string
	Code   codes.Code // gRPC code of a refusal
	Msg    string
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "agent %s of job %s: %s", e.Op, e.JobID, e.Kind)
	if e.Reason != "" {
		b.WriteString(" (" + e.Reason + ")")
	}
	if e.Msg != "" {
		b.WriteString(": " + e.Msg)
	}
	return b.String()
}

// ReasonOf returns the agent's ErrorReason name carried by err, or "".
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// ErrorInfoDomain is the google.rpc.ErrorInfo domain of snapshot-agent refusals.
const ErrorInfoDomain = "snapshot-agent.llm-d-rl-time-slicing"

// StatusReason returns the ErrorReason name a refusal carries: the Reason of the status's
// google.rpc.ErrorInfo detail with domain ErrorInfoDomain (the agent's D-AGENT-2 errorinfo
// encoding), or "" when it has none. The status message is plain text and is never parsed.
func StatusReason(st *status.Status) string {
	if st == nil {
		return ""
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == ErrorInfoDomain {
			return info.GetReason()
		}
	}
	return ""
}

// JobID returns the mirror's agent job id.
func JobID(mirror *corev1.Pod) string { return mirror.Labels[LabelJobID] }

// Suspend asks the agent to checkpoint and freeze the mirror's job. It first requires the job
// in the agent's Status (Q6: suspend only guests the agent lists): the agent answers a Suspend
// of a job it does not know with RELEASED at once.
func (c *Client) Suspend(ctx context.Context, mirror *corev1.Pod, epoch int64) error {
	jobID, deadline, err := c.prepare(ctx, "Suspend", mirror)
	if err != nil {
		return err
	}
	if err := c.requireListed(ctx, jobID); err != nil {
		return err
	}
	op, err := c.run(ctx, "Suspend", jobID, func(rctx context.Context) (string, error) {
		r, err := c.api.Suspend(rctx, &pb.SuspendRequest{JobId: jobID, Epoch: epoch, Deadline: timestamppb.New(deadline)})
		return r.GetOperationId(), err
	})
	if err != nil {
		return err
	}
	if op.GetOutcome() != pb.Outcome_OUTCOME_SUSPENDED {
		return &Error{Op: "Suspend", JobID: jobID, Kind: KindOutcome, Msg: op.GetOutcome().String()}
	}
	return nil
}

// Resume asks the agent to restore and thaw the mirror's job. A completed Resume reports
// RESUMED, or no outcome when the agent runs with --report-resumed-outcome=false (D-AGENT-7);
// both are accepted.
func (c *Client) Resume(ctx context.Context, mirror *corev1.Pod, epoch int64) error {
	jobID, deadline, err := c.prepare(ctx, "Resume", mirror)
	if err != nil {
		return err
	}
	op, err := c.run(ctx, "Resume", jobID, func(rctx context.Context) (string, error) {
		r, err := c.api.Resume(rctx, &pb.ResumeRequest{JobId: jobID, Epoch: epoch, Deadline: timestamppb.New(deadline)})
		return r.GetOperationId(), err
	})
	if err != nil {
		return err
	}
	if o := op.GetOutcome(); o != pb.Outcome_OUTCOME_RESUMED && o != pb.Outcome_OUTCOME_UNSPECIFIED {
		return &Error{Op: "Resume", JobID: jobID, Kind: KindOutcome, Msg: o.String()}
	}
	return nil
}

// Kill asks the agent to kill every process of the mirror's job and returns nil once the kill
// is confirmed (OUTCOME_KILLED).
func (c *Client) Kill(ctx context.Context, mirror *corev1.Pod, reason string) error {
	jobID, deadline, err := c.prepare(ctx, "Kill", mirror)
	if err != nil {
		return err
	}
	op, err := c.run(ctx, "Kill", jobID, func(rctx context.Context) (string, error) {
		r, err := c.api.Kill(rctx, &pb.KillRequest{JobId: jobID, Deadline: timestamppb.New(deadline), Reason: reason})
		return r.GetOperationId(), err
	})
	if err != nil {
		return err
	}
	if op.GetOutcome() != pb.Outcome_OUTCOME_KILLED {
		return &Error{Op: "Kill", JobID: jobID, Kind: KindOutcome, Msg: op.GetOutcome().String()}
	}
	return nil
}

// prepare reads the job id and computes the deadline sent to the agent: the caller's deadline
// minus Grace.
func (c *Client) prepare(ctx context.Context, op string, mirror *corev1.Pod) (string, time.Time, error) {
	jobID := JobID(mirror)
	if jobID == "" {
		return "", time.Time{}, &Error{
			Op: op, Kind: KindInvalid,
			Msg: fmt.Sprintf("mirror %s/%s has no %s label", mirror.Namespace, mirror.Name, LabelJobID),
		}
	}
	callerDeadline, ok := ctx.Deadline()
	if !ok {
		return "", time.Time{}, &Error{Op: op, JobID: jobID, Kind: KindInvalid, Msg: "the call has no deadline"}
	}
	deadline := callerDeadline.Add(-c.opts.Grace)
	if !deadline.After(time.Now()) {
		return "", time.Time{}, &Error{
			Op: op, JobID: jobID, Kind: KindDeadline,
			Msg: fmt.Sprintf("the deadline %s leaves no time for the agent", callerDeadline.Format(time.RFC3339Nano)),
		}
	}
	return jobID, deadline, nil
}

// requireListed polls the agent's Status until it lists the job, retrying transient errors and
// a job not yet listed (the agent's pod watcher may lag a new mirror) until the deadline. A
// FAULTED job fails at once.
func (c *Client) requireListed(ctx context.Context, jobID string) error {
	logger := log.G(ctx).WithField("jobID", jobID)
	delay := c.opts.RetryInitial
	var last *Error
	for {
		rctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
		st, err := c.api.Status(rctx, &pb.StatusRequest{})
		cancel()
		switch {
		case err == nil:
			last = &Error{Op: "Status", JobID: jobID, Kind: KindNotListed, Msg: "the agent does not list the job"}
			for _, j := range st.GetJobStatuses() {
				if j.GetJobId() != jobID {
					continue
				}
				if j.GetState() == pb.JobState_JOB_STATE_FAULTED {
					return &Error{Op: "Status", JobID: jobID, Kind: KindFaulted, Msg: "the agent reports the job FAULTED"}
				}
				return nil
			}
		case transient(err):
			last = refusal("Status", jobID, err)
		default:
			return refusal("Status", jobID, err)
		}
		logger.WithField("retryIn", delay.String()).WithError(last).Warn("agent Status: retrying")
		if !sleep(ctx, delay) {
			return &Error{Op: "Status", JobID: jobID, Kind: KindDeadline, Msg: last.Error()}
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// run starts an operation with start and polls it until it completes, fails or the caller's
// deadline passes. A start that does not reach the agent, and an operation the agent no longer
// knows (GetOperation NotFound after an agent restart), are retried by calling start again:
// the agent returns the same operation for the same epoch and call, and after a restart it
// re-seeds the epoch from the mirror, so a retry is never a second operation.
func (c *Client) run(
	ctx context.Context, op, jobID string, start func(context.Context) (string, error),
) (*pb.GetOperationResponse, error) {
	logger := log.G(ctx).WithField("op", op).WithField("jobID", jobID)
	delay := c.opts.RetryInitial
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return nil, deadlineErr(op, jobID, last)
		}
		rctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
		opID, err := start(rctx)
		cancel()
		if err == nil {
			var lost bool
			var resp *pb.GetOperationResponse
			resp, lost, err = c.poll(ctx, op, jobID, opID)
			if !lost {
				return resp, err
			}
			logger.WithField("operationID", opID).Warn("the agent lost the operation (restart?); calling again with the same epoch")
			last = err
		} else {
			if !transient(err) {
				return nil, refusal(op, jobID, err)
			}
			last = err
		}
		logger.WithField("retryIn", delay.String()).WithError(last).Warn("agent call did not complete; retrying")
		if !sleep(ctx, delay) {
			return nil, deadlineErr(op, jobID, last)
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// poll reads the operation every Poll until it is no longer PENDING. lost reports that the
// agent no longer knows it, so the caller should start it again.
func (c *Client) poll(ctx context.Context, op, jobID, opID string) (*pb.GetOperationResponse, bool, error) {
	delay := c.opts.RetryInitial
	var last error
	for {
		rctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
		resp, err := c.api.GetOperation(rctx, &pb.GetOperationRequest{OperationId: opID})
		cancel()
		wait := c.opts.Poll
		switch {
		case err == nil:
			switch resp.GetStatus() {
			case pb.OperationStatus_OPERATION_STATUS_COMPLETE:
				return resp, false, nil
			case pb.OperationStatus_OPERATION_STATUS_FAILED:
				reason := ""
				if r := resp.GetErrorReason(); r != pb.ErrorReason_ERROR_REASON_UNSPECIFIED {
					reason = r.String()
				}
				return nil, false, &Error{Op: op, JobID: jobID, Kind: KindFailed, Reason: reason, Msg: resp.GetError()}
			}
			last, delay = nil, c.opts.RetryInitial
		case status.Code(err) == codes.NotFound:
			return nil, true, err
		case transient(err):
			last, wait = err, delay
			delay = min(2*delay, c.opts.RetryMax)
		default:
			return nil, false, refusal(op, jobID, err)
		}
		if !sleep(ctx, wait) {
			if last == nil {
				last = fmt.Errorf("operation %s still pending", opID)
			}
			return nil, false, deadlineErr(op, jobID, last)
		}
	}
}

// transient reports errors that say nothing about the job: the agent is unreachable, or one
// RPC timed out (a hung agent). They are retried until the deadline.
func transient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted:
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func refusal(op, jobID string, err error) *Error {
	st := status.Convert(err)
	kind := KindRefused
	if st.Code() == codes.Unimplemented {
		kind = KindUnimplemented
	}
	return &Error{Op: op, JobID: jobID, Kind: kind, Reason: StatusReason(st), Code: st.Code(), Msg: st.Message()}
}

func deadlineErr(op, jobID string, last error) *Error {
	e := &Error{Op: op, JobID: jobID, Kind: KindDeadline}
	if last != nil {
		e.Msg = "last error: " + last.Error()
	}
	return e
}

// sleep waits d, or less if ctx ends first; it reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Frozen reports whether the agent lists the mirror's job as suspended now (JOB_STATE_SUSPENDED).
// A job the agent does not list is not suspended. It is the host fact the guest kubelet's
// restart recovery (M5) trusts over the annotation on the mirror.
func (c *Client) Frozen(ctx context.Context, mirror *corev1.Pod) (bool, error) {
	jobID := JobID(mirror)
	if jobID == "" {
		return false, nil
	}
	rctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
	defer cancel()
	st, err := c.api.Status(rctx, &pb.StatusRequest{})
	if err != nil {
		return false, refusal("Status", jobID, err)
	}
	for _, j := range st.GetJobStatuses() {
		if j.GetJobId() == jobID {
			return j.GetState() == pb.JobState_JOB_STATE_SUSPENDED, nil
		}
	}
	return false, nil
}
