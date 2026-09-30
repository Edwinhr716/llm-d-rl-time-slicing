package hostcmd_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	sapb "github.com/edwinhr716/guest-kubelet/api/snapshotagent/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/freeze"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

// scriptedAgent is a snapshot-agent gRPC server. Each RPC first pops a scripted error for its
// name, if any; otherwise Suspend, Resume and Kill start operation "<rpc>-<n>" and GetOperation
// answers it with ops[<rpc>] (COMPLETE with the matching outcome by default).
type scriptedAgent struct {
	sapb.UnimplementedSnapshotAgentServiceServer

	mu   sync.Mutex
	reqs []string
	errs map[string][]error
	ops  map[string]*sapb.GetOperationResponse
	jobs []*sapb.JobStatus
	// lost makes GetOperation answer NotFound this many times.
	lost int
}

func newScripted() *scriptedAgent {
	return &scriptedAgent{errs: map[string][]error{}, ops: map[string]*sapb.GetOperationResponse{}}
}

func (s *scriptedAgent) pop(rpc string) error {
	q := s.errs[rpc]
	if len(q) == 0 {
		return nil
	}
	s.errs[rpc] = q[1:]
	return q[0]
}

func (s *scriptedAgent) start(rpc, detail string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, rpc+" "+detail)
	if err := s.pop(rpc); err != nil {
		return "", err
	}
	return rpc, nil
}

func (s *scriptedAgent) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reqs...)
}

func (s *scriptedAgent) Suspend(_ context.Context, r *sapb.SuspendRequest) (*sapb.SuspendResponse, error) {
	id, err := s.start("Suspend", fmt.Sprintf("%s %d", r.GetJobId(), r.GetEpoch()))
	return &sapb.SuspendResponse{OperationId: id}, err
}

func (s *scriptedAgent) Resume(_ context.Context, r *sapb.ResumeRequest) (*sapb.ResumeResponse, error) {
	id, err := s.start("Resume", fmt.Sprintf("%s %d", r.GetJobId(), r.GetEpoch()))
	return &sapb.ResumeResponse{OperationId: id}, err
}

func (s *scriptedAgent) Kill(_ context.Context, r *sapb.KillRequest) (*sapb.KillResponse, error) {
	id, err := s.start("Kill", r.GetJobId())
	return &sapb.KillResponse{OperationId: id}, err
}

func (s *scriptedAgent) SuspendAll(_ context.Context, r *sapb.SuspendAllRequest) (*sapb.SuspendAllResponse, error) {
	id, err := s.start("SuspendAll", fmt.Sprintf("%s %d %t", r.GetRole(), r.GetEpoch(), r.GetDeadline().IsValid()))
	return &sapb.SuspendAllResponse{OperationId: id}, err
}

func (s *scriptedAgent) Status(context.Context, *sapb.StatusRequest) (*sapb.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pop("Status"); err != nil {
		return nil, err
	}
	return &sapb.StatusResponse{JobStatuses: s.jobs}, nil
}

var defaultOutcome = map[string]sapb.Outcome{
	"Suspend": sapb.Outcome_OUTCOME_SUSPENDED, "Resume": sapb.Outcome_OUTCOME_RESUMED, "Kill": sapb.Outcome_OUTCOME_KILLED,
}

func (s *scriptedAgent) GetOperation(_ context.Context, r *sapb.GetOperationRequest) (*sapb.GetOperationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pop("GetOperation"); err != nil {
		return nil, err
	}
	if s.lost > 0 {
		s.lost--
		return nil, status.Error(codes.NotFound, "unknown operation")
	}
	if op, ok := s.ops[r.GetOperationId()]; ok {
		return op, nil
	}
	return &sapb.GetOperationResponse{
		Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: defaultOutcome[r.GetOperationId()],
	}, nil
}

func dialScripted(t *testing.T, srv sapb.SnapshotAgentServiceServer, opts ...grpc.DialOption) *hostcmd.AgentClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	sapb.RegisterSnapshotAgentServiceServer(gs, srv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		if err := <-serveErr; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	opts = append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	c := hostcmd.NewAgentClient(conn)
	c.Poll, c.RPCTimeout = 5*time.Millisecond, 200*time.Millisecond
	c.RetryInitial, c.RetryMax = 10*time.Millisecond, 40*time.Millisecond
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func wantRequests(t *testing.T, s *scriptedAgent, want ...string) {
	t.Helper()
	got := s.requests()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

func TestClient_SuspendAllPollsTargetsToTheEnd(t *testing.T) {
	s := newScripted()
	s.ops["SuspendAll"] = &sapb.GetOperationResponse{
		Status: sapb.OperationStatus_OPERATION_STATUS_FAILED,
		Targets: []*sapb.TargetResult{
			{JobId: "j1", Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: sapb.Outcome_OUTCOME_SUSPENDED},
			{JobId: "j2", Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: sapb.Outcome_OUTCOME_RELEASED},
			{JobId: "j3", Status: sapb.OperationStatus_OPERATION_STATUS_FAILED, ErrorReason: sapb.ErrorReason_VERIFY_FAILED},
		},
	}
	ab := &hostcmd.AgentBackend{Client: dialScripted(t, s)}
	res, err := ab.SuspendAll(context.Background(), "background", 42, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	wantRequests(t, s, "SuspendAll background 42 true")
	if res.Complete || !res.Targets["j1"].Done || !res.Targets["j2"].Done || res.Targets["j3"].Done {
		t.Fatalf("result = %+v", res)
	}
	if res.Targets["j2"].Outcome != freeze.OutcomeReleased {
		t.Fatalf("j2 outcome = %q", res.Targets["j2"].Outcome)
	}
}

func TestClient_LostCallIsSentAgainWithTheSameEpoch(t *testing.T) {
	s := newScripted()
	s.errs["Suspend"] = []error{status.Error(codes.Unavailable, "connection refused")}
	ab := &hostcmd.AgentBackend{Client: dialScripted(t, s)}
	out, err := ab.Suspend(context.Background(), "j1", 3, time.Now().Add(time.Second))
	if err != nil || out != freeze.OutcomeSuspended {
		t.Fatalf("outcome %q, err %v", out, err)
	}
	wantRequests(t, s, "Suspend j1 3", "Suspend j1 3")
}

func TestClient_LostOperationIsStartedAgain(t *testing.T) {
	s := newScripted()
	s.lost = 1
	ab := &hostcmd.AgentBackend{Client: dialScripted(t, s)}
	if err := ab.Resume(context.Background(), "j1", 4, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wantRequests(t, s, "Resume j1 4", "Resume j1 4")
}

// errorInfoStatus is a refusal as the agent sends it under D-AGENT-2, option errorinfo.
func errorInfoStatus(t *testing.T, code codes.Code, msg, reason, domain string) error {
	t.Helper()
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: domain})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
}

func TestClient_RefusalErrorInfoIsDecodedAndNotRetried(t *testing.T) {
	// Lead decision D-AGENT-2, option errorinfo: the reason is an ErrorInfo detail.
	s := newScripted()
	s.errs["Suspend"] = []error{errorInfoStatus(t, codes.FailedPrecondition,
		"epoch 3 is lower than the last epoch 7", "STALE_EPOCH", hostcmd.ErrorInfoDomain)}
	_, err := (&hostcmd.AgentBackend{Client: dialScripted(t, s)}).Suspend(context.Background(), "j1", 3, time.Now().Add(time.Second))
	if hostcmd.ReasonOf(err) != "STALE_EPOCH" {
		t.Fatalf("reason of %v = %q", err, hostcmd.ReasonOf(err))
	}
	if last, ok := freeze.LastEpoch(err); !ok || last != 7 {
		t.Fatalf("last epoch = %d %t", last, ok)
	}
	wantRequests(t, s, "Suspend j1 3")
}

func TestClient_RefusalReasonNeedsErrorInfo(t *testing.T) {
	for name, err := range map[string]error{
		"prefix only":  status.Error(codes.FailedPrecondition, "STALE_EPOCH: epoch 3 is lower than the last epoch 7"),
		"other domain": errorInfoStatus(t, codes.FailedPrecondition, "x", "STALE_EPOCH", "example.com"),
		"unspecified":  errorInfoStatus(t, codes.FailedPrecondition, "x", "ERROR_REASON_UNSPECIFIED", hostcmd.ErrorInfoDomain),
	} {
		s := newScripted()
		s.errs["Suspend"] = []error{err}
		ab := &hostcmd.AgentBackend{Client: dialScripted(t, s)}
		_, got := ab.Suspend(context.Background(), "j1", 3, time.Now().Add(time.Second))
		if r := hostcmd.ReasonOf(got); r != "" {
			t.Errorf("%s: reason of %v = %q, want none", name, got, r)
		}
		if _, ok := freeze.LastEpoch(got); ok {
			t.Errorf("%s: a status without the ErrorInfo detail is not a STALE_EPOCH refusal", name)
		}
	}
}

func TestClient_FailedOperationCarriesItsReason(t *testing.T) {
	s := newScripted()
	s.ops["Suspend"] = &sapb.GetOperationResponse{
		Status: sapb.OperationStatus_OPERATION_STATUS_FAILED, ErrorReason: sapb.ErrorReason_DEADLINE_EXCEEDED,
		Error: proto.String("checkpoint did not finish by the deadline"),
	}
	_, err := (&hostcmd.AgentBackend{Client: dialScripted(t, s)}).Suspend(context.Background(), "j1", 1, time.Now().Add(time.Second))
	if hostcmd.ReasonOf(err) != "DEADLINE_EXCEEDED" {
		t.Fatalf("reason of %v = %q", err, hostcmd.ReasonOf(err))
	}
}

func TestClient_UnimplementedIsErrUnimplemented(t *testing.T) {
	c := dialScripted(t, &sapb.UnimplementedSnapshotAgentServiceServer{})
	err := (&hostcmd.AgentBackend{Client: c}).Resume(context.Background(), "j1", 1, time.Now().Add(time.Second))
	if !errors.Is(err, freeze.ErrUnimplemented) {
		t.Fatalf("err = %v, want freeze.ErrUnimplemented", err)
	}
}

func TestClient_JobsTrimsStates(t *testing.T) {
	s := newScripted()
	s.jobs = []*sapb.JobStatus{
		{JobId: "j1", State: sapb.JobState_JOB_STATE_SUSPENDED, Epoch: 5},
		{JobId: "j2", State: sapb.JobState_JOB_STATE_RUNNING},
	}
	jobs, err := (&hostcmd.AgentBackend{Client: dialScripted(t, s)}).Jobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if jobs["j1"] != (freeze.Job{State: freeze.JobSuspended, Epoch: 5}) || jobs["j2"].State != "RUNNING" {
		t.Fatalf("jobs = %+v", jobs)
	}
}

func TestClient_KillReturnsOnceConfirmed(t *testing.T) {
	s := newScripted()
	ab := &hostcmd.AgentBackend{Client: dialScripted(t, s)}
	if err := ab.Kill(context.Background(), "j1", time.Now().Add(time.Second), "test"); err != nil {
		t.Fatal(err)
	}
	s.ops["Kill"] = &sapb.GetOperationResponse{
		Status: sapb.OperationStatus_OPERATION_STATUS_FAILED, ErrorReason: sapb.ErrorReason_KILL_UNCONFIRMED,
	}
	ab = &hostcmd.AgentBackend{Client: dialScripted(t, s)}
	if err := ab.Kill(context.Background(), "j1", time.Now().Add(time.Second), "test"); err == nil {
		t.Fatal("an unconfirmed kill must be an error")
	}
}

// ---- fault injection ----

func faultClient(t *testing.T, s *scriptedAgent) (*hostcmd.AgentBackend, *hostcmd.FaultInjector) {
	t.Helper()
	fi := hostcmd.NewFaultInjector()
	return &hostcmd.AgentBackend{Client: dialScripted(t, s, grpc.WithUnaryInterceptor(fi.Interceptor()))}, fi
}

func TestFault_HangEndsAtTheCallersDeadline(t *testing.T) {
	s := newScripted()
	a, fi := faultClient(t, s)
	if err := fi.Arm("Suspend", hostcmd.FaultHang, -1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := a.Suspend(ctx, "j1", 1, time.Now().Add(400*time.Millisecond)); err == nil {
		t.Fatal("a hung agent must be an error")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s", d)
	}
	if len(s.requests()) != 0 {
		t.Fatal("a hung call never reaches the agent")
	}
	if f := fi.List(); len(f) != 1 || f[0].Hits < 2 {
		t.Fatalf("faults = %+v, want the hang hit on every retry", f)
	}
}

func TestFault_CrashIsRetried(t *testing.T) {
	s := newScripted()
	a, fi := faultClient(t, s)
	if err := fi.Arm("Suspend", hostcmd.FaultCrash, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Suspend(context.Background(), "j1", 1, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wantRequests(t, s, "Suspend j1 1")
}

func TestFault_DropAckResendsTheSameCall(t *testing.T) {
	s := newScripted()
	a, fi := faultClient(t, s)
	if err := fi.Arm("Resume", hostcmd.FaultDropAck, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Resume(context.Background(), "j1", 2, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wantRequests(t, s, "Resume j1 2", "Resume j1 2")
}

func TestFault_PendingRunsOutTheDeadline(t *testing.T) {
	s := newScripted()
	a, fi := faultClient(t, s)
	if err := fi.Arm("GetOperation", hostcmd.FaultPending, -1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := a.Suspend(ctx, "j1", 1, time.Now().Add(100*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
}

func TestFault_UnimplementedAndRefuse(t *testing.T) {
	s := newScripted()
	a, fi := faultClient(t, s)
	if err := fi.Arm("Kill", hostcmd.FaultUnimplemented, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Kill(context.Background(), "j1", time.Now().Add(time.Second), "t"); !errors.Is(err, freeze.ErrUnimplemented) {
		t.Fatalf("err = %v", err)
	}
	if err := fi.Arm("Suspend", hostcmd.FaultRefuse, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Suspend(context.Background(), "j1", 1, time.Now().Add(time.Second)); hostcmd.ReasonOf(err) != "BACKEND_ERROR" {
		t.Fatalf("err = %v", err)
	}
	fi.Clear()
	if _, err := a.Suspend(context.Background(), "j1", 2, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("cleared: %v", err)
	}
}

func TestFault_ArmValidates(t *testing.T) {
	fi := hostcmd.NewFaultInjector()
	if err := fi.Arm("Suspend", "melt", 1); err == nil {
		t.Fatal("unknown kind accepted")
	}
	if err := fi.Arm("Suspend", hostcmd.FaultPending, 1); err == nil {
		t.Fatal("pending applies to GetOperation only")
	}
}
