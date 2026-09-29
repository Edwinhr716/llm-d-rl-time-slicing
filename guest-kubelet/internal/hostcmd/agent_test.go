package hostcmd_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	hcpb "github.com/edwinhr716/guest-kubelet/api/hostcommand/v1alpha1"
	sapb "github.com/edwinhr716/guest-kubelet/api/snapshotagent/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

// fakeAgent is a scripted HostAgent.
type fakeAgent struct {
	ev *events

	mu      sync.Mutex
	fail    map[string]string // job id -> error for the next call
	skip    map[string]bool   // job ids the agent does not list
	callErr error
}

func (a *fakeAgent) result(h *fakeHost) *hostcmd.AgentResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	res := &hostcmd.AgentResult{Complete: true, Targets: map[string]hostcmd.TargetResult{}}
	guests, err := h.Guests()
	if err != nil {
		return res
	}
	for _, g := range guests {
		if g.Mirror == nil {
			continue
		}
		job := g.Mirror.Labels[mirror.LabelJobID]
		if a.skip[job] {
			continue
		}
		if msg, ok := a.fail[job]; ok {
			res.Complete = false
			res.Targets[job] = hostcmd.TargetResult{Error: msg}
			delete(a.fail, job)
			continue
		}
		res.Targets[job] = hostcmd.TargetResult{Done: true}
	}
	return res
}

type agentRig struct {
	*rig
	agent *fakeAgent
}

func newAgentRig(t *testing.T) *agentRig {
	t.Helper()
	fa := &fakeAgent{fail: map[string]string{}, skip: map[string]bool{}}
	r := newRig(t, func(c *hostcmd.Config, r *rig) {
		fa.ev = r.ev
		c.Freezer, c.Agent = nil, &hostBoundAgent{fakeAgent: fa, host: r.host}
	})
	return &agentRig{rig: r, agent: fa}
}

// hostBoundAgent sees the fake host's mirrors, as the real agent sees the node's pods.
type hostBoundAgent struct {
	*fakeAgent
	host *fakeHost
}

func (a *hostBoundAgent) SuspendAll(_ context.Context, epoch int64, _ time.Time) (*hostcmd.AgentResult, error) {
	a.ev.add("SuspendAll %d", epoch)
	if a.callErr != nil {
		return nil, a.callErr
	}
	return a.result(a.host), nil
}

func (a *hostBoundAgent) ResumeAll(ctx context.Context, epoch int64, _ time.Time) (*hostcmd.AgentResult, error) {
	a.ev.add("ResumeAll %d", epoch)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.callErr != nil {
		return nil, a.callErr
	}
	return a.result(a.host), nil
}

func (a *hostBoundAgent) Kill(_ context.Context, job, _ string) error {
	a.ev.add("agent-kill %s", job)
	return nil
}

func TestAgent_VacateSuspendAllWithCommandEpoch(t *testing.T) {
	fix := newAgentRig(t)
	fix.host.addGuest("a")
	fix.host.addGuest("b")
	wantOutcome(t, fix.resume(t, 10), hcpb.Outcome_OUTCOME_RESUMED)
	if fix.ev.count("ResumeAll") != 0 {
		t.Fatal("ResumeAll called with nothing suspended")
	}
	ack := fix.vacate(t, 11, time.Second)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_VACATED)
	for _, g := range []string{"a", "b"} {
		before(t, fix.ev, "hold "+g+" "+mirror.ReasonSuspending, "confirm "+g)
		before(t, fix.ev, "confirm "+g, "annotate "+g+"-m "+mirror.AnnotationGuestEpoch+"=11")
		before(t, fix.ev, "annotate "+g+"-m "+mirror.AnnotationGuestEpoch+"=11", "SuspendAll 11")
		before(t, fix.ev, "SuspendAll 11", "hold "+g+" "+mirror.ReasonSuspended)
	}
	if n := fix.ev.count("SuspendAll"); n != 1 {
		t.Fatalf("SuspendAll calls = %d, want 1", n)
	}
	if fix.ev.count("epoch ") != 0 {
		t.Fatal("per-guest epoch counter used in agent mode")
	}

	ack = fix.resume(t, 12)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_RESUMED)
	before(t, fix.ev, "annotate a-m "+mirror.AnnotationGuestEpoch+"=12", "ResumeAll 12")
	before(t, fix.ev, "ResumeAll 12", "ready a")
	if fix.ev.count("create") != 2 {
		t.Fatalf("mirrors re-created after ResumeAll: %v", fix.ev.list())
	}
}

func TestAgent_KillFailedAndUnlistedTargets(t *testing.T) {
	r := newAgentRig(t)
	r.host.addGuest("a")
	r.host.addGuest("b")
	r.host.addGuest("c")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.agent.fail["a-job"] = "VERIFY_FAILED"
	r.agent.skip["b-job"] = true
	ack := r.vacate(t, 2, time.Second)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_VACATED)
	before(t, r.ev, "SuspendAll 2", "agent-kill a-job")
	before(t, r.ev, "agent-kill a-job", "kill a-m")
	before(t, r.ev, "SuspendAll 2", "agent-kill b-job")
	if r.ev.index("agent-kill c-job") >= 0 {
		t.Fatal("killed a suspended guest")
	}
}

func TestAgent_CallFailureKillsEveryGuest(t *testing.T) {
	r := newAgentRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.agent.callErr = errors.New("Unimplemented")
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	before(t, r.ev, "SuspendAll 2", "agent-kill a-job")
	before(t, r.ev, "kill a-m", "gone a-m")
}

func TestAgent_ResumeFailureKills(t *testing.T) {
	r := newAgentRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	r.agent.fail["a-job"] = "BACKEND_ERROR"
	ack := r.resume(t, 3)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_FAILED)
	before(t, r.ev, "ResumeAll 3", "agent-kill a-job")
	if r.ev.count("ready a") != 1 { // only from the first Resume
		t.Fatal("killed guest released Ready")
	}
	if r.ev.count("create a") != 1 {
		t.Fatal("killed guest got a new mirror in the same Resume")
	}
}

// ---- AgentClient over gRPC ----

type grpcAgent struct {
	sapb.UnimplementedSnapshotAgentServiceServer

	mu      sync.Mutex
	polls   int
	reqs    []string
	pending int // GetOperation answers PENDING this many times
}

func (g *grpcAgent) SuspendAll(_ context.Context, req *sapb.SuspendAllRequest) (*sapb.SuspendAllResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reqs = append(g.reqs, fmt.Sprintf("SuspendAll %s %d %t", req.GetRole(), req.GetEpoch(), req.GetDeadline().IsValid()))
	return &sapb.SuspendAllResponse{OperationId: "op-s"}, nil
}

func (g *grpcAgent) Kill(_ context.Context, req *sapb.KillRequest) (*sapb.KillResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reqs = append(g.reqs, "Kill "+req.GetJobId())
	return &sapb.KillResponse{OperationId: "op-k"}, nil
}

func (g *grpcAgent) GetOperation(_ context.Context, req *sapb.GetOperationRequest) (*sapb.GetOperationResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.polls++
	if g.polls <= g.pending {
		return &sapb.GetOperationResponse{Status: sapb.OperationStatus_OPERATION_STATUS_PENDING}, nil
	}
	if req.GetOperationId() == "op-k" {
		return &sapb.GetOperationResponse{
			Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: sapb.Outcome_OUTCOME_KILLED,
		}, nil
	}
	return &sapb.GetOperationResponse{
		Status: sapb.OperationStatus_OPERATION_STATUS_FAILED,
		Targets: []*sapb.TargetResult{
			{JobId: "j1", Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: sapb.Outcome_OUTCOME_SUSPENDED},
			{JobId: "j2", Status: sapb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: sapb.Outcome_OUTCOME_RELEASED},
			{JobId: "j3", Status: sapb.OperationStatus_OPERATION_STATUS_FAILED, ErrorReason: sapb.ErrorReason_VERIFY_FAILED},
		},
	}, nil
}

func dialFake(t *testing.T, srv sapb.SnapshotAgentServiceServer) *hostcmd.AgentClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	sapb.RegisterSnapshotAgentServiceServer(gs, srv)
	go func() {
		if err := gs.Serve(lis); err != nil {
			t.Log(err)
		}
	}()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := hostcmd.NewAgentClient(conn)
	c.Poll = 5 * time.Millisecond
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestAgentClient_SuspendAllPollsToTheEnd(t *testing.T) {
	fake := &grpcAgent{pending: 3}
	c := dialFake(t, fake)
	res, err := c.SuspendAll(context.Background(), 42, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	req, polls := fake.reqs[0], fake.polls
	fake.mu.Unlock()
	if req != "SuspendAll background 42 true" {
		t.Fatalf("request = %q", req)
	}
	if polls != 4 {
		t.Fatalf("polls = %d, want 4", polls)
	}
	if res.Complete {
		t.Fatal("FAILED operation reported complete")
	}
	if !res.Targets["j1"].Done || !res.Targets["j2"].Done || res.Targets["j3"].Done {
		t.Fatalf("targets = %+v", res.Targets)
	}
	if !strings.Contains(res.Targets["j3"].Error, "VERIFY_FAILED") {
		t.Fatalf("j3 error = %q", res.Targets["j3"].Error)
	}
}

func TestAgentClient_KillAndUnimplemented(t *testing.T) {
	fake := &grpcAgent{}
	c := dialFake(t, fake)
	if err := c.Kill(context.Background(), "j1", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResumeAll(context.Background(), 1, time.Now().Add(time.Second)); err == nil {
		t.Fatal("ResumeAll on an agent without it: want an error (the kill sequence follows)")
	}
}
