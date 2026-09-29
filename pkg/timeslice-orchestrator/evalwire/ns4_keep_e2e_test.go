//go:build evalwire

package evalwire_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// End-to-end tests for decision D-NS-4, option "keep": an in-process
// orchestrator (evalwire), the reference host (evalwire/host) and a fake
// snapshot agent on one node, driven through lend, notice, vacate and grant.

const (
	e2eGroup   = "g"
	e2eNode    = "node-1"
	e2eTrainer = "trainer"
	e2eGuest   = "guest-a"
	e2eNS      = "default"
)

// jobStateSuspended is snapshot-agent JOB_STATE_SUSPENDED (contract §3).
const jobStateSuspended = agentpb.JobState(6)

// recorder keeps the order of agent and host events across components.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(ev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) count(ev string) int {
	n := 0
	for _, e := range r.all() {
		if e == ev {
			n++
		}
	}
	return n
}

// fakeAgent is a snapshot agent whose Snapshot and Restore complete at once.
type fakeAgent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer

	rec    *recorder
	mu     sync.Mutex
	states map[string]agentpb.JobState
}

func (a *fakeAgent) set(job string, state agentpb.JobState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.states[job] = state
}

func (a *fakeAgent) Status(context.Context, *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for job, state := range a.states {
		resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: state})
	}
	return resp, nil
}

func (a *fakeAgent) Snapshot(_ context.Context, req *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_SAVED)
	a.rec.add("snapshot " + req.GetJobId())
	return &agentpb.SnapshotResponse{OperationId: "snap-" + req.GetJobId()}, nil
}

func (a *fakeAgent) Restore(_ context.Context, req *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_RUNNING)
	a.rec.add("restore " + req.GetJobId())
	return &agentpb.RestoreResponse{OperationId: "restore-" + req.GetJobId()}, nil
}

func (a *fakeAgent) GetOperation(context.Context, *agentpb.GetOperationRequest) (*agentpb.GetOperationResponse, error) {
	return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
}

// startAgent serves the fake agent on a free loopback port.
func startAgent(t *testing.T, rec *recorder) (*fakeAgent, int) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("agent listen: %v", err)
	}
	agent := &fakeAgent{rec: rec, states: map[string]agentpb.JobState{
		e2eTrainer: agentpb.JobState_JOB_STATE_RUNNING,
	}}
	srv := grpc.NewServer()
	agentpb.RegisterSnapshotAgentServiceServer(srv, agent)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("agent serve: %v", err)
		}
	}()
	t.Cleanup(srv.Stop)
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected agent address %v", lis.Addr())
	}
	return agent, tcpAddr.Port
}

// fakeExec is the host's Executor. The guest's mirror pod is created on its
// first Ready, as the VK starts mirrors only after the grant, and the agent
// reports the guest RUNNING or SUSPENDED as the host drives it.
type fakeExec struct {
	rec       *recorder
	agent     *fakeAgent
	clientset kubernetes.Interface
}

func (e *fakeExec) Guests() []string { return []string{e2eGuest} }

func (e *fakeExec) SetNotReady(_ context.Context, guest string) error {
	e.rec.add("notready " + guest)
	return nil
}

func (e *fakeExec) Suspend(_ context.Context, guest string, _ time.Time) error {
	e.agent.set(guest, jobStateSuspended)
	e.rec.add("suspend " + guest)
	return nil
}

func (e *fakeExec) Resume(_ context.Context, guest string, _ time.Time) error {
	e.agent.set(guest, agentpb.JobState_JOB_STATE_RUNNING)
	e.rec.add("resume " + guest)
	return nil
}

func (e *fakeExec) SetReady(ctx context.Context, guest string) error {
	e.agent.set(guest, agentpb.JobState_JOB_STATE_RUNNING)
	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mirror-" + guest,
			Namespace: e2eNS,
			Labels: map[string]string{
				"timeslice.io/group":  e2eGroup,
				"timeslice.io/job-id": guest,
				"timeslice.io/role":   "background",
			},
		},
		Spec:   corev1.PodSpec{NodeName: e2eNode, RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	_, err := e.clientset.CoreV1().Pods(e2eNS).Create(ctx, mirror, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	e.rec.add("ready " + guest)
	return nil
}

// proxy forwards TCP connections to the current orchestrator, so the host and
// the trainer keep one address across an orchestrator restart.
type proxy struct {
	lis    net.Listener
	mu     sync.Mutex
	target string
	conns  []net.Conn
}

func startProxy(t *testing.T) *proxy {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &proxy{lis: lis}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *proxy) addr() string { return p.lis.Addr().String() }

// retarget points new connections at addr and drops the open ones.
func (p *proxy) retarget(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = addr
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *proxy) close() {
	_ = p.lis.Close()
	p.retarget("")
}

func (p *proxy) serve() {
	for {
		src, err := p.lis.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		target := p.target
		p.mu.Unlock()
		dst, err := (&net.Dialer{Timeout: time.Second}).Dial("tcp", target)
		if err != nil {
			_ = src.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, src, dst)
		p.mu.Unlock()
		go func() {
			_, _ = io.Copy(dst, src)
			_ = dst.Close()
		}()
		go func() {
			_, _ = io.Copy(src, dst)
			_ = src.Close()
		}()
	}
}

// e2eLogs captures slog output for the duration of a test.
type e2eLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *e2eLogs) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(b)
}

func (l *e2eLogs) contains(parts ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		all := true
		for _, part := range parts {
			all = all && strings.Contains(line, part)
		}
		if all {
			return true
		}
	}
	return false
}

func captureLogs(t *testing.T) *e2eLogs {
	t.Helper()
	logs := &e2eLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// e2eEnv is one node with a trainer pod, a fake agent, an orchestrator behind
// a proxy and the reference host.
type e2eEnv struct {
	clientset *fake.Clientset
	rec       *recorder
	agent     *fakeAgent
	agentPort int
	proxy     *proxy
	orch      *evalwire.Orch
	trainer   pb.TimeSliceOrchestratorServiceClient
}

var e2eArgs = []string{
	"--background-role=true", "--min-bubble=30s", "--notice-window=30s", "--kill-budget=3s",
	"--controller-workers=4", "--resync-period=30s",
}

func newE2EEnv(t *testing.T, ctx context.Context) *e2eEnv {
	t.Helper()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   e2eNode,
			Labels: map[string]string{"group.timeslice.io/" + e2eGroup: "true"},
		},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: "127.0.0.1"},
		}},
	}
	trainerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trainer-0",
			Namespace: e2eNS,
			Labels:    map[string]string{"timeslice.io/group": e2eGroup, "timeslice.io/job-id": e2eTrainer},
		},
		Spec:   corev1.PodSpec{NodeName: e2eNode},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	env := &e2eEnv{clientset: fake.NewClientset(node, trainerPod), rec: &recorder{}}
	env.agent, env.agentPort = startAgent(t, env.rec)
	env.proxy = startProxy(t)
	env.startOrch(t, ctx)
	t.Cleanup(func() { env.orch.Stop() })

	conn, err := grpc.NewClient(env.proxy.addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial orchestrator: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	env.trainer = pb.NewTimeSliceOrchestratorServiceClient(conn)
	return env
}

func (e *e2eEnv) startOrch(t *testing.T, ctx context.Context) {
	t.Helper()
	orch, err := evalwire.Start(ctx, evalwire.Config{Clientset: e.clientset, AgentPort: e.agentPort, Args: e2eArgs})
	if err != nil {
		t.Fatalf("evalwire.Start: %v", err)
	}
	e.orch = orch
	e.proxy.retarget(orch.Addr)
}

func (e *e2eEnv) startHost(t *testing.T, ctx context.Context) {
	t.Helper()
	h, err := host.Start(ctx, host.Config{
		Node:     e2eNode,
		OrchAddr: e.proxy.addr(),
		Exec:     &fakeExec{rec: e.rec, agent: e.agent, clientset: e.clientset},
	})
	if err != nil {
		t.Fatalf("host.Start: %v", err)
	}
	t.Cleanup(h.Stop)
}

// acquire is the trainer's foreground Acquire, retried on errors (the
// orchestrator may still be starting or restarting).
func (e *e2eEnv) acquire(t *testing.T, ctx context.Context, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		actx, cancel := context.WithDeadline(ctx, deadline)
		resp, err := e.trainer.Acquire(actx, &pb.AcquireRequest{JobId: e2eTrainer, GroupId: e2eGroup})
		cancel()
		if err == nil && resp.GetSuccess() {
			e.rec.add("granted " + e2eTrainer)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trainer Acquire not granted within %v: %v (events %v)", timeout, err, e.rec.all())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (e *e2eEnv) yield(t *testing.T, ctx context.Context) {
	t.Helper()
	yctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := e.trainer.Yield(yctx, &pb.YieldRequest{
		JobId: e2eTrainer, GroupId: e2eGroup, ExpectedIdle: durationpb.New(60 * time.Second),
	}); err != nil {
		t.Fatalf("trainer Yield: %v", err)
	}
	e.rec.add("yielded " + e2eTrainer)
}

func (e *e2eEnv) waitEvent(t *testing.T, ev string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for e.rec.count(ev) < n {
		if time.Now().After(deadline) {
			t.Fatalf("event %q #%d not seen within %v (events %v)", ev, n, timeout, e.rec.all())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertOrder checks that want occurs in events in order (as a subsequence).
func assertOrder(t *testing.T, events []string, want ...string) {
	t.Helper()
	i := 0
	for _, ev := range events {
		if i < len(want) && ev == want[i] {
			i++
		}
	}
	if i < len(want) {
		t.Fatalf("events %v do not contain %v in order", events, want)
	}
}

// assertFailClosed checks that the trainer is never restored while the guest
// is up: between a guest Ready (or Resume) and the next trainer restore there
// is a guest Suspend.
func assertFailClosed(t *testing.T, events []string) {
	t.Helper()
	guestUp := false
	for i, ev := range events {
		switch ev {
		case "ready " + e2eGuest, "resume " + e2eGuest:
			guestUp = true
		case "suspend " + e2eGuest:
			guestUp = false
		case "restore " + e2eTrainer:
			if guestUp {
				t.Fatalf("trainer restored over a live guest at event %d: %v", i, events)
			}
		}
	}
}

// TestNS4_Keep_E2E_LendVacateGrantCycles runs two full cycles: the trainer
// yields with expected_idle, the host is granted and starts the guest, the
// trainer acquires, the host vacates (NotReady, Suspend, Yield) and the
// trainer is restored. The second cycle resumes the suspended guest.
func TestNS4_Keep_E2E_LendVacateGrantCycles(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newE2EEnv(t, ctx)

	env.acquire(t, ctx, 30*time.Second)
	env.startHost(t, ctx)
	time.Sleep(2 * time.Second) // the host finds the group and waits in Acquire

	for cycle := 1; cycle <= 2; cycle++ {
		env.yield(t, ctx)
		env.waitEvent(t, "ready "+e2eGuest, cycle, 20*time.Second)
		env.acquire(t, ctx, 25*time.Second)
		if got := env.rec.count("suspend " + e2eGuest); got != cycle {
			t.Fatalf("cycle %d: guest suspended %d times, want %d (events %v)", cycle, got, cycle, env.rec.all())
		}
	}

	events := env.rec.all()
	assertOrder(t, events,
		"granted "+e2eTrainer, "yielded "+e2eTrainer, "snapshot "+e2eTrainer, "ready "+e2eGuest,
		"notready "+e2eGuest, "suspend "+e2eGuest, "restore "+e2eTrainer, "granted "+e2eTrainer,
		"yielded "+e2eTrainer, "snapshot "+e2eTrainer, "resume "+e2eGuest, "ready "+e2eGuest,
		"notready "+e2eGuest, "suspend "+e2eGuest, "restore "+e2eTrainer, "granted "+e2eTrainer)
	assertFailClosed(t, events)

	for _, want := range [][]string{
		{`"msg":"Resume started"`, `"group":"g"`, `"node":"node-1"`},
		{`"msg":"Vacate started"`, `"group":"g"`, `"hosts":["node-1"]`, `"deadline"`},
		{`"msg":"Host clear"`, `"group":"g"`, `"node":"node-1"`, `"how"`},
		{`"msg":"Foreground granted"`, `"group":"g"`, `"job":"trainer"`, `"waited_ms"`},
	} {
		if !logs.contains(want...) {
			t.Errorf("missing log line with %v", want)
		}
	}
}

// TestNS4_Keep_E2E_RestartStaysFailClosed restarts the orchestrator while the
// host holds the grant and the guest runs. The new orchestrator knows nothing
// of the grant; the host's heartbeat (a claim) and the guest's mirror pod keep
// it fail closed, and the trainer's Acquire still gets the node back after
// the host vacates.
func TestNS4_Keep_E2E_RestartStaysFailClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newE2EEnv(t, ctx)

	env.acquire(t, ctx, 30*time.Second)
	env.startHost(t, ctx)
	time.Sleep(2 * time.Second)
	env.yield(t, ctx)
	env.waitEvent(t, "ready "+e2eGuest, 1, 20*time.Second)

	env.orch.Stop()
	env.rec.add("restarted")
	env.startOrch(t, ctx)

	env.acquire(t, ctx, 40*time.Second)
	events := env.rec.all()
	assertOrder(t, events, "ready "+e2eGuest, "restarted", "suspend "+e2eGuest, "granted "+e2eTrainer)
	assertFailClosed(t, events)

	// A further cycle works on the restarted orchestrator.
	env.yield(t, ctx)
	env.waitEvent(t, "ready "+e2eGuest, 2, 20*time.Second)
	env.acquire(t, ctx, 25*time.Second)
	assertFailClosed(t, env.rec.all())
	if got := env.rec.count("suspend " + e2eGuest); got != 2 {
		t.Fatalf("guest suspended %d times, want 2 (events %v)", got, env.rec.all())
	}
}
