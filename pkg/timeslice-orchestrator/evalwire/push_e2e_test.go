//go:build evalwire

package evalwire_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	agentPort = 19001
	hostPort  = 19101
	groupID   = "g"
	trainer   = "trainer"
)

// timeline records what happened, in order, across agents, hosts and the
// test driver.
type timeline struct {
	mu     sync.Mutex
	events []string
}

func (tl *timeline) add(format string, args ...any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.events = append(tl.events, fmt.Sprintf(format, args...))
}

func (tl *timeline) list() []string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return append([]string(nil), tl.events...)
}

// index returns the position of the first event with prefix, or -1.
func index(events []string, prefix string) int {
	for i, e := range events {
		if strings.HasPrefix(e, prefix) {
			return i
		}
	}
	return -1
}

// lastIndex returns the position of the last event with prefix, or -1.
func lastIndex(events []string, prefix string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if strings.HasPrefix(events[i], prefix) {
			return i
		}
	}
	return -1
}

func count(events []string, prefix string) int {
	n := 0
	for _, e := range events {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

// fakeAgent is the snapshot agent of one node for the foreground job:
// operations complete at once.
type fakeAgent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer
	node string
	tl   *timeline
	mu   sync.Mutex
	jobs map[string]agentpb.JobState
}

func (a *fakeAgent) set(job string, st agentpb.JobState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.jobs[job] = st
}

func (a *fakeAgent) Status(context.Context, *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for job, st := range a.jobs {
		resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: st})
	}
	return resp, nil
}

func (a *fakeAgent) Snapshot(_ context.Context, req *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_SAVED)
	a.tl.add("snapshot %s %s", req.GetJobId(), a.node)
	return &agentpb.SnapshotResponse{OperationId: "s-" + a.node}, nil
}

func (a *fakeAgent) Restore(_ context.Context, req *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_RUNNING)
	a.tl.add("restore %s %s", req.GetJobId(), a.node)
	return &agentpb.RestoreResponse{OperationId: "r-" + a.node}, nil
}

func (a *fakeAgent) GetOperation(context.Context, *agentpb.GetOperationRequest) (*agentpb.GetOperationResponse, error) {
	return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
}

// guestExec is the Executor of one node: one guest, scripted suspend time.
type guestExec struct {
	node string
	tl   *timeline
	mu   sync.Mutex
	slow time.Duration
}

func (e *guestExec) setSlow(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.slow = d
}

func (e *guestExec) Guests() []string { return []string{"guest-" + e.node} }

func (e *guestExec) SetNotReady(_ context.Context, g string) error {
	e.tl.add("notready %s", g)
	return nil
}

func (e *guestExec) Suspend(ctx context.Context, g string, _ time.Time) error {
	e.mu.Lock()
	slow := e.slow
	e.mu.Unlock()
	e.tl.add("suspend-start %s", g)
	select {
	case <-time.After(slow):
	case <-ctx.Done():
		return ctx.Err()
	}
	e.tl.add("suspend-done %s", g)
	return nil
}

func (e *guestExec) Resume(_ context.Context, g string, _ time.Time) error {
	e.tl.add("resume %s", g)
	return nil
}

func (e *guestExec) SetReady(_ context.Context, g string) error {
	e.tl.add("ready %s", g)
	return nil
}

// logRecorder keeps the messages of every log record.
type logRecorder struct {
	mu   sync.Mutex
	msgs map[string]int
}

func (r *logRecorder) Enabled(_ context.Context, l slog.Level) bool {
	return l >= slog.LevelInfo
}

//nolint:gocritic // slog.Handler.Handle signature requires passing Record by value
func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs[rec.Message]++
	return nil
}
func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

func (r *logRecorder) seen(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.msgs[msg]
}

type env struct {
	t         *testing.T
	ctx       context.Context
	tl        *timeline
	clientset *fake.Clientset
	agents    []*fakeAgent
	execs     []*guestExec
	nodes     []string
	logs      *logRecorder
	args      []string
}

func newEnv(t *testing.T, ctx context.Context, hosts int) *env {
	t.Helper()
	logs := &logRecorder{msgs: map[string]int{}}
	prev := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(prev) })

	e := &env{t: t, ctx: ctx, tl: &timeline{}, clientset: fake.NewClientset(), logs: logs}
	e.args = []string{
		"--background-role=true", "--min-bubble=30s", "--notice-window=6s", "--kill-budget=1s",
		"--controller-workers=2", "--resync-period=30s", "--foreground-op-timeout=60s",
	}
	for i := range hosts {
		ip := "127.0.0." + strconv.Itoa(i+2)
		e.nodes = append(e.nodes, ip)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: ip, Labels: map[string]string{"group.timeslice.io/" + groupID: "true"}},
			Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}},
		}
		if _, err := e.clientset.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node: %v", err)
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "trainer-" + strconv.Itoa(i), Namespace: "default",
			Labels: map[string]string{"timeslice.io/group": groupID, "timeslice.io/job-id": trainer},
		}, Spec: corev1.PodSpec{NodeName: ip}}
		if _, err := e.clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}

		agent := &fakeAgent{node: ip, tl: e.tl, jobs: map[string]agentpb.JobState{trainer: agentpb.JobState_JOB_STATE_SAVED}}
		e.agents = append(e.agents, agent)
		var lc net.ListenConfig
		lis, err := lc.Listen(ctx, "tcp", net.JoinHostPort(ip, strconv.Itoa(agentPort)))
		if err != nil {
			t.Fatalf("agent listen: %v", err)
		}
		srv := grpc.NewServer()
		agentpb.RegisterSnapshotAgentServiceServer(srv, agent)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)

		exec := &guestExec{node: ip, tl: e.tl}
		e.execs = append(e.execs, exec)
		h, err := host.Start(ctx, host.Config{
			Node: ip, ListenAddr: net.JoinHostPort(ip, strconv.Itoa(hostPort)), Exec: exec,
		})
		if err != nil {
			t.Fatalf("host start: %v", err)
		}
		t.Cleanup(h.Stop)
	}
	return e
}

func (e *env) start() (*evalwire.Orch, pb.TimeSliceOrchestratorServiceClient) {
	e.t.Helper()
	orch, err := evalwire.Start(e.ctx, evalwire.Config{
		Clientset: e.clientset, AgentPort: agentPort, HostPort: hostPort, Args: e.args,
	})
	if err != nil {
		e.t.Fatalf("evalwire.Start: %v", err)
	}
	conn, err := grpc.NewClient(orch.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatalf("dial orchestrator: %v", err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	cl := pb.NewTimeSliceOrchestratorServiceClient(conn)
	// The group exists once the first reconcile observed the labelled nodes.
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := cl.GetGroupStatus(e.ctx, &pb.GetGroupStatusRequest{GroupId: groupID})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("group never known to the orchestrator: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return orch, cl
}

func (e *env) acquire(cl pb.TimeSliceOrchestratorServiceClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(e.ctx, timeout)
	defer cancel()
	resp, err := cl.Acquire(ctx, &pb.AcquireRequest{GroupId: groupID, JobId: trainer})
	if err != nil {
		return err
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("acquire not successful: %v", resp)
	}
	e.tl.add("granted")
	return nil
}

func (e *env) yieldLend(cl pb.TimeSliceOrchestratorServiceClient) {
	e.t.Helper()
	_, err := cl.Yield(e.ctx, &pb.YieldRequest{GroupId: groupID, JobId: trainer, ExpectedIdle: durationpb.New(time.Minute)})
	if err != nil {
		e.t.Fatalf("Yield: %v", err)
	}
	e.tl.add("yielded")
}

func (e *env) waitFor(what string, timeout time.Duration, cond func([]string) bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond(e.tl.list()) {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s; timeline %v", what, e.tl.list())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// checkVacateBeforeGrant checks, over events after from, that every guest
// went NotReady then finished Suspend before any trainer restore and before
// the grant.
func (e *env) checkVacateBeforeGrant(events []string, from int) {
	e.t.Helper()
	tail := events[from:]
	granted := index(tail, "granted")
	if granted < 0 {
		e.t.Fatalf("no grant after %d; timeline %v", from, events)
	}
	firstRestore := index(tail, "restore "+trainer)
	for _, n := range e.nodes {
		g := "guest-" + n
		nr, done := index(tail, "notready "+g), lastIndex(tail, "suspend-done "+g)
		if nr < 0 || done < 0 || nr > index(tail, "suspend-start "+g) {
			e.t.Fatalf("guest %s not NotReady-then-Suspend; timeline %v", g, events)
		}
		if done > granted || (firstRestore >= 0 && done > firstRestore) {
			e.t.Fatalf("guest %s still suspending at restore/grant; timeline %v", g, events)
		}
	}
}

// TestNS4_Push_EndToEnd drives the in-process orchestrator with the
// reference hosts: grant only after every host acked, lend (trainer saved
// before guests resume), vacate again, and a restart in the middle of a
// notice.
func TestNS4_Push_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newEnv(t, ctx, 2)
	orch, cl := e.start()

	// 1. First Acquire (trainer saved): host state unknown, so the hosts
	// vacate before the trainer is restored.
	if err := e.acquire(cl, 30*time.Second); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	e.checkVacateBeforeGrant(e.tl.list(), 0)

	// 2. Lending Yield: the trainer is saved on every node, then the guests
	// resume and become ready.
	e.yieldLend(cl)
	e.waitFor("guests ready", 20*time.Second, func(ev []string) bool { return count(ev, "ready guest-") == len(e.nodes) })
	ev := e.tl.list()
	lastSnapshot := lastIndex(ev, "snapshot "+trainer)
	firstResume := index(ev, "resume guest-")
	if count(ev, "snapshot "+trainer) != len(e.nodes) || lastSnapshot > firstResume {
		t.Fatalf("trainer not saved on every node before the guests resumed; timeline %v", ev)
	}

	// 3. Acquire again: vacate, then restore, then grant.
	mark := len(e.tl.list())
	if err := e.acquire(cl, 30*time.Second); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	ev = e.tl.list()
	e.checkVacateBeforeGrant(ev, mark)
	if count(ev[mark:], "restore "+trainer) != len(e.nodes) {
		t.Fatalf("trainer not restored on every node; timeline %v", ev)
	}

	// 4. Lend again, then restart the orchestrator while the hosts are
	// suspending (slow suspend).
	e.yieldLend(cl)
	e.waitFor("guests ready again", 20*time.Second, func(ev []string) bool {
		return count(ev, "ready guest-") == 2*len(e.nodes)
	})
	for _, x := range e.execs {
		x.setSlow(3 * time.Second)
	}
	mark = len(e.tl.list())
	firstCall := make(chan error, 1)
	go func() { firstCall <- e.acquire(cl, 60*time.Second) }()
	e.waitFor("suspend started", 20*time.Second, func(ev []string) bool {
		return count(ev[mark:], "suspend-start guest-") == len(e.nodes)
	})
	orch.Stop()
	if err := <-firstCall; err == nil {
		t.Fatal("Acquire survived the orchestrator stop")
	}
	orch2, cl2 := e.start()
	defer orch2.Stop()
	if err := e.acquire(cl2, 60*time.Second); err != nil {
		t.Fatalf("Acquire after restart: %v", err)
	}
	ev = e.tl.list()
	e.checkVacateBeforeGrant(ev, mark)
	if n := count(ev[mark:], "suspend-start guest-"); n != len(e.nodes) {
		t.Errorf("guests suspended %d times across the restart, want %d (the new epoch adopts the running suspend)",
			n, len(e.nodes))
	}

	for _, msg := range []string{
		"Vacate started", "Host clear", "Foreground granted", "Resume started", "Host command sent", "Host ack",
	} {
		if e.logs.seen(msg) == 0 {
			t.Errorf("log line %q never emitted", msg)
		}
	}
}
