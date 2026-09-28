// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build evalwire

package evalwire_test

// End-to-end tests of PENDING LEAD DECISION D-NS-4, option hybrid: the
// orchestrator as the binary wires it (evalwire), a fake snapshot agent per
// node, and the reference host (evalwire/host) per node over a recording
// Executor.

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
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
	groupID = "g"
	trainer = "trainer"
)

// ---- log capture ----

type logRec struct {
	msg   string
	level slog.Level
	attrs map[string]string
}

type logSink struct {
	mu   sync.Mutex
	recs []logRec
}

var sink = &logSink{}

type captureHandler struct {
	next  slog.Handler
	attrs []slog.Attr
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	rec := logRec{msg: r.Message, level: r.Level, attrs: map[string]string{}}
	for _, a := range h.attrs {
		rec.attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	sink.mu.Lock()
	sink.recs = append(sink.recs, rec)
	sink.mu.Unlock()
	return h.next.Handle(ctx, r)
}

func (h *captureHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &captureHandler{next: h.next.WithAttrs(as), attrs: append(slices.Clone(h.attrs), as...)}
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return &captureHandler{next: h.next.WithGroup(name), attrs: h.attrs}
}

func (s *logSink) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = nil
}

// find returns the records with msg whose attrs include every key/value in kv.
func (s *logSink) find(msg string, kv ...string) []logRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []logRec
next:
	for _, r := range s.recs {
		if r.msg != msg {
			continue
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if r.attrs[kv[i]] != kv[i+1] {
				continue next
			}
		}
		out = append(out, r)
	}
	return out
}

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(&captureHandler{next: slog.NewTextHandler(os.Stderr, nil)}))
	os.Exit(m.Run())
}

// ---- fake snapshot agent ----

type fakeAgent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer
	mu     sync.Mutex
	states map[string]agentpb.JobState
	ops    map[string]agentpb.JobState // op -> target state
	jobOf  map[string]string
	n      int
	srv    *grpc.Server
}

func startAgent(t *testing.T, addr string) *fakeAgent {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("agent listen %s: %v", addr, err)
	}
	a := &fakeAgent{
		states: map[string]agentpb.JobState{trainer: agentpb.JobState_JOB_STATE_RUNNING},
		ops:    map[string]agentpb.JobState{},
		jobOf:  map[string]string{},
		srv:    grpc.NewServer(),
	}
	agentpb.RegisterSnapshotAgentServiceServer(a.srv, a)
	go func() { _ = a.srv.Serve(lis) }()
	t.Cleanup(a.srv.Stop)
	return a
}

func (a *fakeAgent) Status(context.Context, *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for job, st := range a.states {
		resp.JobStatuses = append(resp.JobStatuses, &agentpb.JobStatus{JobId: job, State: st})
	}
	return resp, nil
}

func (a *fakeAgent) start(job string, target agentpb.JobState) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	op := "op-" + strconv.Itoa(a.n)
	a.ops[op] = target
	a.jobOf[op] = job
	a.states[job] = agentpb.JobState_JOB_STATE_TRANSITIONING
	return op
}

func (a *fakeAgent) Snapshot(_ context.Context, req *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	return &agentpb.SnapshotResponse{OperationId: a.start(req.GetJobId(), agentpb.JobState_JOB_STATE_SAVED)}, nil
}

func (a *fakeAgent) Restore(_ context.Context, req *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	return &agentpb.RestoreResponse{OperationId: a.start(req.GetJobId(), agentpb.JobState_JOB_STATE_RUNNING)}, nil
}

func (a *fakeAgent) GetOperation(_ context.Context, req *agentpb.GetOperationRequest) (*agentpb.GetOperationResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	target, ok := a.ops[req.GetOperationId()]
	if !ok {
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED}, nil
	}
	a.states[a.jobOf[req.GetOperationId()]] = target
	delete(a.ops, req.GetOperationId())
	return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE, ElapsedMs: 1}, nil
}

func (a *fakeAgent) trainerState() agentpb.JobState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.states[trainer]
}

// ---- recording executor ----

type execEvent struct {
	op    string // notready, suspend, suspended, resume, ready
	guest string
	at    time.Time
}

type recExec struct {
	mu      sync.Mutex
	guests  []string
	suspend time.Duration
	events  []execEvent
}

func (e *recExec) add(op, g string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, execEvent{op: op, guest: g, at: time.Now()})
}

func (e *recExec) Guests() []string { return e.guests }

func (e *recExec) SetNotReady(_ context.Context, g string) error {
	e.add("notready", g)
	return nil
}

func (e *recExec) Suspend(_ context.Context, g string, _ time.Time) error {
	e.add("suspend", g)
	e.mu.Lock()
	d := e.suspend
	e.mu.Unlock()
	time.Sleep(d) // real suspend work does not stop when the caller goes away
	e.add("suspended", g)
	return nil
}

func (e *recExec) Resume(_ context.Context, g string, _ time.Time) error {
	e.add("resume", g)
	return nil
}

func (e *recExec) SetReady(_ context.Context, g string) error {
	e.add("ready", g)
	return nil
}

func (e *recExec) snapshot() []execEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.events)
}

// last returns the time of the last event op, or zero.
func (e *recExec) last(op string) time.Time {
	var at time.Time
	for _, ev := range e.snapshot() {
		if ev.op == op {
			at = ev.at
		}
	}
	return at
}

// ---- rig ----

type rig struct {
	t         *testing.T
	ctx       context.Context
	cs        *fake.Clientset
	args      []string
	agentPort int
	hostPort  int
	nodes     []string
	agents    []*fakeAgent
	execs     []*recExec
	hosts     []*host.Host
	orch      *evalwire.Orch
	client    pb.TimeSliceOrchestratorServiceClient
	conn      *grpc.ClientConn
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr).Port
}

// newRig starts the orchestrator, one agent and one host per node (named
// 127.0.0.<2+i>), and a trainer that holds the group.
func newRig(t *testing.T, suspend []time.Duration, extraArgs ...string) *rig {
	t.Helper()
	sink.reset()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	r := &rig{t: t, ctx: ctx, cs: fake.NewClientset(), agentPort: freePort(t), hostPort: freePort(t)}
	r.args = append([]string{
		"--background-role=true", "--min-bubble=30s", "--notice-window=10s", "--kill-budget=2s",
		"--controller-workers=4", "--resync-period=1s", "--foreground-op-timeout=60s",
	}, extraArgs...)
	for i := range suspend {
		ip := fmt.Sprintf("127.0.0.%d", i+2)
		r.nodes = append(r.nodes, ip)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: ip, Labels: map[string]string{"group.timeslice.io/" + groupID: "true"}},
			Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}},
		}
		if _, err := r.cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node: %v", err)
		}
		r.agents = append(r.agents, startAgent(t, net.JoinHostPort(ip, strconv.Itoa(r.agentPort))))
		r.execs = append(r.execs, &recExec{guests: []string{"guest-" + strconv.Itoa(i)}, suspend: suspend[i]})
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "trainer-0", Namespace: "default", UID: "trainer-uid",
		Labels: map[string]string{"timeslice.io/group": groupID, "timeslice.io/job-id": trainer},
	}}
	if _, err := r.cs.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	r.startOrch()
	t.Cleanup(func() { r.orch.Stop() })

	// The trainer holds the group before any host joins.
	if _, err := r.acquire(20 * time.Second); err != nil {
		t.Fatalf("initial trainer Acquire: %v", err)
	}
	for i := range r.nodes {
		r.startHost(i)
	}
	r.waitFor("every host found the group", 10*time.Second, func() bool {
		for _, h := range r.hosts {
			if h.Group() != groupID {
				return false
			}
		}
		return true
	})
	return r
}

func (r *rig) startOrch() {
	r.t.Helper()
	orch, err := evalwire.Start(r.ctx, evalwire.Config{
		Clientset: r.cs, AgentPort: r.agentPort, HostPort: r.hostPort, Args: r.args,
	})
	if err != nil {
		r.t.Fatalf("evalwire.Start: %v", err)
	}
	r.orch = orch
	if r.conn == nil {
		conn, err := grpc.NewClient(orch.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			r.t.Fatalf("dial orchestrator: %v", err)
		}
		r.t.Cleanup(func() { _ = conn.Close() })
		r.conn = conn
		r.client = pb.NewTimeSliceOrchestratorServiceClient(conn)
	}
	r.waitFor("group discovered", 10*time.Second, func() bool {
		resp, err := r.client.ListGroups(r.ctx, &pb.ListGroupsRequest{})
		return err == nil && slices.Contains(resp.GetGroupIds(), groupID)
	})
}

func (r *rig) startHost(i int) {
	r.t.Helper()
	h, err := host.Start(r.ctx, host.Config{
		Node:       r.nodes[i],
		OrchAddr:   r.orch.Addr,
		ListenAddr: net.JoinHostPort(r.nodes[i], strconv.Itoa(r.hostPort)),
		AgentAddr:  net.JoinHostPort(r.nodes[i], strconv.Itoa(r.agentPort)),
		Exec:       r.execs[i],
	})
	if err != nil {
		r.t.Fatalf("host.Start(%s): %v", r.nodes[i], err)
	}
	r.t.Cleanup(h.Stop)
	if i < len(r.hosts) {
		r.hosts[i] = h
	} else {
		r.hosts = append(r.hosts, h)
	}
}

func (r *rig) waitFor(what string, timeout time.Duration, cond func() bool) {
	r.t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.t.Fatalf("timed out after %v waiting for: %s", timeout, what)
}

// acquire runs the trainer's foreground Acquire, retrying transport errors
// (an orchestrator restart), and returns when it was granted.
func (r *rig) acquire(timeout time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	for {
		resp, err := r.client.Acquire(ctx, &pb.AcquireRequest{JobId: trainer, GroupId: groupID})
		if err == nil && resp.GetSuccess() {
			return time.Now(), nil
		}
		if ctx.Err() != nil {
			return time.Time{}, fmt.Errorf("not granted within %v: %w", timeout, ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// yieldLend runs the trainer's Yield with a lend hint.
func (r *rig) yieldLend() time.Time {
	r.t.Helper()
	_, err := r.client.Yield(r.ctx, &pb.YieldRequest{
		JobId: trainer, GroupId: groupID, ExpectedIdle: durationpb.New(60 * time.Second),
	})
	if err != nil {
		r.t.Fatalf("trainer Yield: %v", err)
	}
	return time.Now()
}

// waitServing waits until every host's guests got Resume then Ready after t0.
func (r *rig) waitServing(t0 time.Time) {
	r.t.Helper()
	r.waitFor("every guest resumed and ready", 15*time.Second, func() bool {
		for _, e := range r.execs {
			if !e.last("ready").After(t0) {
				return false
			}
		}
		return true
	})
	for i := range r.agents {
		if st := r.agents[i].trainerState(); st != agentpb.JobState_JOB_STATE_SAVED {
			r.t.Fatalf("guests resumed while the trainer is %v on %s, want SAVED", st, r.nodes[i])
		}
	}
}

// checkVacated checks, for each host, that NotReady came before Suspend and
// that the trainer was granted only after the host's last suspend finished.
func (r *rig) checkVacated(since, granted time.Time) {
	r.t.Helper()
	for i, e := range r.execs {
		var notReady, suspend, done time.Time
		for _, ev := range e.snapshot() {
			if ev.at.Before(since) {
				continue
			}
			switch ev.op {
			case "notready":
				if notReady.IsZero() {
					notReady = ev.at
				}
			case "suspend":
				if suspend.IsZero() {
					suspend = ev.at
				}
			case "suspended":
				done = ev.at
			case "resume", "ready":
				if ev.at.Before(granted) {
					r.t.Fatalf("host %s: %s during the notice", r.nodes[i], ev.op)
				}
			}
		}
		if notReady.IsZero() || suspend.IsZero() || done.IsZero() {
			r.t.Fatalf("host %s did not vacate (notready %v, suspend %v, done %v)", r.nodes[i], notReady, suspend, done)
		}
		if suspend.Before(notReady) {
			r.t.Fatalf("host %s: Suspend before NotReady", r.nodes[i])
		}
		if granted.Before(done) {
			r.t.Fatalf("host %s: trainer granted %v before the guest was suspended",
				r.nodes[i], done.Sub(granted))
		}
	}
}

func (r *rig) requireLog(msg string, kv ...string) {
	r.t.Helper()
	if len(sink.find(msg, kv...)) == 0 {
		r.t.Fatalf("no log line %q with %v", msg, kv)
	}
}

// ---- tests ----

func TestNS4_Hybrid_HandoffCycles(t *testing.T) {
	r := newRig(t, []time.Duration{500 * time.Millisecond})
	for cycle := 1; cycle <= 2; cycle++ {
		y := r.yieldLend()
		r.waitServing(y)
		time.Sleep(time.Second) // serve
		sent := time.Now()
		granted, err := r.acquire(20 * time.Second)
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		r.checkVacated(sent, granted)
		t.Logf("cycle %d: handoff %v", cycle, granted.Sub(sent))
	}
	node := r.nodes[0]
	r.requireLog("Resume started", "group", groupID, "node", node)
	r.requireLog("Vacate started", "group", groupID)
	r.requireLog("Host command sent", "node", node, "command", "resume")
	r.requireLog("Host command sent", "node", node, "command", "vacate")
	r.requireLog("Host ack", "node", node, "command", "resume", "outcome", "resumed")
	r.requireLog("Host ack", "node", node, "command", "vacate", "outcome", "vacated")
	r.requireLog("Host clear", "group", groupID, "node", node, "how", "ack")
	r.requireLog("Foreground granted", "group", groupID, "job", trainer)
	if n := len(sink.find("Host not clear at deadline")); n != 0 {
		t.Fatalf("%d deadline warnings in a healthy run", n)
	}
}

func TestNS4_Hybrid_BarrierWaitsForSlowHost(t *testing.T) {
	r := newRig(t, []time.Duration{200 * time.Millisecond, 3 * time.Second})
	r.waitServing(r.yieldLend())
	sent := time.Now()
	granted, err := r.acquire(20 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.checkVacated(sent, granted)
	if granted.Sub(sent) < 3*time.Second {
		t.Fatalf("granted after %v, before the slow host's 3 s suspend", granted.Sub(sent))
	}
}

func TestNS4_Hybrid_UnreachableHostBlocksGrant(t *testing.T) {
	// T = notice + 4 s - 1 s.
	r := newRig(t, []time.Duration{200 * time.Millisecond, 200 * time.Millisecond},
		"--notice-window=4s", "--kill-budget=1s")
	r.waitServing(r.yieldLend())
	down := r.nodes[1]
	r.hosts[1].Stop() // the host is gone; its guest is still live

	sent := time.Now()
	if _, err := r.acquire(5 * time.Second); err == nil {
		t.Fatal("trainer granted while a host that held the node never acked")
	}
	r.requireLog("Host not clear at deadline", "group", groupID, "node", down)
	r.requireLog("Host ack", "node", down, "command", "vacate", "outcome", "unreachable")
	if w := sink.find("Host not clear at deadline", "node", r.nodes[0]); len(w) != 0 {
		t.Fatalf("the healthy host was reported not clear: %v", w)
	}

	// The host comes back: it does not know its guest, so it suspends it
	// (fail closed), acks, and the trainer is granted.
	r.startHost(1)
	granted, err := r.acquire(20 * time.Second)
	if err != nil {
		t.Fatalf("after the host came back: %v", err)
	}
	r.checkVacated(sent, granted)
}

func TestNS4_Hybrid_RestartMidNotice(t *testing.T) {
	r := newRig(t, []time.Duration{4 * time.Second})
	r.waitServing(r.yieldLend())

	sent := time.Now()
	type result struct {
		at  time.Time
		err error
	}
	done := make(chan result, 1)
	go func() {
		at, err := r.acquire(40 * time.Second)
		done <- result{at, err}
	}()
	r.waitFor("the host started suspending", 5*time.Second, func() bool { return r.execs[0].last("suspend").After(sent) })
	time.Sleep(time.Second)
	r.orch.Stop()
	restartAt := time.Now()
	r.startOrch()

	res := <-done
	if res.err != nil {
		t.Fatalf("trainer after restart: %v", res.err)
	}
	if res.at.Before(restartAt) {
		t.Fatal("trainer granted before the restart, while the guest was suspending")
	}
	r.checkVacated(sent, res.at)
	for _, ev := range r.execs[0].snapshot() {
		if ev.op == "resume" && ev.at.After(sent) && ev.at.Before(res.at) {
			t.Fatal("guest resumed during the notice")
		}
	}

	// The next cycle works on the new process.
	r.waitServing(r.yieldLend())
	sent = time.Now()
	granted, err := r.acquire(20 * time.Second)
	if err != nil {
		t.Fatalf("cycle after restart: %v", err)
	}
	r.checkVacated(sent, granted)
}
