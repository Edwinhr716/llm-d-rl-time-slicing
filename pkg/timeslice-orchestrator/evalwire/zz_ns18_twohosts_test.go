//go:build evalwire

package evalwire_test

// Two-host tests for decision D-NS-18 (hook M5): one group spread over two
// hosts, one trainer pod and one background guest on each, driven through
// the shared evalwire interface (evalwire.Start, host.Start, host.Executor),
// so the file applies to any branch that carries evalwire:
//
//	go test -tags evalwire -race -run 'TestNS18_TwoHosts' ./pkg/timeslice-orchestrator/evalwire/
//
// It asserts that the notice reaches both hosts, that the trainer is not
// granted before both hosts acked (their guests were set NotReady and
// suspended), and that a guest hanging in Suspend on the second host is
// killed within the notice window and kill budget, with the trainer granted
// and no job ever reported FAULTED. The kill is seen either as a Kill call on
// the node's snapshot agent (the agent's Kill RPC, when the proto has it) or
// as a Kill(ctx, guest) call on the host's executor. Every identifier starts
// with ns18 so the file compiles next to other evalwire tests.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	ns18Group   = "g"
	ns18Trainer = "trainer"
	ns18NS      = "default"
	// ns18Notice and ns18Kill are N and K, short so a test takes seconds.
	ns18Notice = 6 * time.Second
	ns18Kill   = 2 * time.Second
	// ns18Slack covers polls and informer lag on top of N + K.
	ns18Slack = 4 * time.Second
)

// ns18Hosts are the two group hosts: node name and loopback address. Each
// node's agent and host listen on its own address, on the same ports.
var ns18Hosts = []struct{ node, ip, guest string }{
	{"host-a", "127.0.0.2", "guest-a"},
	{"host-b", "127.0.0.3", "guest-b"},
}

func ns18Args() []string {
	return []string{
		"--background-role=true", "--min-bubble=30s",
		"--notice-window=" + ns18Notice.String(), "--kill-budget=" + ns18Kill.String(),
		"--controller-workers=4", "--resync-period=30s",
	}
}

// ns18Recorder keeps the order of events across agents, hosts and the trainer.
type ns18Recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *ns18Recorder) add(ev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *ns18Recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *ns18Recorder) count(ev string) int {
	n := 0
	for _, e := range r.all() {
		if e == ev {
			n++
		}
	}
	return n
}

// ns18Agent is one node's snapshot agent. Snapshot and Restore complete at
// once. Kill, when the proto has it, is served by an interceptor (see
// killInterceptor) so this file also compiles against protos without it.
type ns18Agent struct {
	agentpb.UnimplementedSnapshotAgentServiceServer

	node   string
	rec    *ns18Recorder
	onKill func(job string)
	mu     sync.Mutex
	states map[string]agentpb.JobState
	killed map[string]bool
}

func (a *ns18Agent) set(job string, state agentpb.JobState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.states[job] = state
}

func (a *ns18Agent) Status(context.Context, *agentpb.StatusRequest) (*agentpb.StatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resp := &agentpb.StatusResponse{}
	for job, state := range a.states {
		js := &agentpb.JobStatus{JobId: job, State: state}
		if a.killed[job] {
			ns18SetEnum(js, "last_outcome", "OUTCOME_KILLED")
		}
		resp.JobStatuses = append(resp.JobStatuses, js)
	}
	return resp, nil
}

func (a *ns18Agent) Snapshot(_ context.Context, req *agentpb.SnapshotRequest) (*agentpb.SnapshotResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_SAVED)
	a.rec.add("snapshot " + req.GetJobId() + "@" + a.node)
	return &agentpb.SnapshotResponse{OperationId: "snap-" + req.GetJobId()}, nil
}

func (a *ns18Agent) Restore(_ context.Context, req *agentpb.RestoreRequest) (*agentpb.RestoreResponse, error) {
	a.set(req.GetJobId(), agentpb.JobState_JOB_STATE_RUNNING)
	a.rec.add("restore " + req.GetJobId() + "@" + a.node)
	return &agentpb.RestoreResponse{OperationId: "restore-" + req.GetJobId()}, nil
}

func (a *ns18Agent) GetOperation(_ context.Context, req *agentpb.GetOperationRequest) (*agentpb.GetOperationResponse, error) {
	resp := &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE}
	if strings.HasPrefix(req.GetOperationId(), "kill-") {
		ns18SetEnum(resp, "outcome", "OUTCOME_KILLED")
	}
	return resp, nil
}

// killInterceptor answers the agent's Kill RPC without naming its Go types:
// it reads job_id by reflection and builds the response from the method's
// descriptor. Other methods go to the handler.
func (a *ns18Agent) killInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if !strings.HasSuffix(info.FullMethod, "/Kill") {
		return handler(ctx, req)
	}
	msg, ok := req.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Kill request is not a proto message")
	}
	job := ns18GetString(msg, "job_id")
	a.mu.Lock()
	a.killed[job] = true
	a.states[job] = agentpb.JobState_JOB_STATE_IDLE
	a.mu.Unlock()
	a.rec.add("kill " + job + "@" + a.node)
	if a.onKill != nil {
		a.onKill(job)
	}
	resp, err := ns18Response(info.FullMethod)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if fd := resp.ProtoReflect().Descriptor().Fields().ByName("operation_id"); fd != nil {
		resp.ProtoReflect().Set(fd, protoreflect.ValueOfString("kill-"+job))
	}
	return resp, nil
}

// ns18Response builds an empty response message of the gRPC method.
func ns18Response(fullMethod string) (proto.Message, error) {
	svc, method, ok := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	if !ok {
		return nil, fmt.Errorf("bad method %q", fullMethod)
	}
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svc))
	if err != nil {
		return nil, err
	}
	sd, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok || sd.Methods().ByName(protoreflect.Name(method)) == nil {
		return nil, fmt.Errorf("no method %q", fullMethod)
	}
	out := sd.Methods().ByName(protoreflect.Name(method)).Output()
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(out.FullName()); err == nil {
		return mt.New().Interface(), nil
	}
	return dynamicpb.NewMessage(out), nil
}

func ns18GetString(msg proto.Message, field string) string {
	m := msg.ProtoReflect()
	if fd := m.Descriptor().Fields().ByName(protoreflect.Name(field)); fd != nil && fd.Kind() == protoreflect.StringKind {
		return m.Get(fd).String()
	}
	return ""
}

// ns18SetEnum sets an enum field by value name when the message has both.
func ns18SetEnum(msg proto.Message, field, value string) {
	m := msg.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || fd.Enum() == nil {
		return
	}
	if ev := fd.Enum().Values().ByName(protoreflect.Name(value)); ev != nil {
		m.Set(fd, protoreflect.ValueOfEnum(ev.Number()))
	}
}

// ns18Exec is one host's Executor. The guest's mirror pod is created on its
// first Ready. With hang set, the next Suspend blocks until the guest is
// killed or the context ends, and never reports success.
type ns18Exec struct {
	env      *ns18Env
	node     string
	guest    string
	agent    *ns18Agent
	mu       sync.Mutex
	hang     bool
	hanging  chan struct{} // closed when a hung Suspend starts
	killedCh chan struct{} // closed when the guest is killed
	killOnce sync.Once
}

func (e *ns18Exec) Guests() []string { return []string{e.guest} }

func (e *ns18Exec) SetNotReady(_ context.Context, guest string) error {
	e.env.rec.add("notready " + guest)
	return nil
}

func (e *ns18Exec) Suspend(ctx context.Context, guest string, _ time.Time) error {
	e.mu.Lock()
	hang := e.hang
	e.hang = false
	e.mu.Unlock()
	if hang {
		e.env.rec.add("hang " + guest)
		close(e.hanging)
		select {
		case <-e.killedCh:
			return errors.New("guest killed")
		case <-ctx.Done():
			e.env.rec.add("suspend-cancelled " + guest)
			return ctx.Err()
		}
	}
	e.agent.set(guest, agentpb.JobState(6)) // JOB_STATE_SUSPENDED
	e.env.rec.add("suspend " + guest)
	return nil
}

func (e *ns18Exec) Resume(_ context.Context, guest string, _ time.Time) error {
	e.agent.set(guest, agentpb.JobState_JOB_STATE_RUNNING)
	e.env.rec.add("resume " + guest)
	return nil
}

func (e *ns18Exec) SetReady(ctx context.Context, guest string) error {
	e.agent.set(guest, agentpb.JobState_JOB_STATE_RUNNING)
	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror-" + guest, Namespace: ns18NS,
			Labels: map[string]string{
				"timeslice.io/group": ns18Group, "timeslice.io/job-id": guest, "timeslice.io/role": "background",
			},
		},
		Spec:   corev1.PodSpec{NodeName: e.node, RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := e.env.cs.CoreV1().Pods(ns18NS).Create(ctx, mirror, metav1.CreateOptions{}); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		return err
	}
	e.env.rec.add("ready " + guest)
	return nil
}

// Kill is used by hosts whose protocol kills through the executor.
func (e *ns18Exec) Kill(_ context.Context, guest string) error {
	e.env.rec.add("kill " + guest + "@" + e.node)
	e.kill()
	return nil
}

// kill ends the guest: its mirror pod goes away and a hung Suspend returns.
func (e *ns18Exec) kill() {
	e.killOnce.Do(func() {
		_ = e.env.cs.CoreV1().Pods(ns18NS).Delete(context.Background(), "mirror-"+e.guest, metav1.DeleteOptions{})
		close(e.killedCh)
	})
}

// ns18Env is the group: two nodes with a trainer pod each, an agent and a
// host per node, and one orchestrator.
type ns18Env struct {
	cs      *fake.Clientset
	rec     *ns18Recorder
	execs   []*ns18Exec
	orch    *evalwire.Orch
	trainer pb.TimeSliceOrchestratorServiceClient
}

func ns18FreePort(t *testing.T, ips ...string) int {
	t.Helper()
	for range 20 {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(ips[0], "0"))
		if err != nil {
			t.Fatal(err)
		}
		addr, ok := lis.Addr().(*net.TCPAddr)
		if !ok {
			t.Fatalf("listener address %v is not TCP", lis.Addr())
		}
		_ = lis.Close()
		free := true
		for _, ip := range ips[1:] {
			other, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(ip, strconv.Itoa(addr.Port)))
			if err != nil {
				free = false
				break
			}
			_ = other.Close()
		}
		if free {
			return addr.Port
		}
	}
	t.Fatal("no port free on every host address")
	return 0
}

func ns18Start(t *testing.T) *ns18Env {
	t.Helper()
	env := &ns18Env{cs: fake.NewClientset(), rec: &ns18Recorder{}}
	ips := make([]string, 0, len(ns18Hosts))
	for _, h := range ns18Hosts {
		ips = append(ips, h.ip)
	}
	agentPort, hostPort := ns18FreePort(t, ips...), ns18FreePort(t, ips...)

	for i, h := range ns18Hosts {
		objs := []any{
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: h.node, Labels: map[string]string{"group.timeslice.io/" + ns18Group: "true"}},
				Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: h.ip}}},
			},
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "trainer-" + strconv.Itoa(i), Namespace: ns18NS,
					Labels: map[string]string{"timeslice.io/group": ns18Group, "timeslice.io/job-id": ns18Trainer},
				},
				Spec:   corev1.PodSpec{NodeName: h.node},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
		}
		for _, obj := range objs {
			var err error
			switch o := obj.(type) {
			case *corev1.Node:
				_, err = env.cs.CoreV1().Nodes().Create(context.Background(), o, metav1.CreateOptions{})
			case *corev1.Pod:
				_, err = env.cs.CoreV1().Pods(ns18NS).Create(context.Background(), o, metav1.CreateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		agent := &ns18Agent{
			node: h.node, rec: env.rec,
			states: map[string]agentpb.JobState{ns18Trainer: agentpb.JobState_JOB_STATE_RUNNING},
			killed: map[string]bool{},
		}
		exec := &ns18Exec{
			env: env, node: h.node, guest: h.guest, agent: agent,
			hanging: make(chan struct{}), killedCh: make(chan struct{}),
		}
		agent.onKill = func(job string) {
			if job == exec.guest {
				exec.kill()
			}
		}
		env.execs = append(env.execs, exec)
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(h.ip, strconv.Itoa(agentPort)))
		if err != nil {
			t.Fatalf("agent listen on %s: %v", h.ip, err)
		}
		srv := grpc.NewServer(grpc.UnaryInterceptor(agent.killInterceptor))
		agentpb.RegisterSnapshotAgentServiceServer(srv, agent)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
	}

	orch, err := evalwire.Start(context.Background(), evalwire.Config{
		Clientset: env.cs, AgentPort: agentPort, HostPort: hostPort, Args: ns18Args(),
	})
	if err != nil {
		t.Fatalf("evalwire.Start: %v", err)
	}
	env.orch = orch
	t.Cleanup(orch.Stop)

	for i, h := range ns18Hosts {
		hst, err := host.Start(context.Background(), host.Config{
			Node:       h.node,
			OrchAddr:   orch.Addr,
			ListenAddr: net.JoinHostPort(h.ip, strconv.Itoa(hostPort)),
			AgentAddr:  net.JoinHostPort(h.ip, strconv.Itoa(agentPort)),
			Exec:       env.execs[i],
		})
		if err != nil {
			t.Fatalf("host.Start %s: %v", h.node, err)
		}
		t.Cleanup(hst.Stop)
	}

	conn, err := grpc.NewClient(orch.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	env.trainer = pb.NewTimeSliceOrchestratorServiceClient(conn)
	return env
}

// acquire is the trainer's Acquire, retried until granted or timeout.
func (e *ns18Env) acquire(t *testing.T, timeout time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		resp, err := e.trainer.Acquire(ctx, &pb.AcquireRequest{JobId: ns18Trainer, GroupId: ns18Group})
		cancel()
		if err == nil && resp.GetSuccess() {
			e.rec.add("granted " + ns18Trainer)
			return time.Now()
		}
		if time.Now().After(deadline) {
			t.Fatalf("trainer not granted within %v: %v (events %v)", timeout, err, e.rec.all())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (e *ns18Env) yield(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.trainer.Yield(ctx, &pb.YieldRequest{
		JobId: ns18Trainer, GroupId: ns18Group, ExpectedIdle: durationpb.New(60 * time.Second),
	}); err != nil {
		t.Fatalf("trainer Yield: %v", err)
	}
	e.rec.add("yielded " + ns18Trainer)
}

// lend yields the trainer and waits until the guest on every host is up again.
func (e *ns18Env) lend(t *testing.T) {
	t.Helper()
	before := map[string]int{}
	for _, h := range ns18Hosts {
		before[h.guest] = e.rec.count("ready " + h.guest)
	}
	e.yield(t)
	deadline := time.Now().Add(20 * time.Second)
	for _, h := range ns18Hosts {
		for e.rec.count("ready "+h.guest) <= before[h.guest] {
			if time.Now().After(deadline) {
				t.Fatalf("guest %s not ready after the lend (events %v)", h.guest, e.rec.all())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// ns18Index is the position of the last ev at or after from, or -1.
func ns18Index(events []string, from int, ev string) int {
	at := -1
	for i := from; i < len(events); i++ {
		if events[i] == ev {
			at = i
		}
	}
	return at
}

// ns18CheckVacate checks one vacate on events[from:grant]: on every host the
// guest was set NotReady and then suspended (the host's ack) before the
// grant, and a trainer restore on a node came after that node's suspend.
func ns18CheckVacate(t *testing.T, events []string, from, grant int, skipSuspend string) {
	t.Helper()
	window := events[:grant]
	for _, h := range ns18Hosts {
		notready := ns18Index(window, from, "notready "+h.guest)
		if notready < 0 {
			t.Errorf("no notice reached %s before the grant (events %v)", h.node, events[from:])
			continue
		}
		if h.guest == skipSuspend {
			continue
		}
		suspend := ns18Index(window, from, "suspend "+h.guest)
		if suspend < notready {
			t.Errorf("G3: granted before %s acked (notready %d, suspend %d, grant %d): %v",
				h.node, notready, suspend, grant, events[from:])
		}
		if restore := ns18Index(events, from, "restore "+ns18Trainer+"@"+h.node); restore >= 0 && restore < suspend {
			t.Errorf("G2: trainer restored on %s before its guest was suspended: %v", h.node, events[from:])
		}
	}
}

// TestNS18_TwoHosts_NoticeReachesBothAndGrantAfterAllAcks runs two lend and
// grant cycles on the two-host group.
func TestNS18_TwoHosts_NoticeReachesBothAndGrantAfterAllAcks(t *testing.T) {
	env := ns18Start(t)
	env.acquire(t, 30*time.Second)
	time.Sleep(2 * time.Second) // the hosts find the group

	for cycle := 1; cycle <= 2; cycle++ {
		env.lend(t)
		from := len(env.rec.all())
		env.acquire(t, ns18Notice+ns18Kill+ns18Slack+10*time.Second)
		events := env.rec.all()
		grant := ns18Index(events, from, "granted "+ns18Trainer)
		ns18CheckVacate(t, events, from, grant, "")
		for _, h := range ns18Hosts {
			if slices.Contains(events[from:], "kill "+h.guest+"@"+h.node) {
				t.Errorf("cycle %d: healthy guest %s was killed", cycle, h.guest)
			}
		}
		if t.Failed() {
			t.Fatalf("cycle %d failed; events %v", cycle, events)
		}
	}
}

// TestNS18_TwoHosts_HangOnSecondHostKilledWithoutFaulted hangs the second
// host's guest in Suspend. The orchestrator must kill it within N + K and
// grant the trainer, and no agent job may ever be reported FAULTED.
func TestNS18_TwoHosts_HangOnSecondHostKilledWithoutFaulted(t *testing.T) {
	env := ns18Start(t)
	env.acquire(t, 30*time.Second)
	time.Sleep(2 * time.Second)
	env.lend(t)

	hung := env.execs[1]
	hung.mu.Lock()
	hung.hang = true
	hung.mu.Unlock()

	var faulted []string
	var fmu sync.Mutex
	sctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		for sctx.Err() == nil {
			ctx, cancel := context.WithTimeout(sctx, time.Second)
			resp, err := env.trainer.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: ns18Group})
			cancel()
			if err == nil {
				for _, js := range resp.GetAgentJobStates() {
					if js.GetJobState() == pb.SnapshotAgentJobState_STATE_FAULTED {
						fmu.Lock()
						faulted = append(faulted, js.GetAgent()+"/"+js.GetJobId())
						fmu.Unlock()
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	from := len(env.rec.all())
	start := time.Now()
	granted := env.acquire(t, ns18Notice+ns18Kill+ns18Slack+10*time.Second)
	waited := granted.Sub(start)
	time.Sleep(time.Second) // one more status sample after the grant
	stop()
	events := env.rec.all()

	select {
	case <-hung.hanging:
	default:
		t.Fatalf("the second host's guest never started suspending (events %v)", events)
	}
	grant := ns18Index(events, from, "granted "+ns18Trainer)
	ns18CheckVacate(t, events, from, grant, hung.guest)
	kill := ns18Index(events[:grant], from, "kill "+hung.guest+"@"+hung.node)
	if kill < 0 {
		t.Errorf("hung guest %s was not killed before the grant (events %v)", hung.guest, events[from:])
	}
	if limit := ns18Notice + ns18Kill + ns18Slack; waited > limit {
		t.Errorf("granted after %v, want within N + K + slack = %v", waited, limit)
	}
	if slices.Contains(events[from:], "kill "+env.execs[0].guest+"@"+env.execs[0].node) {
		t.Error("the healthy host's guest was killed")
	}
	fmu.Lock()
	defer fmu.Unlock()
	if len(faulted) > 0 {
		t.Errorf("jobs reported FAULTED: %v", faulted)
	}
	t.Logf("waited %v for the grant; events %v", waited, events[from:])
}
