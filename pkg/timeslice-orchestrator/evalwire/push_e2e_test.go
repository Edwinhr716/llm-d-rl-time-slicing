//go:build evalwire

package evalwire_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	e2eGroup   = "g"
	e2eTrainer = "trainer"
)

var e2eNodes = []string{"127.0.0.2", "127.0.0.3"}

// pushArgs are the plan's fixed flags with a short notice window
// (T = notice + 3s).
func pushArgs() []string {
	return []string{
		"--background-role=true",
		"--min-bubble=30s",
		"--notice-window=4s",
		"--kill-budget=1s",
		"--controller-workers=4",
		"--resync-period=30s",
		"--foreground-op-timeout=60s",
	}
}

// logSink captures the default slog logger as JSON.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *logSink) has(t *testing.T, msg string, attrs map[string]any) bool {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for line := range bytes.SplitSeq(s.buf.Bytes(), []byte("\n")) {
		rec := map[string]any{}
		if len(line) == 0 || json.Unmarshal(line, &rec) != nil || rec["msg"] != msg {
			continue
		}
		match := true
		for key, want := range attrs {
			if rec[key] != want {
				match = false
			}
		}
		if match {
			return true
		}
	}
	return false
}

func captureLogs(t *testing.T) *logSink {
	t.Helper()
	sink := &logSink{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(sink, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sink
}

// call is one recorded Executor call.
type call struct {
	guest string
	op    string
	at    time.Time
}

// hostExec is a recording Executor. Suspend of a guest in hang blocks until
// release is closed or its context ends.
type hostExec struct {
	guest   string
	rec     *recorder
	hang    atomic.Bool
	release chan struct{}
	blocked chan struct{}
}

type recorder struct {
	mu    sync.Mutex
	calls []call
}

func (r *recorder) add(guest, op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call{guest: guest, op: op, at: time.Now()})
}

func (r *recorder) ops(guest string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, c := range r.calls {
		if c.guest == guest {
			out = append(out, c.op)
		}
	}
	return out
}

// last returns the time of the last op of any guest.
func (r *recorder) last(op string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	var at time.Time
	for _, c := range r.calls {
		if c.op == op && c.at.After(at) {
			at = c.at
		}
	}
	return at
}

func (e *hostExec) Guests() []string { return []string{e.guest} }

func (e *hostExec) SetNotReady(_ context.Context, guest string) error {
	e.rec.add(guest, "notready")
	return nil
}

func (e *hostExec) Suspend(ctx context.Context, guest string, _ time.Time) error {
	if e.hang.CompareAndSwap(true, false) {
		close(e.blocked)
		select {
		case <-e.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.rec.add(guest, "suspend")
	return nil
}

func (e *hostExec) Resume(_ context.Context, guest string, _ time.Time) error {
	e.rec.add(guest, "resume")
	return nil
}

func (e *hostExec) SetReady(_ context.Context, guest string) error {
	e.rec.add(guest, "ready")
	return nil
}

func guestOf(node string) string { return "guest-" + node }

// cluster is a fake clientset with the group's nodes and one reference host
// per node.
type cluster struct {
	cs       kubernetes.Interface
	hostPort int
	rec      *recorder
	execs    map[string]*hostExec
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	objs := make([]*corev1.Node, 0, len(e2eNodes))
	for _, node := range e2eNodes {
		objs = append(objs, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   node,
				Labels: map[string]string{infrastructure.NodeLabelPrefix + e2eGroup: "true"},
			},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: node},
			}},
		})
	}
	cs := fake.NewClientset()
	for _, obj := range objs {
		if _, err := cs.CoreV1().Nodes().Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	clu := &cluster{cs: cs, hostPort: freePort(t, e2eNodes[0]), rec: &recorder{}, execs: map[string]*hostExec{}}
	for _, node := range e2eNodes {
		clu.execs[node] = &hostExec{
			guest: guestOf(node), rec: clu.rec, release: make(chan struct{}), blocked: make(chan struct{}),
		}
	}
	return clu
}

func (c *cluster) startHosts(t *testing.T) {
	t.Helper()
	for _, node := range e2eNodes {
		hst, err := host.Start(context.Background(), host.Config{
			Node:       node,
			ListenAddr: net.JoinHostPort(node, strconv.Itoa(c.hostPort)),
			Exec:       c.execs[node],
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(hst.Stop)
	}
}

func (c *cluster) startOrch(t *testing.T) *evalwire.Orch {
	t.Helper()
	orch, err := evalwire.Start(context.Background(), evalwire.Config{
		Clientset: c.cs,
		AgentPort: freePort(t, "127.0.0.1"), // no agent: status errors are ignored
		HostPort:  c.hostPort,
		Args:      pushArgs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Stop)
	return orch
}

func freePort(t *testing.T, ip string) int {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address %v is not TCP", lis.Addr())
	}
	port := addr.Port
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func client(t *testing.T, orch *evalwire.Orch) pb.TimeSliceOrchestratorServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(orch.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	})
	api := pb.NewTimeSliceOrchestratorServiceClient(conn)
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := api.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: e2eGroup})
		cancel()
		if err == nil {
			return api
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %s unknown after 15s: %v", e2eGroup, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func acquire(ctx context.Context, api pb.TimeSliceOrchestratorServiceClient) (*pb.AcquireResponse, error) {
	return api.Acquire(ctx, &pb.AcquireRequest{GroupId: e2eGroup, JobId: e2eTrainer})
}

func mustAcquire(t *testing.T, api pb.TimeSliceOrchestratorServiceClient) time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := acquire(ctx, api)
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v", resp, err)
	}
	return time.Now()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestNS4_Push_HandoffCycles runs grant, lend (resume) and grant again: every
// grant comes after every guest was set NotReady then suspended.
func TestNS4_Push_HandoffCycles(t *testing.T) {
	sink := captureLogs(t)
	clu := newCluster(t)
	clu.startHosts(t)
	api := client(t, clu.startOrch(t))

	want := make([]string, 0, 12)
	for cycle := range 3 {
		granted := mustAcquire(t, api)
		want = append(want, "notready", "suspend")
		for _, node := range e2eNodes {
			if got := clu.rec.ops(guestOf(node)); !slices.Equal(got, want) {
				t.Fatalf("cycle %d: %s ops = %v, want %v", cycle, node, got, want)
			}
		}
		if last := clu.rec.last("suspend"); granted.Before(last) {
			t.Fatalf("cycle %d: granted at %v before the last suspend at %v", cycle, granted, last)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := api.Yield(ctx, &pb.YieldRequest{
			GroupId: e2eGroup, JobId: e2eTrainer, ExpectedIdle: durationpb.New(60 * time.Second),
		})
		cancel()
		if err != nil {
			t.Fatalf("cycle %d: Yield: %v", cycle, err)
		}
		want = append(want, "resume", "ready")
		for _, node := range e2eNodes {
			waitFor(t, node+" resumed", func() bool { return slices.Equal(clu.rec.ops(guestOf(node)), want) })
		}
		waitFor(t, "BACKGROUND", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := api.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: e2eGroup})
			return err == nil && resp.GetGroup().GetGroupState() == pb.GroupStatus_STATE_BACKGROUND
		})
	}

	for _, check := range []struct {
		msg   string
		attrs map[string]any
	}{
		{"Vacate started", map[string]any{"group": e2eGroup}},
		{"Host command sent", map[string]any{"node": e2eNodes[1], "command": "vacate"}},
		{"Host ack", map[string]any{"node": e2eNodes[1], "command": "vacate", "outcome": "vacated"}},
		{"Host clear", map[string]any{"group": e2eGroup, "node": e2eNodes[0], "how": "ack"}},
		{"Foreground granted", map[string]any{"group": e2eGroup, "job": e2eTrainer}},
		{"Resume started", map[string]any{"group": e2eGroup, "node": e2eNodes[0]}},
		{"Host ack", map[string]any{"node": e2eNodes[0], "command": "resume", "outcome": "resumed"}},
	} {
		if !sink.has(t, check.msg, check.attrs) {
			t.Errorf("no %q log line with %v", check.msg, check.attrs)
		}
	}
}

// TestNS4_Push_RestartMidNotice restarts the orchestrator while a host is
// still suspending: the new process knows no host state, sweeps every host
// again and grants only after the slow host acked.
func TestNS4_Push_RestartMidNotice(t *testing.T) {
	captureLogs(t)
	clu := newCluster(t)
	slow := clu.execs[e2eNodes[1]]
	slow.hang.Store(true)
	clu.startHosts(t)

	orch1 := clu.startOrch(t)
	api1 := client(t, orch1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := acquire(ctx1, api1)
		first <- err
	}()
	select {
	case <-slow.blocked:
	case <-time.After(15 * time.Second):
		t.Fatal("the slow host never started suspending")
	}
	cancel1() // the trainer's call dies with the process
	orch1.Stop()
	if err := <-first; status.Code(err) == codes.OK {
		t.Fatal("Acquire succeeded on the first process while a host was suspending")
	}

	api2 := client(t, clu.startOrch(t))
	type result struct {
		at  time.Time
		err error
	}
	second := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := acquire(ctx, api2)
		second <- result{time.Now(), err}
	}()
	time.Sleep(time.Second)
	select {
	case res := <-second:
		t.Fatalf("granted after restart while a host is still suspending: %v", res.err)
	default:
	}
	released := time.Now()
	close(slow.release)
	res := <-second
	if res.err != nil {
		t.Fatalf("Acquire after restart: %v", res.err)
	}
	if res.at.Before(released) {
		t.Fatalf("granted at %v before the slow host was released at %v", res.at, released)
	}
	if got := clu.rec.ops(guestOf(e2eNodes[1])); !slices.Equal(got, []string{"notready", "suspend"}) {
		t.Errorf("slow guest ops = %v, want one joined vacate", got)
	}
}

// TestNS4_Push_HungHostNeverGranted: a host that never acks keeps the grant
// held past T (this option has no kill) and is logged not clear at T.
func TestNS4_Push_HungHostNeverGranted(t *testing.T) {
	sink := captureLogs(t)
	clu := newCluster(t)
	hung := clu.execs[e2eNodes[1]]
	hung.hang.Store(true)
	clu.startHosts(t)
	api := client(t, clu.startOrch(t))
	t.Cleanup(func() { close(hung.release) })

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	_, err := acquire(ctx, api)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Acquire = %v, want DeadlineExceeded while a host hangs", err)
	}
	if !sink.has(t, "Host not clear at deadline", map[string]any{"group": e2eGroup, "node": e2eNodes[1]}) {
		t.Error("no Host not clear at deadline line for the hung host")
	}
	if sink.has(t, "Host not clear at deadline", map[string]any{"node": e2eNodes[0]}) {
		t.Error("the healthy host was logged not clear")
	}
	if sink.has(t, "Foreground granted", nil) {
		t.Error("granted while a host hangs")
	}
}
