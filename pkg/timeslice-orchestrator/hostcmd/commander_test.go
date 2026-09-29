package hostcmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"google.golang.org/grpc"
)

const testGroup = "g"

// logSink captures JSON log records.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// records returns every record whose msg is msg.
func (s *logSink) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for line := range bytes.SplitSeq(s.buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		rec := map[string]any{}
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// hasRecord reports whether a record msg has every given attribute.
func (s *logSink) hasRecord(t *testing.T, msg string, attrs map[string]any) bool {
	t.Helper()
	for _, rec := range s.records(t, msg) {
		match := true
		for key, want := range attrs {
			if rec[key] != want {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// fakeHost is a scripted host command endpoint.
type fakeHost struct {
	hcpb.UnimplementedHostCommandServiceServer

	vacate func(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error)
	resume func(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error)
	calls  atomic.Int32
}

func (f *fakeHost) Vacate(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
	f.calls.Add(1)
	if f.vacate != nil {
		return f.vacate(ctx, req)
	}
	return vacated(req), nil
}

func (f *fakeHost) Resume(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
	f.calls.Add(1)
	if f.resume != nil {
		return f.resume(ctx, req)
	}
	return &hcpb.HostAck{
		NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
		Command: hcpb.Command_COMMAND_RESUME, Outcome: hcpb.Outcome_OUTCOME_RESUMED,
	}, nil
}

func vacated(req *hcpb.VacateRequest) *hcpb.HostAck {
	return &hcpb.HostAck{
		NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
		Command: hcpb.Command_COMMAND_VACATE, Outcome: hcpb.Outcome_OUTCOME_VACATED,
	}
}

// serve starts a fake host on a loopback port and returns its address.
func serve(t *testing.T, fh *fakeHost) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	hcpb.RegisterHostCommandServiceServer(srv, fh)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// closedAddr returns a loopback address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type harness struct {
	cmd      *hostcmd.Commander
	sink     *logSink
	enqueued atomic.Int32
}

// newHarness starts a Commander whose nodes resolve through addrs.
func newHarness(t *testing.T, addrs map[string]string, notice, kill time.Duration) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hns := &harness{sink: &logSink{}}
	hns.cmd = hostcmd.New(ctx, hostcmd.Config{
		Resolve: func(node string) (string, error) {
			addr, ok := addrs[node]
			if !ok {
				return "", errors.New("unknown node")
			}
			return addr, nil
		},
		NoticeWindow:      notice,
		KillBudget:        kill,
		Enqueue:           func(string) { hns.enqueued.Add(1) },
		Logger:            slog.New(slog.NewJSONHandler(hns.sink, nil)),
		RetryInterval:     10 * time.Millisecond,
		LateRetryInterval: 20 * time.Millisecond,
		AttemptTimeout:    500 * time.Millisecond,
	})
	t.Cleanup(func() {
		cancel()
		hns.cmd.Close()
	})
	return hns
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func nodes(addrs map[string]string) []string {
	out := make([]string, 0, len(addrs))
	for node := range addrs {
		out = append(out, node)
	}
	return out
}

func TestHostCommand_UnknownHostsFailClosed(t *testing.T) {
	hns := newHarness(t, map[string]string{}, 30*time.Second, 3*time.Second)
	if hns.cmd.AllClear(testGroup) {
		t.Error("a group never synced is clear, want not clear")
	}
	hns.cmd.SyncHosts(testGroup, []string{"node-a"})
	if hns.cmd.AllClear(testGroup) {
		t.Error("a host never commanded is clear, want not clear")
	}
	if got := hns.cmd.HostStates(testGroup)["node-a"]; got != hostcmd.StateUnknown {
		t.Errorf("state = %v, want unknown", got)
	}
}

// An empty group has no host that acked, so it must not count as clear. Its
// barrier waits until hosts appear and ack.
func TestHostCommand_EmptyGroupFailsClosed(t *testing.T) {
	addrs := map[string]string{"node-a": serve(t, &fakeHost{})}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nil)
	if hns.cmd.AllClear(testGroup) {
		t.Fatal("a group without hosts is clear, want not clear")
	}

	hns.cmd.StartVacate(testGroup, time.Now())
	time.Sleep(50 * time.Millisecond)
	if hns.cmd.AllClear(testGroup) || hns.enqueued.Load() != 0 {
		t.Fatal("the barrier of a group without hosts completed")
	}
	if len(hns.sink.records(t, "Vacate started")) != 1 {
		t.Fatal("want one Vacate started line for the empty group")
	}

	// Removing the last host of a clear group makes it not clear again.
	hns.cmd.SyncHosts(testGroup, []string{"node-a"})
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(testGroup) })
	eventually(t, "enqueue", func() bool { return hns.enqueued.Load() == 1 })
	hns.cmd.SyncHosts(testGroup, nil)
	if hns.cmd.AllClear(testGroup) {
		t.Error("a group whose hosts were all removed is clear, want not clear")
	}
}

// A host that joins while a barrier runs is sent the barrier's Vacate, so the
// barrier completes instead of waiting forever.
func TestHostCommand_HostAddedDuringBarrierIsCommanded(t *testing.T) {
	release := make(chan struct{})
	var barrierEpoch atomic.Int64
	slow := &fakeHost{vacate: func(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		barrierEpoch.Store(req.GetEpoch())
		select {
		case <-release:
			return vacated(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	var joinedEpoch atomic.Int64
	joined := &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		joinedEpoch.Store(req.GetEpoch())
		return vacated(req), nil
	}}
	addrs := map[string]string{
		"node-a": serve(t, &fakeHost{}), "node-b": serve(t, slow), "node-c": serve(t, joined),
	}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, []string{"node-a", "node-b"})
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "node-b commanded", func() bool { return slow.calls.Load() >= 1 })

	hns.cmd.SyncHosts(testGroup, []string{"node-a", "node-b", "node-c"})
	eventually(t, "node-c clear", func() bool {
		return hns.cmd.HostStates(testGroup)["node-c"] == hostcmd.StateClear
	})
	if hns.cmd.AllClear(testGroup) || hns.enqueued.Load() != 0 {
		t.Fatal("barrier completed while node-b has not acked")
	}
	if !hns.sink.hasRecord(t, "Host joined a running vacate", map[string]any{"group": testGroup, "node": "node-c"}) {
		t.Error("no Host joined a running vacate line for node-c")
	}
	if len(hns.sink.records(t, "Vacate started")) != 1 {
		t.Error("want one Vacate started line: the join must not start a new barrier")
	}
	if got, want := joinedEpoch.Load(), barrierEpoch.Load(); got != want {
		t.Errorf("node-c epoch = %d, want the barrier epoch %d", got, want)
	}

	close(release)
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(testGroup) })
	eventually(t, "enqueue", func() bool { return hns.enqueued.Load() == 1 })
}

// Forget drops the group's hosts and stops their running commands.
func TestHostCommand_ForgetStopsCommands(t *testing.T) {
	failing := &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		return &hcpb.HostAck{
			NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
			Command: hcpb.Command_COMMAND_VACATE, Outcome: hcpb.Outcome_OUTCOME_FAILED,
		}, nil
	}}
	addrs := map[string]string{"node-a": serve(t, failing)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "retries", func() bool { return failing.calls.Load() >= 2 })

	hns.cmd.Forget(testGroup)
	if got := hns.cmd.HostStates(testGroup); len(got) != 0 {
		t.Errorf("host states after Forget = %v, want none", got)
	}
	if hns.cmd.AllClear(testGroup) {
		t.Error("a forgotten group is clear, want not clear")
	}
	time.Sleep(50 * time.Millisecond) // let an in-flight attempt finish
	calls := failing.calls.Load()
	time.Sleep(100 * time.Millisecond)
	if got := failing.calls.Load(); got != calls {
		t.Errorf("host still commanded after Forget: %d calls, then %d", calls, got)
	}
}

func TestHostCommand_BarrierWaitsForEveryAck(t *testing.T) {
	release := make(chan struct{})
	slow := &fakeHost{vacate: func(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		select {
		case <-release:
			return vacated(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	fast := &fakeHost{}
	addrs := map[string]string{"node-a": serve(t, fast), "node-b": serve(t, slow)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))

	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "node-a clear", func() bool {
		return hns.cmd.HostStates(testGroup)["node-a"] == hostcmd.StateClear
	})
	if hns.cmd.AllClear(testGroup) {
		t.Fatal("all clear while node-b has not acked")
	}
	if hns.enqueued.Load() != 0 {
		t.Fatal("reconcile enqueued before every host acked")
	}

	close(release)
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(testGroup) })
	eventually(t, "enqueue", func() bool { return hns.enqueued.Load() == 1 })

	if len(hns.sink.records(t, "Vacate started")) != 1 {
		t.Error("want one Vacate started line")
	}
	for _, node := range []string{"node-a", "node-b"} {
		if !hns.sink.hasRecord(t, "Host command sent", map[string]any{"node": node, "command": "vacate"}) {
			t.Errorf("no Host command sent line for %s", node)
		}
		if !hns.sink.hasRecord(t, "Host ack", map[string]any{"node": node, "command": "vacate", "outcome": "vacated"}) {
			t.Errorf("no Host ack line for %s", node)
		}
		if !hns.sink.hasRecord(t, "Host clear", map[string]any{"group": testGroup, "node": node, "how": "ack"}) {
			t.Errorf("no Host clear line for %s", node)
		}
	}

	// A second notice while every host is clear sends nothing.
	calls := fast.calls.Load() + slow.calls.Load()
	hns.cmd.StartVacate(testGroup, time.Now())
	if got := fast.calls.Load() + slow.calls.Load(); got != calls {
		t.Errorf("clear hosts were commanded again: %d calls, want %d", got, calls)
	}
}

func TestHostCommand_UnreachableHostNotClearAtDeadline(t *testing.T) {
	addrs := map[string]string{"node-a": serve(t, &fakeHost{}), "node-b": closedAddr(t)}
	hns := newHarness(t, addrs, 300*time.Millisecond, 100*time.Millisecond)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))

	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "not clear at deadline", func() bool {
		return hns.sink.hasRecord(t, "Host not clear at deadline", map[string]any{"group": testGroup, "node": "node-b"})
	})
	if hns.sink.hasRecord(t, "Host not clear at deadline", map[string]any{"node": "node-a"}) {
		t.Error("node-a acked but was logged not clear")
	}
	if hns.cmd.AllClear(testGroup) {
		t.Error("all clear with an unreachable host")
	}
	if hns.cmd.Reachable(testGroup, "node-b") {
		t.Error("node-b reachable, want unreachable after failed commands")
	}
	if len(hns.sink.records(t, "Host not clear at deadline")) != 1 {
		t.Error("want the not-clear line once per host per barrier")
	}
}

func TestHostCommand_LateAckIgnored(t *testing.T) {
	var first atomic.Bool
	fh := &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		ack := vacated(req)
		if !first.Swap(true) {
			ack.Epoch = req.GetEpoch() - 1 // an ack for an older command
		}
		return ack, nil
	}}
	addrs := map[string]string{"node-a": serve(t, fh)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())

	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(testGroup) })
	if len(hns.sink.records(t, "Host ack ignored: epoch or command does not match")) != 1 {
		t.Error("the mismatched ack was not ignored")
	}
	if fh.calls.Load() < 2 {
		t.Error("the command was not retried after a mismatched ack")
	}
}

func TestHostCommand_StaleEpochMovesPast(t *testing.T) {
	// The host saw a much higher epoch, for example from a process whose
	// clock ran ahead.
	hostEpoch := time.Now().Add(time.Hour).UnixNano()
	fh := &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		if req.GetEpoch() <= hostEpoch {
			return &hcpb.HostAck{
				NodeName: req.GetNodeName(), Epoch: req.GetEpoch(), Command: hcpb.Command_COMMAND_VACATE,
				Outcome: hcpb.Outcome_OUTCOME_STALE_EPOCH, CurrentEpoch: hostEpoch,
			}, nil
		}
		return vacated(req), nil
	}}
	addrs := map[string]string{"node-a": serve(t, fh)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(testGroup) })
}

func TestHostCommand_ResumeSupersedesVacate(t *testing.T) {
	release := make(chan struct{})
	var resumes atomic.Int32
	fh := &fakeHost{
		vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
			<-release // the ack arrives after the resume was sent
			return vacated(req), nil
		},
		resume: func(_ context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
			resumes.Add(1)
			return &hcpb.HostAck{
				NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
				Command: hcpb.Command_COMMAND_RESUME, Outcome: hcpb.Outcome_OUTCOME_RESUMED,
			}, nil
		},
	}
	addrs := map[string]string{"node-a": serve(t, fh)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))

	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "vacate sent", func() bool { return fh.calls.Load() == 1 })
	hns.cmd.Resume(testGroup)
	eventually(t, "resume acked", func() bool {
		return hns.sink.hasRecord(t, "Host ack", map[string]any{"node": "node-a", "command": "resume", "outcome": "resumed"})
	})
	close(release)
	time.Sleep(100 * time.Millisecond)

	if got := hns.cmd.HostStates(testGroup)["node-a"]; got != hostcmd.StateLent {
		t.Errorf("state = %v, want lent: a vacate ack for a superseded epoch was acted on", got)
	}
	if !hns.cmd.Lent(testGroup) || hns.cmd.AllClear(testGroup) {
		t.Error("want lent and not clear after resume")
	}
	if !hns.sink.hasRecord(t, "Resume started", map[string]any{"group": testGroup, "node": "node-a"}) {
		t.Error("no Resume started line")
	}
	if resumes.Load() != 1 {
		t.Errorf("resumes = %d, want 1", resumes.Load())
	}
}
