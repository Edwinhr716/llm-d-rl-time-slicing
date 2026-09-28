package hostcmd_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
)

type call struct {
	node    string
	command string
	epoch   int64
}

// fakeClient answers host commands with a per-node script. A script entry
// returns the ack (or error) for one call; the last entry repeats. A nil
// script blocks until the call's context is done.
type fakeClient struct {
	mu      sync.Mutex
	calls   []call
	suspend map[string][]func(ctx context.Context, epoch int64) (*agentpb.HostCommandAck, error)
	resume  map[string][]func(ctx context.Context, epoch int64) (*agentpb.HostCommandAck, error)
}

type step = func(ctx context.Context, epoch int64) (*agentpb.HostCommandAck, error)

func newFakeClient() *fakeClient {
	return &fakeClient{suspend: map[string][]step{}, resume: map[string][]step{}}
}

func ack(outcome agentpb.HostCommandOutcome) step {
	return func(_ context.Context, epoch int64) (*agentpb.HostCommandAck, error) {
		return &agentpb.HostCommandAck{Epoch: epoch, Outcome: outcome}, nil
	}
}

func blockUntil(ch <-chan struct{}, then step) step {
	return func(ctx context.Context, epoch int64) (*agentpb.HostCommandAck, error) {
		select {
		case <-ch:
			return then(ctx, epoch)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func hang(ctx context.Context, _ int64) (*agentpb.HostCommandAck, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeClient) next(script map[string][]step, node, command string, epoch int64) step {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{node: node, command: command, epoch: epoch})
	steps := script[node]
	if len(steps) == 0 {
		return hang
	}
	s := steps[0]
	if len(steps) > 1 {
		script[node] = steps[1:]
	}
	return s
}

func (f *fakeClient) SuspendAll(
	ctx context.Context, node string, req *agentpb.SuspendAllRequest,
) (*agentpb.HostCommandAck, error) {
	return f.next(f.suspend, node, hostcmd.CommandSuspendAll, req.GetEpoch())(ctx, req.GetEpoch())
}

func (f *fakeClient) ResumeAll(
	ctx context.Context, node string, req *agentpb.ResumeAllRequest,
) (*agentpb.HostCommandAck, error) {
	return f.next(f.resume, node, hostcmd.CommandResumeAll, req.GetEpoch())(ctx, req.GetEpoch())
}

func (f *fakeClient) callsFor(node, command string) []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []call
	for _, c := range f.calls {
		if c.node == node && c.command == command {
			out = append(out, c)
		}
	}
	return out
}

// recorder captures log records.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // slog.Handler.Handle signature requires passing Record by value
func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

// find returns the attributes of every record with message msg.
func (r *recorder) find(msg string) []map[string]slog.Value {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []map[string]slog.Value
	for i := range r.records {
		rec := &r.records[i]
		if rec.Message != msg {
			continue
		}
		attrs := map[string]slog.Value{"level": slog.StringValue(rec.Level.String())}
		rec.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value
			return true
		})
		out = append(out, attrs)
	}
	return out
}

func captureLogs(t *testing.T) *recorder {
	t.Helper()
	rec := &recorder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

type enqueueCounter struct {
	mu     sync.Mutex
	groups []string
	ch     chan string
}

func newEnqueueCounter() *enqueueCounter { return &enqueueCounter{ch: make(chan string, 16)} }

func (e *enqueueCounter) enqueue(group string) {
	e.mu.Lock()
	e.groups = append(e.groups, group)
	e.mu.Unlock()
	e.ch <- group
}

func (e *enqueueCounter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.groups)
}

func newCommander(client hostcmd.Client, enq *enqueueCounter, n, k time.Duration) *hostcmd.Commander {
	return hostcmd.NewCommander(hostcmd.Config{
		Client:        client,
		NoticeWindow:  n,
		KillBudget:    k,
		Enqueue:       enq.enqueue,
		RetryInterval: 10 * time.Millisecond,
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNS4_Push_VacateWaitsForEveryAck(t *testing.T) {
	rec := captureLogs(t)
	client := newFakeClient()
	release := map[string]chan struct{}{"n1": make(chan struct{}), "n2": make(chan struct{}), "n3": make(chan struct{})}
	for node, ch := range release {
		client.suspend[node] = []step{blockUntil(ch, ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR))}
	}
	enq := newEnqueueCounter()
	cmd := newCommander(client, enq, 30*time.Second, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	noticeAt := time.Now()
	if cmd.Vacate(ctx, "g", []string{"n3", "n1", "n2"}, noticeAt) {
		t.Fatal("Vacate reported clear before any ack")
	}
	// A second call over the same nodes keeps the round.
	if cmd.Vacate(ctx, "g", []string{"n1", "n2", "n3"}, noticeAt) {
		t.Fatal("second Vacate reported clear before any ack")
	}
	waitFor(t, "a command to every host", func() bool {
		return len(client.callsFor("n1", hostcmd.CommandSuspendAll)) == 1 &&
			len(client.callsFor("n2", hostcmd.CommandSuspendAll)) == 1 &&
			len(client.callsFor("n3", hostcmd.CommandSuspendAll)) == 1
	})

	close(release["n1"])
	close(release["n2"])
	waitFor(t, "two hosts clear", func() bool { return len(rec.find("Host clear")) == 2 })
	if cmd.Clear("g") {
		t.Fatal("Clear with one host not acked")
	}
	if enq.count() != 0 {
		t.Fatal("enqueued before every host acked")
	}

	close(release["n3"])
	select {
	case g := <-enq.ch:
		if g != "g" {
			t.Fatalf("enqueued %q, want g", g)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not enqueued after the last ack")
	}
	if !cmd.Clear("g") {
		t.Fatal("not Clear after every ack")
	}
	if !cmd.Vacate(ctx, "g", []string{"n1", "n2", "n3"}, noticeAt) {
		t.Fatal("Vacate after clear started a new round")
	}

	started := rec.find("Vacate started")
	if len(started) != 1 {
		t.Fatalf("Vacate started logged %d times, want 1", len(started))
	}
	if got := started[0]["hosts"].Int64(); got != 3 {
		t.Errorf("Vacate started hosts = %d, want 3", got)
	}
	wantDeadline := noticeAt.Add(27 * time.Second)
	if got := started[0]["deadline"].Time(); !got.Equal(wantDeadline) {
		t.Errorf("Vacate started deadline = %v, want %v", got, wantDeadline)
	}
	for _, a := range rec.find("Host clear") {
		if a["how"].String() != "ack" || a["group"].String() != "g" {
			t.Errorf("Host clear attrs = %v", a)
		}
	}
	if n := len(rec.find("Host command sent")); n != 3 {
		t.Errorf("Host command sent logged %d times, want 3", n)
	}
	if n := len(rec.find("Host ack")); n != 3 {
		t.Errorf("Host ack logged %d times, want 3", n)
	}
}

func TestNS4_Push_AckForOtherEpochIsDropped(t *testing.T) {
	captureLogs(t)
	client := newFakeClient()
	wrongEpoch := func(_ context.Context, epoch int64) (*agentpb.HostCommandAck, error) {
		return &agentpb.HostCommandAck{Epoch: epoch - 1, Outcome: agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR}, nil
	}
	client.suspend["n1"] = []step{wrongEpoch, wrongEpoch, ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	enq := newEnqueueCounter()
	cmd := newCommander(client, enq, 30*time.Second, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "clear", func() bool { return cmd.Clear("g") })
	calls := client.callsFor("n1", hostcmd.CommandSuspendAll)
	if len(calls) != 3 {
		t.Fatalf("got %d calls, want 3 (two dropped acks, then CLEAR)", len(calls))
	}
	for _, c := range calls[1:] {
		if c.epoch != calls[0].epoch {
			t.Errorf("a dropped ack changed the epoch: %d then %d", calls[0].epoch, c.epoch)
		}
	}
}

func TestNS4_Push_UnreachableHostIsRetriedWithSameEpoch(t *testing.T) {
	captureLogs(t)
	client := newFakeClient()
	unreachable := func(context.Context, int64) (*agentpb.HostCommandAck, error) {
		return nil, errors.New("connection refused")
	}
	client.suspend["n1"] = []step{unreachable, unreachable, ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	enq := newEnqueueCounter()
	cmd := newCommander(client, enq, 30*time.Second, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "clear", func() bool { return cmd.Clear("g") })
	calls := client.callsFor("n1", hostcmd.CommandSuspendAll)
	if len(calls) != 3 {
		t.Fatalf("got %d calls, want 3", len(calls))
	}
	if calls[0].epoch != calls[2].epoch {
		t.Errorf("transport retries must reuse the epoch (join): %d then %d", calls[0].epoch, calls[2].epoch)
	}
}

func TestNS4_Push_FailedAckIsRetriedWithNewEpoch(t *testing.T) {
	captureLogs(t)
	client := newFakeClient()
	client.suspend["n1"] = []step{
		ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_FAILED),
		ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR),
	}
	enq := newEnqueueCounter()
	cmd := newCommander(client, enq, 30*time.Second, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "clear", func() bool { return cmd.Clear("g") })
	calls := client.callsFor("n1", hostcmd.CommandSuspendAll)
	if len(calls) != 2 || calls[1].epoch <= calls[0].epoch {
		t.Fatalf("want a retry with a higher epoch after FAILED, got %+v", calls)
	}
}

func TestNS4_Push_HostNotClearAtDeadlineIsLogged(t *testing.T) {
	rec := captureLogs(t)
	client := newFakeClient()
	client.suspend["fast"] = []step{ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	client.suspend["hung"] = []step{hang}
	enq := newEnqueueCounter()
	// T = notice + N - K = notice + 100 ms.
	cmd := newCommander(client, enq, 300*time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.Vacate(ctx, "g", []string{"fast", "hung"}, time.Now())
	waitFor(t, "deadline warning", func() bool { return len(rec.find("Host not clear at deadline")) > 0 })
	warns := rec.find("Host not clear at deadline")
	if len(warns) != 1 || warns[0]["node"].String() != "hung" || warns[0]["level"].String() != "WARN" {
		t.Fatalf("deadline warnings = %v, want one WARN for node hung", warns)
	}
	if cmd.Clear("g") {
		t.Fatal("Clear with a hung host (there is no kill path)")
	}
}

func TestNS4_Push_ResumeReplacesVacateRound(t *testing.T) {
	rec := captureLogs(t)
	client := newFakeClient()
	client.suspend["n1"] = []step{hang}
	client.resume["n1"] = []step{ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_RESUMED)}
	enq := newEnqueueCounter()
	cmd := newCommander(client, enq, 30*time.Second, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "suspend sent", func() bool { return len(client.callsFor("n1", hostcmd.CommandSuspendAll)) == 1 })
	cmd.Resume(ctx, "g", []string{"n1"})
	cmd.Resume(ctx, "g", []string{"n1"}) // kept: same nodes
	waitFor(t, "resume acked", func() bool {
		for _, a := range rec.find("Host ack") {
			if a["command"].String() == hostcmd.CommandResumeAll {
				return true
			}
		}
		return false
	})
	if cmd.Clear("g") {
		t.Fatal("Clear after Resume")
	}
	suspends := client.callsFor("n1", hostcmd.CommandSuspendAll)
	resumes := client.callsFor("n1", hostcmd.CommandResumeAll)
	if len(resumes) != 1 || resumes[0].epoch <= suspends[0].epoch {
		t.Fatalf("resume must use a higher epoch than the vacate: suspends %+v resumes %+v", suspends, resumes)
	}
	if n := len(rec.find("Resume started")); n != 1 {
		t.Errorf("Resume started logged %d times, want 1", n)
	}

	// The next vacate is a new round with a higher epoch still.
	client.mu.Lock()
	client.suspend["n1"] = []step{ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	client.mu.Unlock()
	cmd.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "clear", func() bool { return cmd.Clear("g") })
	suspends = client.callsFor("n1", hostcmd.CommandSuspendAll)
	if last := suspends[len(suspends)-1]; last.epoch <= resumes[0].epoch {
		t.Errorf("second vacate epoch %d not above resume epoch %d", last.epoch, resumes[0].epoch)
	}
}

func TestNS4_Push_NoHostsIsClear(t *testing.T) {
	captureLogs(t)
	cmd := newCommander(newFakeClient(), newEnqueueCounter(), 30*time.Second, 3*time.Second)
	if !cmd.Vacate(context.Background(), "g", nil, time.Now()) || !cmd.Clear("g") {
		t.Fatal("a group without hosts must be clear at once")
	}
}

func TestNS4_Push_UnknownStateIsNotClear(t *testing.T) {
	cmd := newCommander(newFakeClient(), newEnqueueCounter(), 30*time.Second, 3*time.Second)
	if cmd.Clear("g") {
		t.Fatal("a fresh commander (orchestrator restart) must not report hosts clear")
	}
}

func TestNS4_Push_EpochsIncreaseAcrossRestart(t *testing.T) {
	captureLogs(t)
	first := newFakeClient()
	first.suspend["n1"] = []step{ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c1 := newCommander(first, newEnqueueCounter(), 30*time.Second, 3*time.Second)
	c1.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "first clear", func() bool { return c1.Clear("g") })

	time.Sleep(time.Millisecond)
	second := newFakeClient()
	second.suspend["n1"] = []step{ack(agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR)}
	c2 := newCommander(second, newEnqueueCounter(), 30*time.Second, 3*time.Second)
	c2.Vacate(ctx, "g", []string{"n1"}, time.Now())
	waitFor(t, "second clear", func() bool { return c2.Clear("g") })

	e1 := first.callsFor("n1", hostcmd.CommandSuspendAll)[0].epoch
	e2 := second.callsFor("n1", hostcmd.CommandSuspendAll)[0].epoch
	if e2 <= e1 {
		t.Fatalf("restarted commander epoch %d not above %d", e2, e1)
	}
}
