//go:build evalwire

package host_test

import (
	"context"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// recExec records calls; Suspend blocks while gate is set.
type recExec struct {
	mu     sync.Mutex
	guests []string
	calls  []string
	gate   chan struct{}
}

func (e *recExec) record(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, s)
}

func (e *recExec) Guests() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.guests...)
}

func (e *recExec) SetNotReady(_ context.Context, g string) error {
	e.record("notready:" + g)
	return nil
}

func (e *recExec) Suspend(ctx context.Context, g string, _ time.Time) error {
	e.mu.Lock()
	gate := e.gate
	e.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.record("suspend:" + g)
	return nil
}

func (e *recExec) Resume(_ context.Context, g string, _ time.Time) error {
	e.record("resume:" + g)
	return nil
}

func (e *recExec) SetReady(_ context.Context, g string) error {
	e.record("ready:" + g)
	return nil
}

func (e *recExec) callList() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func startHost(t *testing.T, exec *recExec) agentpb.HostCommandServiceClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h, err := host.Start(ctx, host.Config{Node: "n1", ListenAddr: "127.0.0.1:0", Exec: exec})
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		h.Stop()
		cancel()
	})
	return agentpb.NewHostCommandServiceClient(conn)
}

func suspendReq(epoch int64) *agentpb.SuspendAllRequest {
	return &agentpb.SuspendAllRequest{Group: "g", Epoch: epoch, Deadline: timestamppb.New(time.Now().Add(time.Minute))}
}

func resumeReq(epoch int64) *agentpb.ResumeAllRequest {
	return &agentpb.ResumeAllRequest{Group: "g", Epoch: epoch, Deadline: timestamppb.New(time.Now().Add(time.Minute))}
}

// suspend and resume report an RPC error with Errorf, so they may run in
// goroutines; the nil ack they then return reads as an unset outcome.
func suspend(t *testing.T, cl agentpb.HostCommandServiceClient, epoch int64) *agentpb.HostCommandAck {
	t.Helper()
	ack, err := cl.SuspendAll(context.Background(), suspendReq(epoch))
	if err != nil {
		t.Errorf("SuspendAll(epoch %d): %v", epoch, err)
	}
	return ack
}

func resume(t *testing.T, cl agentpb.HostCommandServiceClient, epoch int64) *agentpb.HostCommandAck {
	t.Helper()
	ack, err := cl.ResumeAll(context.Background(), resumeReq(epoch))
	if err != nil {
		t.Errorf("ResumeAll(epoch %d): %v", epoch, err)
	}
	return ack
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNS4_Push_HostSuspendsThenResumesInOrder(t *testing.T) {
	exec := &recExec{guests: []string{"a", "b"}}
	cl := startHost(t, exec)
	ctx := context.Background()

	ack, err := cl.SuspendAll(ctx, suspendReq(10))
	if err != nil || ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR || ack.GetEpoch() != 10 {
		t.Fatalf("SuspendAll = %v, %v", ack, err)
	}
	ack, err = cl.ResumeAll(ctx, resumeReq(11))
	if err != nil || ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_RESUMED || ack.GetEpoch() != 11 {
		t.Fatalf("ResumeAll = %v, %v", ack, err)
	}
	want := []string{"notready:a", "suspend:a", "notready:b", "suspend:b", "resume:a", "ready:a", "resume:b", "ready:b"}
	if got := exec.callList(); !equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func TestNS4_Push_HostEpochRules(t *testing.T) {
	exec := &recExec{guests: []string{"a"}}
	cl := startHost(t, exec)

	if ack := suspend(t, cl, 10); ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR {
		t.Fatalf("first SuspendAll = %v", ack)
	}
	// Same epoch, same command: replayed, nothing runs again.
	if ack := suspend(t, cl, 10); ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR {
		t.Fatalf("replayed SuspendAll = %v", ack)
	}
	// Same epoch, other command: stale.
	if ack := resume(t, cl, 10); ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_STALE_EPOCH {
		t.Fatalf("ResumeAll with a used epoch = %v", ack)
	}
	// Lower epoch: stale.
	if ack := resume(t, cl, 9); ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_STALE_EPOCH {
		t.Fatalf("ResumeAll with a lower epoch = %v", ack)
	}
	if got := exec.callList(); !equal(got, []string{"notready:a", "suspend:a"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestNS4_Push_HostHigherEpochAdoptsOrQueues(t *testing.T) {
	gate := make(chan struct{})
	exec := &recExec{guests: []string{"a"}, gate: gate}
	cl := startHost(t, exec)

	first := make(chan *agentpb.HostCommandAck, 1)
	go func() {
		ack := suspend(t, cl, 10)
		first <- ack
	}()
	waitCalls(t, exec, 1) // notready:a; Suspend now blocks

	// Higher epoch, same command: adopts the running suspend.
	adopted := make(chan *agentpb.HostCommandAck, 1)
	go func() {
		ack := suspend(t, cl, 20)
		adopted <- ack
	}()
	time.Sleep(50 * time.Millisecond)
	// Higher epoch, other command: queued behind the running suspend.
	resumed := make(chan *agentpb.HostCommandAck, 1)
	go func() {
		ack := resume(t, cl, 30)
		resumed <- ack
	}()
	time.Sleep(50 * time.Millisecond)
	if got := exec.callList(); !equal(got, []string{"notready:a"}) {
		t.Fatalf("the running suspend was not left to finish: %v", got)
	}

	close(gate)
	if ack := <-adopted; ack.GetEpoch() != 20 || ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR {
		t.Fatalf("adopting caller ack = %v", ack)
	}
	if ack := <-first; ack.GetEpoch() != 20 {
		t.Fatalf("first caller ack = %v, want the adopted epoch 20 (the orchestrator drops it)", ack)
	}
	if ack := <-resumed; ack.GetEpoch() != 30 || ack.GetOutcome() != agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_RESUMED {
		t.Fatalf("queued ResumeAll ack = %v", ack)
	}
	want := []string{"notready:a", "suspend:a", "resume:a", "ready:a"}
	if got := exec.callList(); !equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func waitCalls(t *testing.T, exec *recExec, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(exec.callList()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d calls, got %v", n, exec.callList())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
