//go:build evalwire

package host_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/evalwire/host"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// recExec records every Executor call per guest. A non-nil block channel
// makes Suspend wait for it (or for its context).
type recExec struct {
	guests []string

	mu        sync.Mutex
	calls     map[string][]string
	deadlines []time.Time
	block     chan struct{}
	blocked   chan string
}

func newRecExec(guests ...string) *recExec {
	return &recExec{guests: guests, calls: map[string][]string{}, blocked: make(chan string, 16)}
}

func (e *recExec) record(guest, call string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[guest] = append(e.calls[guest], call)
}

func (e *recExec) callsOf(guest string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.calls[guest])
}

func (e *recExec) Guests() []string { return e.guests }

func (e *recExec) SetNotReady(_ context.Context, guest string) error {
	e.record(guest, "notready")
	return nil
}

func (e *recExec) Suspend(ctx context.Context, guest string, deadline time.Time) error {
	e.mu.Lock()
	block := e.block
	e.deadlines = append(e.deadlines, deadline)
	e.mu.Unlock()
	if block != nil {
		e.blocked <- guest
		select {
		case <-block:
		case <-ctx.Done():
			e.record(guest, "suspend-aborted")
			return ctx.Err()
		}
	}
	e.record(guest, "suspend")
	return nil
}

func (e *recExec) Resume(_ context.Context, guest string, _ time.Time) error {
	e.record(guest, "resume")
	return nil
}

func (e *recExec) SetReady(_ context.Context, guest string) error {
	e.record(guest, "ready")
	return nil
}

func startHost(t *testing.T, exec host.Executor) hcpb.HostCommandServiceClient {
	t.Helper()
	hst, err := host.Start(context.Background(), host.Config{Node: "node-a", ListenAddr: "127.0.0.1:0", Exec: exec})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hst.Stop)
	conn, err := grpc.NewClient(hst.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	})
	return hcpb.NewHostCommandServiceClient(conn)
}

func vacate(t *testing.T, client hcpb.HostCommandServiceClient, epoch int64, deadline time.Time) *hcpb.HostAck {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ack, err := client.Vacate(ctx, &hcpb.VacateRequest{
		GroupId: "g", NodeName: "node-a", Epoch: epoch, Deadline: timestamppb.New(deadline),
	})
	if err != nil {
		t.Errorf("Vacate(%d): %v", epoch, err)
		return nil
	}
	return ack
}

func resume(t *testing.T, client hcpb.HostCommandServiceClient, epoch int64) *hcpb.HostAck {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ack, err := client.Resume(ctx, &hcpb.ResumeRequest{
		GroupId: "g", NodeName: "node-a", Epoch: epoch, Deadline: timestamppb.New(time.Now().Add(time.Minute)),
	})
	if err != nil {
		t.Errorf("Resume(%d): %v", epoch, err)
		return nil
	}
	return ack
}

func TestNS4_Push_HostVacatesNotReadyThenSuspend(t *testing.T) {
	exec := newRecExec("guest-1", "guest-2")
	client := startHost(t, exec)
	deadline := time.Now().Add(27 * time.Second).Truncate(time.Microsecond)

	ack := vacate(t, client, 1, deadline)
	if ack.GetOutcome() != hcpb.Outcome_OUTCOME_VACATED || ack.GetEpoch() != 1 ||
		ack.GetCommand() != hcpb.Command_COMMAND_VACATE || ack.GetNodeName() != "node-a" {
		t.Fatalf("ack = %v, want VACATED for epoch 1", ack)
	}
	for _, guest := range exec.guests {
		if got := exec.callsOf(guest); !slices.Equal(got, []string{"notready", "suspend"}) {
			t.Errorf("%s calls = %v, want [notready suspend]", guest, got)
		}
	}
	for _, got := range exec.deadlines {
		if !got.Equal(deadline) {
			t.Errorf("Suspend deadline = %v, want %v", got, deadline)
		}
	}

	// Commands are idempotent: suspended guests are not suspended again.
	if ack := vacate(t, client, 2, deadline); ack.GetOutcome() != hcpb.Outcome_OUTCOME_VACATED {
		t.Fatalf("second ack = %v, want VACATED", ack)
	}
	if got := exec.callsOf("guest-1"); len(got) != 2 {
		t.Errorf("guest-1 calls = %v after a repeated vacate, want no new calls", got)
	}

	ack = resume(t, client, 3)
	if ack.GetOutcome() != hcpb.Outcome_OUTCOME_RESUMED || ack.GetCommand() != hcpb.Command_COMMAND_RESUME {
		t.Fatalf("resume ack = %v, want RESUMED", ack)
	}
	for _, guest := range exec.guests {
		want := []string{"notready", "suspend", "resume", "ready"}
		if got := exec.callsOf(guest); !slices.Equal(got, want) {
			t.Errorf("%s calls = %v, want %v", guest, got, want)
		}
	}
}

func TestNS4_Push_HostFencesStaleEpoch(t *testing.T) {
	exec := newRecExec("guest-1")
	client := startHost(t, exec)
	vacate(t, client, 10, time.Now().Add(time.Minute))

	ack := resume(t, client, 5)
	if ack.GetOutcome() != hcpb.Outcome_OUTCOME_STALE_EPOCH || ack.GetCurrentEpoch() != 10 {
		t.Fatalf("ack = %v, want STALE_EPOCH with current epoch 10", ack)
	}
	if got := exec.callsOf("guest-1"); slices.Contains(got, "resume") {
		t.Errorf("a stale resume acted: %v", got)
	}
}

func TestNS4_Push_HostJoinsRunningVacate(t *testing.T) {
	exec := newRecExec("guest-1")
	exec.block = make(chan struct{})
	client := startHost(t, exec)
	deadline := time.Now().Add(time.Minute)

	acks := make(chan *hcpb.HostAck, 2)
	go func() { acks <- vacate(t, client, 1, deadline) }()
	<-exec.blocked
	go func() { acks <- vacate(t, client, 2, deadline) }()
	time.Sleep(100 * time.Millisecond)
	close(exec.block)

	epochs := map[int64]bool{}
	for range 2 {
		ack := <-acks
		if ack.GetOutcome() != hcpb.Outcome_OUTCOME_VACATED {
			t.Fatalf("ack = %v, want VACATED", ack)
		}
		epochs[ack.GetEpoch()] = true
	}
	if !epochs[1] || !epochs[2] {
		t.Errorf("acked epochs = %v, want each caller's epoch", epochs)
	}
	if got := exec.callsOf("guest-1"); !slices.Equal(got, []string{"notready", "suspend"}) {
		t.Errorf("calls = %v, want one suspend for the joined vacate", got)
	}
}

func TestNS4_Push_HostResumeAbortsVacate(t *testing.T) {
	exec := newRecExec("guest-1")
	exec.block = make(chan struct{}) // never closed: the vacate hangs
	client := startHost(t, exec)

	vacated := make(chan *hcpb.HostAck, 1)
	go func() { vacated <- vacate(t, client, 1, time.Now().Add(time.Minute)) }()
	<-exec.blocked

	if ack := resume(t, client, 2); ack.GetOutcome() != hcpb.Outcome_OUTCOME_RESUMED {
		t.Fatalf("resume ack = %v, want RESUMED", ack)
	}
	if ack := <-vacated; ack.GetOutcome() != hcpb.Outcome_OUTCOME_ABORTED {
		t.Fatalf("vacate ack = %v, want ABORTED", ack)
	}
	want := []string{"notready", "suspend-aborted", "resume", "ready"}
	if got := exec.callsOf("guest-1"); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}
