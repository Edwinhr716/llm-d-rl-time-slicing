package hold_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	orchv1 "github.com/edwinhr716/guest-kubelet/api/timeslice_orchestrator/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/hold"
)

// fakeOrch answers GetGroupStatus with a settable state, or an error while down.
type fakeOrch struct {
	mu       sync.Mutex
	state    orchv1.GroupStatus_State
	down     bool
	requests []*orchv1.GetGroupStatusRequest
}

func (f *fakeOrch) set(state orchv1.GroupStatus_State, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state, f.down = state, down
}

func (f *fakeOrch) lastRequest() *orchv1.GetGroupStatusRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return nil
	}
	return f.requests[len(f.requests)-1]
}

func (f *fakeOrch) GetGroupStatus(
	_ context.Context, req *orchv1.GetGroupStatusRequest, _ ...grpc.CallOption,
) (*orchv1.GetGroupStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if f.down {
		return nil, errors.New("orchestrator down")
	}
	return &orchv1.GetGroupStatusResponse{Group: &orchv1.GroupStatus{GroupId: req.GetGroupId(), GroupState: f.state}}, nil
}

// grpcServer serves fakeOrch's GetGroupStatus over the generated stubs.
type grpcServer struct {
	orchv1.UnimplementedTimeSliceOrchestratorServiceServer
	orch *fakeOrch
}

func (s grpcServer) GetGroupStatus(
	ctx context.Context, req *orchv1.GetGroupStatusRequest,
) (*orchv1.GetGroupStatusResponse, error) {
	return s.orch.GetGroupStatus(ctx, req)
}

// unreachableAfter is the watcher's L in these tests.
const unreachableAfter = 300 * time.Millisecond

type decision struct {
	held  bool
	state string
	at    time.Time
}

// startWatcher runs a watcher with short timings and returns the channel of its decisions.
func startWatcher(t *testing.T, client hold.StatusClient) <-chan decision {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	decisions := make(chan decision, 1000)
	watcher := &hold.Watcher{
		Client: client, Group: "g1", Participant: "vk/host-a",
		Interval: 10 * time.Millisecond, UnreachableAfter: unreachableAfter, RPCTimeout: 50 * time.Millisecond,
		OnPoll: func(_ context.Context, held bool, state string) {
			decisions <- decision{held: held, state: state, at: time.Now()}
		},
	}
	done := make(chan error, 1)
	go func() { done <- watcher.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	})
	return decisions
}

// waitFor returns the first decision with the given state, failing after 2 s.
func waitFor(t *testing.T, decisions <-chan decision, state string) decision {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case got := <-decisions:
			if got.state == state {
				return got
			}
		case <-timeout:
			t.Fatalf("no decision with state %s", state)
		}
	}
}

func TestCordonWhileHeld_NsCordon_IsHeld(t *testing.T) {
	want := map[orchv1.GroupStatus_State]bool{
		orchv1.GroupStatus_STATE_LOCKED:    true,
		orchv1.GroupStatus_STATE_SWITCHING: true,
		orchv1.GroupStatus_STATE_VACATING:  true,
	}
	for value := range orchv1.GroupStatus_State_name {
		state := orchv1.GroupStatus_State(value)
		if got := hold.IsHeld(state); got != want[state] {
			t.Errorf("IsHeld(%s) = %v, want %v", state, got, want[state])
		}
	}
}

func TestCordonWhileHeld_NsCordon_WatcherFollowsState(t *testing.T) {
	orch := &fakeOrch{state: orchv1.GroupStatus_STATE_LOCKED}
	decisions := startWatcher(t, orch)
	if got := waitFor(t, decisions, "STATE_LOCKED"); !got.held {
		t.Error("LOCKED must be held")
	}
	req := orch.lastRequest()
	if req.GetGroupId() != "g1" || req.GetParticipantId() != "vk/host-a" {
		t.Errorf("request group=%q participant=%q, want g1 and vk/host-a", req.GetGroupId(), req.GetParticipantId())
	}
	orch.set(orchv1.GroupStatus_STATE_IDLE_YIELDED, false)
	if got := waitFor(t, decisions, "STATE_IDLE_YIELDED"); got.held {
		t.Error("IDLE_YIELDED must not be held")
	}
	orch.set(orchv1.GroupStatus_STATE_VACATING, false)
	if got := waitFor(t, decisions, "STATE_VACATING"); !got.held {
		t.Error("VACATING must be held")
	}
	orch.set(orchv1.GroupStatus_STATE_BACKGROUND, false)
	if got := waitFor(t, decisions, "STATE_BACKGROUND"); got.held {
		t.Error("BACKGROUND must not be held")
	}
}

func TestCordonWhileHeld_NsCordon_WatcherFailsClosed(t *testing.T) {
	orch := &fakeOrch{down: true}
	start := time.Now()
	decisions := startWatcher(t, orch)
	got := waitFor(t, decisions, hold.Unreachable)
	if !got.held {
		t.Error("an unreachable orchestrator must count as held")
	}
	if elapsed := got.at.Sub(start); elapsed < unreachableAfter {
		t.Errorf("UNREACHABLE after %v, want no decision before UnreachableAfter (%v)", elapsed, unreachableAfter)
	}
}

func TestCordonWhileHeld_NsCordon_WatcherShortOutageNoDecision(t *testing.T) {
	orch := &fakeOrch{state: orchv1.GroupStatus_STATE_IDLE}
	decisions := startWatcher(t, orch)
	waitFor(t, decisions, "STATE_IDLE")
	orch.set(orchv1.GroupStatus_STATE_IDLE, true)
	time.Sleep(50 * time.Millisecond) // let a poll answered before the outage drain
	for len(decisions) > 0 {
		<-decisions
	}
	time.Sleep(150 * time.Millisecond) // outage so far about 200ms, shorter than unreachableAfter
	if n := len(decisions); n != 0 {
		t.Errorf("%d decisions during a short outage, want none", n)
	}
	waitFor(t, decisions, hold.Unreachable)
	orch.set(orchv1.GroupStatus_STATE_IDLE, false)
	if got := waitFor(t, decisions, "STATE_IDLE"); got.held {
		t.Error("IDLE after recovery must not be held")
	}
}

func TestCordonWhileHeld_NsCordon_WatcherOverGRPC(t *testing.T) {
	orch := &fakeOrch{state: orchv1.GroupStatus_STATE_SWITCHING}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	orchv1.RegisterTimeSliceOrchestratorServiceServer(srv, grpcServer{orch: orch})
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	decisions := startWatcher(t, orchv1.NewTimeSliceOrchestratorServiceClient(conn))
	if got := waitFor(t, decisions, "STATE_SWITCHING"); !got.held {
		t.Error("SWITCHING must be held")
	}
	if p := orch.lastRequest().GetParticipantId(); p != "vk/host-a" {
		t.Errorf("participant_id over gRPC = %q, want vk/host-a", p)
	}
}
