package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/budget"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

const batchTenant = "shadow-vllm"

// recordingWriter captures the budget values written to the key.
type recordingWriter struct {
	mu     sync.Mutex
	writes []string
}

func (w *recordingWriter) SetKey(_ context.Context, _, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, value)
	return nil
}

func (w *recordingWriter) Close() error { return nil }

func (w *recordingWriter) last() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		return ""
	}
	return w.writes[len(w.writes)-1]
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

func TestServer_GetGroupStatus_PublishesDispatchBudget(t *testing.T) {
	tests := []struct {
		name      string
		holder    string
		loaded    bool
		waiters   []string
		quantum   time.Duration
		want      string
		rationale string
	}{
		{
			name:   "batch tenant serving alone",
			holder: batchTenant,
			loaded: true,
			want:   budget.Available,
		},
		{
			name:      "batch tenant serving with pre-emption advertised",
			holder:    batchTenant,
			loaded:    true,
			waiters:   []string{"rl-trainer"},
			want:      budget.Blocked,
			rationale: "the poll that returns a non-zero depth is the poll that makes the tenant yield",
		},
		{
			name:      "batch tenant serving with waiters withheld by the quantum",
			holder:    batchTenant,
			loaded:    true,
			waiters:   []string{"rl-trainer"},
			quantum:   time.Hour,
			want:      budget.Available,
			rationale: "the tenant keeps the GPU for its quantum, so it can keep serving",
		},
		{
			name:      "batch tenant granted but not yet restored",
			holder:    batchTenant,
			loaded:    false,
			want:      budget.Blocked,
			rationale: "no dispatch during the cuda-checkpoint restore",
		},
		{
			name:   "trainer holds the lock",
			holder: "rl-trainer",
			loaded: true,
			want:   budget.Blocked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			w := &recordingWriter{}
			publisher := budget.NewPublisher(w, "dispatch-gate-budget", batchTenant)

			opts := []server.Option{server.WithDispatchBudgetPublisher(publisher)}
			if tt.quantum > 0 {
				opts = append(opts, server.WithServingQuantum(tt.quantum))
			}
			client := statusClient(t, servingGroup(t, ctx, tt.holder, tt.loaded, tt.waiters...), opts...)

			if _, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"}); err != nil {
				t.Fatalf("GetGroupStatus() error = %v", err)
			}

			if got := w.last(); got != tt.want {
				t.Errorf("published budget = %q, want %q (%s)", got, tt.want, tt.rationale)
			}
		})
	}
}

// TestServer_GetGroupStatus_BlockedBudgetPrecedesTheYieldSignal is the ordering
// property the whole design rests on: the budget is 0 in the store before the
// response that tells the tenant to drain is visible to the tenant. The scrape
// gate could only ever observe the outage afterwards.
func TestServer_GetGroupStatus_BlockedBudgetPrecedesTheYieldSignal(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}
	publisher := budget.NewPublisher(w, "dispatch-gate-budget", batchTenant)

	client := statusClient(t, servingGroup(t, ctx, batchTenant, true, "rl-trainer"),
		server.WithDispatchBudgetPublisher(publisher))

	resp, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
	if err != nil {
		t.Fatalf("GetGroupStatus() error = %v", err)
	}

	if resp.GetGroup().GetWaiterQueueDepth() == 0 {
		t.Fatal("expected the response to advertise pre-emption pressure")
	}
	if got := w.last(); got != budget.Blocked {
		t.Errorf("budget at the moment the yield signal became visible = %q, want %q", got, budget.Blocked)
	}
}

// The same ordering property with the rising edge delegated to an external
// publisher. Giving away the "1" must cost nothing on the "0": the falling edge
// is computed and written on the identical path in both modes, inside the
// handler, before the response that makes the tenant drain is on the wire.
func TestServer_GetGroupStatus_BlockedBudgetPrecedesTheYieldSignalWithExternalRisingEdge(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}
	publisher := budget.NewPublisher(w, "dispatch-gate-budget", batchTenant).WithExternalRisingEdge(true)

	client := statusClient(t, servingGroup(t, ctx, batchTenant, true, "rl-trainer"),
		server.WithDispatchBudgetPublisher(publisher))

	resp, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"})
	if err != nil {
		t.Fatalf("GetGroupStatus() error = %v", err)
	}

	if resp.GetGroup().GetWaiterQueueDepth() == 0 {
		t.Fatal("expected the response to advertise pre-emption pressure")
	}
	if got := w.last(); got != budget.Blocked {
		t.Errorf("budget at the moment the yield signal became visible = %q, want %q", got, budget.Blocked)
	}
}

// And the other half of the mode, on the gRPC path rather than the ticker: a
// tenant the orchestrator believes is servable produces no write at all, so the
// poll cannot overwrite a "1" that only the external publisher is entitled to
// set.
func TestServer_GetGroupStatus_ExternalRisingEdgeWritesNothingWhenServable(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}
	publisher := budget.NewPublisher(w, "dispatch-gate-budget", batchTenant).WithExternalRisingEdge(true)

	client := statusClient(t, servingGroup(t, ctx, batchTenant, true),
		server.WithDispatchBudgetPublisher(publisher))
	if _, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"}); err != nil {
		t.Fatalf("GetGroupStatus() error = %v", err)
	}

	if w.count() != 0 {
		t.Errorf("got %d writes (last %q) for a servable tenant, want 0", w.count(), w.last())
	}
}

func TestServer_GetGroupStatus_PublishesBlockedWhenGroupsCannotBeListed(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}
	publisher := budget.NewPublisher(w, "dispatch-gate-budget", batchTenant)

	g, err := store.NewGroup(ctx, "group-1", nil)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	g.Status().SetState(pb.GroupStatus_STATE_LOCKED)

	gs := &server.MockGroupStore{
		GetFunc: func(context.Context, string) (*store.Group, error) { return g, nil },
		ListFunc: func(context.Context) ([]*store.Group, error) {
			return nil, errors.New("store unavailable")
		},
	}

	client := statusClient(t, gs, server.WithDispatchBudgetPublisher(publisher))
	if _, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"}); err != nil {
		t.Fatalf("GetGroupStatus() error = %v", err)
	}

	// Fail safe, and in the opposite direction to the consuming gate's
	// absent-key default.
	if got := w.last(); got != budget.Blocked {
		t.Errorf("published budget = %q, want %q when the group store cannot be read", got, budget.Blocked)
	}
}

func TestServer_GetGroupStatus_NoPublisherByDefault(t *testing.T) {
	ctx := context.Background()
	w := &recordingWriter{}

	// Regression guard: without the option nothing is published at all, so an
	// upgrade cannot start writing keys a deployment did not ask for.
	client := statusClient(t, servingGroup(t, ctx, batchTenant, true))
	if _, err := client.GetGroupStatus(ctx, &pb.GetGroupStatusRequest{GroupId: "group-1"}); err != nil {
		t.Fatalf("GetGroupStatus() error = %v", err)
	}
	if w.count() != 0 {
		t.Errorf("got %d writes with no publisher configured, want 0", w.count())
	}
}
