package budget

// Internal, for the same reason as the open-delay tests: the interaction
// between the two rising-edge settings is only observable through the clock and
// the counters.

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The whole point of the mode, stated as writes: every Blocked lands, every
// Available disappears. The Blocked writes are not deduplicated, because the
// consuming gate reads an absent key as full capacity and this publisher is the
// only thing that will ever recreate it closed.
func TestPublisher_ExternalRisingEdgeWrites(t *testing.T) {
	tests := []struct {
		name      string
		external  bool
		openDelay time.Duration
		publish   []string
		want      []string
		wantHeld  float64
		wantSkips float64
		rationale string
	}{
		{
			name:      "available is not written at all",
			external:  true,
			publish:   []string{Available},
			want:      nil,
			wantSkips: 1,
			rationale: "writing 0 here would fight the external publisher; writing 1 is the bug being fixed",
		},
		{
			name:      "blocked is written on every call including repeats",
			external:  true,
			publish:   []string{Blocked, Blocked, Blocked},
			want:      []string{Blocked, Blocked, Blocked},
			rationale: "the idempotent rewrite is what survives an eviction",
		},
		{
			name:      "falling edge lands, rising edge does not",
			external:  true,
			publish:   []string{Blocked, Available, Available, Blocked},
			want:      []string{Blocked, Blocked},
			wantSkips: 2,
			rationale: "the orchestrator keeps the edge it can prove and gives away the one it cannot",
		},
		{
			name:      "open delay is not applied when the rising edge is external",
			external:  true,
			openDelay: 2 * time.Second,
			publish:   []string{Available, Blocked, Available},
			want:      []string{Blocked},
			wantHeld:  0,
			wantSkips: 2,
			rationale: "a hold-down on an edge that is never written is dead configuration, not a second gate",
		},
		{
			name:      "disabled is the existing behaviour",
			external:  false,
			publish:   []string{Blocked, Available, Available, Blocked},
			want:      []string{Blocked, Available, Available, Blocked},
			rationale: "the default must not change any deployment that did not ask for this",
		},
		{
			name:      "disabled with an open delay still holds the rising edge",
			external:  false,
			openDelay: 2 * time.Second,
			publish:   []string{Available},
			want:      []string{Blocked},
			wantHeld:  1,
			rationale: "turning the flag off must leave the hold-down exactly as it was",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &recordingWriter{}
			clk := &fixedClock{t: time.Unix(1790104764, 0)}
			p := withClock(NewPublisher(w, "k", "shadow-vllm").
				WithOpenDelay(tt.openDelay).
				WithExternalRisingEdge(tt.external), clk)
			ctx := context.Background()

			held := testutil.ToFloat64(metrics.DispatchBudgetHeldTotal)
			skips := testutil.ToFloat64(metrics.DispatchBudgetRisingEdgeSkippedTotal)

			for _, v := range tt.publish {
				mustPublish(t, p, ctx, v)
			}

			got := w.recorded()
			if len(got) != len(tt.want) {
				t.Fatalf("got %d writes %v, want %d %v (%s)", len(got), got, len(tt.want), tt.want, tt.rationale)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("write %d = %q, want %q (%s)", i, got[i], tt.want[i], tt.rationale)
				}
			}

			if d := testutil.ToFloat64(metrics.DispatchBudgetHeldTotal) - held; d != tt.wantHeld {
				t.Errorf("held counter rose by %v, want %v", d, tt.wantHeld)
			}
			if d := testutil.ToFloat64(metrics.DispatchBudgetRisingEdgeSkippedTotal) - skips; d != tt.wantSkips {
				t.Errorf("skipped counter rose by %v, want %v", d, tt.wantSkips)
			}
		})
	}
}

// A skipped write is not a write, and must not be counted as one in either
// direction: an "ok" would overstate how current the key is, an "error" would
// page somebody for the mode working as designed.
func TestPublisher_ExternalRisingEdgeSkipIsNotAWrite(t *testing.T) {
	w := &recordingWriter{}
	p := NewPublisher(w, "k", "shadow-vllm").WithExternalRisingEdge(true)

	ok := testutil.ToFloat64(metrics.DispatchBudgetWritesTotal.WithLabelValues("ok"))
	failed := testutil.ToFloat64(metrics.DispatchBudgetWritesTotal.WithLabelValues("error"))

	mustPublish(t, p, context.Background(), Available)

	if d := testutil.ToFloat64(metrics.DispatchBudgetWritesTotal.WithLabelValues("ok")) - ok; d != 0 {
		t.Errorf("ok write counter rose by %v on a skipped write, want 0", d)
	}
	if d := testutil.ToFloat64(metrics.DispatchBudgetWritesTotal.WithLabelValues("error")) - failed; d != 0 {
		t.Errorf("error write counter rose by %v on a skipped write, want 0", d)
	}
}

// Last() is a statement about this publisher's own writes, not about the key.
// In this mode the two genuinely diverge — the external publisher may have set
// the key to 1 — and reporting anything other than what was written here would
// be inventing knowledge the orchestrator gave up on purpose.
func TestPublisher_ExternalRisingEdgeLastTracksOwnWritesOnly(t *testing.T) {
	w := &recordingWriter{}
	p := NewPublisher(w, "k", "shadow-vllm").WithExternalRisingEdge(true)
	ctx := context.Background()

	mustPublish(t, p, ctx, Blocked)
	if p.Last() != Blocked {
		t.Fatalf("Last() = %q, want %q", p.Last(), Blocked)
	}

	mustPublish(t, p, ctx, Available)
	if p.Last() != Blocked {
		t.Errorf("Last() after a skipped Available = %q, want the last value actually written, %q", p.Last(), Blocked)
	}
}

func TestPublisher_ExternalRisingEdgeDefaultsOff(t *testing.T) {
	p := NewPublisher(&recordingWriter{}, "k", "shadow-vllm")
	if p.ExternalRisingEdge() {
		t.Error("ExternalRisingEdge() = true by default, want false")
	}
	if !p.WithExternalRisingEdge(true).ExternalRisingEdge() {
		t.Error("ExternalRisingEdge() = false after WithExternalRisingEdge(true)")
	}
}

func TestPublisher_WithExternalRisingEdgeIsNilSafe(t *testing.T) {
	var p *Publisher
	if got := p.WithExternalRisingEdge(true); got != nil {
		t.Errorf("WithExternalRisingEdge() on nil = %v, want nil", got)
	}
	if p.ExternalRisingEdge() {
		t.Error("ExternalRisingEdge() on nil = true, want false")
	}
}
