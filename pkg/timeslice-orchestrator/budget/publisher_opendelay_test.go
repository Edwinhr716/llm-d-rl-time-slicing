package budget

// Internal, because the hold-down is a function of wall-clock time and the
// only honest way to test it is to control the clock.

import (
	"context"
	"sync"
	"testing"
	"time"
)

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

func (w *recordingWriter) recorded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.writes...)
}

// fixedClock is a hand-cranked clock.
type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fixedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func withClock(p *Publisher, c *fixedClock) *Publisher {
	p.now = c.now
	return p
}

// The headline behaviour: an open delay must suppress the rising edge for
// exactly as long as it is configured for, and must not touch the falling one.
//
// The falling edge is where this signal's whole advantage lives — it is
// written inside GetGroupStatus before the response that makes the tenant
// yield — so delaying it would destroy the property the design exists for.
func TestPublisher_OpenDelayHoldsTheRisingEdgeOnly(t *testing.T) {
	w := &recordingWriter{}
	clk := &fixedClock{t: time.Unix(1790104764, 0)}
	p := withClock(NewPublisher(w, "k", "shadow-vllm").WithOpenDelay(2*time.Second), clk)
	ctx := context.Background()

	// Tenant is not servable.
	mustPublish(t, p, ctx, Blocked)

	// Becomes servable. The hold-down starts now; nothing may open yet.
	mustPublish(t, p, ctx, Available)
	clk.advance(1900 * time.Millisecond)
	mustPublish(t, p, ctx, Available)

	// Delay elapsed.
	clk.advance(200 * time.Millisecond)
	mustPublish(t, p, ctx, Available)

	// Falling edge: immediate, no delay, same call.
	mustPublish(t, p, ctx, Blocked)

	want := []string{Blocked, Blocked, Blocked, Available, Blocked}
	got := w.recorded()
	if len(got) != len(want) {
		t.Fatalf("got %d writes %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("write %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// A blip back to Blocked inside the hold-down has to restart it. Otherwise a
// tenant that briefly reacquires and yields again would bank credit toward
// opening the gate it never actually earned.
func TestPublisher_OpenDelayRestartsAfterABlockedBlip(t *testing.T) {
	w := &recordingWriter{}
	clk := &fixedClock{t: time.Unix(1790104764, 0)}
	p := withClock(NewPublisher(w, "k", "shadow-vllm").WithOpenDelay(2*time.Second), clk)
	ctx := context.Background()

	mustPublish(t, p, ctx, Available)
	clk.advance(1900 * time.Millisecond)
	mustPublish(t, p, ctx, Blocked) // blip: resets the hold-down
	mustPublish(t, p, ctx, Available)
	clk.advance(1900 * time.Millisecond)
	mustPublish(t, p, ctx, Available)

	for i, v := range w.recorded() {
		if v != Blocked {
			t.Fatalf("write %d = %q, want %q: the hold-down did not restart", i, v, Blocked)
		}
	}

	clk.advance(200 * time.Millisecond)
	mustPublish(t, p, ctx, Available)
	got := w.recorded()
	if got[len(got)-1] != Available {
		t.Errorf("last write = %q, want %q once the restarted delay elapsed", got[len(got)-1], Available)
	}
}

// Default must be the measured-but-unmitigated behaviour, so that adding the
// parameter cannot change any existing deployment.
func TestPublisher_ZeroOpenDelayPublishesTheRisingEdgeImmediately(t *testing.T) {
	w := &recordingWriter{}
	clk := &fixedClock{t: time.Unix(1790104764, 0)}
	p := withClock(NewPublisher(w, "k", "shadow-vllm"), clk)

	if p.OpenDelay() != 0 {
		t.Fatalf("OpenDelay() = %v, want 0 by default", p.OpenDelay())
	}
	mustPublish(t, p, context.Background(), Available)
	if got := w.recorded(); len(got) != 1 || got[0] != Available {
		t.Errorf("writes = %v, want [%q]", got, Available)
	}
}

func TestPublisher_WithOpenDelayIsNilSafe(t *testing.T) {
	var p *Publisher
	if got := p.WithOpenDelay(2 * time.Second); got != nil {
		t.Errorf("WithOpenDelay() on nil = %v, want nil", got)
	}
	if got := p.OpenDelay(); got != 0 {
		t.Errorf("OpenDelay() on nil = %v, want 0", got)
	}
}

func mustPublish(t *testing.T, p *Publisher, ctx context.Context, v string) {
	t.Helper()
	if err := p.Publish(ctx, v); err != nil {
		t.Fatalf("Publish(%q) error = %v", v, err)
	}
}
