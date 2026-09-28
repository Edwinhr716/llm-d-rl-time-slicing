package server

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

type gateHosts struct {
	mu    sync.Mutex
	clear bool
}

func (g *gateHosts) Clear(string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.clear
}

func (g *gateHosts) Vacate(context.Context, string, []string, time.Time) bool {
	return g.Clear("")
}

func (g *gateHosts) Resume(context.Context, string, []string) {}

type grantRecorder struct {
	mu    sync.Mutex
	attrs []map[string]slog.Value
}

func (r *grantRecorder) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // slog.Handler.Handle signature requires passing Record by value
func (r *grantRecorder) Handle(_ context.Context, rec slog.Record) error {
	if rec.Message != "Foreground granted" {
		return nil
	}
	a := map[string]slog.Value{}
	rec.Attrs(func(attr slog.Attr) bool {
		a[attr.Key] = attr.Value
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attrs = append(r.attrs, a)
	return nil
}
func (r *grantRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *grantRecorder) WithGroup(string) slog.Handler      { return r }

// TestNS4_Push_AcquireNotGrantedBeforeHostsClear: even with the lock held and
// the context loaded, the foreground is not granted until every host acked.
func TestNS4_Push_AcquireNotGrantedBeforeHostsClear(t *testing.T) {
	rec := &grantRecorder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := context.Background()
	group, err := store.NewGroup(ctx, "g", &MockGroupLockStore{lock: "trainer"})
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	group.Status().SetLoadedJob("trainer")

	hosts := &gateHosts{}
	ctrl := controller.NewController(nil, nil, &MockWorkQueue{}, nil, nil)
	ctrl.Hosts = hosts
	srv := NewServer(ctrl,
		&MockGroupStore{GetFunc: func(context.Context, string) (*store.Group, error) { return group, nil }},
		&MockJobStore{})

	start := time.Now()
	if resp, err, done := srv.defaultCheckAcquire(ctx, "g", "trainer", start); done {
		t.Fatalf("granted before hosts clear: resp=%v err=%v", resp, err)
	}

	hosts.mu.Lock()
	hosts.clear = true
	hosts.mu.Unlock()
	resp, err, done := srv.defaultCheckAcquire(ctx, "g", "trainer", start)
	if !done || err != nil || !resp.GetSuccess() {
		t.Fatalf("not granted once hosts clear: resp=%v err=%v done=%v", resp, err, done)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attrs) != 1 {
		t.Fatalf("Foreground granted logged %d times, want 1", len(rec.attrs))
	}
	a := rec.attrs[0]
	if a["group"].String() != "g" || a["job"].String() != "trainer" || a["waited_ms"].Kind() != slog.KindInt64 {
		t.Errorf("Foreground granted attrs = %v", a)
	}
}
