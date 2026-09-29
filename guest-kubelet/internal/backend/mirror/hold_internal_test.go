package mirror

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// holdPokes records, at each OnHoldChange call, what Hold answered and which freezer calls had
// already happened.
type holdPokes struct {
	mu    sync.Mutex
	seen  []string
	b     *Backend
	calls func() []string
}

func (p *holdPokes) poke() {
	held, _ := p.b.Hold()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, map[bool]string{true: "held", false: "free"}[held]+"@"+strings.Join(p.calls(), "+"))
}

func (p *holdPokes) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func holdHarness(t *testing.T) (*harness, *fakeFreezer, *holdPokes) {
	t.Helper()
	h, ff := suspendHarness(t)
	p := &holdPokes{b: h.b, calls: ff.callList}
	h.b.opts.Suspend.OnHoldChange = p.poke
	return h, ff, p
}

func waitHold(t *testing.T, b *Backend, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got, _ := b.Hold(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Hold never became %v", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCordonWhileHeld_NsCordon_HoldFollowsSuspendState(t *testing.T) {
	h, _, p := holdHarness(t)
	if held, reason := h.b.Hold(); held {
		t.Fatalf("a running guest must not hold the node: %s", reason)
	}
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	waitHold(t, h.b, true)
	if _, reason := h.b.Hold(); reason != "1 guest(s) suspended" {
		t.Errorf("reason = %q", reason)
	}
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	waitHold(t, h.b, false)
	// One poke when Suspending is visible, before the freeze; one when Resuming is visible,
	// before the thaw.
	if got, want := strings.Join(p.list(), ","), "held@,free@freeze:1"; got != want {
		t.Fatalf("pokes = %s, want %s", got, want)
	}
}

func TestCordonWhileHeld_NsCordon_RollbackReleasesHold(t *testing.T) {
	h, ff, p := holdHarness(t)
	ff.suspendErr = errors.New("stuck task")
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error")
	}
	waitHold(t, h.b, false)
	if got, want := strings.Join(p.list(), ","), "held@,free@freeze:1+thaw:1"; got != want {
		t.Fatalf("pokes = %s, want %s", got, want)
	}
}

func TestCordonWhileHeld_NsCordon_StuckSuspendingStaysHeld(t *testing.T) {
	// Freeze and thaw both fail: the guest stays Suspending (NotReady), and the node stays held,
	// so no new guest lands next to a process in an unknown state.
	h, ff, _ := holdHarness(t)
	ff.suspendErr, ff.resumeErr = errors.New("stuck task"), errors.New("no thaw")
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err == nil {
		t.Fatal("want an error")
	}
	waitHold(t, h.b, true)
}

func TestCordonWhileHeld_Skip_NoHookSuspendResume(t *testing.T) {
	// Option skip: no OnHoldChange. Suspend and resume work exactly as without the option.
	h, ff := suspendHarness(t)
	if _, err := h.b.Suspend(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.b.Resume(context.Background(), "ns", "vllm"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(ff.callList(), ","), "freeze:1,thaw:2,readycheck"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
}
