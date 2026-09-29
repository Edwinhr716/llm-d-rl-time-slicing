package mirror

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// holdPokes records, at each OnHoldChange call, what Hold answered and which agent calls had
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

func holdHarness(t *testing.T) (*harness, *fakeAgent, *holdPokes) {
	t.Helper()
	h, fa := suspendHarness(t)
	p := &holdPokes{b: h.b, calls: fa.callList}
	h.b.opts.Suspend.OnHoldChange = p.poke
	return h, fa, p
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
	rig, _, p := holdHarness(t)
	if held, reason := rig.b.Hold(); held {
		t.Fatalf("a running guest must not hold the node: %s", reason)
	}
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	waitHold(t, rig.b, true)
	if _, reason := rig.b.Hold(); reason != "1 guest(s) suspended" {
		t.Errorf("reason = %q", reason)
	}
	if _, err := rig.b.Resume(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	waitHold(t, rig.b, false)
	// One poke when Suspending is visible, before the agent call; one when Resuming is visible,
	// before the agent's Resume.
	if got, want := strings.Join(p.list(), ","), "held@,free@status+suspend:1"; got != want {
		t.Fatalf("pokes = %s, want %s", got, want)
	}
}

func TestCordonWhileHeld_NsCordon_RollbackReleasesHold(t *testing.T) {
	rig, _, p := holdHarness(t)
	rig.mu.Lock()
	rig.writeBack = false // NotReady never confirmed: rolled back before any agent call
	rig.mu.Unlock()
	rig.b.opts.Suspend.NotReadyTimeout = 200 * time.Millisecond
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil {
		t.Fatal("want an error")
	}
	waitHold(t, rig.b, false)
	if got, want := strings.Join(p.list(), ","), "held@,free@"; got != want {
		t.Fatalf("pokes = %s, want %s", got, want)
	}
}

func TestCordonWhileHeld_NsCordon_KilledMirrorHeldUntilGone(t *testing.T) {
	// The agent fails the suspend: the kill sequence deletes the mirror with normal grace. Until
	// the mirror is gone the node stays held; the fake API server removes it at once.
	rig, fa, p := holdHarness(t)
	fa.suspendErr = errors.New("BACKEND_ERROR")
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil {
		t.Fatal("want an error")
	}
	waitHold(t, rig.b, false)
	if got := p.list(); len(got) < 2 || got[0] != "held@" {
		t.Fatalf("pokes = %v", got)
	}
}

func TestCordonWhileHeld_NsCordon_HoldFollowsAgentState(t *testing.T) {
	// The agent reports the job SUSPENDED while the mirror still shows Running (a guest kubelet
	// that stopped before recording it): the node is held from the agent's state, and the
	// mirror is recorded Suspended, which keeps the guest NotReady.
	rig, fa, p := holdHarness(t)
	job := rig.mirror("vllm-m").Labels[LabelJobID]
	fa.setJob(job, freeze.JobSuspended)
	rig.b.refreshAgentState(context.Background())
	if held, reason := rig.b.Hold(); !held {
		t.Fatalf("an agent-suspended job must hold the node: %s", reason)
	}
	if len(p.list()) == 0 {
		t.Fatal("an agent state change must poke the hold")
	}
	if s := rig.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != StateSuspended {
		t.Fatalf("state = %q, want %s", s, StateSuspended)
	}
	rig.settled("NotReady", notReady)
	rig.neverReadyWhileSuspended()

	// The agent state alone holds: a mirror cleared by hand is still held while the agent says
	// SUSPENDED.
	g, err := rig.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.b.setSuspendState(context.Background(), g, "", keepEpoch); err != nil {
		t.Fatal(err)
	}
	if err := rig.b.waitInformerState(context.Background(), g, "", time.Second); err != nil {
		t.Fatal(err)
	}
	if held, reason := rig.b.Hold(); !held || !strings.Contains(reason, "per the snapshot agent") {
		t.Fatalf("held = %v, reason %q", held, reason)
	}
}

func TestCordonWhileHeld_NsCordon_StaleAgentReadIgnored(t *testing.T) {
	// An operation finished between the Status read and the repair: the repair is skipped.
	rig, fa, _ := holdHarness(t)
	job := rig.mirror("vllm-m").Labels[LabelJobID]
	fa.setJob(job, freeze.JobSuspended)
	jobs, err := fa.Jobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seq := rig.b.locks.unlocks()
	rig.b.locks.tryLock("other")
	rig.b.locks.unlock("other")
	rig.b.reconcileAgentState(context.Background(), jobs, seq)
	if s := rig.mirror("vllm-m").Annotations[AnnotationSuspendState]; s != "" {
		t.Fatalf("state = %q, want no repair from a stale read", s)
	}
}

func TestCordonWhileHeld_Skip_NoHookSuspendResume(t *testing.T) {
	// Option skip: no OnHoldChange. Suspend and resume work exactly as without the option.
	rig, fa := suspendHarness(t)
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.b.Resume(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got, want := fa.String(), "status,suspend:1,resume:2,readycheck"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
}
