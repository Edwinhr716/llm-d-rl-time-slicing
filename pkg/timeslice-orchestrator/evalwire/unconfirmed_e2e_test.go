//go:build evalwire

package evalwire_test

// Fake-agent tests of the blocked grant (the default on an unconfirmed guest
// Kill), on the kill-path cluster (orcha4_e2e_test.go): node 127.0.0.3's host never acks and
// its agent cannot confirm the Kill until the test lets it. N = 4s, K = 1s,
// so the Kill goes out at notice + 3s and the unconfirmed-kill decision is at
// notice + 4s.

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// setMode switches the agent's Kill mode for the next Kill.
func (a *fakeAgent) setMode(mode string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mode = mode
}

// acquireResult is the outcome of a background Acquire.
type acquireResult struct {
	resp *pb.AcquireResponse
	took time.Duration
	err  error
}

// acquireAsync starts the trainer's Acquire, bounded by 30s.
func acquireAsync(api pb.TimeSliceOrchestratorServiceClient) <-chan acquireResult {
	out := make(chan acquireResult, 1)
	go func() {
		resp, took, err := timedAcquire(api, 30*time.Second)
		out <- acquireResult{resp, took, err}
	}()
	return out
}

// mustStillWait fails if the Acquire has returned.
func mustStillWait(t *testing.T, res <-chan acquireResult, when string) {
	t.Helper()
	select {
	case r := <-res:
		t.Fatalf("trainer's Acquire returned %v, %v after %v, %s: granted over an unconfirmed kill",
			r.resp, r.err, r.took, when)
	default:
	}
}

// newUnconfirmedCluster is the ORCH-A4 cluster with the hung, unconfirmed
// host 127.0.0.3.
func newUnconfirmedCluster(t *testing.T) *a4Cluster {
	t.Helper()
	clu := newA4Cluster(t, map[string]string{e2eNodes[0]: killConfirm, e2eNodes[1]: killUnconfirmed})
	hung := clu.execs[e2eNodes[1]]
	hung.hang.Store(true)
	t.Cleanup(func() { close(hung.release) })
	clu.startHostsOn(t, e2eNodes...)
	return clu
}

// events returns the Warning events with reason, by involved object name.
func events(t *testing.T, clu *a4Cluster, reason string) []corev1.Event {
	t.Helper()
	list, err := clu.cs.CoreV1().Events("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []corev1.Event
	for _, ev := range list.Items {
		if ev.Reason == reason && ev.Type == corev1.EventTypeWarning {
			out = append(out, ev)
		}
	}
	return out
}

// TestUnconfirmedKill_Block_E2E_HeldUntilConfirmed: the orchestrator never
// grants over the unconfirmed kill, alerts, keeps killing, and grants without
// vram_unconfirmed once the agent confirms a later Kill.
func TestUnconfirmedKill_Block_E2E_HeldUntilConfirmed(t *testing.T) {
	sink := captureLogs(t)
	clu := newUnconfirmedCluster(t)
	orch := clu.startOrch(t)
	api := client(t, orch)
	waitGuestsKnown(t, sink)
	hung := e2eNodes[1]
	before := scrapeCounter(t, orch.MetricsAddr, "timeslice_kill_unconfirmed_total")

	res := acquireAsync(api)
	time.Sleep(a4Notice + 2*time.Second)
	mustStillWait(t, res, "2s after N")
	for _, check := range []struct {
		msg   string
		attrs map[string]any
	}{
		{"Kill sent", map[string]any{"node": hung, "reason": "deadline"}},
		{"Kill attempt not confirmed", map[string]any{"node": hung, "signal": "agent-kill-unconfirmed"}},
		{"Kill unconfirmed", map[string]any{"group": e2eGroup, "node": hung, "action": "block"}},
		{"Grant blocked", map[string]any{"group": e2eGroup, "node": hung, "action": "block"}},
	} {
		if !sink.has(t, check.msg, check.attrs) {
			t.Errorf("no %q log line with %v", check.msg, check.attrs)
		}
	}
	if sink.has(t, "Host clear", map[string]any{"node": hung}) {
		t.Error("hung host cleared over an unconfirmed kill")
	}
	if got := scrapeCounter(t, orch.MetricsAddr, "timeslice_kill_unconfirmed_total") - before; got != 1 {
		t.Errorf("timeslice_kill_unconfirmed_total grew by %v, want 1", got)
	}
	gauge := fmt.Sprintf("timeslice_grant_blocked{group=%q,node=%q}", e2eGroup, hung)
	if v := scrapeCounter(t, orch.MetricsAddr, gauge); v != 1 {
		t.Errorf("%s = %v, want 1", gauge, v)
	}
	if n := len(clu.agents[hung].killList()); n < 2 {
		t.Errorf("Kill sent %d times while blocked, want retries", n)
	}
	ev := events(t, clu, "KillUnconfirmed")
	if len(ev) != 1 || ev[0].InvolvedObject.Name != "mirror-"+hung || ev[0].InvolvedObject.Namespace != "guests" {
		t.Errorf("KillUnconfirmed events = %+v, want one on guests/mirror-%s", ev, hung)
	}

	confirmed := time.Now()
	clu.agents[hung].setMode(killConfirm)
	r := <-res
	if r.err != nil || !r.resp.GetSuccess() {
		t.Fatalf("Acquire = %v, %v after the Kill was confirmed", r.resp, r.err)
	}
	if r.resp.GetVramUnconfirmed() {
		t.Error("vram_unconfirmed set after a blocked grant")
	}
	if wait := time.Since(confirmed); wait > 3*time.Second {
		t.Errorf("granted %v after the agent could confirm, want within a Kill retry and an Acquire poll", wait)
	}
	if !sink.has(t, "Host clear", map[string]any{"node": hung, "how": "kill"}) {
		t.Error("no Host clear how=kill line after the confirmed kill")
	}
	if !sink.has(t, "Foreground granted", map[string]any{"group": e2eGroup, "vram_unconfirmed": false}) {
		t.Error("no Foreground granted line with vram_unconfirmed=false")
	}
	if !sink.has(t, "Grant block cleared", map[string]any{"node": hung}) {
		t.Error("no Grant block cleared line")
	}
}

// TestUnconfirmedKill_Block_E2E_Restart: an orchestrator restarted while a
// grant is blocked rebuilds the block from the agent (it kills again and the
// agent still cannot confirm) and still never grants.
func TestUnconfirmedKill_Block_E2E_Restart(t *testing.T) {
	sink := captureLogs(t)
	clu := newUnconfirmedCluster(t)
	hung := e2eNodes[1]
	orch1 := clu.startOrch(t)
	api1 := client(t, orch1)
	waitGuestsKnown(t, sink)

	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := acquire(ctx1, api1)
		first <- err
	}()
	waitFor(t, "the first block", func() bool {
		return sink.has(t, "Grant blocked", map[string]any{"node": hung, "action": "block"})
	})
	cancel1() // the trainer's call dies with the process
	orch1.Stop()
	if err := <-first; err == nil {
		t.Fatal("Acquire succeeded on the first process over an unconfirmed kill")
	}
	killsBefore := len(clu.agents[hung].killList())

	api2 := client(t, clu.startOrch(t))
	resp, took, err := timedAcquire(api2, a4Notice+3*time.Second)
	if err == nil {
		t.Fatalf("Acquire after restart = %v after %v, want it held over the unconfirmed kill", resp, took)
	}
	if len(clu.agents[hung].killList()) <= killsBefore {
		t.Error("restarted orchestrator did not ask the agent again")
	}
	if sink.has(t, "Foreground granted", nil) {
		t.Error("granted over an unconfirmed kill")
	}
}
