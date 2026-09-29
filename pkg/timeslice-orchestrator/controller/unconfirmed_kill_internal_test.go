package controller

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Tests of the blocked grant (the default) and of the events of both paths.
// The grant tests of each unconfirmed signal are in kill_internal_test.go and
// run with --grant-unconfirmed.

// kubeRecord is one UnconfirmedKube call.
type kubeRecord struct {
	kind, reason, job, node string
	at                      time.Time
}

// fakeKube records UnconfirmedKube calls.
type fakeKube struct {
	mu    sync.Mutex
	calls []kubeRecord
}

func (k *fakeKube) record(r *kubeRecord) {
	k.mu.Lock()
	defer k.mu.Unlock()
	r.at = time.Now()
	k.calls = append(k.calls, *r)
}

func (k *fakeKube) GuestEvent(_ context.Context, _, job, node, reason, _ string) error {
	k.record(&kubeRecord{kind: "guest-event", reason: reason, job: job, node: node})
	return nil
}

func (k *fakeKube) ForegroundEvent(_ context.Context, _, job, reason, _ string) error {
	k.record(&kubeRecord{kind: "foreground-event", reason: reason, job: job})
	return nil
}

// of returns the recorded calls of one kind.
func (k *fakeKube) of(kind string) []kubeRecord {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(k.calls), func(r kubeRecord) bool { return r.kind != kind })
}

// agentUnconfirmed makes the fixture's agent fail every Kill with
// KILL_UNCONFIRMED.
func agentUnconfirmed(fx *killFixture) {
	fx.agent.OperationFunc = func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		msg := "device memory still mapped"
		return &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED, Error: &msg,
			ErrorReason: agentpb.ErrorReason_KILL_UNCONFIRMED,
		}, nil
	}
}

// agentConfirms makes the fixture's agent confirm every Kill.
func agentConfirms(fx *killFixture) {
	fx.agent.OperationFunc = func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		return &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: agentpb.Outcome_OUTCOME_KILLED,
		}, nil
	}
}

// newUnconfirmedFixture is a kill fixture with N = unconfirmedN and
// K = unconfirmedK, an agent that never confirms the Kill and a fake
// UnconfirmedKube.
func newUnconfirmedFixture(t *testing.T) (*killFixture, *fakeKube, time.Time) {
	t.Helper()
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	agentUnconfirmed(fx)
	kube := &fakeKube{}
	fx.ctrl.Kube = kube
	return fx, kube, noticeAt
}

// passFor runs the kill path every 20 ms for d and reports whether the host
// became clear.
func (fx *killFixture) passFor(d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func (fx *killFixture) blocked() bool {
	fx.ctrl.killMu.Lock()
	defer fx.ctrl.killMu.Unlock()
	return fx.ctrl.blocks[killKey(killGroup, killNode, killGuest)] != nil
}

func grantBlockedGauge() float64 {
	return testutil.ToFloat64(metrics.GrantBlocked.WithLabelValues(killGroup, killNode))
}

// hostClearedHow returns how the kill node's host was cleared, or "".
func hostClearedHow(fx *killFixture) string {
	fx.hosts.mu.Lock()
	defer fx.hosts.mu.Unlock()
	return fx.hosts.cleared[killNode]
}

// checkNeverGranted checks that nothing handed the host back.
func checkNeverGranted(t *testing.T, fx *killFixture) {
	t.Helper()
	if got := hostClearedHow(fx); got != "" {
		t.Errorf("host cleared (%s) over an unconfirmed kill", got)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed set while the grant is blocked")
	}
	if fx.guest.UnconfirmedKill(killNode) {
		t.Error("guest marked handed back while the grant is blocked")
	}
}

// TestUnconfirmedKill_Grant_RecordsEvent: the hand-back of
// --grant-unconfirmed records a KillUnconfirmed event on the guest and no
// GrantBlocked event.
func TestUnconfirmedKill_Grant_RecordsEvent(t *testing.T) {
	fx, kube, noticeAt := newUnconfirmedFixture(t)
	fx.group.Spec().GetWaitingJobQueue().Enqueue("trainer")
	unconfirmedCase(t, fx, noticeAt, unconfirmedKillAgent)
	events := kube.of("guest-event")
	if len(events) != 1 || events[0].reason != eventKillUnconfirmed || events[0].node != killNode {
		t.Errorf("guest events = %+v, want one %s on %s", events, eventKillUnconfirmed, killNode)
	}
	if ev := kube.of("foreground-event"); len(ev) != 0 {
		t.Errorf("foreground events = %+v on a granted host, want none", ev)
	}
	if fx.blocked() {
		t.Error("block recorded by --grant-unconfirmed")
	}
}

// TestUnconfirmedKill_Block_NeverGrantsThenGrantsOnConfirm: by default the
// host is never handed back over an unconfirmed Kill; the operator is alerted
// once at the decision, the Kill keeps being retried, and the grant goes
// ahead (without vram_unconfirmed) once a later Kill is confirmed.
func TestUnconfirmedKill_Block_NeverGrantsThenGrantsOnConfirm(t *testing.T) {
	fx, kube, noticeAt := newUnconfirmedFixture(t)
	fx.group.Spec().GetWaitingJobQueue().Enqueue("trainer")
	before := testutil.ToFloat64(metrics.KillUnconfirmedTotal)

	if fx.passFor(unconfirmedN - 50*time.Millisecond) {
		t.Fatal("host clear before N")
	}
	if fx.blocked() {
		t.Error("blocked before the decision at N")
	}
	// Past the decision and one Kill retry (killRetryInterval).
	if fx.passFor(time.Until(noticeAt.Add(unconfirmedN + killRetryInterval + 300*time.Millisecond))) {
		t.Fatal("host handed back over an unconfirmed kill")
	}
	checkNeverGranted(t, fx)
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	if got := testutil.ToFloat64(metrics.KillUnconfirmedTotal) - before; got != 1 {
		t.Errorf("timeslice_kill_unconfirmed_total grew by %v, want 1 (once per barrier)", got)
	}
	if got := grantBlockedGauge(); got != 1 {
		t.Errorf("timeslice_grant_blocked = %v, want 1", got)
	}
	if n := len(fx.killCalls()); n < 2 {
		t.Errorf("Kill sent %d times, want it retried while blocked", n)
	}
	if ev := kube.of("guest-event"); len(ev) != 1 || ev[0].reason != eventKillUnconfirmed {
		t.Errorf("guest events = %+v, want one %s", ev, eventKillUnconfirmed)
	}
	if ev := kube.of("foreground-event"); len(ev) != 1 || ev[0].reason != eventGrantBlocked || ev[0].job != "trainer" {
		t.Errorf("foreground events = %+v, want one %s on trainer", ev, eventGrantBlocked)
	}

	agentConfirms(fx)
	if !fx.passFor(killRetryInterval + 500*time.Millisecond) {
		t.Fatal("host not handed back after the Kill was confirmed")
	}
	if got := hostClearedHow(fx); got != clearHowKill {
		t.Errorf("cleared how = %q, want %q", got, clearHowKill)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("grant after a confirmed kill carries vram_unconfirmed")
	}
	if !fx.guest.Killed(killNode) {
		t.Error("guest not marked killed")
	}
	if fx.blocked() || grantBlockedGauge() != 0 {
		t.Error("block not released after the Kill was confirmed")
	}
}

// TestUnconfirmedKill_Block_ReleasedWhenVacated: the agent reporting the
// guest SUSPENDED (vacated) releases the block and clears the host.
func TestUnconfirmedKill_Block_ReleasedWhenVacated(t *testing.T) {
	fx, _, noticeAt := newUnconfirmedFixture(t)
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 100*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_SUSPENDED)
	fx.ctrl.markAgentSeen(killNode)
	if !fx.passFor(500 * time.Millisecond) {
		t.Fatal("host not clear after the guest was vacated")
	}
	if fx.blocked() || grantBlockedGauge() != 0 {
		t.Error("block not released after the guest was vacated")
	}
}

// TestUnconfirmedKill_Block_ReleasedWhenBarrierEnds: no running barrier, no
// block.
func TestUnconfirmedKill_Block_ReleasedWhenBarrierEnds(t *testing.T) {
	fx, _, noticeAt := newUnconfirmedFixture(t)
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 100*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	fx.hosts.mu.Lock()
	fx.hosts.running = false
	fx.hosts.mu.Unlock()
	fx.ctrl.killOverdueHosts(context.Background(), fx.group)
	if fx.blocked() || grantBlockedGauge() != 0 {
		t.Error("block not released when the barrier ended")
	}
}

// TestUnconfirmedKill_Block_RederivedAfterRestart: the block lives in memory
// only; a new controller (an orchestrator restart) rebuilds it from the agent
// by killing again, and still never grants.
func TestUnconfirmedKill_Block_RederivedAfterRestart(t *testing.T) {
	fx, _, noticeAt := newUnconfirmedFixture(t)
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 100*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	sent := len(fx.killCalls())

	old := fx.ctrl
	fx.ctrl = NewController(old.groupStore, old.jobStore, old.queue, nil, fx.agent)
	fx.ctrl.Hosts = fx.hosts
	fx.ctrl.KillPollInterval = old.KillPollInterval
	if fx.passFor(unconfirmedK + 300*time.Millisecond) {
		t.Fatal("restarted controller handed the host back over an unconfirmed kill")
	}
	checkNeverGranted(t, fx)
	if len(fx.killCalls()) <= sent {
		t.Error("restarted controller did not ask the agent again")
	}
	if !fx.blocked() {
		t.Error("restarted controller did not rebuild the block")
	}
}
