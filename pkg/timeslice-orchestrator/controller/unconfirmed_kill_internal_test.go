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

// Tests of D-NS-6 (--unconfirmed-kill). The grant tests that predate the flag
// are in kill_internal_test.go and run with the default.

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

func (k *fakeKube) NodeEvent(_ context.Context, node, reason, _ string) error {
	k.record(&kubeRecord{kind: "node-event", reason: reason, node: node})
	return nil
}

func (k *fakeKube) DeleteGuestMirror(_ context.Context, _, job, node string) (int, error) {
	k.record(&kubeRecord{kind: "delete-mirror", job: job, node: node})
	return 1, nil
}

// all returns every recorded call.
func (k *fakeKube) all() []kubeRecord {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.calls)
}

// of returns the recorded calls of one kind.
func (k *fakeKube) of(kind string) []kubeRecord {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(k.calls), func(r kubeRecord) bool { return r.kind != kind })
}

// resumeHosts counts the resumes of a barrierHosts.
type resumeHosts struct {
	*barrierHosts
	mu      sync.Mutex
	resumes int
}

func (h *resumeHosts) Resume(string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resumes++
}

func (h *resumeHosts) resumed() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.resumes
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
// K = unconfirmedK, an agent that never confirms the Kill, the given
// --unconfirmed-kill value and a fake UnconfirmedKube.
func newUnconfirmedFixture(t *testing.T, mode string) (*killFixture, *fakeKube, time.Time) {
	t.Helper()
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	agentUnconfirmed(fx)
	kube := &fakeKube{}
	fx.ctrl.UnconfirmedKill = mode
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

func escalations(step string) float64 {
	return testutil.ToFloat64(metrics.UnconfirmedEscalationsTotal.WithLabelValues(step))
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
		t.Error("vram_unconfirmed set by a block option")
	}
	if fx.guest.UnconfirmedKill(killNode) {
		t.Error("guest marked handed back by a block option")
	}
}

func TestUnconfirmedKill_ValidateFlags(t *testing.T) {
	for _, mode := range []string{UnconfirmedKillGrant, UnconfirmedKillBlock, UnconfirmedKillEscalate} {
		if err := ValidateUnconfirmedKill(mode); err != nil {
			t.Errorf("ValidateUnconfirmedKill(%q) = %v", mode, err)
		}
	}
	for _, mode := range []string{"", "other", "Grant"} {
		if ValidateUnconfirmedKill(mode) == nil {
			t.Errorf("ValidateUnconfirmedKill(%q) accepted", mode)
		}
	}
	got, err := ParseUnconfirmedEscalateAfter(DefaultUnconfirmedEscalateAfter)
	if err != nil || got != [2]time.Duration{10 * time.Second, 40 * time.Second} {
		t.Errorf("ParseUnconfirmedEscalateAfter(default) = %v, %v", got, err)
	}
	for _, v := range []string{"", "10s", "10s,40s,60s", "40s,10s", "10s,10s", "0s,10s", "-1s,10s", "x,10s"} {
		if _, err := ParseUnconfirmedEscalateAfter(v); err == nil {
			t.Errorf("ParseUnconfirmedEscalateAfter(%q) accepted", v)
		}
	}
}

// TestUnconfirmedKill_Grant_DefaultIsGrant: an unset flag is today's grant.
func TestUnconfirmedKill_Grant_DefaultIsGrant(t *testing.T) {
	fx, kube, noticeAt := newUnconfirmedFixture(t, "")
	unconfirmedCase(t, fx, noticeAt, unconfirmedKillAgent)
	events := kube.of("guest-event")
	if len(events) != 1 || events[0].reason != eventKillUnconfirmed || events[0].node != killNode {
		t.Errorf("guest events = %+v, want one %s on %s", events, eventKillUnconfirmed, killNode)
	}
	if len(kube.of("delete-mirror")) != 0 || len(kube.of("node-event")) != 0 {
		t.Error("grant escalated")
	}
}

// TestUnconfirmedKill_Block_NeverGrantsThenGrantsOnConfirm: block never
// hands the host back over an unconfirmed Kill; it alerts once at the
// decision, keeps retrying the Kill, and grants (without vram_unconfirmed)
// once a later Kill is confirmed.
func TestUnconfirmedKill_Block_NeverGrantsThenGrantsOnConfirm(t *testing.T) {
	fx, kube, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillBlock)
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
		t.Fatal("block handed the host back over an unconfirmed kill")
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
	if len(kube.of("delete-mirror")) != 0 || len(kube.of("node-event")) != 0 {
		t.Error("block escalated")
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
	fx, _, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillBlock)
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
	fx, _, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillBlock)
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
// by killing again, and still never grants (D-NS-6 H8).
func TestUnconfirmedKill_Block_RederivedAfterRestart(t *testing.T) {
	fx, _, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillBlock)
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 100*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	sent := len(fx.killCalls())

	old := fx.ctrl
	fx.ctrl = NewController(old.groupStore, old.jobStore, old.queue, nil, fx.agent)
	fx.ctrl.Hosts = fx.hosts
	fx.ctrl.KillPollInterval = old.KillPollInterval
	fx.ctrl.UnconfirmedKill = UnconfirmedKillBlock
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

// TestUnconfirmedKill_Escalate_Ladder: escalate blocks like block, deletes
// the mirror pod at decision + E1 and marks the node not lendable at
// decision + E2; confirming the Kill grants and clears the mark.
func TestUnconfirmedKill_Escalate_Ladder(t *testing.T) {
	const e1, e2 = 150 * time.Millisecond, 300 * time.Millisecond
	fx, kube, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillEscalate)
	fx.ctrl.UnconfirmedEscalateAfter = [2]time.Duration{e1, e2}
	hosts := &resumeHosts{barrierHosts: fx.hosts}
	fx.ctrl.Hosts = hosts
	step1, step2 := escalations("1"), escalations("2")
	decideAt := noticeAt.Add(unconfirmedN)

	if fx.passFor(time.Until(decideAt.Add(e2 + 200*time.Millisecond))) {
		t.Fatal("escalate handed the host back over an unconfirmed kill")
	}
	checkNeverGranted(t, fx)
	if grantBlockedGauge() != 1 {
		t.Error("timeslice_grant_blocked not set")
	}

	deletes := kube.of("delete-mirror")
	if len(deletes) != 1 || deletes[0].node != killNode || deletes[0].job != killGuest {
		t.Fatalf("mirror deletes = %+v, want one for %s on %s", deletes, killGuest, killNode)
	}
	if at := deletes[0].at.Sub(decideAt); at < e1 || at > e1+250*time.Millisecond {
		t.Errorf("step 1 at decision + %v, want at + %v", at, e1)
	}
	nodeEvents := kube.of("node-event")
	if len(nodeEvents) != 1 || nodeEvents[0].reason != eventNodeNotLendable || nodeEvents[0].node != killNode {
		t.Fatalf("node events = %+v, want one %s on %s", nodeEvents, eventNodeNotLendable, killNode)
	}
	if at := nodeEvents[0].at.Sub(decideAt); at < e2 || at > e2+250*time.Millisecond {
		t.Errorf("step 2 at decision + %v, want at + %v", at, e2)
	}
	if escalations("1")-step1 != 1 || escalations("2")-step2 != 1 {
		t.Error("timeslice_unconfirmed_escalations_total{step} not counted once per step")
	}
	if got := testutil.ToFloat64(metrics.NodeFailed.WithLabelValues(killNode)); got != 1 {
		t.Errorf("timeslice_node_failed = %v, want 1", got)
	}
	if got := fx.ctrl.nodeNotLendable(killNode); got != notLendableEscalated {
		t.Errorf("nodeNotLendable = %q, want %q", got, notLendableEscalated)
	}
	fx.group.Spec().SetLend(true)
	if err := fx.ctrl.resumeIfLent(context.Background(), fx.group); err != nil {
		t.Fatal(err)
	}
	if hosts.resumed() != 0 {
		t.Error("a node marked not lendable was lent")
	}
	fx.group.Spec().SetLend(false)

	agentConfirms(fx)
	if !fx.passFor(killRetryInterval + 500*time.Millisecond) {
		t.Fatal("host not handed back after the Kill was confirmed")
	}
	if got := hostClearedHow(fx); got != clearHowKill {
		t.Errorf("cleared how = %q, want %q", got, clearHowKill)
	}
	if fx.ctrl.nodeNotLendable(killNode) != "" {
		t.Error("not-lendable mark kept after the Kill was confirmed")
	}
	if testutil.ToFloat64(metrics.NodeFailed.WithLabelValues(killNode)) != 0 || grantBlockedGauge() != 0 {
		t.Error("gauges not cleared after the Kill was confirmed")
	}
	fx.group.Spec().SetLend(true)
	if err := fx.ctrl.resumeIfLent(context.Background(), fx.group); err != nil {
		t.Fatal(err)
	}
	if hosts.resumed() != 1 {
		t.Error("node not lent again after the Kill was confirmed")
	}
}

// TestUnconfirmedKill_Escalate_StopsWhenReleased: a block released before
// its steps are due never escalates.
func TestUnconfirmedKill_Escalate_StopsWhenReleased(t *testing.T) {
	const e1, e2 = 200 * time.Millisecond, 400 * time.Millisecond
	fx, kube, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillEscalate)
	fx.ctrl.UnconfirmedEscalateAfter = [2]time.Duration{e1, e2}
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 50*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	fx.hosts.mu.Lock()
	fx.hosts.running = false
	fx.hosts.mu.Unlock()
	fx.ctrl.killOverdueHosts(context.Background(), fx.group)
	time.Sleep(e2 + 200*time.Millisecond)
	if len(kube.of("delete-mirror")) != 0 || len(kube.of("node-event")) != 0 {
		t.Errorf("escalated after the block was released: %+v", kube.all())
	}
	if fx.ctrl.nodeNotLendable(killNode) != "" {
		t.Error("node marked not lendable after the block was released")
	}
}

// TestUnconfirmedKill_Escalate_ReleasedWhenGroupDeleted: deleting the group
// drops its block, stops the ladder and clears timeslice_grant_blocked.
func TestUnconfirmedKill_Escalate_ReleasedWhenGroupDeleted(t *testing.T) {
	const e1, e2 = 200 * time.Millisecond, 400 * time.Millisecond
	fx, kube, noticeAt := newUnconfirmedFixture(t, UnconfirmedKillEscalate)
	fx.ctrl.UnconfirmedEscalateAfter = [2]time.Duration{e1, e2}
	fx.passFor(time.Until(noticeAt.Add(unconfirmedN + 50*time.Millisecond)))
	if !fx.blocked() {
		t.Fatal("no block recorded")
	}
	ctx := context.Background()
	if err := fx.ctrl.groupStore.Delete(ctx, killGroup); err != nil {
		t.Fatal(err)
	}
	fx.ctrl.forgetHostsIfGroupDeleted(ctx, killGroup)
	if fx.blocked() {
		t.Error("block kept after the group was deleted")
	}
	if got := grantBlockedGauge(); got != 0 {
		t.Errorf("timeslice_grant_blocked = %v after the group was deleted, want 0", got)
	}
	time.Sleep(e2 + 200*time.Millisecond)
	if len(kube.of("delete-mirror")) != 0 || len(kube.of("node-event")) != 0 {
		t.Errorf("escalated after the group was deleted: %+v", kube.all())
	}
}

// TestUnconfirmedKill_Escalate_NotBeforeDecision: nothing escalates before
// the decision at N.
func TestUnconfirmedKill_Escalate_NotBeforeDecision(t *testing.T) {
	fx, kube, _ := newUnconfirmedFixture(t, UnconfirmedKillEscalate)
	fx.ctrl.UnconfirmedEscalateAfter = [2]time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if fx.passFor(unconfirmedN - 100*time.Millisecond) {
		t.Fatal("host clear before N")
	}
	time.Sleep(50 * time.Millisecond)
	if fx.blocked() || len(kube.all()) != 0 {
		t.Errorf("acted before the decision: blocked=%v calls=%+v", fx.blocked(), kube.all())
	}
}
