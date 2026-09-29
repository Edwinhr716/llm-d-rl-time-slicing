package controller_test

import (
	"context"
	"sync"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Tests for decision D-NS-6, --unconfirmed-kill: what happens after a guest
// Kill reached the agent but was not confirmed. TestUnconfirmedKill_Grant_*
// (with TestKill_Unconfirmed_* in kill_test.go) cover "grant",
// TestUnconfirmedKill_Block_* "block" and TestUnconfirmedKill_Escalate_*
// "escalate".

// confirmKills makes every later Kill complete with the guest gone and no
// device memory.
func (a *ns4Agent) confirmKills() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.killMode = ""
	for job := range a.deviceBytes {
		a.deviceBytes[job] = 0
	}
}

// fakeKube records what the controller asks of Kubernetes.
type fakeKube struct {
	mu      sync.Mutex
	events  []fakeKubeCall
	deletes []fakeKubeCall
}

type fakeKubeCall struct{ group, job, node, reason string }

func (k *fakeKube) WarnPods(_ context.Context, groupID, jobID, node, reason, _ string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.events = append(k.events, fakeKubeCall{groupID, jobID, node, reason})
	return nil
}

func (k *fakeKube) DeletePodsGracefully(_ context.Context, groupID, jobID, node string) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.deletes = append(k.deletes, fakeKubeCall{group: groupID, job: jobID, node: node})
	return 1, nil
}

func (k *fakeKube) eventCount(reason, job string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := 0
	for _, e := range k.events {
		if e.reason == reason && e.job == job {
			n++
		}
	}
	return n
}

func (k *fakeKube) deleteCalls() []fakeKubeCall {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]fakeKubeCall(nil), k.deletes...)
}

func grantBlockedGauge() float64 {
	return testutil.ToFloat64(metrics.GrantBlocked.WithLabelValues(ns4Group, ns4Node))
}

func nodeFailedGauge() float64 {
	return testutil.ToFloat64(metrics.NodeFailed.WithLabelValues(ns4Node))
}

func escalations(step string) float64 {
	return testutil.ToFloat64(metrics.UnconfirmedEscalationsTotal.WithLabelValues(step))
}

// unconfirmedSignals are the agent kill modes of ns4Agent and the signal each
// produces.
var unconfirmedSignals = []struct{ mode, signal string }{
	{"pending", "kill-timeout"},
	{"unconfirmed", "agent-kill-unconfirmed"},
	{"device-bytes", "device-bytes"},
}

func TestUnconfirmedKill_FlagValues(t *testing.T) {
	for _, mode := range []string{"grant", "block", "escalate"} {
		if err := controller.ValidateUnconfirmedKill(mode); err != nil {
			t.Errorf("ValidateUnconfirmedKill(%q) = %v", mode, err)
		}
	}
	for _, mode := range []string{"", "keep", "ns-block", "Grant"} {
		if err := controller.ValidateUnconfirmedKill(mode); err == nil {
			t.Errorf("ValidateUnconfirmedKill(%q) = nil, want an error", mode)
		}
	}
	got, err := controller.ParseEscalateAfter(controller.DefaultEscalateAfterFlag)
	if err != nil || got != controller.DefaultEscalateAfter {
		t.Errorf("ParseEscalateAfter(default) = %v, %v, want %v", got, err, controller.DefaultEscalateAfter)
	}
	if got != [2]time.Duration{10 * time.Second, 40 * time.Second} {
		t.Errorf("default escalation = %v, want 10s,40s", got)
	}
	if got, err := controller.ParseEscalateAfter(" 1s , 2m "); err != nil || got != [2]time.Duration{time.Second, 2 * time.Minute} {
		t.Errorf("ParseEscalateAfter(1s,2m) = %v, %v", got, err)
	}
	for _, bad := range []string{"", "10s", "10s,40s,50s", "x,40s", "0s,40s", "40s,10s", "10s,10s", "-1s,5s"} {
		if _, err := controller.ParseEscalateAfter(bad); err == nil {
			t.Errorf("ParseEscalateAfter(%q) = nil error", bad)
		}
	}
}

// TestUnconfirmedKill_Grant_IsTheDefault: a new controller grants.
func TestUnconfirmedKill_Grant_IsTheDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	if fx.ctrl.UnconfirmedKill != controller.UnconfirmedKillGrant {
		t.Errorf("default UnconfirmedKill = %q, want %q", fx.ctrl.UnconfirmedKill, controller.UnconfirmedKillGrant)
	}
	if fx.ctrl.EscalateAfter != controller.DefaultEscalateAfter {
		t.Errorf("default EscalateAfter = %v, want %v", fx.ctrl.EscalateAfter, controller.DefaultEscalateAfter)
	}
}

// TestUnconfirmedKill_Grant_AlertsAndGrantsAtN: under grant an unconfirmed
// Kill is counted once, logged as "Kill unconfirmed" action=grant with a
// KillUnconfirmed event on the guest, and the node goes back at N with
// vram_unconfirmed.
func TestUnconfirmedKill_Grant_AlertsAndGrantsAtN(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	kube := &fakeKube{}
	fx.ctrl.Kube = kube
	fx.ctrl.UnconfirmedKill = controller.UnconfirmedKillGrant
	fx.agent.setKillMode("pending", 0)
	before := testutil.ToFloat64(metrics.KillUnconfirmedTotal)
	noticeAt := startNotice(t, ctx, fx, 3*time.Second, time.Second)

	eventually(t, "the restore", 6*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	if at := fx.agent.firstRestore().Sub(noticeAt); at < 3*time.Second-50*time.Millisecond {
		t.Errorf("restored %v after the notice, before N = 3s", at)
	}
	if !fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed not set")
	}
	if !logs.contains(`"msg":"Kill unconfirmed"`, `"action":"grant"`, `"signal":"kill-timeout"`, `"elapsed_ms"`) {
		t.Error(`missing "Kill unconfirmed" action=grant`)
	}
	if got := testutil.ToFloat64(metrics.KillUnconfirmedTotal) - before; got != 1 {
		t.Errorf("timeslice_kill_unconfirmed_total grew by %v, want 1", got)
	}
	if n := kube.eventCount(controller.EventKillUnconfirmed, ns4Guest); n != 1 {
		t.Errorf("KillUnconfirmed events = %d, want 1", n)
	}
	if n := kube.eventCount(controller.EventGrantBlocked, ns4Fg); n != 0 {
		t.Errorf("GrantBlocked events = %d under grant, want 0", n)
	}
	if len(kube.deleteCalls()) != 0 || logs.contains(`"msg":"Escalation"`) {
		t.Error("escalation under grant")
	}
}

// startBlocked starts a notice with N = 2s, K = 500ms under mode and waits
// until the foreground is held past N on an unconfirmed Kill.
func startBlocked(t *testing.T, ctx context.Context, fx *ns4Fixture, logs *ns4Logs, mode, killMode string) time.Time {
	t.Helper()
	fx.ctrl.UnconfirmedKill = mode
	fx.agent.setKillMode(killMode, 0)
	noticeAt := startNotice(t, ctx, fx, 2*time.Second, 500*time.Millisecond)
	eventually(t, "Grant blocked", 6*time.Second, func() bool {
		return logs.contains(`"msg":"Grant blocked"`, `"group":"group-1"`, `"node":"node-1"`, `"job":"guest-1"`, `"since_ms"`)
	})
	return noticeAt
}

// TestUnconfirmedKill_Block_HoldsUntilConfirmed: for every signal, block
// never hands the node back past N; the Kill is retried, "Grant blocked" and
// timeslice_grant_blocked report the wait, vram_unconfirmed is never set and
// nothing escalates. Once the agent confirms the guest gone the trainer is
// restored at once.
func TestUnconfirmedKill_Block_HoldsUntilConfirmed(t *testing.T) {
	for _, tc := range unconfirmedSignals {
		t.Run(tc.signal, func(t *testing.T) {
			logs := captureNS4Logs(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
			kube := &fakeKube{}
			fx.ctrl.Kube = kube
			before := testutil.ToFloat64(metrics.KillUnconfirmedTotal)
			noticeAt := startBlocked(t, ctx, fx, logs, controller.UnconfirmedKillBlock, tc.mode)

			// Past N + 2s: still held.
			time.Sleep(time.Until(noticeAt.Add(4 * time.Second)))
			if r := fx.agent.counts().restores; r != 0 {
				t.Fatalf("restores = %d past N on an unconfirmed kill, want 0", r)
			}
			if kills := fx.agent.killCalls(); len(kills) < 2 {
				t.Errorf("kills = %d, want the Kill retried", len(kills))
			}
			if !logs.contains(`"msg":"Kill unconfirmed"`, `"action":"block"`, `"signal":"`+tc.signal+`"`) {
				t.Errorf(`missing "Kill unconfirmed" action=block signal=%s`, tc.signal)
			}
			if got := testutil.ToFloat64(metrics.KillUnconfirmedTotal) - before; got != 1 {
				t.Errorf("timeslice_kill_unconfirmed_total grew by %v, want 1 (once per guest)", got)
			}
			if g := grantBlockedGauge(); g != 1 {
				t.Errorf("timeslice_grant_blocked = %v, want 1", g)
			}
			if n := kube.eventCount(controller.EventGrantBlocked, ns4Fg); n != 1 {
				t.Errorf("GrantBlocked events on the trainer = %d, want 1", n)
			}
			if n := kube.eventCount(controller.EventKillUnconfirmed, ns4Guest); n != 1 {
				t.Errorf("KillUnconfirmed events on the guest = %d, want 1", n)
			}
			if fx.group.Spec().TakeVramUnconfirmed() {
				t.Error("vram_unconfirmed set under block")
			}
			if logs.contains(`"msg":"Node handed back with an unconfirmed kill"`) {
				t.Error("node handed back under block")
			}
			if len(kube.deleteCalls()) != 0 || logs.contains(`"msg":"Escalation"`) {
				t.Error("escalation under block")
			}

			fx.agent.confirmKills()
			confirmedAt := time.Now()
			eventually(t, "the restore", 5*time.Second, func() bool { return fx.agent.counts().restores == 1 })
			if d := fx.agent.firstRestore().Sub(confirmedAt); d > 2500*time.Millisecond {
				t.Errorf("restored %v after the agent confirmed, want within 2.5s", d)
			}
			if !logs.contains(`"msg":"Kill confirmed"`, `"node":"node-1"`, `"job":"guest-1"`, `"elapsed_ms"`) {
				t.Error(`missing "Kill confirmed"`)
			}
			if !logs.contains(`"msg":"Held guest released"`) {
				t.Error(`missing "Held guest released"`)
			}
			if g := grantBlockedGauge(); g != 0 {
				t.Errorf("timeslice_grant_blocked = %v after release, want 0", g)
			}
			if fx.group.Spec().TakeVramUnconfirmed() {
				t.Error("vram_unconfirmed set after the confirmed kill")
			}
		})
	}
}

// TestUnconfirmedKill_Block_HoldOutlivesMirrorPod: the guest's mirror pod
// going away (the job leaves the store) does not release the hold; only the
// agent reporting the guest off the accelerator does.
func TestUnconfirmedKill_Block_HoldOutlivesMirrorPod(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	startBlocked(t, ctx, fx, logs, controller.UnconfirmedKillBlock, "pending")

	if err := fx.jobStore.Delete(ctx, ns4Group, ns4Guest); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	fx.ctrl.EnqueueWork(ns4Group)
	killsAt := len(fx.agent.killCalls())
	time.Sleep(2500 * time.Millisecond)
	if r := fx.agent.counts().restores; r != 0 {
		t.Fatalf("restores = %d after the mirror pod went, agent still reports the guest, want 0", r)
	}
	if kills := len(fx.agent.killCalls()); kills <= killsAt {
		t.Errorf("kills = %d, want the Kill retried after the mirror pod went (was %d)", kills, killsAt)
	}

	fx.agent.set(ns4Guest, agentpb.JobState_JOB_STATE_SUSPENDED)
	eventually(t, "the restore", 5*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	if !logs.contains(`"msg":"Held guest released"`, `"how":"agent-status"`) {
		t.Error(`missing "Held guest released" how=agent-status`)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed set under block")
	}
}

// TestUnconfirmedKill_Escalate_Ladder: escalate blocks like block, deletes
// the mirror pod gracefully at step 1 and marks the node not lendable at
// step 2, then releases everything once the agent confirms.
func TestUnconfirmedKill_Escalate_Ladder(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	kube := &fakeKube{}
	fx.ctrl.Kube = kube
	fx.ctrl.EscalateAfter = [2]time.Duration{time.Second, 2 * time.Second}
	step1, step2 := escalations("1"), escalations("2")
	startBlocked(t, ctx, fx, logs, controller.UnconfirmedKillEscalate, "unconfirmed")
	if !logs.contains(`"msg":"Kill unconfirmed"`, `"action":"escalate"`) {
		t.Error(`missing "Kill unconfirmed" action=escalate`)
	}

	eventually(t, "escalation step 1", 5*time.Second, func() bool {
		return logs.contains(`"msg":"Escalation"`, `"step":1`, `"action":"delete-mirror-pod"`, `"node":"node-1"`)
	})
	eventually(t, "escalation step 2", 5*time.Second, func() bool {
		return logs.contains(`"msg":"Escalation"`, `"step":2`, `"action":"mark-node-not-lendable"`)
	})
	eventually(t, "Node not lendable", 2*time.Second, func() bool {
		return logs.contains(`"msg":"Node not lendable"`, `"node":"node-1"`, `"reason":"kill-unconfirmed"`)
	})
	deletes := kube.deleteCalls()
	if len(deletes) != 1 || deletes[0].job != ns4Guest || deletes[0].node != ns4Node {
		t.Errorf("graceful deletes = %+v, want one of %s on %s", deletes, ns4Guest, ns4Node)
	}
	if d := escalations("1") - step1; d != 1 {
		t.Errorf("escalations{step=1} grew by %v, want 1", d)
	}
	if d := escalations("2") - step2; d != 1 {
		t.Errorf("escalations{step=2} grew by %v, want 1", d)
	}
	if g := nodeFailedGauge(); g != 1 {
		t.Errorf("timeslice_node_failed = %v, want 1", g)
	}
	if n := kube.eventCount(controller.EventNodeNotLendable, ns4Fg); n != 1 {
		t.Errorf("NodeNotLendable events on the trainer = %d, want 1", n)
	}
	if n := kube.eventCount(controller.EventNodeNotLendable, ns4Guest); n != 1 {
		t.Errorf("NodeNotLendable events on the guest = %d, want 1", n)
	}
	if r := fx.agent.counts().restores; r != 0 {
		t.Fatalf("restores = %d while escalating, want 0", r)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed set under escalate")
	}

	fx.agent.confirmKills()
	eventually(t, "the restore", 5*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	if !logs.contains(`"msg":"Kill confirmed"`, `"job":"guest-1"`) {
		t.Error(`missing "Kill confirmed"`)
	}
	if g := nodeFailedGauge(); g != 0 {
		t.Errorf("timeslice_node_failed = %v after release, want 0", g)
	}
	if g := grantBlockedGauge(); g != 0 {
		t.Errorf("timeslice_grant_blocked = %v after release, want 0", g)
	}
	if len(kube.deleteCalls()) != 1 {
		t.Errorf("graceful deletes = %d, want exactly 1", len(kube.deleteCalls()))
	}
}

// TestUnconfirmedKill_Escalate_NoStepsBeforeDue: with the default ladder
// (10s, 40s) nothing escalates in the first seconds of a hold.
func TestUnconfirmedKill_Escalate_NoStepsBeforeDue(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	kube := &fakeKube{}
	fx.ctrl.Kube = kube
	noticeAt := startBlocked(t, ctx, fx, logs, controller.UnconfirmedKillEscalate, "pending")
	time.Sleep(time.Until(noticeAt.Add(4 * time.Second)))
	if logs.contains(`"msg":"Escalation"`) || len(kube.deleteCalls()) != 0 {
		t.Error("escalated before step 1 was due")
	}
	if r := fx.agent.counts().restores; r != 0 {
		t.Errorf("restores = %d, want 0", r)
	}
}
