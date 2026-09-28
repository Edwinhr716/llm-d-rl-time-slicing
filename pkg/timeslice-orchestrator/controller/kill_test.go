package controller_test

import (
	"context"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Tests for the kill path (kill.go) and the unconfirmed-kill decision
// (unconfirmed_kill.go, D-NS-6 default "keep").

// startNotice sets short notice timing, adds a live guest and a foreground
// Acquire, runs the controller and returns the notice time.
func startNotice(t *testing.T, ctx context.Context, fx *ns4Fixture, n, k time.Duration) time.Time {
	t.Helper()
	fx.ctrl.NoticeWindow = n
	fx.ctrl.KillBudget = k
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().RequestLock(ns4Fg)
	fx.run(t, ctx)
	eventually(t, "the notice", 5*time.Second, func() bool { return !fx.group.Spec().NoticeAt().IsZero() })
	return fx.group.Spec().NoticeAt()
}

// TestKill_HungGuestKilledAtDeadline: a guest that never suspends is killed
// at T and the trainer is restored within N.
func TestKill_HungGuestKilledAtDeadline(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	noticeAt := startNotice(t, ctx, fx, 3*time.Second, time.Second)

	eventually(t, "the restore", 6*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	kills := fx.agent.killCalls()
	if len(kills) != 1 || kills[0].reason != "deadline" || kills[0].job != ns4Guest {
		t.Fatalf("kills = %+v, want one deadline kill of %s", kills, ns4Guest)
	}
	if at := kills[0].at.Sub(noticeAt); at < 2*time.Second-50*time.Millisecond {
		t.Errorf("kill sent %v after the notice, before T = 2s", at)
	}
	if at := fx.agent.firstRestore().Sub(noticeAt); at > 3*time.Second {
		t.Errorf("restored %v after the notice, want within N = 3s", at)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed set after a confirmed kill")
	}
	for _, want := range [][]string{
		{`"msg":"Kill sent"`, `"reason":"deadline"`, `"job":"guest-1"`},
		{`"msg":"Guest killed"`, `"job":"guest-1"`},
		{`"msg":"Host clear"`, `"how":"kill"`},
	} {
		if !logs.contains(want...) {
			t.Errorf("missing log line with %v", want)
		}
	}
}

// TestKill_FaultedGuestKilledAtOnce: the agent reports the guest FAULTED; it
// is killed without a notice.
func TestKill_FaultedGuestKilledAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.group.Spec().SetActiveJob(ns4Fg)
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_FAULTED)
	fx.run(t, ctx)

	eventually(t, "the kill", 5*time.Second, func() bool { return len(fx.agent.killCalls()) == 1 })
	if k := fx.agent.killCalls()[0]; k.reason != "guest-faulted" {
		t.Errorf("kill reason = %q, want guest-faulted", k.reason)
	}
	guest, err := fx.jobStore.Get(ctx, ns4Group, ns4Guest)
	if err != nil {
		t.Fatalf("Get guest: %v", err)
	}
	eventually(t, "the guest killed", 2*time.Second, func() bool { return guest.Killed(ns4Node) })
	time.Sleep(1500 * time.Millisecond)
	if n := len(fx.agent.killCalls()); n != 1 {
		t.Errorf("kills = %d after a confirmed kill, want 1", n)
	}
}

// TestKill_VKUnseenRevokesAndKills: the participant holding the grant is not
// seen for L; its grant is taken back and its guest killed.
func TestKill_VKUnseenRevokesAndKills(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_IDLE)
	fx.ctrl.BackgroundLiveness = time.Second
	spec := fx.group.Spec()
	spec.RegisterParticipant(ns4Node, ns4VK, time.Now())
	spec.Grant(ns4Node)
	fx.addGuest(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.run(t, ctx)

	eventually(t, "the grant revoked", 5*time.Second, func() bool { return !spec.Granted(ns4Node) })
	eventually(t, "the kill", 3*time.Second, func() bool { return len(fx.agent.killCalls()) == 1 })
	if k := fx.agent.killCalls()[0]; k.reason != "vk-unseen" {
		t.Errorf("kill reason = %q, want vk-unseen", k.reason)
	}
	if !logs.contains(`"msg":"Background grant revoked"`, `"reason":"vk-unseen"`) {
		t.Error(`missing "Background grant revoked" reason=vk-unseen`)
	}
}

// TestKill_UnreachableAgentRetried: Kill fails twice as if the agent were
// down; it is retried about every second and the trainer is restored once it
// gets through.
func TestKill_UnreachableAgentRetried(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	fx.agent.setKillMode("", 2)
	startNotice(t, ctx, fx, 6*time.Second, 2*time.Second)

	eventually(t, "the restore", 10*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	kills := fx.agent.killCalls()
	if len(kills) != 3 || !kills[0].failed || !kills[1].failed || kills[2].failed {
		t.Fatalf("kills = %+v, want two failed then one sent", kills)
	}
	for i := 1; i < len(kills); i++ {
		if gap := kills[i].at.Sub(kills[i-1].at); gap < 950*time.Millisecond || gap > 1600*time.Millisecond {
			t.Errorf("kill %d sent %v after the previous one, want about 1s", i, gap)
		}
	}
	if !logs.contains(`"msg":"Agent unreachable, kill will be retried"`, `"attempt":2`) {
		t.Error(`missing "Agent unreachable, kill will be retried" attempt=2`)
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed set after a confirmed kill")
	}
}

// TestKill_Unconfirmed_GrantAtNWithVramUnconfirmed: each signal that a Kill
// reached the agent but was not confirmed goes through onKillUnconfirmed;
// today's default hands the node back at N with vram_unconfirmed and never
// lends it again while the guest may be there.
func TestKill_Unconfirmed_GrantAtNWithVramUnconfirmed(t *testing.T) {
	for _, tc := range []struct{ mode, signal string }{
		{"pending", "kill-timeout"},
		{"unconfirmed", "agent-kill-unconfirmed"},
		{"device-bytes", "device-bytes"},
	} {
		t.Run(tc.signal, func(t *testing.T) {
			logs := captureNS4Logs(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
			fx.agent.setKillMode(tc.mode, 0)
			noticeAt := startNotice(t, ctx, fx, 3*time.Second, time.Second)

			eventually(t, "the restore", 6*time.Second, func() bool { return fx.agent.counts().restores == 1 })
			if at := fx.agent.firstRestore().Sub(noticeAt); at < 3*time.Second-50*time.Millisecond {
				t.Errorf("restored %v after the notice, before N = 3s", at)
			}
			if !logs.contains(`"msg":"Kill not confirmed"`, `"signal":"`+tc.signal+`"`) {
				t.Errorf(`missing "Kill not confirmed" signal=%s`, tc.signal)
			}
			if !logs.contains(`"msg":"Node handed back with an unconfirmed kill"`, `"signal":"`+tc.signal+`"`,
				`"vramUnconfirmed":true`) {
				t.Errorf(`missing "Node handed back with an unconfirmed kill" signal=%s`, tc.signal)
			}
			if !fx.group.Spec().TakeVramUnconfirmed() {
				t.Error("vram_unconfirmed not set for the grant after an unconfirmed kill")
			}
			guest, err := fx.jobStore.Get(ctx, ns4Group, ns4Guest)
			if err != nil {
				t.Fatalf("Get guest: %v", err)
			}
			if !guest.UnconfirmedKill(ns4Node) {
				t.Error("guest not marked with an unconfirmed kill")
			}

			// The trainer yields with a lend hint and a participant waits:
			// the node is not lent while the guest may still be there.
			eventually(t, "the trainer loaded", 5*time.Second, func() bool { return fx.group.Status().LoadedJob() == ns4Fg })
			spec := fx.group.Spec()
			if err := spec.Yield(ctx, ns4Fg); err != nil {
				t.Fatalf("Yield: %v", err)
			}
			spec.SetLend(true)
			spec.RegisterParticipant(ns4Node, ns4VK, time.Now())
			fx.ctrl.EnqueueWork(ns4Group)
			time.Sleep(1500 * time.Millisecond)
			if spec.Granted(ns4Node) {
				t.Error("node lent again after an unconfirmed kill")
			}
		})
	}
}
