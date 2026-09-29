package controller_test

import (
	"context"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
)

// The kill path (kill.go) with foreground wait option B (async-requeue,
// D-ORCH-1). Kills run before the node pass, so a restore checked on a requeue
// must not delay or repeat them.

// TestForegroundWait_Async_KillHungGuestThenRestore: a guest that never
// suspends is killed at T, and the trainer's restore is then started and
// tracked on a requeue until the job is loaded.
func TestForegroundWait_Async_KillHungGuestThenRestore(t *testing.T) {
	logs := captureNS4Logs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_SAVED)
	fx.ctrl.ForegroundWait = controller.ForegroundWaitAsyncRequeue
	noticeAt := startNotice(t, ctx, fx, 3*time.Second, time.Second)

	eventually(t, "the restore", 6*time.Second, func() bool { return fx.agent.counts().restores == 1 })
	eventually(t, "the trainer loaded", 5*time.Second, func() bool { return fx.group.Status().LoadedJob() == ns4Fg })
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
	if n := fx.agent.counts().restores; n != 1 {
		t.Errorf("restores = %d, want 1", n)
	}
	for _, want := range [][]string{
		{`"msg":"Guest killed"`, `"job":"guest-1"`},
		{`"msg":"Foreground operation started"`, `"type":"restore"`},
		{`"msg":"Foreground operation finished"`, `"type":"restore"`, `"outcome":"complete"`},
	} {
		if !logs.contains(want...) {
			t.Errorf("missing log line with %v", want)
		}
	}
}

// TestForegroundWait_Async_FaultedGuestKilledAtOnce: a FAULTED guest is killed
// once, without a notice, in async mode too.
func TestForegroundWait_Async_FaultedGuestKilledAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newNS4Fixture(t, ctx, agentpb.JobState_JOB_STATE_RUNNING)
	fx.ctrl.ForegroundWait = controller.ForegroundWaitAsyncRequeue
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
