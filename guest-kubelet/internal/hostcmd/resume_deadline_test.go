package hostcmd_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	hcpb "github.com/edwinhr716/guest-kubelet/api/hostcommand/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

func withResumeBudget(d time.Duration) rigOpt {
	return func(c *hostcmd.Config, _ *rig) { c.ResumeBudget = d }
}

// setEngine makes the guest's mirror serve (ContainersReady) or not.
func (h *fakeHost) setEngine(name string, serving bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.guests[name].Mirror.DeepCopy()
	status := corev1.ConditionFalse
	if serving {
		status = corev1.ConditionTrue
	}
	m.Status.Conditions = []corev1.PodCondition{{Type: corev1.ContainersReady, Status: status}}
	h.guests[name].Mirror = m
}

func (r *rig) resumeBy(t *testing.T, epoch int64, deadline time.Time) *hcpb.HostAck {
	t.Helper()
	ack, err := r.srv.Resume(context.Background(), &hcpb.ResumeRequest{
		GroupId: testGroup, NodeName: testNode, Epoch: epoch, Deadline: timestamppb.New(deadline),
	})
	if err != nil {
		t.Fatalf("Resume(%d): %v", epoch, err)
	}
	return ack
}

// An engine that starts cold and serves after the Resume deadline (but within ResumeBudget past
// it) is RESUMED, not FAILED: a FAILED ack only makes the orchestrator retry.
func TestResume_SlowEngineServesPastDeadline(t *testing.T) {
	r := newRig(t, withResumeBudget(300*time.Millisecond))
	r.host.neverUp = true
	r.host.addGuest("a")
	acks := make(chan *hcpb.HostAck, 1)
	go func() { acks <- r.resumeBy(t, 1, time.Now().Add(20*time.Millisecond)) }()
	eventually(t, func() bool { return r.ev.index("create a") >= 0 })
	time.Sleep(400 * time.Millisecond) // past the deadline, and past the least run time
	r.host.setEngine("a", true)
	wantOutcome(t, <-acks, hcpb.Outcome_OUTCOME_RESUMED)
	if r.ev.index("ready a") < 0 {
		t.Fatal("guest not released Ready")
	}
}

// A retry of a Resume after its deadline (the orchestrator retries with the same epoch and the
// same deadline) runs with a fresh bound: its API calls do not fail on an expired context.
func TestResume_RetryAfterDeadlineRuns(t *testing.T) {
	r := newRig(t, withResumeBudget(100*time.Millisecond))
	r.host.neverUp = true
	r.host.addGuest("a")
	deadline := time.Now().Add(10 * time.Millisecond)
	wantOutcome(t, r.resumeBy(t, 1, deadline), hcpb.Outcome_OUTCOME_FAILED) // never served
	r.host.setEngine("a", true)
	ack := r.resumeBy(t, 1, deadline) // the same stale deadline
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_RESUMED)
}

// Agent mode: a Resume retried after its deadline reads the mirror and calls ResumeAll with a
// live context, so the suspended guest is resumed, not killed.
func TestAgent_ResumeAfterDeadlineDoesNotKill(t *testing.T) {
	r := newAgentRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	ack := r.resumeBy(t, 3, time.Now().Add(-time.Second))
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_RESUMED)
	before(t, r.ev, "ResumeAll 3", "ready a")
	if r.ev.count("agent-kill") != 0 {
		t.Fatalf("a guest was killed: %v", r.ev.list())
	}
}

// Agent mode: a guest whose engine never served is deleted by a vacate, not suspended, so the
// agent never checkpoints a half-started engine (VERIFY_FAILED on ResumeAll, then a kill loop).
// The guest is not killed and starts again on the next Resume.
func TestAgent_VacateDeletesNeverServedGuest(t *testing.T) {
	r := newAgentRig(t)
	r.host.neverUp = true
	r.host.addGuest("a")
	res := make(chan *hcpb.HostAck, 1)
	go func() { res <- r.resume(t, 1) }()
	eventually(t, func() bool { return r.ev.index("create a") >= 0 })
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	wantOutcome(t, <-res, hcpb.Outcome_OUTCOME_ABORTED)
	before(t, r.ev, "confirm a", "vacate-delete a")
	before(t, r.ev, "vacate-delete a", "gone a-m")
	if n := r.ev.count("SuspendAll"); n != 0 {
		t.Fatalf("SuspendAll calls = %d, want 0 (nothing worth a snapshot)", n)
	}
	if r.ev.count("agent-kill") != 0 || r.ev.count("kill ") != 0 {
		t.Fatalf("guest killed: %v", r.ev.list())
	}
	r.host.neverUp = false
	wantOutcome(t, r.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	if n := r.ev.count("create a"); n != 2 {
		t.Fatalf("mirrors created = %d, want 2 (started again)", n)
	}
}

// Agent mode: a guest that served once is suspended even if its engine does not serve at the
// moment of the vacate (its accelerator state is worth the snapshot).
func TestAgent_VacateSuspendsServedGuestNotServingNow(t *testing.T) {
	r := newAgentRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.host.setEngine("a", false)
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	if r.ev.index("SuspendAll 2") < 0 || r.ev.index("vacate-delete a") >= 0 {
		t.Fatalf("served guest not suspended: %v", r.ev.list())
	}
}

// A cold start longer than ResumeBudget past the deadline (image pull, model load) is still
// RESUMED within EngineStartBudget, so it gives no failed ack.
func TestResume_ColdStartWithinEngineStartBudget(t *testing.T) {
	r := newRig(t, withResumeBudget(50*time.Millisecond), func(c *hostcmd.Config, _ *rig) {
		c.EngineStartBudget = 5 * time.Second
	})
	r.host.neverUp = true
	r.host.addGuest("c")
	acks := make(chan *hcpb.HostAck, 1)
	go func() { acks <- r.resumeBy(t, 1, time.Now().Add(20*time.Millisecond)) }()
	eventually(t, func() bool { return r.ev.index("create c") >= 0 })
	time.Sleep(300 * time.Millisecond) // past the deadline plus ResumeBudget
	r.host.setEngine("c", true)
	wantOutcome(t, <-acks, hcpb.Outcome_OUTCOME_RESUMED)
}
