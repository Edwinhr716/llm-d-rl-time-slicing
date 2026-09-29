package hostcmd_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func resumed(req *hcpb.ResumeRequest) *hcpb.HostAck {
	return &hcpb.HostAck{
		NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
		Command: hcpb.Command_COMMAND_RESUME, Outcome: hcpb.Outcome_OUTCOME_RESUMED,
	}
}

// TestORCHA6_Push_SlowResumeIsNotAFailure: a host acks a resume only once its
// guests run again, which may take longer than AttemptTimeout (500 ms here).
// The resume is bounded by its budget, so a slow resume succeeds on its first
// call and is never counted as a failed command.
func TestORCHA6_Push_SlowResumeIsNotAFailure(t *testing.T) {
	fake := &fakeHost{
		resume: func(ctx context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(900 * time.Millisecond):
			}
			return resumed(req), nil
		},
	}
	addrs := map[string]string{"node-a": serve(t, fake)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "clear", func() bool { return hns.cmd.AllClear(testGroup) })

	hns.cmd.Resume(testGroup)
	eventually(t, "resume acked", func() bool {
		return hns.sink.hasRecord(t, "Host ack", map[string]any{"node": "node-a", "command": "resume", "outcome": "resumed"})
	})
	if got := fake.calls.Load(); got != 2 {
		t.Errorf("host calls = %d, want 2 (one vacate, one resume)", got)
	}
	if hns.sink.hasRecord(t, "Host command failed", map[string]any{"command": "resume"}) {
		t.Error("a slow resume was counted as a failed command")
	}
}

// TestORCHA6_Push_ResumeFailuresDoNotCarryIntoVacate: resumes that failed (for
// example cut short while the guests were still resuming) do not time the
// background liveness L of the vacate that follows. The host answers the
// vacate (it is reachable, only slow), so it is never failing and never
// vk-unseen.
func TestORCHA6_Push_ResumeFailuresDoNotCarryIntoVacate(t *testing.T) {
	var failResume atomic.Bool
	failResume.Store(true)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var vacates atomic.Int32
	fake := &fakeHost{
		vacate: func(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
			if vacates.Add(1) == 1 {
				return vacated(req), nil
			}
			select { // the second vacate hangs: the guest is slow to suspend
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return vacated(req), nil
			}
		},
		resume: func(_ context.Context, req *hcpb.ResumeRequest) (*hcpb.HostAck, error) {
			if failResume.Load() {
				return nil, status.Error(codes.DeadlineExceeded, "guest still resuming")
			}
			return resumed(req), nil
		},
	}
	addrs := map[string]string{"node-a": serve(t, fake)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "clear", func() bool { return hns.cmd.AllClear(testGroup) })

	hns.cmd.Resume(testGroup)
	eventually(t, "resume failed", func() bool {
		return hns.sink.hasRecord(t, "Host command failed", map[string]any{"node": "node-a", "command": "resume"})
	})

	// The bubble ends while the resume is still failing.
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "vacate sent", func() bool { return vacates.Load() == 2 })
	if !hns.sink.hasRecord(t, "Vacate starts a new failure count: earlier failures were resumes",
		map[string]any{"node": "node-a"}) {
		t.Error("no Vacate starts a new failure count line")
	}
	time.Sleep(200 * time.Millisecond)
	bar, ok := hns.cmd.Barrier(testGroup)
	if !ok || len(bar.NotClear) != 1 {
		t.Fatalf("barrier = %+v, %v, want node-a not clear", bar, ok)
	}
	if fs := bar.NotClear[0].FailingSince; !fs.IsZero() {
		t.Errorf("FailingSince = %v, want zero: resume failures made a reachable host look unseen", fs)
	}
}

// TestORCHA6_Push_FailedVacateAckEnqueuesOnce: a host that answers a vacate
// FAILED before T is reachable, so no failed command enqueues the group. The
// Commander enqueues it once for the epoch, so the kill path can kill a guest
// the failed suspend left FAULTED without waiting for T; the retries do not
// enqueue again.
func TestORCHA6_Push_FailedVacateAckEnqueuesOnce(t *testing.T) {
	fake := &fakeHost{
		vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
			return &hcpb.HostAck{
				NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
				Command: hcpb.Command_COMMAND_VACATE, Outcome: hcpb.Outcome_OUTCOME_FAILED,
				Error: "suspend failed",
			}, nil
		},
	}
	addrs := map[string]string{"node-a": serve(t, fake)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())

	eventually(t, "enqueue", func() bool { return hns.enqueued.Load() == 1 })
	eventually(t, "retries", func() bool { return fake.calls.Load() >= 10 })
	if got := hns.enqueued.Load(); got != 1 {
		t.Errorf("enqueued = %d after %d FAILED acks, want 1", got, fake.calls.Load())
	}
	if !hns.sink.hasRecord(t, "Host vacate failed before the deadline", map[string]any{"node": "node-a"}) {
		t.Error("no Host vacate failed before the deadline line")
	}
	if hns.cmd.AllClear(testGroup) {
		t.Error("a host that answered FAILED counts as clear")
	}
}

// TestORCHA6_Push_ForgetLogsEmptyRegistry: D-ORCH-4 stage 2, when a group is
// deleted its last "Host registry updated" line lists no hosts, so the old
// hosts do not look registered after the cleanup.
func TestORCHA6_Push_ForgetLogsEmptyRegistry(t *testing.T) {
	hns := newHarness(t, map[string]string{}, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, []string{"node-a"})
	hns.cmd.Forget(testGroup)
	hns.cmd.Forget(testGroup) // a group already forgotten logs nothing
	recs := hns.sink.records(t, "Host registry updated")
	if len(recs) != 2 {
		t.Fatalf("Host registry updated lines = %d, want 2 (sync, forget)", len(recs))
	}
	last := recs[1]
	if last["group"] != testGroup || last["reason"] != "group deleted" {
		t.Errorf("forget line = %v, want group %s and reason group deleted", last, testGroup)
	}
	if hosts, ok := last["hosts"].([]any); !ok || len(hosts) != 0 {
		t.Errorf("hosts after Forget = %v, want an empty list", last["hosts"])
	}
}
