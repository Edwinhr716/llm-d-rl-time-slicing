package hostcmd_test

import (
	"context"
	"slices"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
)

// hungHost never acks a vacate: each call blocks until its context ends.
func hungHost() *fakeHost {
	return &fakeHost{vacate: func(ctx context.Context, _ *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
}

// TestORCHA4_Barrier_ListsNotClearHosts: Barrier reports the deadline T, N,
// K and each host that has not acked, with when its commands started failing.
// The first failed vacate asks the reconcile loop to look at the group.
func TestORCHA4_Barrier_ListsNotClearHosts(t *testing.T) {
	addrs := map[string]string{"node-a": serve(t, &fakeHost{}), "node-b": closedAddr(t), "node-c": serve(t, hungHost())}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	if _, ok := hns.cmd.Barrier(testGroup); ok {
		t.Fatal("a barrier before StartVacate")
	}

	noticeAt := time.Now()
	hns.cmd.StartVacate(testGroup, noticeAt)
	eventually(t, "node-a clear and node-b failing", func() bool {
		bar, ok := hns.cmd.Barrier(testGroup)
		return ok && len(bar.NotClear) == 2 && !bar.NotClear[0].FailingSince.IsZero()
	})
	bar, _ := hns.cmd.Barrier(testGroup)
	if !bar.NoticeAt.Equal(noticeAt) || !bar.Deadline.Equal(noticeAt.Add(27*time.Second)) {
		t.Errorf("barrier notice %v deadline %v, want %v and notice + N - K", bar.NoticeAt, bar.Deadline, noticeAt)
	}
	if bar.NoticeWindow != 30*time.Second || bar.KillBudget != 3*time.Second {
		t.Errorf("barrier N=%v K=%v, want 30s and 3s", bar.NoticeWindow, bar.KillBudget)
	}
	if bar.NotClear[0].Node != "node-b" || bar.NotClear[1].Node != "node-c" {
		t.Errorf("not clear = %+v, want node-b then node-c", bar.NotClear)
	}
	if !bar.NotClear[1].FailingSince.IsZero() {
		t.Error("the hung but reachable host has FailingSince set")
	}
	if bar.NotClear[1].State != hostcmd.StateVacating {
		t.Errorf("hung host state = %v, want vacating", bar.NotClear[1].State)
	}
	eventually(t, "enqueue on the first failed vacate", func() bool { return hns.enqueued.Load() >= 1 })
}

// TestORCHA4_ClearByOrchestrator_FinishesBarrier: the kill path marks a hung
// host clear; its command is stopped and the barrier finishes.
func TestORCHA4_ClearByOrchestrator_FinishesBarrier(t *testing.T) {
	hung := hungHost()
	addrs := map[string]string{"node-a": serve(t, &fakeHost{}), "node-b": serve(t, hung)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, nodes(addrs))
	hns.cmd.StartVacate(testGroup, time.Now())
	eventually(t, "node-a clear", func() bool {
		return hns.cmd.HostStates(testGroup)["node-a"] == hostcmd.StateClear
	})
	if hns.cmd.AllClear(testGroup) {
		t.Fatal("all clear while node-b hangs")
	}

	hns.cmd.ClearByOrchestrator(testGroup, "node-b", "kill")
	if !hns.cmd.AllClear(testGroup) {
		t.Fatal("not all clear after ClearByOrchestrator")
	}
	if _, ok := hns.cmd.Barrier(testGroup); ok {
		t.Error("the barrier still runs after every host is clear")
	}
	eventually(t, "enqueue", func() bool { return hns.enqueued.Load() >= 1 })
	if !hns.sink.hasRecord(t, "Host clear", map[string]any{"group": testGroup, "node": "node-b", "how": "kill"}) {
		t.Error("no Host clear how=kill line")
	}
	if len(hns.sink.records(t, "All hosts clear")) != 1 {
		t.Error("want one All hosts clear line")
	}
	// Clearing again, or an unknown host, changes nothing.
	hns.cmd.ClearByOrchestrator(testGroup, "node-b", "kill")
	hns.cmd.ClearByOrchestrator(testGroup, "node-x", "kill")
	if got := len(hns.sink.records(t, "Host clear")); got != 2 {
		t.Errorf("Host clear lines = %d, want 2", got)
	}
}

// TestORCHA4_HostRegistryUpdatedLog: D-ORCH-4 H5, one line per change of the
// host set, with the sorted hosts.
func TestORCHA4_HostRegistryUpdatedLog(t *testing.T) {
	hns := newHarness(t, map[string]string{}, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, []string{"node-b", "node-a"})
	hns.cmd.SyncHosts(testGroup, []string{"node-a", "node-b"})
	hns.cmd.SyncHosts(testGroup, []string{"node-a"})
	recs := hns.sink.records(t, "Host registry updated")
	if len(recs) != 2 {
		t.Fatalf("Host registry updated lines = %d, want 2", len(recs))
	}
	got := make([][]string, 0, len(recs))
	for _, rec := range recs {
		if rec["group"] != testGroup {
			t.Errorf("group = %v, want %s", rec["group"], testGroup)
		}
		raw, ok := rec["hosts"].([]any)
		if !ok {
			t.Fatalf("hosts = %v, want a list", rec["hosts"])
		}
		hosts := make([]string, 0, len(raw))
		for _, h := range raw {
			name, ok := h.(string)
			if !ok {
				t.Fatalf("host %v is not a string", h)
			}
			hosts = append(hosts, name)
		}
		got = append(got, hosts)
	}
	if !slices.Equal(got[0], []string{"node-a", "node-b"}) || !slices.Equal(got[1], []string{"node-a"}) {
		t.Errorf("hosts = %v, want [node-a node-b] then [node-a]", got)
	}
}

// TestORCHA4_HostJoinedAfterDeadlineEnqueues: a host that joins a barrier
// after T is sent the barrier's Vacate and asks the reconcile loop to look at
// the group, so the kill path acts on it at once.
func TestORCHA4_HostJoinedAfterDeadlineEnqueues(t *testing.T) {
	failing := func() *fakeHost {
		return &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
			return &hcpb.HostAck{
				NodeName: req.GetNodeName(), Epoch: req.GetEpoch(),
				Command: hcpb.Command_COMMAND_VACATE, Outcome: hcpb.Outcome_OUTCOME_FAILED,
			}, nil
		}}
	}
	late := failing()
	addrs := map[string]string{"node-a": serve(t, failing()), "node-b": serve(t, late)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(testGroup, []string{"node-a"})
	hns.cmd.StartVacate(testGroup, time.Now().Add(-time.Minute))
	eventually(t, "enqueue at T", func() bool { return hns.enqueued.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if got := hns.enqueued.Load(); got != 1 {
		t.Fatalf("enqueued = %d before the join, want 1", got)
	}

	hns.cmd.SyncHosts(testGroup, []string{"node-a", "node-b"})
	eventually(t, "enqueue on the late join", func() bool { return hns.enqueued.Load() == 2 })
	eventually(t, "node-b commanded", func() bool { return late.calls.Load() >= 1 })
	bar, ok := hns.cmd.Barrier(testGroup)
	if !ok || len(bar.NotClear) != 2 || bar.NotClear[1].Node != "node-b" || bar.NotClear[1].State != hostcmd.StateVacating {
		t.Fatalf("barrier = %+v, %v; want node-a and node-b vacating", bar, ok)
	}
	if !hns.sink.hasRecord(t, "Host not clear at deadline", map[string]any{"group": testGroup, "node": "node-b"}) {
		t.Error("no Host not clear at deadline line for the late host")
	}
	if len(hns.sink.records(t, "Vacate started")) != 1 {
		t.Error("want one Vacate started line: the join must not start a new barrier")
	}
}
