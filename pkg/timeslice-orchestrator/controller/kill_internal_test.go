package controller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/client-go/util/workqueue"
)

const (
	killGroup = "g"
	killNode  = "node-1"
	killGuest = "guest-1"
)

// barrierHosts is a HostCommander with a scripted running barrier. Hosts the
// kill path clears leave NotClear.
type barrierHosts struct {
	mu      sync.Mutex
	bar     hostcmd.Barrier
	running bool
	cleared map[string]string
}

func (h *barrierHosts) SyncHosts(string, []string) {}

func (h *barrierHosts) Lent(string) bool { return false }

func (h *barrierHosts) StartVacate(string, time.Time) {}

func (h *barrierHosts) Resume(string) {}

func (h *barrierHosts) Forget(string) {}

func (h *barrierHosts) AllClear(string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.bar.NotClear) == 0
}

func (h *barrierHosts) Barrier(string) (hostcmd.Barrier, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	bar := h.bar
	bar.NotClear = slices.Clone(h.bar.NotClear)
	return bar, h.running
}

func (h *barrierHosts) ClearByOrchestrator(_, node, how string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cleared == nil {
		h.cleared = map[string]string{}
	}
	h.cleared[node] = how
	h.bar.NotClear = slices.DeleteFunc(h.bar.NotClear, func(s hostcmd.HostStatus) bool { return s.Node == node })
	if len(h.bar.NotClear) == 0 {
		h.running = false
	}
}

func (h *barrierHosts) clearedHow(node string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cleared[node]
}

// killCall is one Kill the controller sent.
type killCall struct {
	node, job, reason string
	deadline          time.Time
	at                time.Time
}

type killFixture struct {
	ctrl  *Controller
	queue workqueue.TypedRateLimitingInterface[string]
	group *store.Group
	guest *store.Job
	hosts *barrierHosts
	agent *MockSnapshotAgentStore

	mu    sync.Mutex
	kills []killCall
}

// newKillFixture builds group g on node-1 with one guest RUNNING there and a
// barrier noticed at noticeAt with N and K. The agent confirms every Kill
// unless the test changes agent.
func newKillFixture(t *testing.T, noticeAt time.Time, window, budget time.Duration) *killFixture {
	t.Helper()
	ctx := context.Background()
	groupStore := store.NewGroupStore(store.NewMemLockStore())
	jobStore := store.NewJobStore()
	group, _, err := groupStore.GetOrCreate(ctx, killGroup)
	if err != nil {
		t.Fatal(err)
	}
	group.Status().SetNodes([]string{killNode})
	guest := store.NewJob(killGroup, killGuest)
	guest.SetRole(store.RoleBackground)
	guest.SetPodNodes([]string{killNode})
	guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_RUNNING)
	if err := jobStore.Put(ctx, guest); err != nil {
		t.Fatal(err)
	}
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "kill-test"},
	)
	t.Cleanup(queue.ShutDown)
	fx := &killFixture{group: group, guest: guest, queue: queue}
	fx.hosts = &barrierHosts{running: true, bar: hostcmd.Barrier{
		NoticeAt: noticeAt, Deadline: noticeAt.Add(window - budget), NoticeWindow: window, KillBudget: budget,
		NotClear: []hostcmd.HostStatus{{Node: killNode, State: hostcmd.StateVacating}},
	}}
	fx.agent = &MockSnapshotAgentStore{
		KillFunc: func(_ context.Context, node, job, reason string, deadline time.Time) (*agentpb.KillResponse, error) {
			fx.mu.Lock()
			defer fx.mu.Unlock()
			fx.kills = append(fx.kills, killCall{node: node, job: job, reason: reason, deadline: deadline, at: time.Now()})
			return &agentpb.KillResponse{OperationId: "kill-op"}, nil
		},
		OperationFunc: func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
			return &agentpb.GetOperationResponse{
				Status: agentpb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: agentpb.Outcome_OUTCOME_KILLED,
			}, nil
		},
	}
	fx.ctrl = NewController(groupStore, jobStore, queue, nil, fx.agent)
	fx.ctrl.Hosts = fx.hosts
	fx.ctrl.KillPollInterval = 10 * time.Millisecond
	return fx
}

func (fx *killFixture) killCalls() []killCall {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return slices.Clone(fx.kills)
}

// passUntilClear runs the kill path the way the reconcile loop does, every
// 20 ms, until the host is clear or the bound passes.
func (fx *killFixture) passUntilClear(t *testing.T, bound time.Duration) time.Time {
	t.Helper()
	end := time.Now().Add(bound)
	for time.Now().Before(end) {
		if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
			return time.Now()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Time{}
}

func TestORCHA4_Kill_NotBeforeT(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host clear before T")
	}
	if got := fx.killCalls(); len(got) != 0 {
		t.Fatalf("Kill sent before T: %+v", got)
	}
}

// TestORCHA4_Kill_HungHostAtT: a host not clear at T has its live guest
// killed with deadline now + K, and is marked clear once the Kill is
// confirmed.
func TestORCHA4_Kill_HungHostAtT(t *testing.T) {
	window, budget := 400*time.Millisecond, 100*time.Millisecond
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, window, budget)
	cleared := fx.passUntilClear(t, 2*time.Second)
	if cleared.IsZero() {
		t.Fatal("host never cleared")
	}
	calls := fx.killCalls()
	if len(calls) != 1 {
		t.Fatalf("Kill calls = %+v, want one", calls)
	}
	call := calls[0]
	if call.node != killNode || call.job != killGuest || call.reason != killReasonDeadline {
		t.Errorf("Kill = %+v, want %s %s reason %s", call, killNode, killGuest, killReasonDeadline)
	}
	if call.at.Before(noticeAt.Add(window - budget)) {
		t.Errorf("Kill sent at %v, before T", call.at.Sub(noticeAt))
	}
	if d := call.deadline.Sub(call.at); d < budget-10*time.Millisecond || d > budget+10*time.Millisecond {
		t.Errorf("Kill deadline = sent + %v, want sent + K (%v)", d, budget)
	}
	if cleared.After(noticeAt.Add(window + 200*time.Millisecond)) {
		t.Errorf("host cleared at notice + %v, want by N (%v)", cleared.Sub(noticeAt), window)
	}
	if got := fx.hosts.clearedHow(killNode); got != clearHowKill {
		t.Errorf("cleared how = %q, want %q", got, clearHowKill)
	}
	if !fx.guest.Killed(killNode) {
		t.Error("guest not marked killed")
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("a confirmed kill flagged vram_unconfirmed")
	}
}

// TestORCHA4_Kill_VKUnseenForL: a host whose commands have failed for L has
// its guests killed before T.
func TestORCHA4_Kill_VKUnseenForL(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.ctrl.BackgroundLiveness = time.Second
	fx.hosts.bar.NotClear[0].FailingSince = time.Now().Add(-1500 * time.Millisecond)
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host not cleared after L")
	}
	calls := fx.killCalls()
	if len(calls) != 1 || calls[0].reason != killReasonVKUnseen {
		t.Fatalf("Kill calls = %+v, want one with reason %s", calls, killReasonVKUnseen)
	}

	// Failing for less than L: nothing yet.
	fx2 := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx2.ctrl.BackgroundLiveness = time.Second
	fx2.hosts.bar.NotClear[0].FailingSince = time.Now()
	if fx2.ctrl.killOverdueHosts(context.Background(), fx2.group) || len(fx2.killCalls()) != 0 {
		t.Fatal("killed before L")
	}
}

// TestORCHA4_Kill_AgentUnreachableHolds: a Kill that never reaches the agent
// keeps the host not clear (fail closed) and is retried.
func TestORCHA4_Kill_AgentUnreachableHolds(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	var attempts int
	var mu sync.Mutex
	fx.agent.KillFunc = func(context.Context, string, string, string, time.Time) (*agentpb.KillResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		return nil, errors.New("connection refused")
	}
	before := testutil.ToFloat64(metrics.KillUnconfirmedTotal)
	if !fx.passUntilClear(t, 1500*time.Millisecond).IsZero() {
		t.Fatal("host cleared while its agent is unreachable")
	}
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got < 2 {
		t.Errorf("Kill attempts = %d, want a retry after %v", got, killRetryInterval)
	}
	if testutil.ToFloat64(metrics.KillUnconfirmedTotal) != before {
		t.Error("an undelivered Kill counted as unconfirmed")
	}
}

// TestORCHA4_Kill_NoGuest: a host with no known guest is cleared only when its
// agent answered within L; otherwise it is held (fail closed).
func TestORCHA4_Kill_NoGuest(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	fx.guest.SetRole(store.RoleForeground) // not a guest
	if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host cleared while no agent was seen")
	}
	fx.ctrl.markAgentSeen(killNode)
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host not cleared with no guest and a live agent")
	}
	if got := fx.hosts.clearedHow(killNode); got != clearHowNoLiveGuest {
		t.Errorf("cleared how = %q, want %q", got, clearHowNoLiveGuest)
	}
	if len(fx.killCalls()) != 0 {
		t.Error("Kill sent with no guest")
	}
}

// TestORCHA4_Kill_SuspendedGuestIsVacated: a guest the agent reports
// SUSPENDED is vacated and not killed.
func TestORCHA4_Kill_SuspendedGuestIsVacated(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_SUSPENDED)
	fx.ctrl.markAgentSeen(killNode)
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) || len(fx.killCalls()) != 0 {
		t.Fatal("a SUSPENDED guest was killed or held")
	}
}

// TestORCHA4_Kill_FaultedGuestAnyTime: a guest the agent reports FAULTED is
// killed without a barrier.
func TestORCHA4_Kill_FaultedGuestAnyTime(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.hosts.running = false
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_FAULTED)
	fx.ctrl.killFaultedGuests(context.Background(), fx.group)
	calls := fx.killCalls()
	if len(calls) != 1 || calls[0].reason != killReasonFaulted {
		t.Fatalf("Kill calls = %+v, want one with reason %s", calls, killReasonFaulted)
	}
	if !fx.guest.Killed(killNode) {
		t.Error("faulted guest not marked killed")
	}
	fx.ctrl.killFaultedGuests(context.Background(), fx.group)
	if len(fx.killCalls()) != 1 {
		t.Error("a killed guest was killed again")
	}
}

// N and K of the unconfirmed-kill cases.
const (
	unconfirmedN = 600 * time.Millisecond
	unconfirmedK = 100 * time.Millisecond
)

// unconfirmedCase runs one unconfirmed Kill signal and checks today's H2
// default: the host is handed back at noticeAt + N, not before, with
// vram_unconfirmed and the metric.
func unconfirmedCase(t *testing.T, fx *killFixture, noticeAt time.Time, wantSignal string) {
	t.Helper()
	window := unconfirmedN
	before := testutil.ToFloat64(metrics.KillUnconfirmedTotal)
	cleared := fx.passUntilClear(t, 3*time.Second)
	if cleared.IsZero() {
		t.Fatal("host never handed back")
	}
	if cleared.Before(noticeAt.Add(window)) {
		t.Errorf("handed back at notice + %v, before N (%v)", cleared.Sub(noticeAt), window)
	}
	if cleared.After(noticeAt.Add(window + 300*time.Millisecond)) {
		t.Errorf("handed back at notice + %v, want right after N (%v)", cleared.Sub(noticeAt), window)
	}
	if got := fx.hosts.clearedHow(killNode); got != clearHowUnconfirmedKill {
		t.Errorf("cleared how = %q, want %q", got, clearHowUnconfirmedKill)
	}
	if !fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("the next grant is not flagged vram_unconfirmed")
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("vram_unconfirmed not cleared once taken")
	}
	if got := testutil.ToFloat64(metrics.KillUnconfirmedTotal) - before; got != 1 {
		t.Errorf("timeslice_kill_unconfirmed_total grew by %v, want 1", got)
	}
	if !fx.guest.UnconfirmedKill(killNode) || fx.guest.Killed(killNode) {
		t.Error("guest not marked unconfirmed, or marked killed")
	}
	rec := fx.ctrl.killRecordFor(killKey(killGroup, killNode, killGuest), killReasonDeadline, noticeAt)
	if rec.signal != wantSignal {
		t.Errorf("signal = %q, want %q", rec.signal, wantSignal)
	}
}

func TestUnconfirmedKill_Grant_AgentKillUnconfirmed(t *testing.T) {
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	fx.agent.OperationFunc = func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		msg := "device memory still mapped"
		return &agentpb.GetOperationResponse{
			Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED, Error: &msg,
			ErrorReason: agentpb.ErrorReason_KILL_UNCONFIRMED,
		}, nil
	}
	unconfirmedCase(t, fx, noticeAt, unconfirmedKillAgent)
}

func TestUnconfirmedKill_Grant_NotCompleteByK(t *testing.T) {
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	fx.agent.OperationFunc = func(context.Context, string, string) (*agentpb.GetOperationResponse, error) {
		return &agentpb.GetOperationResponse{Status: agentpb.OperationStatus_OPERATION_STATUS_PENDING}, nil
	}
	unconfirmedCase(t, fx, noticeAt, unconfirmedKillTimeout)
}

func TestUnconfirmedKill_Grant_DeviceBytesAfterComplete(t *testing.T) {
	noticeAt := time.Now()
	fx := newKillFixture(t, noticeAt, unconfirmedN, unconfirmedK)
	fx.agent.GetStatusFunc = func(context.Context, string) (*agentpb.StatusResponse, error) {
		return &agentpb.StatusResponse{JobStatuses: []*agentpb.JobStatus{
			{JobId: killGuest, State: agentpb.JobState_JOB_STATE_RUNNING, DeviceBytes: 1 << 30},
		}}, nil
	}
	unconfirmedCase(t, fx, noticeAt, unconfirmedDeviceBytes)
}

// TestUnconfirmedKill_Grant_NotBeforeKillBudget: the hand-back never comes
// before the Kill had its full budget K, even when N already ran out.
func TestUnconfirmedKill_Grant_NotBeforeKillBudget(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-10*time.Second), 400*time.Millisecond, 100*time.Millisecond)
	start := time.Now()
	if fx.ctrl.onKillUnconfirmed(context.Background(), killGroup, killNode, killGuest, start).grant {
		t.Fatal("granted before since + K")
	}
	time.Sleep(120 * time.Millisecond)
	decision := fx.ctrl.onKillUnconfirmed(context.Background(), killGroup, killNode, killGuest, start)
	if !decision.grant || !decision.vramUnconfirmed {
		t.Fatalf("onKillUnconfirmed = %+v after K and N, want grant with vram_unconfirmed", decision)
	}
}

// TestUnconfirmedKill_NotLentAgain: a guest handed back after an unconfirmed
// Kill blocks the next lend until the agent reports it vacated.
func TestUnconfirmedKill_NotLentAgain(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.guest.SetUnconfirmedKill(killNode)
	if got := fx.ctrl.unconfirmedGuestOn(context.Background(), fx.group); got.job != killGuest || got.node != killNode {
		t.Fatalf("unconfirmedGuestOn = %+v, want %s on %s", got, killGuest, killNode)
	}
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_SUSPENDED)
	if got := fx.ctrl.unconfirmedGuestOn(context.Background(), fx.group); got.job != "" {
		t.Fatalf("unconfirmedGuestOn = %+v after the agent reports it suspended, want none", got)
	}
}

// realCommander returns a hostcmd.Commander whose hosts cannot be reached,
// with a counter of the groups it enqueued.
func realCommander(t *testing.T, window, budget time.Duration) (*hostcmd.Commander, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var enqueued atomic.Int32
	cmd := hostcmd.New(ctx, hostcmd.Config{
		Resolve:           func(string) (string, error) { return "", errors.New("unreachable") },
		NoticeWindow:      window,
		KillBudget:        budget,
		Enqueue:           func(string) { enqueued.Add(1) },
		RetryInterval:     10 * time.Millisecond,
		LateRetryInterval: 20 * time.Millisecond,
		AttemptTimeout:    100 * time.Millisecond,
	})
	t.Cleanup(func() {
		cancel()
		cmd.Close()
	})
	return cmd, &enqueued
}

// TestORCHA4_Kill_EmptyGroupThenLateJoin: with the real Commander, a barrier
// of a group with no hosts holds past T and the kill path sends nothing (fail
// closed). A host that joins after T is sent the barrier's Vacate, the
// reconcile loop is asked to look again, and the kill path kills its guest and
// clears it, which finishes the barrier.
func TestORCHA4_Kill_EmptyGroupThenLateJoin(t *testing.T) {
	window, budget := 400*time.Millisecond, 100*time.Millisecond
	fx := newKillFixture(t, time.Now(), window, budget)
	cmd, enqueued := realCommander(t, window, budget)
	fx.ctrl.Hosts = cmd
	ctx := context.Background()

	cmd.SyncHosts(killGroup, nil)
	cmd.StartVacate(killGroup, time.Now().Add(-time.Second))
	if _, ok := cmd.Barrier(killGroup); !ok {
		t.Fatal("no barrier for a group without hosts")
	}
	deadline := time.Now().Add(2 * time.Second)
	for enqueued.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fx.ctrl.killOverdueHosts(ctx, fx.group) {
		t.Fatal("a group without hosts is clear past T")
	}
	if len(fx.killCalls()) != 0 {
		t.Fatal("Kill sent for a group without hosts")
	}

	before := enqueued.Load()
	cmd.SyncHosts(killGroup, []string{killNode})
	deadline = time.Now().Add(2 * time.Second)
	for enqueued.Load() <= before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if enqueued.Load() <= before {
		t.Fatal("a host that joined after T did not enqueue the group")
	}
	bar, ok := cmd.Barrier(killGroup)
	if !ok || len(bar.NotClear) != 1 || bar.NotClear[0].Node != killNode {
		t.Fatalf("barrier = %+v, %v; want node-1 not clear", bar, ok)
	}
	if cleared := fx.passUntilClear(t, 2*time.Second); cleared.IsZero() {
		t.Fatal("the late host was never cleared by the kill path")
	}
	calls := fx.killCalls()
	if len(calls) != 1 || calls[0].node != killNode || calls[0].reason != killReasonDeadline {
		t.Fatalf("Kill calls = %+v, want one on %s with reason %s", calls, killNode, killReasonDeadline)
	}
	if _, ok := cmd.Barrier(killGroup); ok {
		t.Error("the barrier still runs after the kill path cleared every host")
	}
	if got := cmd.HostStates(killGroup)[killNode]; got != hostcmd.StateClear {
		t.Errorf("node-1 state = %v, want clear", got)
	}
}

// TestORCHA4_Kill_ForgetDropsKillState: when the group is deleted, the
// controller forgets its hosts and drops its kill records and hold log marks,
// and keeps those of other groups.
func TestORCHA4_Kill_ForgetDropsKillState(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	ctx := context.Background()
	fx.ctrl.killRecordFor(killKey(killGroup, killNode, killGuest), killReasonDeadline, time.Time{})
	fx.ctrl.killRecordFor(killKey("other", killNode, killGuest), killReasonDeadline, time.Time{})
	fx.ctrl.firstHoldLog(killGroup, killNode, time.Time{})
	fx.ctrl.firstHoldLog("other", killNode, time.Time{})

	fx.ctrl.forgetHostsIfGroupDeleted(ctx, killGroup)
	if len(fx.ctrl.kills) != 2 || len(fx.ctrl.holdLogged) != 2 {
		t.Fatal("kill state dropped for a live group")
	}

	if err := fx.ctrl.groupStore.Delete(ctx, killGroup); err != nil {
		t.Fatal(err)
	}
	fx.ctrl.forgetHostsIfGroupDeleted(ctx, killGroup)
	if _, ok := fx.ctrl.kills[killKey(killGroup, killNode, killGuest)]; ok {
		t.Error("kill record of the deleted group kept")
	}
	if _, ok := fx.ctrl.holdLogged[killGroup+"\x00"+killNode]; ok {
		t.Error("hold log mark of the deleted group kept")
	}
	if len(fx.ctrl.kills) != 1 || len(fx.ctrl.holdLogged) != 1 {
		t.Errorf("kill state of other groups = %d records, %d marks; want 1 and 1",
			len(fx.ctrl.kills), len(fx.ctrl.holdLogged))
	}
}

// TestORCHA6_Kill_NotDueRequeuesWithinASecond: while a notice runs and a host
// is not clear and not due, the group is looked at again within noticeRequeue
// rather than only at T, so a guest that turns FAULTED during a hung suspend
// is killed before T.
func TestORCHA6_Kill_NotDueRequeuesWithinASecond(t *testing.T) {
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	queue := fx.queue
	if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host clear before T")
	}
	if n := queue.Len(); n != 0 {
		t.Fatalf("queue length = %d right after the pass, want 0 (no hot loop)", n)
	}
	time.Sleep(noticeRequeue + 200*time.Millisecond)
	if n := queue.Len(); n != 1 {
		t.Fatalf("queue length = %d after %v, want the group requeued", n, noticeRequeue)
	}

	// The next look finds the guest FAULTED and kills it long before T.
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_FAULTED)
	fx.ctrl.killFaultedGuests(context.Background(), fx.group)
	calls := fx.killCalls()
	if len(calls) != 1 || calls[0].reason != killReasonFaulted {
		t.Fatalf("Kill calls = %+v, want one with reason %s", calls, killReasonFaulted)
	}
	if left := time.Until(fx.hosts.bar.Deadline); left < 20*time.Second {
		t.Errorf("killed %v before T, want well before", left)
	}
}

// TestORCHA6_Kill_StopsWhenHostAcks: a Kill sent at T whose operation poll
// wedges stops as soon as the host acks the vacate, so the reconcile goes on
// to the grant instead of holding the group until K runs out. The guest is
// vacated by the host, so nothing is marked killed or unconfirmed and the
// grant is not flagged vram_unconfirmed.
func TestORCHA6_Kill_StopsWhenHostAcks(t *testing.T) {
	const budget = 2 * time.Second
	window := 30 * time.Second
	fx := newKillFixture(t, time.Now().Add(budget-window), window, budget) // T is now
	fx.agent.OperationFunc = func(ctx context.Context, _, _ string) (*agentpb.GetOperationResponse, error) {
		<-ctx.Done() // wedged until the Kill's context ends
		return nil, ctx.Err()
	}
	ack := time.AfterFunc(200*time.Millisecond, func() {
		fx.hosts.mu.Lock()
		defer fx.hosts.mu.Unlock()
		fx.hosts.bar.NotClear = nil // the host's own vacate ack
	})
	t.Cleanup(func() { ack.Stop() })

	start := time.Now()
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host not clear after the pass")
	}
	if took := time.Since(start); took > budget/2 {
		t.Errorf("kill path held the group %v, want it to stop at the host ack (K = %v)", took, budget)
	}
	if calls := fx.killCalls(); len(calls) != 1 || calls[0].reason != killReasonDeadline {
		t.Errorf("Kill calls = %+v, want one with reason %s", calls, killReasonDeadline)
	}
	if fx.guest.Killed(killNode) || fx.guest.UnconfirmedKill(killNode) {
		t.Error("guest marked killed or unconfirmed although the host vacated it")
	}
	if fx.group.Spec().TakeVramUnconfirmed() {
		t.Error("grant flagged vram_unconfirmed")
	}
	if how := fx.hosts.clearedHow(killNode); how != "" {
		t.Errorf("host cleared by the orchestrator (%q), want by its own ack", how)
	}
}

// TestORCHA6_Kill_EarlyUnconfirmedRetryEndsByN: D-NS-6 B1. The Kill sent at T
// fails at once with KILL_UNCONFIRMED. The retry a second later hangs, but it
// is bounded by the unconfirmed-kill decision (noticeAt + N here), so the
// host is handed back at N and not a full K after the retry.
func TestORCHA6_Kill_EarlyUnconfirmedRetryEndsByN(t *testing.T) {
	const budget = 2 * time.Second
	window := 3 * time.Second
	noticeAt := time.Now().Add(budget - window) // T is now, N ends in 2 s
	fx := newKillFixture(t, noticeAt, window, budget)
	fx.agent.OperationFunc = func(ctx context.Context, _, _ string) (*agentpb.GetOperationResponse, error) {
		if len(fx.killCalls()) == 1 {
			msg := "device memory still mapped"
			return &agentpb.GetOperationResponse{
				Status: agentpb.OperationStatus_OPERATION_STATUS_FAILED, Error: &msg,
				ErrorReason: agentpb.ErrorReason_KILL_UNCONFIRMED,
			}, nil
		}
		<-ctx.Done() // the retry hangs
		return nil, ctx.Err()
	}
	cleared := fx.passUntilClear(t, 4*time.Second)
	if cleared.IsZero() {
		t.Fatal("host never handed back")
	}
	grantAt := noticeAt.Add(window)
	if cleared.Before(grantAt) {
		t.Errorf("handed back at notice + %v, before N (%v)", cleared.Sub(noticeAt), window)
	}
	if late := cleared.Sub(grantAt); late > 300*time.Millisecond {
		t.Errorf("handed back %v after N, want at N: the retry Kill held the hand-back", late)
	}
	calls := fx.killCalls()
	if len(calls) != 2 {
		t.Fatalf("Kill calls = %d, want 2 (T and one retry)", len(calls))
	}
	if d := calls[1].deadline; d.After(grantAt.Add(50 * time.Millisecond)) {
		t.Errorf("retry Kill deadline is %v after N, want it to end by N", d.Sub(grantAt))
	}
	if got := fx.hosts.clearedHow(killNode); got != clearHowUnconfirmedKill {
		t.Errorf("cleared how = %q, want %q", got, clearHowUnconfirmedKill)
	}
}

// TestORCHA6_Kill_HostsKilledTogether: D-NS-6 B2. Two hosts are overdue at T
// and each Kill hangs for its full K. The hosts are vacated at the same time,
// so the pass takes about K, not 2K, and both are handed back.
func TestORCHA6_Kill_HostsKilledTogether(t *testing.T) {
	const budget = 1 * time.Second
	window := 30 * time.Second
	fx := newKillFixture(t, time.Now().Add(budget-window), window, budget) // T is now
	const node2, guest2 = "node-2", "guest-2"
	fx.group.Status().SetNodes([]string{killNode, node2})
	g2 := store.NewJob(killGroup, guest2)
	g2.SetRole(store.RoleBackground)
	g2.SetPodNodes([]string{node2})
	g2.UpdateContextState(node2, pb.SnapshotAgentJobState_STATE_RUNNING)
	if err := fx.ctrl.jobStore.Put(context.Background(), g2); err != nil {
		t.Fatal(err)
	}
	fx.hosts.bar.NotClear = append(fx.hosts.bar.NotClear, hostcmd.HostStatus{Node: node2, State: hostcmd.StateVacating})
	fx.agent.OperationFunc = func(ctx context.Context, _, _ string) (*agentpb.GetOperationResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("hosts not clear after one pass")
	}
	if took := time.Since(start); took > budget*3/2 {
		t.Errorf("pass took %v, want about K (%v): the hosts were killed one after another", took, budget)
	}
	calls := fx.killCalls()
	if len(calls) != 2 {
		t.Fatalf("Kill calls = %+v, want one per host", calls)
	}
	if gap := calls[1].at.Sub(calls[0].at).Abs(); gap > budget/2 {
		t.Errorf("second Kill sent %v after the first, want both at T", gap)
	}
	for _, n := range []string{killNode, node2} {
		if got := fx.hosts.clearedHow(n); got != clearHowUnconfirmedKill {
			t.Errorf("%s cleared how = %q, want %q", n, got, clearHowUnconfirmedKill)
		}
	}
}
