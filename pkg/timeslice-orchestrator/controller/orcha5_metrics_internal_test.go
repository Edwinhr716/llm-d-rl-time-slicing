package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The metrics are process globals and the kill fixture always uses group g,
// node node-1 and guest guest-1, so these tests compare deltas.

func guestKills(reason string) float64 {
	return testutil.ToFloat64(metrics.GuestKillsTotal.WithLabelValues(reason, killNode))
}

func TestORCHA5_KillMetricReason(t *testing.T) {
	for reason, want := range map[string]string{
		killReasonDeadline: metrics.KillReasonDeadline,
		killReasonFaulted:  metrics.KillReasonFaulted,
		killReasonVKUnseen: metrics.KillReasonDisconnected,
	} {
		if got := killMetricReason(reason); got != want {
			t.Errorf("killMetricReason(%q) = %q, want %q", reason, got, want)
		}
	}
}

// TestORCHA5_GuestKills_DeadlineCountedOnce: a hung host killed at T counts
// one deadline kill, and later passes do not count it again.
func TestORCHA5_GuestKills_DeadlineCountedOnce(t *testing.T) {
	before := guestKills(metrics.KillReasonDeadline)
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	if fx.passUntilClear(t, 2*time.Second).IsZero() {
		t.Fatal("host never cleared")
	}
	fx.ctrl.killOverdueHosts(context.Background(), fx.group)
	if got := guestKills(metrics.KillReasonDeadline) - before; got != 1 {
		t.Errorf("deadline kills = %v, want 1", got)
	}
}

// TestORCHA5_GuestKills_RetriesCountOnce: a Kill that fails once and is then
// delivered counts once, when the agent accepts it.
func TestORCHA5_GuestKills_RetriesCountOnce(t *testing.T) {
	before := guestKills(metrics.KillReasonDeadline)
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	var attempts atomic.Int32
	fx.agent.KillFunc = func(context.Context, string, string, string, time.Time) (*agentpb.KillResponse, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("connection refused")
		}
		return &agentpb.KillResponse{OperationId: "kill-op"}, nil
	}
	if fx.passUntilClear(t, 3*time.Second).IsZero() {
		t.Fatal("host never cleared")
	}
	if attempts.Load() < 2 {
		t.Fatalf("Kill attempts = %d, want a retry", attempts.Load())
	}
	if got := guestKills(metrics.KillReasonDeadline) - before; got != 1 {
		t.Errorf("deadline kills = %v, want 1", got)
	}
}

func TestORCHA5_GuestKills_Faulted(t *testing.T) {
	before := guestKills(metrics.KillReasonFaulted)
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.hosts.running = false
	fx.guest.UpdateContextState(killNode, pb.SnapshotAgentJobState_STATE_FAULTED)
	fx.ctrl.killFaultedGuests(context.Background(), fx.group)
	fx.ctrl.killFaultedGuests(context.Background(), fx.group)
	if got := guestKills(metrics.KillReasonFaulted) - before; got != 1 {
		t.Errorf("faulted kills = %v, want 1", got)
	}
}

func TestORCHA5_GuestKills_Disconnected(t *testing.T) {
	before := guestKills(metrics.KillReasonDisconnected)
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.ctrl.BackgroundLiveness = time.Second
	fx.hosts.bar.NotClear[0].FailingSince = time.Now().Add(-1500 * time.Millisecond)
	if !fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host not cleared after L")
	}
	if got := guestKills(metrics.KillReasonDisconnected) - before; got != 1 {
		t.Errorf("disconnected kills = %v, want 1", got)
	}
}

// TestORCHA5_AgentUnreachable_KillNotDelivered: an undelivered Kill sets
// timeslice_agent_unreachable for the node; the agent answering clears it.
func TestORCHA5_AgentUnreachable_KillNotDelivered(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	fx.agent.KillFunc = func(context.Context, string, string, string, time.Time) (*agentpb.KillResponse, error) {
		return nil, errors.New("connection refused")
	}
	gauge := metrics.AgentUnreachable.WithLabelValues(killNode)
	fx.ctrl.killOverdueHosts(context.Background(), fx.group)
	if got := testutil.ToFloat64(gauge); got != 1 {
		t.Fatalf("agent_unreachable = %v after an undelivered Kill, want 1", got)
	}
	fx.ctrl.markAgentSeen(killNode)
	if got := testutil.ToFloat64(gauge); got != 0 {
		t.Errorf("agent_unreachable = %v after the agent answered, want 0", got)
	}
}

// TestORCHA5_AgentUnreachable_Hold: a host held because its agent is not seen
// sets timeslice_agent_unreachable.
func TestORCHA5_AgentUnreachable_Hold(t *testing.T) {
	fx := newKillFixture(t, time.Now().Add(-time.Second), 400*time.Millisecond, 100*time.Millisecond)
	fx.guest.SetRole(store.RoleForeground) // no guest known on the host
	gauge := metrics.AgentUnreachable.WithLabelValues(killNode)
	gauge.Set(0)
	if fx.ctrl.killOverdueHosts(context.Background(), fx.group) {
		t.Fatal("host cleared while no agent was seen")
	}
	if got := testutil.ToFloat64(gauge); got != 1 {
		t.Fatalf("agent_unreachable = %v while held, want 1", got)
	}
	fx.ctrl.markAgentSeen(killNode)
	if got := testutil.ToFloat64(gauge); got != 0 {
		t.Errorf("agent_unreachable = %v after the agent answered, want 0", got)
	}
}

// offwindowSeries returns the value of the off-window series of the kill
// fixture's guest, and whether it exists.
func offwindowSeries(t *testing.T) (float64, bool) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(metrics.GuestOffwindowSeconds)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"group_id": killGroup, "job_id": killGuest, "node": killNode}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			match := 0
			for _, l := range m.GetLabel() {
				if want[l.GetName()] == l.GetValue() {
					match++
				}
			}
			if match == len(want) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// offwindowFixture is a kill fixture whose agent reports the guest in state.
func offwindowFixture(t *testing.T, state *atomic.Int32) *killFixture {
	t.Helper()
	fx := newKillFixture(t, time.Now(), 30*time.Second, 3*time.Second)
	fx.hosts.running = false
	fx.agent.GetStatusFunc = func(context.Context, string) (*agentpb.StatusResponse, error) {
		return &agentpb.StatusResponse{JobStatuses: []*agentpb.JobStatus{
			{JobId: killGuest, State: agentpb.JobState(state.Load())},
		}}, nil
	}
	t.Cleanup(func() { metrics.GuestOffwindowSeconds.DeleteLabelValues(killGroup, killGuest, killNode) })
	return fx
}

// TestORCHA5_Offwindow_GaugeAndAlertOnce: a guest the agent reports SUSPENDED
// gets an off-window gauge that grows; past the limit the alert counts once;
// the agent reporting it RUNNING again ends the off-window and drops the
// series.
func TestORCHA5_Offwindow_GaugeAndAlertOnce(t *testing.T) {
	ctx := context.Background()
	var state atomic.Int32
	state.Store(int32(agentpb.JobState_JOB_STATE_SUSPENDED))
	fx := offwindowFixture(t, &state)
	fx.ctrl.MaxServingOffwindow = 100 * time.Millisecond
	exceeded := metrics.GuestOffwindowExceededTotal.WithLabelValues(killGroup)
	before := testutil.ToFloat64(exceeded)

	if err := fx.ctrl.observeNodeJobContext(ctx, killGroup, killNode); err != nil {
		t.Fatal(err)
	}
	if _, ok := offwindowSeries(t); !ok {
		t.Fatal("no off-window series after the agent reported SUSPENDED")
	}
	time.Sleep(50 * time.Millisecond)
	fx.ctrl.updateOffwindows(ctx)
	v, _ := offwindowSeries(t)
	if v < 0.04 || v > 0.1 {
		t.Errorf("off-window = %vs after 50ms, want about 0.05", v)
	}
	if got := testutil.ToFloat64(exceeded) - before; got != 0 {
		t.Errorf("alert counted %v times before the limit", got)
	}

	time.Sleep(100 * time.Millisecond)
	fx.ctrl.updateOffwindows(ctx)
	fx.ctrl.updateOffwindows(ctx)
	if got := testutil.ToFloat64(exceeded) - before; got != 1 {
		t.Errorf("alert counted %v times past the limit, want 1", got)
	}
	// A later reconcile that still sees SUSPENDED keeps the same off-window.
	if err := fx.ctrl.observeNodeJobContext(ctx, killGroup, killNode); err != nil {
		t.Fatal(err)
	}
	fx.ctrl.updateOffwindows(ctx)
	if v, _ := offwindowSeries(t); v < 0.15 {
		t.Errorf("off-window = %vs, want it to keep counting from the first SUSPENDED", v)
	}

	state.Store(int32(agentpb.JobState_JOB_STATE_RUNNING))
	fx.ctrl.updateOffwindows(ctx)
	if _, ok := offwindowSeries(t); ok {
		t.Error("off-window series still there after the agent reported RUNNING")
	}
	if got := testutil.ToFloat64(exceeded) - before; got != 1 {
		t.Errorf("alert counted %v times, want 1", got)
	}
}

// TestORCHA5_Offwindow_EndsWhenGuestGone: the off-window of a guest whose
// mirror pod left the store ends.
func TestORCHA5_Offwindow_EndsWhenGuestGone(t *testing.T) {
	ctx := context.Background()
	var state atomic.Int32
	state.Store(int32(agentpb.JobState_JOB_STATE_SUSPENDED))
	fx := offwindowFixture(t, &state)
	if err := fx.ctrl.observeNodeJobContext(ctx, killGroup, killNode); err != nil {
		t.Fatal(err)
	}
	if _, ok := offwindowSeries(t); !ok {
		t.Fatal("no off-window series")
	}
	if err := fx.ctrl.jobStore.Delete(ctx, killGroup, killGuest); err != nil {
		t.Fatal(err)
	}
	fx.ctrl.updateOffwindows(ctx)
	if _, ok := offwindowSeries(t); ok {
		t.Error("off-window series still there after the guest left the store")
	}
}

// TestORCHA5_Offwindow_AgentErrorKeepsCounting: an agent that does not answer
// leaves the off-window running.
func TestORCHA5_Offwindow_AgentErrorKeepsCounting(t *testing.T) {
	ctx := context.Background()
	var state atomic.Int32
	state.Store(int32(agentpb.JobState_JOB_STATE_SUSPENDED))
	fx := offwindowFixture(t, &state)
	if err := fx.ctrl.observeNodeJobContext(ctx, killGroup, killNode); err != nil {
		t.Fatal(err)
	}
	fx.agent.GetStatusFunc = func(context.Context, string) (*agentpb.StatusResponse, error) {
		return nil, errors.New("connection refused")
	}
	time.Sleep(20 * time.Millisecond)
	fx.ctrl.updateOffwindows(ctx)
	if v, ok := offwindowSeries(t); !ok || v <= 0 {
		t.Errorf("off-window = %v (present %v) with the agent unreachable, want it still counting", v, ok)
	}
}

// TestORCHA5_Offwindow_ForegroundNotTracked: a foreground job the agent
// reports SUSPENDED has no off-window.
func TestORCHA5_Offwindow_ForegroundNotTracked(t *testing.T) {
	ctx := context.Background()
	var state atomic.Int32
	state.Store(int32(agentpb.JobState_JOB_STATE_SUSPENDED))
	fx := offwindowFixture(t, &state)
	fx.guest.SetRole(store.RoleForeground)
	if err := fx.ctrl.observeNodeJobContext(ctx, killGroup, killNode); err != nil {
		t.Fatal(err)
	}
	if _, ok := offwindowSeries(t); ok {
		t.Error("a foreground job got an off-window series")
	}
}

func TestORCHA5_Offwindow_DefaultLimit(t *testing.T) {
	c := NewController(nil, nil, nil, nil, nil)
	if c.MaxServingOffwindow != 4*time.Minute {
		t.Errorf("default MaxServingOffwindow = %v, want 4m", c.MaxServingOffwindow)
	}
}

// zeroSeries reports whether c has a series with exactly labels, and its
// counter value (0 for other types).
func zeroSeries(t *testing.T, c prometheus.Collector, labels map[string]string) (float64, bool) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, l := range m.GetLabel() {
				if labels[l.GetName()] != l.GetValue() {
					match = false
				}
			}
			if match {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestORCHA5_GuestSeriesStartAtZero: the first reconcile of a guest creates
// its node's kill series and its group's off-window alert counter at zero, so
// a backend that takes the first sample as the baseline counts the first kill.
func TestORCHA5_GuestSeriesStartAtZero(t *testing.T) {
	const group, node = "orcha5-zero", "orcha5-zero-node"
	c := NewController(nil, nil, nil, nil, nil)
	c.noteGuestState(group, "guest-z", node, pb.SnapshotAgentJobState_STATE_RUNNING)
	for _, reason := range []string{metrics.KillReasonDeadline, metrics.KillReasonFaulted, metrics.KillReasonDisconnected} {
		v, ok := zeroSeries(t, metrics.GuestKillsTotal, map[string]string{"reason": reason, "node": node})
		if !ok || v != 0 {
			t.Errorf("guest_kills_total{reason=%s} = %v, exists %v; want 0, true", reason, v, ok)
		}
	}
	if v, ok := zeroSeries(t, metrics.GuestOffwindowExceededTotal, map[string]string{"group_id": group}); !ok || v != 0 {
		t.Errorf("guest_offwindow_exceeded_total = %v, exists %v; want 0, true", v, ok)
	}
}
