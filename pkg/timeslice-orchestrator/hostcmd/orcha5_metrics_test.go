package hostcmd_test

import (
	"context"
	"testing"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The metrics are process globals, so each test uses its own group and node
// names and compares deltas.

// seriesValue is a histogram's sample count and sum, or a gauge's value in
// sum.
type seriesValue struct {
	count uint64
	sum   float64
}

// series finds the series of c with exactly the given labels and reports
// whether it exists.
func series(t *testing.T, c prometheus.Collector, labels map[string]string) (seriesValue, bool) {
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
			if !match {
				continue
			}
			if h := m.GetHistogram(); h != nil {
				return seriesValue{count: h.GetSampleCount(), sum: h.GetSampleSum()}, true
			}
			return seriesValue{sum: m.GetGauge().GetValue()}, true
		}
	}
	return seriesValue{}, false
}

// sampleCount returns the sample count of a histogram series.
func sampleCount(t *testing.T, c prometheus.Collector, labels map[string]string) uint64 {
	t.Helper()
	v, _ := series(t, c, labels)
	return v.count
}

// countOK returns the sample count of a histogram series and whether it exists.
func countOK(t *testing.T, c prometheus.Collector, labels map[string]string) (uint64, bool) {
	t.Helper()
	v, ok := series(t, c, labels)
	return v.count, ok
}

// TestORCHA5_NoticeSeconds: the barrier's finish observes the time from the
// notice to every host clear.
func TestORCHA5_NoticeSeconds(t *testing.T) {
	const group = "orcha5-notice"
	addrs := map[string]string{"orcha5-notice-a": serve(t, &fakeHost{})}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(group, nodes(addrs))
	labels := map[string]string{"group_id": group}
	before, _ := series(t, metrics.NoticeSeconds, labels)

	hns.cmd.StartVacate(group, time.Now().Add(-200*time.Millisecond))
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(group) })
	after, _ := series(t, metrics.NoticeSeconds, labels)
	if n := after.count - before.count; n != 1 {
		t.Fatalf("notice_seconds samples = %d, want 1", n)
	}
	if d := after.sum - before.sum; d < 0.2 || d > 5 {
		t.Errorf("notice_seconds = %vs, want it measured from the notice (>= 0.2s)", d)
	}
	if !hns.sink.hasRecord(t, "All hosts clear", map[string]any{"group": group}) {
		t.Error("no All hosts clear line")
	}
}

// TestORCHA5_HostVacateAndResumeSeconds: a host that acks observes one vacate
// latency with how=ack, and its Resume ack observes one resume latency.
func TestORCHA5_HostVacateAndResumeSeconds(t *testing.T) {
	const group, node = "orcha5-ack", "orcha5-ack-a"
	fh := &fakeHost{vacate: func(_ context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		time.Sleep(50 * time.Millisecond)
		return vacated(req), nil
	}}
	addrs := map[string]string{node: serve(t, fh)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(group, nodes(addrs))
	ack := map[string]string{"node": node, "how": "ack"}
	res := map[string]string{"node": node}
	beforeAck := sampleCount(t, metrics.HostVacateSeconds, ack)
	beforeRes := sampleCount(t, metrics.HostResumeSeconds, res)

	hns.cmd.StartVacate(group, time.Now())
	eventually(t, "all clear", func() bool { return hns.cmd.AllClear(group) })
	after, _ := series(t, metrics.HostVacateSeconds, ack)
	if n := after.count - beforeAck; n != 1 {
		t.Fatalf("host_vacate_seconds{how=ack} samples = %d, want 1", n)
	}
	if after.sum < 0.05 {
		t.Errorf("host_vacate_seconds sum = %vs, want at least the host's 50ms", after.sum)
	}

	hns.cmd.Resume(group)
	eventually(t, "resume observed", func() bool {
		c := sampleCount(t, metrics.HostResumeSeconds, res)
		return c-beforeRes == 1
	})
	// A second notice after the resume is a new vacate: one more sample.
	hns.cmd.StartVacate(group, time.Now())
	eventually(t, "all clear again", func() bool { return hns.cmd.AllClear(group) })
	if c := sampleCount(t, metrics.HostVacateSeconds, ack); c-beforeAck != 2 {
		t.Errorf("host_vacate_seconds{how=ack} samples = %d after two vacates, want 2", c-beforeAck)
	}
}

// TestORCHA5_HostVacateSeconds_Kill: a host the orchestrator clears by a kill
// observes one vacate latency with how=kill; the late ack adds nothing.
func TestORCHA5_HostVacateSeconds_Kill(t *testing.T) {
	const group, node = "orcha5-kill", "orcha5-kill-a"
	release := make(chan struct{})
	fh := &fakeHost{vacate: func(ctx context.Context, req *hcpb.VacateRequest) (*hcpb.HostAck, error) {
		select {
		case <-release:
			return vacated(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	addrs := map[string]string{node: serve(t, fh)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(group, nodes(addrs))
	kill := map[string]string{"node": node, "how": "kill"}
	ack := map[string]string{"node": node, "how": "ack"}
	beforeKill := sampleCount(t, metrics.HostVacateSeconds, kill)
	beforeAck := sampleCount(t, metrics.HostVacateSeconds, ack)

	hns.cmd.StartVacate(group, time.Now())
	eventually(t, "vacate sent", func() bool { return fh.calls.Load() >= 1 })
	hns.cmd.ClearByOrchestrator(group, node, "kill")
	hns.cmd.ClearByOrchestrator(group, node, "kill")
	close(release)
	time.Sleep(50 * time.Millisecond)
	if c := sampleCount(t, metrics.HostVacateSeconds, kill); c-beforeKill != 1 {
		t.Errorf("host_vacate_seconds{how=kill} samples = %d, want 1", c-beforeKill)
	}
	if c := sampleCount(t, metrics.HostVacateSeconds, ack); c != beforeAck {
		t.Errorf("host_vacate_seconds{how=ack} samples = %d, want %d", c, beforeAck)
	}
}

// TestORCHA5_BackgroundParticipants: a synced host counts 1, drops to 0 when
// its commands fail, and its series goes when it leaves the group.
func TestORCHA5_BackgroundParticipants(t *testing.T) {
	const group = "orcha5-bp"
	up, down := "orcha5-bp-up", "orcha5-bp-down"
	addrs := map[string]string{up: serve(t, &fakeHost{}), down: closedAddr(t)}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(group, nodes(addrs))
	for _, node := range []string{up, down} {
		if got := testutil.ToFloat64(metrics.BackgroundParticipants.WithLabelValues(node)); got != 1 {
			t.Errorf("background_participants{%s} = %v after sync, want 1", node, got)
		}
	}

	hns.cmd.StartVacate(group, time.Now())
	eventually(t, "down host counted out", func() bool {
		return testutil.ToFloat64(metrics.BackgroundParticipants.WithLabelValues(down)) == 0
	})
	eventually(t, "up host clear", func() bool { return hns.cmd.Reachable(group, up) })
	if got := testutil.ToFloat64(metrics.BackgroundParticipants.WithLabelValues(up)); got != 1 {
		t.Errorf("background_participants{%s} = %v, want 1", up, got)
	}

	hns.cmd.SyncHosts(group, []string{up})
	if _, ok := series(t, metrics.BackgroundParticipants, map[string]string{"node": down}); ok {
		t.Error("series of a removed host still there")
	}
	hns.cmd.Forget(group)
	if _, ok := series(t, metrics.BackgroundParticipants, map[string]string{"node": up}); ok {
		t.Error("series of a forgotten group's host still there")
	}
}

// TestORCHA5_HostSeriesStartAtZero: registering a host creates its vacate
// (every how) and resume series, and the group's notice series, at zero, so a
// backend that takes the first sample as the baseline counts the first one.
func TestORCHA5_HostSeriesStartAtZero(t *testing.T) {
	const group, node = "orcha5-zero", "orcha5-zero-a"
	addrs := map[string]string{node: serve(t, &fakeHost{})}
	hns := newHarness(t, addrs, 30*time.Second, 3*time.Second)
	hns.cmd.SyncHosts(group, nodes(addrs))
	for _, how := range []string{"ack", "kill", "unconfirmed-kill", "no-live-guest"} {
		if c, ok := countOK(t, metrics.HostVacateSeconds, map[string]string{"node": node, "how": how}); !ok || c != 0 {
			t.Errorf("host_vacate_seconds{how=%s} count = %d, exists %v; want 0, true", how, c, ok)
		}
	}
	if c, ok := countOK(t, metrics.HostResumeSeconds, map[string]string{"node": node}); !ok || c != 0 {
		t.Errorf("host_resume_seconds count = %d, exists %v; want 0, true", c, ok)
	}
	if c, ok := countOK(t, metrics.NoticeSeconds, map[string]string{"group_id": group}); !ok || c != 0 {
		t.Errorf("notice_seconds count = %d, exists %v; want 0, true", c, ok)
	}
}
