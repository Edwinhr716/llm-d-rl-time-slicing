package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// durationBuckets spans 1ms to 10min: operation times range from near-instant
// (uncontended lock acquisition) to minutes (snapshot/restore of large models).
var durationBuckets = []float64{0.001, 0.01, 0.1, 1, 10, 60, 300, 600}

var (
	// QueueDepth tracks the current number of jobs waiting in the queue for a group lock.
	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_orchestrator_queue_depth",
			Help: "Current number of jobs waiting in the queue for a group lock.",
		},
		[]string{"group_id"},
	)

	// AcquireWaitDuration tracks the time spent by a job waiting in Acquire() until lock acquisition & context restoration.
	AcquireWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_orchestrator_acquire_wait_duration_seconds",
			Help:    "Time spent by a job waiting in Acquire() until lock acquisition and context restoration.",
			Buckets: durationBuckets,
		},
		[]string{"group_id"},
	)

	// AgentOperationDuration tracks the duration of snapshot and restore operations as reported by snapshot agents.
	AgentOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_orchestrator_agent_operation_duration_seconds",
			Help:    "Duration of snapshot and restore operations as reported by snapshot agents.",
			Buckets: durationBuckets,
		},
		[]string{"group_id", "job_id", "node", "operation"},
	)

	// DeferredSnapshotsTotal tracks the number of times a snapshot was deferred during Yield() due to zero waiters.
	DeferredSnapshotsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_orchestrator_deferred_snapshots_total",
			Help: "Number of times a snapshot was deferred during Yield() due to zero waiters.",
		},
		[]string{"group_id"},
	)

	// QuantumSuppressedPollsTotal tracks GetGroupStatus polls whose waiter queue depth was
	// reported as zero because the lock holder was still inside its minimum serving quantum.
	QuantumSuppressedPollsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_orchestrator_quantum_suppressed_polls_total",
			Help: "Number of GetGroupStatus polls whose waiter queue depth was withheld by the minimum serving quantum.",
		},
		[]string{"group_id"},
	)

	// DispatchBudget is the last dispatch budget successfully published for the
	// batch tenant: 1 when it can serve, 0 when it cannot. Unlabeled because a
	// publisher drives exactly one key for exactly one batch tenant.
	DispatchBudget = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "timeslice_orchestrator_dispatch_budget",
			Help: "Last dispatch budget published for the batch tenant (1 = can serve, 0 = cannot).",
		},
	)

	// DispatchBudgetWritesTotal counts attempts to publish the dispatch budget,
	// by outcome. A rising "error" rate means the gate is running on a stale
	// key, which is only safe while the stale value is 0.
	DispatchBudgetWritesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_orchestrator_dispatch_budget_writes_total",
			Help: "Number of dispatch budget publish attempts, by outcome.",
		},
		[]string{"result"},
	)

	// DispatchBudgetHeldTotal counts evaluations that wanted to publish 1 but
	// published 0 because the rising-edge hold-down had not elapsed. Divided by
	// the publish interval this is roughly the seconds of serving time given up
	// to avoid opening the gate before the tenant's endpoint is routable.
	DispatchBudgetHeldTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "timeslice_orchestrator_dispatch_budget_held_total",
			Help: "Number of dispatch budget evaluations held at 0 by the rising-edge open delay.",
		},
	)

	// DispatchBudgetRisingEdgeSkippedTotal counts evaluations that computed 1 but
	// wrote nothing because the rising edge was delegated to an external
	// publisher. Nothing is wrong when this climbs; it is the mode working. It is
	// how the mode's one failure is diagnosed, though: if this is rising and the
	// batch tenant is still getting no traffic, then nothing is publishing the 1,
	// and the gate is wedged closed rather than merely late.
	DispatchBudgetRisingEdgeSkippedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total",
			Help: "Number of dispatch budget evaluations that computed 1 but were not written, because the rising edge is published externally.",
		},
	)

	// KillUnconfirmedTotal counts guest Kills that were not confirmed when the
	// unconfirmed-kill decision was taken (decision D-NS-6), once per guest,
	// node and vacate barrier, whatever --unconfirmed-kill does next. With
	// --unconfirmed-kill=grant each one makes the next foreground grant carry
	// AcquireResponse.vram_unconfirmed = true.
	KillUnconfirmedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "timeslice_kill_unconfirmed_total",
			Help: "Number of guest kills that were not confirmed when the unconfirmed-kill decision was taken.",
		},
	)

	// GrantBlocked is 1 while a host holds back the foreground grant because a
	// guest Kill on it is not confirmed (--unconfirmed-kill=block or escalate).
	GrantBlocked = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_grant_blocked",
			Help: "1 while the foreground grant of the group is held back by an unconfirmed guest kill on the node.",
		},
		[]string{"group", "node"},
	)

	// UnconfirmedEscalationsTotal counts escalation steps taken for unconfirmed
	// guest Kills (--unconfirmed-kill=escalate), by step.
	UnconfirmedEscalationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_unconfirmed_escalations_total",
			Help: "Number of escalation steps taken for unconfirmed guest kills, by step.",
		},
		[]string{"step"},
	)

	// NodeFailed is 1 while a node is marked not lendable by the last
	// escalation step for an unconfirmed guest Kill.
	NodeFailed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_node_failed",
			Help: "1 while the node is marked not lendable after an unconfirmed guest kill was escalated.",
		},
		[]string{"node"},
	)
)

// noticeBuckets spans 100ms to 5min: the notice window N is tens of seconds
// (30s proposed), and a vacate that runs to the kill at T lands near N - K.
var noticeBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 120, 300}

// Guest kill reasons, the values of the reason label of GuestKillsTotal.
const (
	// KillReasonDeadline: the guest's host was not clear at T.
	KillReasonDeadline = "deadline"
	// KillReasonFaulted: the agent reported the guest FAULTED.
	KillReasonFaulted = "faulted"
	// KillReasonDisconnected: the guest's host was unseen for L.
	KillReasonDisconnected = "disconnected"
)

// Guest time-slicing metrics (ORCH-A5). The host metrics describe the
// per-host command endpoint the orchestrator pushes vacate and resume to
// (decision D-NS-4, option ns-push-vk).
var (
	// NoticeSeconds is the time from the start of a notice to "every host of
	// the group is clear" (every guest vacated), per vacate barrier. It sizes
	// the notice window N from real traffic.
	NoticeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_notice_seconds",
			Help:    "Time from the start of a notice until every guest of the group is vacated.",
			Buckets: noticeBuckets,
		},
		[]string{"group_id"},
	)

	// GuestKillsTotal counts guests the orchestrator killed through their
	// snapshot agent, once per guest per notice, when the agent first
	// accepts the Kill.
	GuestKillsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_guest_kills_total",
			Help: "Number of guests killed by the orchestrator, by reason (deadline, faulted, disconnected) and node.",
		},
		[]string{"reason", "node"},
	)

	// AgentUnreachable is 1 while a grant is blocked because the snapshot
	// agent of the node cannot be reached, and 0 once the agent answers.
	AgentUnreachable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_agent_unreachable",
			Help: "1 while a grant is blocked on the node's unreachable snapshot agent, else 0.",
		},
		[]string{"node"},
	)

	// ForegroundWaitSeconds is how long each granted foreground Acquire
	// waited. It should never exceed N plus the foreground's restore.
	ForegroundWaitSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_foreground_wait_seconds",
			Help:    "Time a granted foreground Acquire waited, including the notice to guests.",
			Buckets: noticeBuckets,
		},
		[]string{"group_id"},
	)

	// GuestOffwindowSeconds is how long each guest has been suspended, as
	// reported by its snapshot agent. The series is removed when the guest
	// runs again or is gone.
	GuestOffwindowSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_guest_offwindow_seconds",
			Help: "How long the guest has been suspended, from its snapshot agent's state.",
		},
		[]string{"group_id", "job_id", "node"},
	)

	// MaxServingOffwindowSeconds is --max-serving-offwindow, the alert
	// threshold for GuestOffwindowSeconds. 0 means the alert is off.
	MaxServingOffwindowSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "timeslice_max_serving_offwindow_seconds",
			Help: "Alert threshold for timeslice_guest_offwindow_seconds (--max-serving-offwindow); 0 = off.",
		},
	)

	// GuestOffwindowExceededTotal counts guest off-windows that passed
	// --max-serving-offwindow, once per off-window.
	GuestOffwindowExceededTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "timeslice_guest_offwindow_exceeded_total",
			Help: "Number of guest off-windows that passed --max-serving-offwindow.",
		},
		[]string{"group_id"},
	)

	// BackgroundParticipants is 1 while the node's host command endpoint (its
	// VK) is present: registered and answering commands. A host never
	// commanded counts as present. 0 means every notice to it ends in a kill.
	BackgroundParticipants = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "timeslice_background_participants",
			Help: "1 when the node's background participant (VK) answers host commands, 0 when it does not.",
		},
		[]string{"node"},
	)

	// HostVacateSeconds is the time from a Vacate sent to a host until the
	// host is clear, by how it cleared: "ack" (the host acked), or "kill",
	// "unconfirmed-kill", "no-live-guest" (the orchestrator vacated it).
	HostVacateSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_host_vacate_seconds",
			Help:    "Time from a Vacate sent to a host until the host is clear, by node and how it cleared.",
			Buckets: noticeBuckets,
		},
		[]string{"node", "how"},
	)

	// HostResumeSeconds is the time from a Resume sent to a host until the
	// host acked it.
	HostResumeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "timeslice_host_resume_seconds",
			Help:    "Time from a Resume sent to a host until the host acked it, by node.",
			Buckets: noticeBuckets,
		},
		[]string{"node"},
	)
)

// Managed Prometheus backends, such as Google Cloud Managed Service for
// Prometheus, take the first sample of a counter or histogram series as its
// baseline, so a series born at 1 loses its first event: a demo with one kill
// would show none. The Init functions create the series at zero before
// anything is counted.

// hostVacateHows are the how label values of HostVacateSeconds.
var hostVacateHows = []string{"ack", "kill", "unconfirmed-kill", "no-live-guest"}

// InitHostSeries creates the per-host series of a background host at zero.
func InitHostSeries(node string) {
	for _, how := range hostVacateHows {
		HostVacateSeconds.WithLabelValues(node, how)
	}
	HostResumeSeconds.WithLabelValues(node)
}

// InitGuestSeries creates the kill series of a node that runs a guest, and
// the off-window alert counter of its group, at zero.
func InitGuestSeries(groupID, node string) {
	for _, reason := range []string{KillReasonDeadline, KillReasonFaulted, KillReasonDisconnected} {
		GuestKillsTotal.WithLabelValues(reason, node)
	}
	GuestOffwindowExceededTotal.WithLabelValues(groupID)
}

// InitGroupSeries creates the notice and foreground wait series of a group at
// zero.
func InitGroupSeries(groupID string) {
	NoticeSeconds.WithLabelValues(groupID)
	ForegroundWaitSeconds.WithLabelValues(groupID)
}

// CleanupGroup removes gauge series labeled with the given group so stale
// values don't persist in /metrics after the group is deleted. Cumulative
// metrics (histograms, counters) are left intact so group ID reuse doesn't
// appear as a counter reset.
func CleanupGroup(groupID string) {
	QueueDepth.DeletePartialMatch(prometheus.Labels{"group_id": groupID})
	GuestOffwindowSeconds.DeletePartialMatch(prometheus.Labels{"group_id": groupID})
}

// Register registers all timeslice orchestrator Prometheus metrics with the default registry.
func Register() {
	prometheus.MustRegister(
		QueueDepth,
		AcquireWaitDuration,
		AgentOperationDuration,
		DeferredSnapshotsTotal,
		QuantumSuppressedPollsTotal,
		DispatchBudget,
		DispatchBudgetWritesTotal,
		DispatchBudgetHeldTotal,
		DispatchBudgetRisingEdgeSkippedTotal,
		KillUnconfirmedTotal,
		NoticeSeconds,
		GuestKillsTotal,
		AgentUnreachable,
		ForegroundWaitSeconds,
		GuestOffwindowSeconds,
		MaxServingOffwindowSeconds,
		GuestOffwindowExceededTotal,
		BackgroundParticipants,
		HostVacateSeconds,
		HostResumeSeconds,
		GrantBlocked,
		UnconfirmedEscalationsTotal,
		NodeFailed,
	)
}
