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
)

// CleanupGroup removes gauge series labeled with the given group so stale
// values don't persist in /metrics after the group is deleted. Cumulative
// metrics (histograms, counters) are left intact so group ID reuse doesn't
// appear as a counter reset.
func CleanupGroup(groupID string) {
	QueueDepth.DeletePartialMatch(prometheus.Labels{"group_id": groupID})
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
	)
}
