// Package budget publishes the batch tenant's dispatch budget to an external
// key/value store, so that a dispatcher in front of the batch tenant is told
// explicitly when the tenant can serve instead of having to infer it.
//
// The consumer this exists for is llm-d-async's `redis` dispatch gate, which
// reads a single key and treats it as a budget in [0,1]: 0 refuses dispatch,
// 1 admits it. That gate has two failure modes with opposite polarity — an
// unreadable Redis fails CLOSED (0), but an ABSENT key fails OPEN (1). The
// absent-key default is why this publisher writes the key unconditionally at
// startup and rewrites it on every evaluation rather than only on change: a
// key that is evicted, flushed or never written leaves the gate wide open.
package budget

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

const (
	// Available is the value published when the batch tenant can serve.
	Available = "1"
	// Blocked is the value published when it cannot.
	Blocked = "0"

	// DefaultKey matches the llm-d-async redis gate's own default budget_key,
	// so a deployment that sets neither still agrees on the key name.
	DefaultKey = "dispatch-gate-budget"

	// writeTimeout bounds a single write so a slow or unreachable store cannot
	// stall the gRPC handler that triggered it.
	writeTimeout = 2 * time.Second
)

// KeyWriter writes a value to a named key in an external store.
type KeyWriter interface {
	SetKey(ctx context.Context, key, value string) error
	Close() error
}

// For reports the dispatch budget implied by a single group snapshot.
//
// It is Available only when all of the following hold:
//
//   - the batch tenant, and not the trainer, holds the group lock;
//   - its context is restored on the nodes, i.e. the snapshot has a non-zero
//     ServingSince() — the same predicate the minimum serving quantum is
//     measured from, so the two cannot disagree about what "serving" means.
//     (This is equivalent to the group being in STATE_LOCKED for that job,
//     because the controller only sets loadedJob once the restore completes);
//   - no pre-emption pressure is being advertised to it. advertisedWaiters is
//     the depth GetGroupStatus would report, i.e. after the quantum has had
//     its say, because that is the value the tenant acts on: the instant it
//     goes non-zero the tenant drains and yields the GPU.
//
// The third condition is what makes the signal precede the outage rather than
// follow it. Everything else is a statement about the present; that one is a
// statement about the immediate future.
func For(snap *store.GroupSnapshot, batchJob string, advertisedWaiters int) string {
	if snap == nil || batchJob == "" {
		return Blocked
	}
	if snap.LockingJob != batchJob {
		// The trainer holds the lock, or nobody does.
		return Blocked
	}
	if snap.ServingSince().IsZero() {
		// Granted, but the context restore has not finished — or a holder
		// recovered from the lock store, whose grant time is not persisted.
		return Blocked
	}
	if advertisedWaiters > 0 {
		// The tenant is about to be told to drain and yield.
		return Blocked
	}
	return Available
}

// Publisher writes the dispatch budget for one batch tenant to one key.
type Publisher struct {
	writer             KeyWriter
	key                string
	batchJob           string
	openDelay          time.Duration
	externalRisingEdge bool
	now                func() time.Time

	mu             sync.Mutex
	last           string
	availableSince time.Time
}

// NewPublisher returns a Publisher that writes the budget for batchJob to key.
func NewPublisher(writer KeyWriter, key, batchJob string) *Publisher {
	if key == "" {
		key = DefaultKey
	}
	return &Publisher{writer: writer, key: key, batchJob: batchJob, now: time.Now}
}

// WithOpenDelay holds the Blocked -> Available transition back by d, and
// returns p.
//
// The two edges of this signal are not symmetric, and a single run measured
// the asymmetry directly (results/redis-signal-gate).
//
// The falling edge is safe for free: the budget is computed from state the
// orchestrator owns and is written inside the same GetGroupStatus call whose
// response makes the tenant drain, so `0` is on the wire strictly before the
// tenant stops serving. Measured publish lag was -0.64 s mean over 28 real
// trainer edges, negative on 27 of them.
//
// The rising edge is not. ServingSince() becomes non-zero when the lock is
// held and the context is restored, which is strictly EARLIER than the moment
// kube-proxy will route to the tenant again — the readiness probe still has to
// pass and the Endpoints object still has to propagate. Measured gap: 0.74 to
// 1.84 s, median 1.61 s. Opening the gate into that gap is worse than opening
// it late, because a blackout accumulates a backlog and the dispatcher empties
// the whole backlog into the first instant the gate is open. In that run it
// cost 59 of 140 requests, every one of them a terminal connection-refused
// within 1.67 s of the key going to 1.
//
// d is a hold-down, not a readiness signal. It is the right shape of fix only
// because the orchestrator cannot see the tenant's readiness; if the tenant
// itself can be made to publish its own rising edge, prefer that and leave
// this at zero.
func (p *Publisher) WithOpenDelay(d time.Duration) *Publisher {
	if p == nil {
		return nil
	}
	p.openDelay = d
	return p
}

// OpenDelay returns the configured rising-edge hold-down.
func (p *Publisher) OpenDelay() time.Duration {
	if p == nil {
		return 0
	}
	return p.openDelay
}

// WithExternalRisingEdge makes this publisher write only Blocked, never
// Available, and returns p.
//
// WithOpenDelay explains why the rising edge is the dangerous one: it is derived
// from ServingSince(), which becomes non-zero at CUDA context restore, a
// measured 0.74-1.84 s (median 1.61 s) before kube-proxy will route to the
// tenant's Service again. A hold-down mitigates that with a constant, and a
// constant cannot observe the thing it is waiting for: too short and the
// accumulated backlog lands on a socket that is still refusing connections (59
// of 140 requests in the measured run), too long and serving time is thrown away
// on every cycle.
//
// This mode gives the rising edge away instead of guessing at it. The
// orchestrator publishes only Blocked; something that can actually see the
// backend — a node-level supervisor probing vLLM directly — publishes Available
// once it is reachable. The split follows who knows what: the orchestrator is
// the only thing that knows a yield is coming, and the last thing to learn that
// serving has resumed.
//
// What does NOT change is the falling edge. Blocked is still written on every
// call, undelayed and undeduplicated, because the consuming gate reads an ABSENT
// key as full capacity; a publisher that fell silent after one 0 would let a
// Redis eviction open the gate with nobody left to close it. A computed
// Available is skipped outright rather than written as 0 — writing 0 over the
// external publisher's 1 would just be the two of them fighting over the key at
// 1 Hz.
//
// The failure mode this buys is that if nothing ever publishes Available the
// gate stays shut forever and the batch tenant never serves. It is visible in
// timeslice_orchestrator_dispatch_budget_rising_edge_skipped_total, which climbs
// exactly while the orchestrator believes the tenant could be serving.
//
// A configured open delay is ignored in this mode: it is a mitigation for an
// edge that is no longer written here.
func (p *Publisher) WithExternalRisingEdge(enabled bool) *Publisher {
	if p == nil {
		return nil
	}
	p.externalRisingEdge = enabled
	return p
}

// ExternalRisingEdge reports whether the rising edge is delegated externally.
func (p *Publisher) ExternalRisingEdge() bool {
	if p == nil {
		return false
	}
	return p.externalRisingEdge
}

// holdDown applies the rising-edge hold-down, returning the value that should
// actually be written. Blocked is never delayed: the falling edge is the one
// that has to be early.
func (p *Publisher) holdDown(value string) string {
	if p.openDelay <= 0 {
		return value
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if value != Available {
		p.availableSince = time.Time{}
		return value
	}
	now := p.now()
	if p.availableSince.IsZero() {
		p.availableSince = now
	}
	if now.Sub(p.availableSince) < p.openDelay {
		metrics.DispatchBudgetHeldTotal.Inc()
		return Blocked
	}
	return Available
}

// Key returns the key being published to.
func (p *Publisher) Key() string { return p.key }

// BatchJob returns the job ID of the batch tenant whose availability is published.
func (p *Publisher) BatchJob() string { return p.batchJob }

// Publish writes value to the key. It writes on every call, including when the
// value is unchanged: the consuming gate fails OPEN on an absent key, so a
// deduplicating publisher would turn a single eviction into an unbounded
// window in which the gate admits traffic to a frozen engine.
//
// The write is given its own deadline and is deliberately not cancelled by the
// caller's context: a Blocked write must land even if the poll that prompted
// it goes away.
// A configured open delay is applied here rather than by the caller, so that
// every path that can publish — the gRPC handler and the background ticker
// alike — is held back by the same clock. The same is true of an externally
// owned rising edge: both paths must skip the Available write, or the 1 Hz
// ticker would overwrite the external publisher's 1 a moment after the gRPC
// handler declined to.
func (p *Publisher) Publish(ctx context.Context, value string) error {
	if p == nil {
		return nil
	}

	if p.externalRisingEdge {
		if value == Available {
			// Deliberately no write of any kind, and no DispatchBudget gauge
			// update: the key belongs to the external publisher from here until
			// the next Blocked, and this process no longer knows its value.
			metrics.DispatchBudgetRisingEdgeSkippedTotal.Inc()
			return nil
		}
	} else {
		value = p.holdDown(value)
	}

	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	err := p.writer.SetKey(wctx, p.key, value)

	p.mu.Lock()
	previous := p.last
	if err == nil {
		p.last = value
	}
	p.mu.Unlock()

	if err != nil {
		metrics.DispatchBudgetWritesTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to publish dispatch budget to key %q: %w", p.key, err)
	}

	metrics.DispatchBudgetWritesTotal.WithLabelValues("ok").Inc()
	if value == Available {
		metrics.DispatchBudget.Set(1)
	} else {
		metrics.DispatchBudget.Set(0)
	}
	if previous != value {
		slog.InfoContext(ctx, "Published dispatch budget",
			"key", p.key,
			"batchJob", p.batchJob,
			"value", value,
			"previous", previous,
		)
	}
	return nil
}

// Last returns the last value successfully written, or "" if none has been.
func (p *Publisher) Last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

// Close releases the writer's resources.
func (p *Publisher) Close() error {
	if p == nil {
		return nil
	}
	return p.writer.Close()
}
