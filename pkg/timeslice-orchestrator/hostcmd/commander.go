package hostcmd

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Command names, as logged.
const (
	CommandSuspendAll = "SuspendAll"
	CommandResumeAll  = "ResumeAll"
)

const (
	defaultRetryInterval = 500 * time.Millisecond
	maxRetryInterval     = 5 * time.Second
	// callSlack is added to the time left until the deadline to bound one
	// command call. A call that runs out is sent again with the same epoch,
	// which joins the command still running on the host.
	callSlack   = 500 * time.Millisecond
	minCallTime = time.Second

	outcomeClear   = agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_CLEAR
	outcomeResumed = agentpb.HostCommandOutcome_HOST_COMMAND_OUTCOME_RESUMED
)

// Config configures a Commander.
type Config struct {
	// Client sends the commands.
	Client Client
	// NoticeWindow is N and KillBudget is K: hosts must be clear by
	// T = notice + N - K.
	NoticeWindow time.Duration
	KillBudget   time.Duration
	// Enqueue asks the controller to reconcile a group. It is called when
	// every host of the group is clear.
	Enqueue func(groupID string)
	// RetryInterval is the first wait before a failed command is sent again.
	// Zero means 500 ms.
	RetryInterval time.Duration
	// Now returns the current time. Nil means time.Now.
	Now func() time.Time
}

type phase int

const (
	// phaseUnknown: nothing is known about the hosts (for example after an
	// orchestrator restart). Not clear: a vacate round is needed.
	phaseUnknown phase = iota
	// phaseLent: hosts were told to resume their guests.
	phaseLent
	// phaseVacating: hosts were told to suspend; not every host acked yet.
	phaseVacating
	// phaseClear: every host acked SuspendAll with CLEAR.
	phaseClear
)

type groupState struct {
	phase    phase
	round    int64 // epoch of the current round; also its identity
	deadline time.Time
	nodes    []string
	clear    map[string]bool
	cancel   context.CancelFunc
	timer    *time.Timer
}

// Commander runs vacate and resume rounds over the hosts of each group.
// It is safe for concurrent use.
type Commander struct {
	cfg Config

	mu        sync.Mutex
	groups    map[string]*groupState
	lastEpoch int64
}

// NewCommander returns a Commander.
func NewCommander(cfg Config) *Commander {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = defaultRetryInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Enqueue == nil {
		cfg.Enqueue = func(string) {}
	}
	return &Commander{cfg: cfg, groups: make(map[string]*groupState)}
}

// nextEpoch returns a new epoch. Epochs are wall-clock nanoseconds, bumped to
// stay strictly increasing, so a restarted orchestrator still issues epochs
// above the ones its predecessor sent. Assumes c.mu is held.
func (c *Commander) nextEpoch() int64 {
	e := c.cfg.Now().UnixNano()
	if e <= c.lastEpoch {
		e = c.lastEpoch + 1
	}
	c.lastEpoch = e
	return e
}

func (c *Commander) newEpoch() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextEpoch()
}

// state returns the state of group, creating it. Assumes c.mu is held.
func (c *Commander) state(group string) *groupState {
	st, ok := c.groups[group]
	if !ok {
		st = &groupState{}
		c.groups[group] = st
	}
	return st
}

// endRound stops the goroutines and the deadline timer of the current round.
// Assumes c.mu is held.
func (st *groupState) endRound() {
	if st.cancel != nil {
		st.cancel()
		st.cancel = nil
	}
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
}

func sameNodes(a, b []string) bool {
	return slices.Equal(a, b)
}

// Clear reports whether every host of group acked the last vacate round with
// CLEAR and nothing was resumed since.
func (c *Commander) Clear(group string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	return ok && st.phase == phaseClear
}

// Vacate makes sure a vacate round runs for group over nodes, with the
// deadline T = noticeAt + N - K, and reports whether every host is clear. A
// round already running over the same nodes, or already clear, is kept.
// Anything else (unknown state, lent, other nodes) starts a new round with a
// new epoch. The round's goroutines live until ctx is done or the round is
// replaced.
func (c *Commander) Vacate(ctx context.Context, group string, nodes []string, noticeAt time.Time) bool {
	nodes = slices.Sorted(slices.Values(nodes))
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state(group)
	switch {
	case st.phase == phaseClear && sameNodes(st.nodes, nodes):
		return true
	case st.phase == phaseVacating && sameNodes(st.nodes, nodes):
		return false
	}

	st.endRound()
	st.round = c.nextEpoch()
	st.deadline = noticeAt.Add(c.cfg.NoticeWindow - c.cfg.KillBudget)
	st.nodes = nodes
	st.clear = make(map[string]bool, len(nodes))
	slog.InfoContext(ctx, "Vacate started", "group", group, "hosts", len(nodes), "nodes", nodes,
		"deadline", st.deadline, "epoch", st.round)
	if len(nodes) == 0 {
		st.phase = phaseClear
		return true
	}
	st.phase = phaseVacating

	roundCtx, cancel := context.WithCancel(ctx)
	st.cancel = cancel
	round := st.round
	st.timer = time.AfterFunc(max(st.deadline.Sub(c.cfg.Now()), 0), func() {
		c.deadlinePassed(roundCtx, group, round)
	})
	for _, node := range nodes {
		go c.suspendNode(roundCtx, group, round, node, st.deadline)
	}
	return false
}

// Resume starts a resume round for group over nodes unless one already runs
// over the same nodes. It ends any vacate round: hosts finish a suspend they
// started before they resume (epoch rules of HostCommandService).
func (c *Commander) Resume(ctx context.Context, group string, nodes []string) {
	nodes = slices.Sorted(slices.Values(nodes))
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state(group)
	if st.phase == phaseLent && sameNodes(st.nodes, nodes) {
		return
	}
	st.endRound()
	st.round = c.nextEpoch()
	st.phase = phaseLent
	st.nodes = nodes
	st.clear = nil
	st.deadline = c.cfg.Now().Add(c.cfg.NoticeWindow)
	roundCtx, cancel := context.WithCancel(ctx)
	st.cancel = cancel
	for _, node := range nodes {
		slog.InfoContext(ctx, "Resume started", "group", group, "node", node, "epoch", st.round)
		go c.resumeNode(roundCtx, group, st.round, node, st.deadline)
	}
}

// current reports whether round is still the running round of group in phase p.
func (c *Commander) current(group string, round int64, p phase) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.groups[group]
	return ok && st.round == round && st.phase == p
}

func (c *Commander) deadlinePassed(ctx context.Context, group string, round int64) {
	c.mu.Lock()
	st, ok := c.groups[group]
	if !ok || st.round != round || st.phase != phaseVacating {
		c.mu.Unlock()
		return
	}
	var missing []string
	for _, node := range st.nodes {
		if !st.clear[node] {
			missing = append(missing, node)
		}
	}
	c.mu.Unlock()
	for _, node := range missing {
		slog.WarnContext(ctx, "Host not clear at deadline", "group", group, "node", node, "epoch", round)
	}
}

// markClear records node's CLEAR ack for round. When that makes every host
// clear, the group is clear and the controller is asked to reconcile it.
func (c *Commander) markClear(ctx context.Context, group string, round int64, node string) {
	c.mu.Lock()
	st, ok := c.groups[group]
	if !ok || st.round != round || st.phase != phaseVacating {
		c.mu.Unlock()
		return
	}
	st.clear[node] = true
	slog.InfoContext(ctx, "Host clear", "group", group, "node", node, "how", "ack", "epoch", round)
	allClear := len(st.clear) == len(st.nodes)
	if allClear {
		st.phase = phaseClear
		st.endRound()
	}
	c.mu.Unlock()
	if allClear {
		c.cfg.Enqueue(group)
	}
}

// callTimeout bounds one command call: until the deadline plus a little
// slack, and never less than minCallTime.
func (c *Commander) callTimeout(deadline time.Time) time.Duration {
	return max(deadline.Sub(c.cfg.Now())+callSlack, minCallTime)
}

// sleep waits d or until ctx is done, and reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(d time.Duration) time.Duration {
	return min(2*d, maxRetryInterval)
}

// suspendNode sends SuspendAll to node until it acks CLEAR for round, the
// round is replaced, or ctx is done. A transport error or timeout sends the
// same epoch again (the host joins the running command). FAILED or
// STALE_EPOCH sends a new epoch, since the host would replay the old ack.
func (c *Commander) suspendNode(ctx context.Context, group string, round int64, node string, deadline time.Time) {
	epoch := round
	backoff := c.cfg.RetryInterval
	for ctx.Err() == nil && c.current(group, round, phaseVacating) {
		req := &agentpb.SuspendAllRequest{
			Group:    group,
			Role:     agentpb.HostRole_HOST_ROLE_BACKGROUND,
			Epoch:    epoch,
			Deadline: timestamppb.New(deadline),
		}
		slog.InfoContext(ctx, "Host command sent", "group", group, "node", node, "command", CommandSuspendAll,
			"deadline", deadline, "epoch", epoch)
		callCtx, cancel := context.WithTimeout(ctx, c.callTimeout(deadline))
		ack, err := c.cfg.Client.SuspendAll(callCtx, node, req)
		cancel()
		if c.handleAck(ctx, group, node, CommandSuspendAll, epoch, ack, err, outcomeClear) {
			c.markClear(ctx, group, round, node)
			return
		}
		if ack != nil && ack.GetEpoch() == epoch {
			// FAILED or STALE_EPOCH: the host recorded an ack for this epoch.
			epoch = c.newEpoch()
		}
		if !sleep(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// resumeNode sends ResumeAll to node until it acks RESUMED for round, the
// round is replaced, or ctx is done. Retries follow suspendNode.
func (c *Commander) resumeNode(ctx context.Context, group string, round int64, node string, deadline time.Time) {
	epoch := round
	backoff := c.cfg.RetryInterval
	for ctx.Err() == nil && c.current(group, round, phaseLent) {
		req := &agentpb.ResumeAllRequest{
			Group:    group,
			Role:     agentpb.HostRole_HOST_ROLE_BACKGROUND,
			Epoch:    epoch,
			Deadline: timestamppb.New(deadline),
		}
		slog.InfoContext(ctx, "Host command sent", "group", group, "node", node, "command", CommandResumeAll,
			"deadline", deadline, "epoch", epoch)
		callCtx, cancel := context.WithTimeout(ctx, c.callTimeout(deadline))
		ack, err := c.cfg.Client.ResumeAll(callCtx, node, req)
		cancel()
		if c.handleAck(ctx, group, node, CommandResumeAll, epoch, ack, err, outcomeResumed) {
			return
		}
		if ack != nil && ack.GetEpoch() == epoch {
			epoch = c.newEpoch()
		}
		if !sleep(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// handleAck logs the outcome of one call and reports whether it is the
// wanted outcome for epoch. An ack for another epoch is dropped.
func (c *Commander) handleAck(
	ctx context.Context,
	group, node, command string,
	epoch int64,
	ack *agentpb.HostCommandAck,
	err error,
	want agentpb.HostCommandOutcome,
) bool {
	if err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(ctx, "Host command failed", "group", group, "node", node, "command", command,
				"epoch", epoch, "error", err)
		}
		return false
	}
	slog.InfoContext(ctx, "Host ack", "group", group, "node", node, "command", command,
		"outcome", ack.GetOutcome().String(), "epoch", ack.GetEpoch(), "error", ack.GetError())
	if ack.GetEpoch() != epoch {
		slog.InfoContext(ctx, "Dropping host ack for another epoch", "group", group, "node", node,
			"command", command, "want", epoch, "got", ack.GetEpoch())
		return false
	}
	return ack.GetOutcome() == want
}
