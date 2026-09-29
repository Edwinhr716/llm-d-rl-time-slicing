// Package hostcmd is the orchestrator's client for the per-host command
// endpoint (decision D-NS-4, option ns-push-vk). It keeps a registry of the
// hosts of each group, fans a vacate command out to every host with the
// deadline T = notice + N - K, holds the foreground grant until every host has
// acked (the barrier), commands resume when the accelerator is lent, fences
// late acks with command epochs, and tracks host liveness from command
// failures instead of polls.
package hostcmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// State is what the orchestrator knows about one host.
type State int

const (
	// StateUnknown: this process has not commanded the host yet. The host
	// may be running guests (for example after an orchestrator restart), so
	// it counts as not clear (fail closed).
	StateUnknown State = iota
	// StateLent: a resume was sent. The host may run guests.
	StateLent
	// StateVacating: a vacate was sent and not acked yet.
	StateVacating
	// StateClear: the host acked the vacate of its current epoch.
	StateClear
)

func (s State) String() string {
	switch s {
	case StateUnknown:
		return "unknown"
	case StateLent:
		return "lent"
	case StateVacating:
		return "vacating"
	case StateClear:
		return "clear"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Command names used in log lines.
const (
	commandVacate = "vacate"
	commandResume = "resume"
)

// Defaults for the retry timing.
const (
	// DefaultRetryInterval is the pause between attempts before T.
	DefaultRetryInterval = 200 * time.Millisecond
	// DefaultLateRetryInterval is the pause between attempts after T.
	DefaultLateRetryInterval = 1 * time.Second
	// DefaultAttemptTimeout bounds one command call after T, and every
	// resume call.
	DefaultAttemptTimeout = 5 * time.Second
)

// Config configures a Commander.
type Config struct {
	// Resolve returns the host:port of the command endpoint of a node.
	Resolve func(node string) (string, error)
	// NoticeWindow is N and KillBudget is K: a vacate must finish by
	// T = notice + N - K.
	NoticeWindow time.Duration
	KillBudget   time.Duration
	// Enqueue asks the reconcile loop to look at a group, for example when
	// its barrier completes. May be nil.
	Enqueue func(group string)
	// Logger is used for every log line. Nil means slog.Default().
	Logger *slog.Logger
	// Retry timing. Zero values take the defaults above.
	RetryInterval     time.Duration
	LateRetryInterval time.Duration
	AttemptTimeout    time.Duration
}

// host is the registry entry of one node of a group.
type host struct {
	node  string
	state State
	// epoch is the epoch of the last command sent to the host. An ack for
	// any other epoch is ignored.
	epoch int64
	// failures counts consecutive failed commands. Zero means reachable.
	failures int
	// failingSince is when the current run of failed commands started. Zero
	// while the host is reachable.
	failingSince time.Time
	// notClearLogged is set once the host was logged as not clear at the
	// current barrier's deadline.
	notClearLogged bool
	// cancel stops the command goroutine of this host, if one runs.
	cancel context.CancelFunc
}

// groupHosts is the registry of one group.
type groupHosts struct {
	hosts map[string]*host
	// barrier is set while a vacate barrier runs.
	barrier *barrier
}

// barrier is one vacate fan-out.
type barrier struct {
	epoch    int64
	noticeAt time.Time
	deadline time.Time
	started  time.Time
	timer    *time.Timer
}

// Commander sends host commands and keeps the host registry.
type Commander struct {
	cfg    Config
	log    *slog.Logger
	ctx    context.Context // lifetime of the command goroutines
	epochs epochSource

	mu      sync.Mutex
	groups  map[string]*groupHosts
	clients map[string]*client
	wg      sync.WaitGroup
}

type client struct {
	conn *grpc.ClientConn
	api  hcpb.HostCommandServiceClient
}

// New returns a Commander whose command goroutines live until ctx is done.
// Call Close after ctx is done to release connections.
func New(ctx context.Context, cfg Config) *Commander {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultRetryInterval
	}
	if cfg.LateRetryInterval <= 0 {
		cfg.LateRetryInterval = DefaultLateRetryInterval
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = DefaultAttemptTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Commander{
		cfg:     cfg,
		log:     logger,
		ctx:     ctx,
		groups:  make(map[string]*groupHosts),
		clients: make(map[string]*client),
	}
}

// Close waits for the command goroutines to stop (the context passed to New
// must be done) and closes every connection.
func (c *Commander) Close() {
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, cl := range c.clients {
		if err := cl.conn.Close(); err != nil {
			c.log.Warn("Failed to close host command connection", "address", addr, "error", err)
		}
		delete(c.clients, addr)
	}
}

// SyncHosts sets the hosts of a group from the node watch. A new host starts
// unknown (not clear). While a vacate barrier runs, a new host is sent the
// barrier's Vacate at once, so the barrier can complete. A host that joins
// after the barrier's deadline T also asks the reconcile loop to look at the
// group, so the kill path (ORCH-A4), which ran before this sync, acts on it
// now. A removed host is forgotten and its command stopped.
func (c *Commander) SyncHosts(group string, nodes []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh := c.groupLocked(group)
	want := make(map[string]bool, len(nodes))
	changed := false
	joinedLate := false
	for _, node := range nodes {
		want[node] = true
		if _, ok := gh.hosts[node]; ok {
			continue
		}
		hst := &host{node: node, state: StateUnknown}
		gh.hosts[node] = hst
		changed = true
		if bar := gh.barrier; bar != nil {
			c.log.Info("Host joined a running vacate", "group", group, "node", node,
				"deadline", bar.deadline, "epoch", bar.epoch)
			c.sendLocked(group, hst, commandVacate, bar.epoch, bar.deadline)
			if !time.Now().Before(bar.deadline) {
				hst.notClearLogged = true
				joinedLate = true
				c.log.Warn("Host not clear at deadline", "group", group, "node", node, "epoch", bar.epoch,
					"deadline", bar.deadline, "reachable", true)
			}
		}
	}
	for node, hst := range gh.hosts {
		if !want[node] {
			if hst.cancel != nil {
				hst.cancel()
			}
			delete(gh.hosts, node)
			changed = true
		}
	}
	if changed {
		hosts := make([]string, 0, len(gh.hosts))
		for node := range gh.hosts {
			hosts = append(hosts, node)
		}
		sort.Strings(hosts)
		c.log.Info("Host registry updated", "group", group, "hosts", hosts)
	}
	if joinedLate && c.cfg.Enqueue != nil {
		go c.cfg.Enqueue(group)
	}
	c.finishBarrierIfClearLocked(group, gh)
}

// Forget drops a group from the registry and stops its commands. The
// controller calls it when the group is deleted from the store.
func (c *Commander) Forget(group string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok {
		return
	}
	for _, hst := range gh.hosts {
		if hst.cancel != nil {
			hst.cancel()
		}
	}
	if gh.barrier != nil {
		gh.barrier.timer.Stop()
	}
	delete(c.groups, group)
}

// AllClear reports whether every host of the group acked a vacate and none
// was lent since. A group with no hosts, and a group never synced, is not
// clear (fail closed).
func (c *Commander) AllClear(group string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok {
		return false
	}
	return allClearLocked(gh)
}

// Lent reports whether any host of the group was sent a resume and has not
// acked a vacate since.
func (c *Commander) Lent(group string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok {
		return false
	}
	for _, hst := range gh.hosts {
		if hst.state == StateLent {
			return true
		}
	}
	return false
}

// HostStates returns the state of every host of the group, for tests and
// status.
func (c *Commander) HostStates(group string) map[string]State {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]State)
	if gh, ok := c.groups[group]; ok {
		for node, hst := range gh.hosts {
			out[node] = hst.state
		}
	}
	return out
}

// Reachable reports whether the last command to the node succeeded. A host
// never commanded is reachable.
func (c *Commander) Reachable(group, node string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gh, ok := c.groups[group]; ok {
		if hst, ok := gh.hosts[node]; ok {
			return hst.failures == 0
		}
	}
	return false
}

// Barrier is the running vacate barrier of a group, as the kill path sees it.
type Barrier struct {
	// NoticeAt is when the notice started, and Deadline is
	// T = NoticeAt + NoticeWindow - KillBudget.
	NoticeAt     time.Time
	Deadline     time.Time
	NoticeWindow time.Duration
	KillBudget   time.Duration
	// NotClear lists the hosts that have not acked the vacate, sorted by node.
	NotClear []HostStatus
}

// HostStatus is one host that is not clear.
type HostStatus struct {
	Node  string
	State State
	// FailingSince is when the current run of failed commands to the host
	// started, or zero if the last command reached it.
	FailingSince time.Time
}

// Barrier returns the running vacate barrier of the group, if any.
func (c *Commander) Barrier(group string) (Barrier, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok || gh.barrier == nil {
		return Barrier{}, false
	}
	out := Barrier{
		NoticeAt:     gh.barrier.noticeAt,
		Deadline:     gh.barrier.deadline,
		NoticeWindow: c.cfg.NoticeWindow,
		KillBudget:   c.cfg.KillBudget,
	}
	for _, hst := range gh.hosts {
		if hst.state == StateClear {
			continue
		}
		out.NotClear = append(out.NotClear, HostStatus{Node: hst.node, State: hst.state, FailingSince: hst.failingSince})
	}
	sort.Slice(out.NotClear, func(i, j int) bool { return out.NotClear[i].Node < out.NotClear[j].Node })
	return out, true
}

// ClearByOrchestrator marks a host clear without its ack, because the
// orchestrator vacated it itself (how is "kill", "unconfirmed-kill" or
// "no-live-guest"). The running command is stopped and fenced: a late ack of
// it is ignored. The next Resume commands the host again as usual.
func (c *Commander) ClearByOrchestrator(group, node, how string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok {
		return
	}
	hst, ok := gh.hosts[node]
	if !ok || hst.state == StateClear {
		return
	}
	if hst.cancel != nil {
		hst.cancel()
		hst.cancel = nil
	}
	hst.epoch = c.epochs.Next()
	hst.state = StateClear
	c.log.Info("Host clear", "group", group, "node", node, "how", how, "epoch", hst.epoch)
	c.finishBarrierIfClearLocked(group, gh)
}

// StartVacate starts the vacate barrier of a group for the notice that
// started at noticeAt, unless one already runs or every host is clear. Every
// host that is not clear is sent Vacate with a new epoch and the deadline
// T = noticeAt + N - K. A running resume is superseded.
func (c *Commander) StartVacate(group string, noticeAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh := c.groupLocked(group)
	if gh.barrier != nil || allClearLocked(gh) {
		return
	}
	now := time.Now()
	bar := &barrier{
		epoch:    c.epochs.Next(),
		noticeAt: noticeAt,
		deadline: noticeAt.Add(c.cfg.NoticeWindow - c.cfg.KillBudget),
		started:  now,
	}
	gh.barrier = bar
	var nodes []string
	for _, hst := range gh.hosts {
		if hst.state == StateClear {
			continue
		}
		nodes = append(nodes, hst.node)
		c.sendLocked(group, hst, commandVacate, bar.epoch, bar.deadline)
	}
	sort.Strings(nodes)
	c.log.Info("Vacate started", "group", group, "hosts", nodes, "deadline", bar.deadline, "epoch", bar.epoch)
	bar.timer = time.AfterFunc(time.Until(bar.deadline), func() { c.deadlinePassed(group, bar) })
}

// Resume commands every host of the group to resume its guests. Each host is
// marked lent at once (fail closed: from now on it may run guests).
func (c *Commander) Resume(group string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh := c.groupLocked(group)
	if gh.barrier != nil {
		gh.barrier.timer.Stop()
		gh.barrier = nil
	}
	epoch := c.epochs.Next()
	deadline := time.Now().Add(c.cfg.NoticeWindow)
	for _, hst := range gh.hosts {
		c.log.Info("Resume started", "group", group, "node", hst.node, "epoch", epoch)
		c.sendLocked(group, hst, commandResume, epoch, deadline)
	}
}

// groupLocked returns the registry of a group, creating it. c.mu is held.
func (c *Commander) groupLocked(group string) *groupHosts {
	gh, ok := c.groups[group]
	if !ok {
		gh = &groupHosts{hosts: make(map[string]*host)}
		c.groups[group] = gh
	}
	return gh
}

// allClearLocked reports whether the group has hosts and every one is clear.
// No hosts means nothing acked, so the group is not clear (fail closed).
func allClearLocked(gh *groupHosts) bool {
	if len(gh.hosts) == 0 {
		return false
	}
	for _, hst := range gh.hosts {
		if hst.state != StateClear {
			return false
		}
	}
	return true
}

// sendLocked replaces the host's running command, if any, with a new one.
// c.mu is held.
func (c *Commander) sendLocked(group string, hst *host, command string, epoch int64, deadline time.Time) {
	if hst.cancel != nil {
		hst.cancel()
	}
	ctx, cancel := context.WithCancel(c.ctx)
	hst.cancel = cancel
	hst.epoch = epoch
	hst.notClearLogged = false
	if command == commandVacate {
		hst.state = StateVacating
	} else {
		hst.state = StateLent
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		c.run(ctx, group, hst.node, command, epoch, deadline)
	}()
}

// run sends one command to one host until it is acked for its epoch, it is
// superseded, or the Commander stops.
func (c *Commander) run(ctx context.Context, group, node, command string, epoch int64, deadline time.Time) {
	for {
		if ctx.Err() != nil {
			return
		}
		ack, err := c.call(ctx, group, node, command, epoch, deadline)
		if ctx.Err() != nil {
			return
		}
		next, done := c.handle(group, node, command, epoch, ack, err)
		if done {
			return
		}
		epoch = next
		wait := c.cfg.RetryInterval
		if command == commandResume || !time.Now().Before(deadline) {
			wait = c.cfg.LateRetryInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// call sends one attempt. Before T a vacate attempt is bounded by T, so a
// hung host is known at T; after T, and for resume, by AttemptTimeout.
func (c *Commander) call(
	ctx context.Context, group, node, command string, epoch int64, deadline time.Time,
) (*hcpb.HostAck, error) {
	api, err := c.client(node)
	if err != nil {
		return nil, err
	}
	callDeadline := time.Now().Add(c.cfg.AttemptTimeout)
	if command == commandVacate && time.Now().Before(deadline) {
		callDeadline = deadline
	}
	callCtx, cancel := context.WithDeadline(ctx, callDeadline)
	defer cancel()
	c.log.Info("Host command sent", "group", group, "node", node, "command", command,
		"deadline", deadline, "epoch", epoch)
	if command == commandVacate {
		return api.Vacate(callCtx, &hcpb.VacateRequest{
			GroupId: group, NodeName: node, Epoch: epoch, Deadline: timestamppb.New(deadline),
		})
	}
	return api.Resume(callCtx, &hcpb.ResumeRequest{
		GroupId: group, NodeName: node, Epoch: epoch, Deadline: timestamppb.New(deadline),
	})
}

// handle records the result of one attempt. It returns the epoch for the
// next attempt and whether the command is finished.
func (c *Commander) handle(
	group, node, command string, epoch int64, ack *hcpb.HostAck, callErr error,
) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok {
		return epoch, true
	}
	hst, ok := gh.hosts[node]
	if !ok || hst.epoch != epoch {
		// Superseded by a newer command or forgotten.
		return epoch, true
	}
	if callErr != nil {
		c.failedLocked(group, hst, command, callErr)
		return epoch, false
	}
	outcome := outcomeName(ack.GetOutcome())
	wantCommand := hcpb.Command_COMMAND_VACATE
	if command == commandResume {
		wantCommand = hcpb.Command_COMMAND_RESUME
	}
	if ack.GetEpoch() != epoch || ack.GetCommand() != wantCommand {
		// A late ack for an older command: never acted on.
		c.log.Warn("Host ack ignored: epoch or command does not match", "group", group, "node", node,
			"command", command, "epoch", epoch, "ackEpoch", ack.GetEpoch(), "ackCommand", ack.GetCommand().String())
		c.failedLocked(group, hst, command, errors.New("mismatched ack"))
		return epoch, false
	}
	c.log.Info("Host ack", "group", group, "node", node, "command", command, "outcome", outcome,
		"epoch", epoch, "error", ack.GetError())
	switch ack.GetOutcome() {
	case hcpb.Outcome_OUTCOME_VACATED:
		if command != commandVacate {
			break
		}
		c.reachableLocked(group, hst)
		hst.state = StateClear
		c.log.Info("Host clear", "group", group, "node", node, "how", "ack", "epoch", epoch)
		c.finishBarrierIfClearLocked(group, gh)
		return epoch, true
	case hcpb.Outcome_OUTCOME_RESUMED:
		if command != commandResume {
			break
		}
		c.reachableLocked(group, hst)
		return epoch, true
	case hcpb.Outcome_OUTCOME_STALE_EPOCH:
		// The host has seen a higher epoch, for example from an earlier
		// orchestrator process whose clock ran ahead. Move past it.
		c.reachableLocked(group, hst)
		c.epochs.Observe(ack.GetCurrentEpoch())
		hst.epoch = c.epochs.Next()
		return hst.epoch, false
	default:
	}
	// OUTCOME_FAILED, OUTCOME_ABORTED or an outcome that does not fit the
	// command: the host answered, so it is reachable, but not done.
	c.reachableLocked(group, hst)
	return epoch, false
}

func (c *Commander) failedLocked(group string, hst *host, command string, err error) {
	hst.failures++
	if hst.failures == 1 {
		hst.failingSince = time.Now()
		c.log.Warn("Host command failed", "group", group, "node", hst.node, "command", command,
			"epoch", hst.epoch, "error", err)
		// The reconcile loop times the background liveness L from here
		// (ORCH-A4 kill path).
		if command == commandVacate && c.cfg.Enqueue != nil {
			go c.cfg.Enqueue(group)
		}
	}
}

func (c *Commander) reachableLocked(group string, hst *host) {
	if hst.failures > 0 {
		c.log.Info("Host command succeeded after failures", "group", group, "node", hst.node,
			"failures", hst.failures)
	}
	hst.failures = 0
	hst.failingSince = time.Time{}
}

// finishBarrierIfClearLocked ends the barrier once every host is clear and
// asks the reconcile loop to grant. c.mu is held.
func (c *Commander) finishBarrierIfClearLocked(group string, gh *groupHosts) {
	if gh.barrier == nil || !allClearLocked(gh) {
		return
	}
	bar := gh.barrier
	bar.timer.Stop()
	gh.barrier = nil
	c.log.Info("All hosts clear", "group", group, "epoch", bar.epoch,
		"elapsed_ms", time.Since(bar.started).Milliseconds())
	if c.cfg.Enqueue != nil {
		go c.cfg.Enqueue(group)
	}
}

// deadlinePassed logs every host that is not clear at T and asks the reconcile
// loop to look at the group: its kill path (ORCH-A4, controller/kill.go)
// decides what happens to a host that is not clear at the deadline.
func (c *Commander) deadlinePassed(group string, bar *barrier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gh, ok := c.groups[group]
	if !ok || gh.barrier != bar {
		return
	}
	if len(gh.hosts) == 0 {
		c.log.Warn("No hosts at deadline", "group", group, "epoch", bar.epoch, "deadline", bar.deadline)
	}
	for _, hst := range gh.hosts {
		if hst.state == StateClear || hst.notClearLogged {
			continue
		}
		hst.notClearLogged = true
		c.log.Warn("Host not clear at deadline", "group", group, "node", hst.node, "epoch", hst.epoch,
			"deadline", bar.deadline, "reachable", hst.failures == 0)
	}
	if c.cfg.Enqueue != nil {
		go c.cfg.Enqueue(group)
	}
}

// client returns the connection to a node's command endpoint.
func (c *Commander) client(node string) (hcpb.HostCommandServiceClient, error) {
	addr, err := c.cfg.Resolve(node)
	if err != nil {
		return nil, fmt.Errorf("resolve host command address of node %s: %w", node, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[addr]; ok {
		return cl.api, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial host command endpoint %s: %w", addr, err)
	}
	cl := &client{conn: conn, api: hcpb.NewHostCommandServiceClient(conn)}
	c.clients[addr] = cl
	return cl.api, nil
}

func outcomeName(o hcpb.Outcome) string {
	switch o {
	case hcpb.Outcome_OUTCOME_VACATED:
		return "vacated"
	case hcpb.Outcome_OUTCOME_RESUMED:
		return "resumed"
	case hcpb.Outcome_OUTCOME_FAILED:
		return "failed"
	case hcpb.Outcome_OUTCOME_STALE_EPOCH:
		return "stale_epoch"
	case hcpb.Outcome_OUTCOME_ABORTED:
		return "aborted"
	default:
		return "unspecified"
	}
}

// epochSource hands out strictly increasing command epochs. They start from
// the wall clock in nanoseconds, so a restarted orchestrator continues above
// the epochs of the process before it.
type epochSource struct {
	mu   sync.Mutex
	last int64
}

// Next returns a new epoch, greater than every epoch returned or observed.
func (e *epochSource) Next() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UnixNano()
	if now <= e.last {
		now = e.last + 1
	}
	e.last = now
	return now
}

// Observe records an epoch seen elsewhere, so Next returns a greater one.
func (e *epochSource) Observe(v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v > e.last {
		e.last = v
	}
}
