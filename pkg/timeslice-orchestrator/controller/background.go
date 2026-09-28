package controller

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	agentpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	"google.golang.org/protobuf/encoding/protowire"
)

// This file is the minimal lend / notice / vacate / grant path for the
// background participant protocol (decision D-NS-4, option "keep"): the VK is
// a lock participant that learns of a notice by polling vacate_within and
// hands its grant back with Yield(ROLE_BACKGROUND). There is no kill path;
// at T the orchestrator only logs the hosts that are not clear.

// Defaults for the notice timing. They mirror the server's defaults; the
// command line sets both through --notice-window and --kill-budget.
const (
	defaultNoticeWindow = 30 * time.Second
	defaultKillBudget   = 3 * time.Second
)

// noticeRequeueInterval bounds the requeue delay while a notice runs, so the
// orchestrator sees hosts clear (or not clear at T) without waiting for a
// resync.
const noticeRequeueInterval = 1 * time.Second

// The snapshot-agent API in this tree predates the AGENT-A1 contract. The two
// values the orchestrator needs from it are read off the wire instead of from
// a local copy of the new proto: proto3 enums are open, so JOB_STATE_SUSPENDED
// arrives as JobState(6), and JobStatus.last_outcome (field 3) arrives as an
// unknown field.
const agentJobStateSuspended = agentpb.JobState(6)

const agentJobStatusLastOutcome = protowire.Number(3)

const agentOutcomeKilled = 4

// agentReportsKilled reports whether a job status carries
// last_outcome = OUTCOME_KILLED.
func agentReportsKilled(js *agentpb.JobStatus) bool {
	raw := js.ProtoReflect().GetUnknown()
	killed := false
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return false
		}
		raw = raw[n:]
		if num == agentJobStatusLastOutcome && typ == protowire.VarintType {
			v, m := protowire.ConsumeVarint(raw)
			if m < 0 {
				return false
			}
			killed = v == agentOutcomeKilled
			raw = raw[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return false
		}
		raw = raw[m:]
	}
	return killed
}

// noticeTrack remembers, for one notice of one group, which per-host log
// lines were already written.
type noticeTrack struct {
	noticeAt time.Time
	cleared  map[string]bool
	warned   map[string]bool
}

func newNoticeTrack(noticeAt time.Time) *noticeTrack {
	return &noticeTrack{
		noticeAt: noticeAt,
		cleared:  make(map[string]bool),
		warned:   make(map[string]bool),
	}
}

// vacateDeadline returns T = noticeAt + N - K.
func (c *Controller) vacateDeadline(noticeAt time.Time) time.Time {
	return noticeAt.Add(c.NoticeWindow - c.KillBudget)
}

// liveGuests returns the group nodes on which a background guest is live. A
// guest counts as vacated only when the agent reports it SUSPENDED, or
// last_outcome = KILLED, or its mirror pod is gone or terminal (terminal
// mirrors are dropped by the infrastructure observer). Every other state,
// including "the agent never reported it", is live.
func (c *Controller) liveGuests(ctx context.Context, groupID string) (map[string]bool, error) {
	jobs, err := c.jobStore.ListByGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs for group %s: %w", groupID, err)
	}
	live := make(map[string]bool)
	for _, job := range jobs {
		if !job.Background() {
			continue
		}
		states := job.ContextState()
		nodes := job.PodNodes()
		for node := range states {
			if !slices.Contains(nodes, node) {
				nodes = append(nodes, node)
			}
		}
		for _, node := range nodes {
			if states[node] == pb.SnapshotAgentJobState_STATE_SUSPENDED || job.Killed(node) {
				continue
			}
			live[node] = true
		}
	}
	return live, nil
}

// busyNodes returns, per group node, whether it is busy: its participant
// holds a grant or a claim, or a guest is live on it. The guard and the
// notice use "busy", which is what makes them fail closed.
func busyNodes(group *store.Group, live map[string]bool) map[string]bool {
	busy := make(map[string]bool)
	for _, node := range group.Status().Nodes() {
		busy[node] = live[node] || group.Spec().NodeHeld(node)
	}
	return busy
}

// anyNodeBusy reports whether any node of the group is busy.
func (c *Controller) anyNodeBusy(ctx context.Context, group *store.Group) (bool, error) {
	live, err := c.liveGuests(ctx, group.ID())
	if err != nil {
		return false, err
	}
	for _, busy := range busyNodes(group, live) {
		if busy {
			return true, nil
		}
	}
	return false, nil
}

// foregroundWants reports whether a foreground job holds or waits for the lock.
func foregroundWants(group *store.Group) bool {
	return group.Spec().LockingJob() != "" || group.Spec().GetWaitingJobQueue().Len() > 0
}

// prepareLend decides whether this reconcile lends the accelerator to the
// background. The foreground Yield only recorded a hint. It lends when the
// hint is set, no foreground job holds or waits, no notice runs, and every
// node of the group has its participant blocked in Acquire(ROLE_BACKGROUND).
// When it lends, it clears the active job so the node loop snapshots the
// foreground off the accelerator; grantIfVacant then grants each node.
func (c *Controller) prepareLend(ctx context.Context, group *store.Group) bool {
	spec := group.Spec()
	if !spec.Lend() {
		return false
	}
	if foregroundWants(group) {
		spec.SetLend(false)
		return false
	}
	if !spec.NoticeAt().IsZero() {
		return false
	}
	nodes := group.Status().Nodes()
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if !spec.ParticipantBlocked(node) && !spec.Granted(node) {
			// Keep the hint: the participant may be between its Yield and
			// its next Acquire. A foreground Acquire clears it.
			slog.DebugContext(ctx, "Lend pending: a node has no participant waiting", "node", node)
			return false
		}
	}
	if active := spec.ActiveJob(); active != "" {
		slog.InfoContext(ctx, "Lending to the background: taking the foreground off the accelerator",
			"group", group.ID(), "job", active)
		spec.SetActiveJob("")
	}
	return true
}

// grantIfVacant grants node to its blocked participant once no foreground job
// is RUNNING or TRANSITIONING there and no guest is live there. It reports
// whether the node is granted after the call.
func (c *Controller) grantIfVacant(ctx context.Context, group *store.Group, node string) (bool, error) {
	spec := group.Spec()
	if spec.Granted(node) {
		return true, nil
	}
	if foregroundWants(group) || !spec.NoticeAt().IsZero() || !spec.ParticipantBlocked(node) {
		return false, nil
	}
	jobs, err := c.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		return false, fmt.Errorf("failed to list jobs for group %s: %w", group.ID(), err)
	}
	for _, job := range jobs {
		if job.Background() {
			continue
		}
		switch job.ContextState()[node] {
		case pb.SnapshotAgentJobState_STATE_RUNNING, pb.SnapshotAgentJobState_STATE_TRANSITIONING:
			return false, nil
		default:
		}
	}
	live, err := c.liveGuests(ctx, group.ID())
	if err != nil {
		return false, err
	}
	if live[node] {
		return false, nil
	}
	if !spec.Grant(node) {
		return false, nil
	}
	slog.InfoContext(ctx, "Resume started", "group", group.ID(), "node", node)
	return true, nil
}

// reconcileNotice starts a notice when a foreground job holds or waits while
// a node is busy, ends it when no node is busy, and writes the per-host log
// lines: "Host clear" once per host and notice, and "Host not clear at
// deadline" once per host and notice when T passes. There is no kill path.
func (c *Controller) reconcileNotice(ctx context.Context, group *store.Group, live map[string]bool) {
	spec := group.Spec()
	busy := busyNodes(group, live)
	nodes := group.Status().Nodes()
	slices.Sort(nodes)
	var busyList []string
	for _, node := range nodes {
		if busy[node] {
			busyList = append(busyList, node)
		}
	}

	now := time.Now()
	noticeAt := spec.NoticeAt()
	if foregroundWants(group) && len(busyList) > 0 {
		noticeAt = spec.EnsureNotice(now)
		if noticeAt.Equal(now) {
			slog.InfoContext(ctx, "Vacate started", "group", group.ID(), "hosts", busyList,
				"deadline", c.vacateDeadline(noticeAt))
		}
	}
	if noticeAt.IsZero() {
		c.dropNotice(group.ID())
		return
	}

	track := c.noticeTrackFor(group.ID(), noticeAt)
	deadline := c.vacateDeadline(noticeAt)
	for _, node := range busyList {
		spec.AddNoticeHost(node)
		if !now.Before(deadline) && !track.warned[node] {
			track.warned[node] = true
			slog.WarnContext(ctx, "Host not clear at deadline", "group", group.ID(), "node", node,
				"held", spec.NodeHeld(node), "liveGuest", live[node])
		}
	}
	for _, node := range spec.NoticeHosts() {
		if busy[node] || track.cleared[node] {
			continue
		}
		track.cleared[node] = true
		how := "agent-status"
		if spec.YieldedInNotice(node) {
			how = "yield"
		}
		slog.InfoContext(ctx, "Host clear", "group", group.ID(), "node", node, "how", how)
	}

	if len(busyList) == 0 {
		spec.ClearNotice()
	}
}

func (c *Controller) noticeTrackFor(groupID string, noticeAt time.Time) *noticeTrack {
	c.noticeMu.Lock()
	defer c.noticeMu.Unlock()
	if c.notices == nil {
		c.notices = make(map[string]*noticeTrack)
	}
	track, ok := c.notices[groupID]
	if !ok || !track.noticeAt.Equal(noticeAt) {
		track = newNoticeTrack(noticeAt)
		c.notices[groupID] = track
	}
	return track
}

func (c *Controller) dropNotice(groupID string) {
	c.noticeMu.Lock()
	defer c.noticeMu.Unlock()
	delete(c.notices, groupID)
}

// delayedAdder is implemented by client-go's delaying queues.
type delayedAdder interface {
	AddAfter(item string, duration time.Duration)
}

// requeueDuringNotice requeues the group after min(1 s, T - now) while a
// notice runs, so hosts clearing and T passing are seen without a resync.
func (c *Controller) requeueDuringNotice(group *store.Group) {
	noticeAt := group.Spec().NoticeAt()
	if noticeAt.IsZero() {
		return
	}
	delay := noticeRequeueInterval
	if untilT := time.Until(c.vacateDeadline(noticeAt)); untilT > 0 && untilT < delay {
		delay = untilT
	}
	if q, ok := c.queue.(delayedAdder); ok {
		q.AddAfter(group.ID(), delay)
		return
	}
	id := group.ID()
	time.AfterFunc(delay, func() { c.queue.Add(id) })
}

// noticeHostsPending reports whether a notice runs and one of its hosts has
// not been logged clear yet. Until every host is clear the foreground's
// context is not reported loaded, so a foreground Acquire cannot succeed
// between a host clearing and the reconcile loop seeing it.
func (c *Controller) noticeHostsPending(group *store.Group) bool {
	spec := group.Spec()
	noticeAt := spec.NoticeAt()
	if noticeAt.IsZero() {
		return false
	}
	track := c.noticeTrackFor(group.ID(), noticeAt)
	c.noticeMu.Lock()
	defer c.noticeMu.Unlock()
	for _, node := range spec.NoticeHosts() {
		if !track.cleared[node] {
			return true
		}
	}
	return false
}

// nodeBusy reports whether node's participant holds a grant or a claim, or a
// guest is live on node.
func (c *Controller) nodeBusy(ctx context.Context, groupID, node string) (bool, error) {
	group, err := c.groupStore.Get(ctx, groupID)
	if err != nil {
		return false, fmt.Errorf("failed to get group %s from store: %w", groupID, err)
	}
	if group.Spec().NodeHeld(node) {
		return true, nil
	}
	live, err := c.liveGuests(ctx, groupID)
	if err != nil {
		return false, err
	}
	return live[node], nil
}
