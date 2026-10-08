package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// HostCommander commands the hosts of a group to vacate and resume (decision
// D-NS-4, option ns-push-vk). It is implemented by hostcmd.Commander.
type HostCommander interface {
	// SyncHosts sets the hosts of a group from the node watch.
	SyncHosts(group string, nodes []string)
	// AllClear reports whether every host acked a vacate and none was lent
	// since.
	AllClear(group string) bool
	// Lent reports whether any host was commanded to resume.
	Lent(group string) bool
	// StartVacate starts the vacate barrier for the notice that started at
	// noticeAt, unless one already runs.
	StartVacate(group string, noticeAt time.Time)
	// Resume commands every host of the group to resume its guests.
	Resume(group string)
	// Barrier returns the running vacate barrier of the group, if any.
	Barrier(group string) (hostcmd.Barrier, bool)
	// ClearByOrchestrator marks a host clear without its ack, because the
	// kill path vacated it (how names the way).
	ClearByOrchestrator(group, node, how string)
	// Forget drops the group and stops its commands.
	Forget(group string)
}

// forgetHostsIfGroupDeleted drops the host registry and the kill path state
// (kill records and hold log marks, kill.go) of a group that the observe step
// deleted from the store, so host state, commands and kill state do not
// outlive the group.
func (c *Controller) forgetHostsIfGroupDeleted(ctx context.Context, groupID string) {
	if _, err := c.groupStore.Get(ctx, groupID); errors.Is(err, store.ErrNotFound) {
		slog.InfoContext(ctx, "Group deleted: forgetting its hosts")
		c.Hosts.Forget(groupID)
		c.forgetKills(groupID)
	}
}

// holdForHosts syncs the hosts of the group and reports whether the reconcile
// must stop before promotion and node work because some host has not acked a
// vacate (fail closed). When a foreground job waits or holds the lock, it
// starts the vacate barrier.
//
// The kill path (killOverdueHosts, kill.go) runs just before, in
// reconcileGroup, and may have cleared hosts itself.
func (c *Controller) holdForHosts(ctx context.Context, group *store.Group) bool {
	groupID := group.ID()
	c.Hosts.SyncHosts(groupID, group.Status().Nodes())
	if c.Hosts.AllClear(groupID) {
		return false
	}
	spec := group.Spec()
	switch {
	case spec.GetWaitingJobQueue().Len() > 0:
		// A foreground job waits: the notice starts now unless it runs.
		c.Hosts.StartVacate(groupID, spec.EnsureNotice(time.Now()))
	case spec.LockingJob() != "":
		// A foreground job holds the lock while hosts are unknown, for
		// example after an orchestrator restart: sweep them. No notice is
		// recorded because nothing waits for a grant.
		c.Hosts.StartVacate(groupID, time.Now())
	default:
		// Nothing foreground wants the accelerator: hosts stay lent.
	}
	slog.DebugContext(ctx, "Holding promotion until every host acked a vacate")
	return true
}

// notLendableUnconfirmedKill is the reason a node is not lent while a guest
// handed back after an unconfirmed Kill is not vacated (D-NS-6 grant).
const notLendableUnconfirmedKill = "unconfirmed-kill"

// lendWanted reports whether the foreground lent the accelerator and nothing
// foreground wants it back.
func lendWanted(spec *store.GroupSpec) bool {
	return spec.Lend() && spec.LockingJob() == "" &&
		spec.GetWaitingJobQueue().Len() == 0 && spec.NoticeAt().IsZero()
}

// prepareLend clears the active job when the accelerator is lent, so the node
// loop snapshots the foreground job before the hosts resume.
func (c *Controller) prepareLend(ctx context.Context, group *store.Group) {
	spec := group.Spec()
	if !lendWanted(spec) || spec.ActiveJob() == "" {
		return
	}
	slog.InfoContext(ctx, "Lend: saving the foreground job before the hosts resume", "job", spec.ActiveJob())
	spec.SetActiveJob("")
}

// resumeIfLent commands the hosts to resume once the accelerator is lent and
// no foreground job holds context on the group's nodes.
func (c *Controller) resumeIfLent(ctx context.Context, group *store.Group) error {
	spec := group.Spec()
	if !lendWanted(spec) || spec.ActiveJob() != "" || c.Hosts.Lent(group.ID()) {
		return nil
	}
	if guest := c.unconfirmedGuestOn(ctx, group); guest.job != "" {
		// A guest handed back after an unconfirmed Kill may still be on the
		// node: do not lend it again until the agent says it is gone.
		slog.WarnContext(ctx, "Node not lendable", "node", guest.node, "reason", notLendableUnconfirmedKill,
			"job", guest.job)
		return nil
	}
	busy, err := c.foregroundResident(ctx, group)
	if err != nil {
		return err
	}
	if busy {
		return nil
	}
	c.Hosts.Resume(group.ID())
	return nil
}

// lendRetryInterval is how soon a lend held because an agent did not answer
// is looked at again.
const lendRetryInterval = 1 * time.Second

// markAgentFailed records that a status call to the agent of node failed.
func (c *Controller) markAgentFailed(node string) {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if c.agentFailed == nil {
		c.agentFailed = make(map[string]time.Time)
	}
	c.agentFailed[node] = time.Now()
}

// agentAnsweredLast reports whether the agent of node answered a status call
// and the last status call to it did not fail. Only then do the context
// states of node describe what is on the accelerator now.
func (c *Controller) agentAnsweredLast(node string) bool {
	c.killMu.Lock()
	defer c.killMu.Unlock()
	seen, ok := c.agentSeen[node]
	return ok && !c.agentFailed[node].After(seen)
}

// foregroundResident reports whether any foreground job is RUNNING or
// TRANSITIONING on a node of the group, or may be: a node whose agent did not
// answer its last status call counts as resident (fail closed), because its
// context states are unknown or stale and the foreground job's snapshot is
// not confirmed. Guests do not count.
func (c *Controller) foregroundResident(ctx context.Context, group *store.Group) (bool, error) {
	for _, node := range group.Status().Nodes() {
		if !c.agentAnsweredLast(node) {
			slog.WarnContext(ctx, "Not lending: the snapshot agent did not answer, so the foreground job is not known to be saved",
				"node", node)
			c.queue.AddAfter(group.ID(), lendRetryInterval)
			return true, nil
		}
	}
	jobs, err := c.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		return false, fmt.Errorf("failed to list jobs for group %s: %w", group.ID(), err)
	}
	for _, job := range jobs {
		if job.Background() {
			continue
		}
		states := job.ContextState()
		for _, node := range group.Status().Nodes() {
			switch states[node] {
			case pb.SnapshotAgentJobState_STATE_RUNNING, pb.SnapshotAgentJobState_STATE_TRANSITIONING:
				return true, nil
			default:
			}
		}
	}
	return false, nil
}
