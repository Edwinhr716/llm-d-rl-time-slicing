package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// HostCommander commands the hosts of a group (D-NS-4 ns-push-agent): each
// host's snapshot agent suspends or resumes every guest on the host and acks.
type HostCommander interface {
	// Clear reports whether every host of the group acked its last vacate
	// command and nothing was resumed since.
	Clear(groupID string) bool
	// Vacate makes sure the hosts are told to vacate by
	// noticeAt + N - K and reports whether all of them are clear.
	Vacate(ctx context.Context, groupID string, nodes []string, noticeAt time.Time) bool
	// Resume tells the hosts to resume their guests.
	Resume(ctx context.Context, groupID string, nodes []string)
}

// HostsClear reports whether the foreground may be granted groupID as far as
// the hosts are concerned: always without a HostCommander.
func (c *Controller) HostsClear(groupID string) bool {
	return c.Hosts == nil || c.Hosts.Clear(groupID)
}

// foregroundWants reports whether a foreground job holds or waits for the lock.
func foregroundWants(group *store.Group) bool {
	spec := group.Spec()
	return spec.LockingJob() != "" || spec.GetWaitingJobQueue().Len() > 0
}

// holdForHosts runs before the node loop. When the foreground holds or waits
// for the lock and the hosts are not clear, it starts the notice and the
// vacate round and reports true: nothing may be restored until every host
// acked. When the foreground lends the accelerator (lend hint, no one waiting)
// it clears the active job so the node loop snapshots the foreground.
func (c *Controller) holdForHosts(ctx context.Context, group *store.Group) bool {
	spec := group.Spec()
	if foregroundWants(group) {
		noticeAt := spec.NoticeAt()
		if noticeAt.IsZero() {
			noticeAt = time.Now()
		}
		if c.Hosts.Vacate(ctx, group.ID(), group.Status().Nodes(), noticeAt) {
			// Every host is clear: the notice, if any, is over.
			spec.ClearNotice()
			return false
		}
		spec.EnsureNotice(noticeAt)
		slog.InfoContext(ctx, "Holding the foreground until every host is clear")
		return true
	}
	if spec.Lend() && spec.ActiveJob() != "" {
		slog.InfoContext(ctx, "Lending the accelerator: saving the foreground", "job", spec.ActiveJob())
		spec.SetActiveJob("")
	}
	return false
}

// resumeIfLent runs after the node loop. When the foreground lends the
// accelerator and no foreground job is resident on any node any more, it
// tells the hosts to resume their guests.
func (c *Controller) resumeIfLent(ctx context.Context, group *store.Group) error {
	spec := group.Spec()
	if !spec.Lend() || foregroundWants(group) || spec.ActiveJob() != "" {
		return nil
	}
	jobs, err := c.jobStore.ListByGroup(ctx, group.ID())
	if err != nil {
		return fmt.Errorf("failed to list jobs for group %s: %w", group.ID(), err)
	}
	nodes := group.Status().Nodes()
	for _, job := range jobs {
		states := job.ContextState()
		for _, node := range nodes {
			switch states[node] {
			case pb.SnapshotAgentJobState_STATE_RUNNING, pb.SnapshotAgentJobState_STATE_TRANSITIONING:
				return fmt.Errorf("lend pending: job %s still resident on node %s", job.JobID(), node)
			}
		}
	}
	c.Hosts.Resume(ctx, group.ID(), nodes)
	return nil
}
