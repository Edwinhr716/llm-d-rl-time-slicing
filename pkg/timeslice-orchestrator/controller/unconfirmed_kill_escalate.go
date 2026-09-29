package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// This file is option "escalate" of decision D-NS-6
// (--unconfirmed-kill=escalate): "block" (unconfirmed_kill_block.go) plus an
// escalation ladder counted from the first unconfirmed Kill, with the steps
// set by --unconfirmed-escalate-after (default 10s,40s):
//
//   - at once: alert ("Kill unconfirmed", timeslice_kill_unconfirmed_total, a
//     KillUnconfirmed event) and retry the Kill every second, as in block;
//   - step 1: delete the guest's mirror pod on the node, gracefully (the
//     pod's own grace period, never 0). The delete does not finish while the
//     guest is stuck, and the hold outlives the pod either way;
//   - step 2: mark the node not lendable (timeslice_node_failed{node} = 1, a
//     NodeNotLendable event, "Node not lendable") and keep the foreground's
//     Acquire waiting.
//
// It never force-deletes a pod and never touches a Node object. The node
// becomes lendable again, and the foreground is granted, once the agent
// confirms the guest gone.

// Escalation actions, logged as "action".
const (
	escalationDeleteMirror = "delete-mirror-pod"
	escalationNotLendable  = "mark-node-not-lendable"
)

// notLendableReason is the reason a node is marked not lendable.
const notLendableReason = "kill-unconfirmed"

// unconfirmedEscalate is option "escalate": block, then run the ladder.
func (c *Controller) unconfirmedEscalate(
	ctx context.Context, group *store.Group, jobID, node string, rec *killRecord, pastN bool,
) unconfirmedDecision {
	decision := c.unconfirmedBlock(ctx, group, jobID, node, rec, pastN)
	c.escalateHold(ctx, group, c.ensureHold(group.ID(), jobID, node, rec))
	return decision
}

// escalateAfter returns the two escalation steps, with the defaults for any
// that is unset.
func (c *Controller) escalateAfter() [2]time.Duration {
	after := c.EscalateAfter
	for i := range after {
		if after[i] <= 0 {
			after[i] = DefaultEscalateAfter[i]
		}
	}
	return after
}

// escalateHold runs the escalation steps that are due for a hold.
func (c *Controller) escalateHold(ctx context.Context, group *store.Group, hold *unconfirmedHold) {
	after := c.escalateAfter()
	held := time.Since(hold.since)
	if hold.steps < 1 && held >= after[0] {
		hold.steps = 1
		c.escalateDeleteMirror(ctx, hold)
	}
	if hold.steps < 2 && held >= after[1] {
		hold.steps = 2
		c.escalateNotLendable(ctx, group, hold)
	}
}

// escalateDeleteMirror is step 1: a graceful delete of the guest's mirror
// pod on the node.
func (c *Controller) escalateDeleteMirror(ctx context.Context, hold *unconfirmedHold) {
	metrics.UnconfirmedEscalationsTotal.WithLabelValues("1").Inc()
	slog.WarnContext(ctx, "Escalation", "group", hold.groupID, "node", hold.node, "job", hold.jobID,
		"step", 1, "action", escalationDeleteMirror, "held_ms", time.Since(hold.since).Milliseconds())
	if c.Kube == nil {
		slog.WarnContext(ctx, "No Kubernetes client, mirror pod not deleted", "group", hold.groupID,
			"node", hold.node, "job", hold.jobID)
		return
	}
	kctx, cancel := context.WithTimeout(ctx, kubeActionTimeout)
	defer cancel()
	deleted, err := c.Kube.DeletePodsGracefully(kctx, hold.groupID, hold.jobID, hold.node)
	if err != nil {
		slog.WarnContext(ctx, "Failed to delete mirror pod", "group", hold.groupID, "node", hold.node,
			"job", hold.jobID, "error", err)
		return
	}
	slog.InfoContext(ctx, "Mirror pod delete requested", "group", hold.groupID, "node", hold.node,
		"job", hold.jobID, "pods", deleted)
}

// escalateNotLendable is step 2: the node is not lent again until the hold
// is released; the foreground keeps waiting.
func (c *Controller) escalateNotLendable(ctx context.Context, group *store.Group, hold *unconfirmedHold) {
	metrics.UnconfirmedEscalationsTotal.WithLabelValues("2").Inc()
	slog.WarnContext(ctx, "Escalation", "group", hold.groupID, "node", hold.node, "job", hold.jobID,
		"step", 2, "action", escalationNotLendable, "held_ms", time.Since(hold.since).Milliseconds())
	c.killMu.Lock()
	if c.notLendable == nil {
		c.notLendable = make(map[string]string)
	}
	c.notLendable[hold.node] = notLendableReason
	hold.notLendable = true
	c.killMu.Unlock()
	metrics.NodeFailed.WithLabelValues(hold.node).Set(1)
	slog.WarnContext(ctx, "Node not lendable", "node", hold.node, "reason", notLendableReason,
		"group", hold.groupID, "job", hold.jobID)
	msg := fmt.Sprintf("Node %s is not lendable: the kill of guest %s is not confirmed", hold.node, hold.jobID)
	c.warnPods(ctx, hold.groupID, foregroundJobOf(group), "", EventNodeNotLendable, msg)
	c.warnPods(ctx, hold.groupID, hold.jobID, hold.node, EventNodeNotLendable, msg)
}
