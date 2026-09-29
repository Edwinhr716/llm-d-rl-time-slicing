package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/metrics"
)

// This file escalates a blocked grant (unconfirmed_kill_block.go) on a ladder
// measured from the unconfirmed-kill decision (T + K), with the steps from
// --unconfirmed-escalate-after=<E1>,<E2>:
//
//	T + K       alert; the Kill is retried every killRetryInterval (kill.go)
//	T + K + E1  step 1: delete the guest's mirror pods gracefully (the pod's
//	            own grace period, never 0)
//	T + K + E2  step 2: mark the node not lendable (Warning event on the Node,
//	            timeslice_node_failed{node}); the foreground keeps waiting
//
// The steps run on timers, so a reconcile pass blocked on a Kill poll does not
// delay them. They stop when the block is released: the Kill is confirmed,
// the guest is otherwise vacated, or the barrier ends.

// Escalation actions, as logged.
const (
	escalateActionDeleteMirror = "delete-mirror-pod"
	escalateActionNotLendable  = "node-not-lendable"
)

// notLendableEscalated is the reason a node is not lendable after step 2.
const notLendableEscalated = "unconfirmed-kill-escalated"

// escalateAfter returns E1 and E2.
func (c *Controller) escalateAfter() [2]time.Duration {
	after := c.UnconfirmedEscalateAfter
	if after[0] <= 0 || after[1] <= after[0] {
		def, err := ParseUnconfirmedEscalateAfter(DefaultUnconfirmedEscalateAfter)
		if err == nil {
			after = def
		}
	}
	return after
}

// startEscalation arms the two escalation steps of a block.
func (c *Controller) startEscalation(ctx context.Context, st *blockState) {
	after := c.escalateAfter()
	c.killMu.Lock()
	defer c.killMu.Unlock()
	if st.released {
		return
	}
	arm := func(step int, after time.Duration) *time.Timer {
		return time.AfterFunc(time.Until(st.decidedAt.Add(after)), func() {
			defer handleCrash(ctx)
			c.escalationStep(ctx, st, step)
		})
	}
	st.escalation = append(st.escalation, arm(1, after[0]), arm(2, after[1]))
}

// escalationStep runs one step of the ladder, unless the block was released
// or the controller is stopping.
func (c *Controller) escalationStep(ctx context.Context, st *blockState, step int) {
	if ctx.Err() != nil {
		return
	}
	c.killMu.Lock()
	if st.released {
		c.killMu.Unlock()
		return
	}
	if step == 2 {
		if c.notLendable == nil {
			c.notLendable = make(map[string]string)
		}
		c.notLendable[st.node] = notLendableEscalated
		st.notLendable = true
	}
	c.killMu.Unlock()

	metrics.UnconfirmedEscalationsTotal.WithLabelValues(strconv.Itoa(step)).Inc()
	since := time.Since(st.since).Milliseconds()
	switch step {
	case 1:
		deleted := 0
		c.kubeCall(ctx, "delete mirror pod", func(ctx context.Context, k UnconfirmedKube) error {
			n, err := k.DeleteGuestMirror(ctx, st.group, st.job, st.node)
			deleted = n
			return err
		})
		slog.WarnContext(ctx, "Escalation", "group", st.group, "node", st.node, "job", st.job,
			"step", step, "action", escalateActionDeleteMirror, "since_ms", since, "deleted", deleted)
	default:
		metrics.NodeFailed.WithLabelValues(st.node).Set(1)
		slog.WarnContext(ctx, "Escalation", "group", st.group, "node", st.node, "job", st.job,
			"step", step, "action", escalateActionNotLendable, "since_ms", since)
		slog.WarnContext(ctx, "Node not lendable", "node", st.node, "reason", notLendableEscalated,
			"group", st.group, "job", st.job)
		c.kubeCall(ctx, "event "+eventNodeNotLendable, func(ctx context.Context, k UnconfirmedKube) error {
			return k.NodeEvent(ctx, st.node, eventNodeNotLendable, fmt.Sprintf(
				"Kill of guest %s not confirmed for %v: node not lent again until the agent confirms it gone; "+
					"the foreground grant of group %s stays blocked",
				st.job, time.Since(st.decidedAt).Round(time.Second), st.group))
		})
	}
}
