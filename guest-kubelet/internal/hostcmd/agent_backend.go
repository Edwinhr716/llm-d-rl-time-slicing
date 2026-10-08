package hostcmd

import (
	"context"
	"fmt"
	"time"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// AgentBackend is freeze.Agent over the gRPC client: what the mirror backend calls for M4.
type AgentBackend struct {
	Client *AgentClient
}

var _ freeze.Agent = (*AgentBackend)(nil)

// Jobs implements freeze.Agent.
func (a *AgentBackend) Jobs(ctx context.Context) (map[string]freeze.Job, error) {
	st, err := a.Client.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]freeze.Job, len(st))
	for _, j := range st {
		out[j.GetJobId()] = freeze.Job{State: trimEnum(j.GetState().String(), "JOB_STATE_"), Epoch: j.GetEpoch()}
	}
	return out, nil
}

// Suspend implements freeze.Agent.
func (a *AgentBackend) Suspend(ctx context.Context, jobID string, epoch int64, deadline time.Time) (string, error) {
	res, err := a.Client.SuspendJob(ctx, jobID, epoch, deadline)
	if err != nil {
		return "", err
	}
	switch res.Outcome {
	case freeze.OutcomeSuspended, freeze.OutcomeReleased:
		return res.Outcome, nil
	default:
		return "", fmt.Errorf("suspend %s: unexpected outcome %q", jobID, res.Outcome)
	}
}

// Resume implements freeze.Agent.
func (a *AgentBackend) Resume(ctx context.Context, jobID string, epoch int64, deadline time.Time) error {
	res, err := a.Client.ResumeJob(ctx, jobID, epoch, deadline)
	if err != nil {
		return err
	}
	if res.Outcome != freeze.OutcomeResumed {
		return fmt.Errorf("resume %s: unexpected outcome %q", jobID, res.Outcome)
	}
	return nil
}

// Kill implements freeze.Agent.
func (a *AgentBackend) Kill(ctx context.Context, jobID string, deadline time.Time, reason string) error {
	return a.Client.KillJob(ctx, jobID, deadline, reason)
}

// SuspendAll implements freeze.Agent. The role is the client's (background).
func (a *AgentBackend) SuspendAll(ctx context.Context, _ string, epoch int64, deadline time.Time) (*freeze.HostResult, error) {
	res, err := a.Client.SuspendAll(ctx, epoch, deadline)
	return hostResult(res), err
}

// ResumeAll implements freeze.Agent.
func (a *AgentBackend) ResumeAll(ctx context.Context, _ string, epoch int64, deadline time.Time) (*freeze.HostResult, error) {
	res, err := a.Client.ResumeAll(ctx, epoch, deadline)
	return hostResult(res), err
}

func hostResult(res *AgentResult) *freeze.HostResult {
	if res == nil {
		return nil
	}
	out := &freeze.HostResult{Complete: res.Complete, Error: res.Error, Targets: make(map[string]freeze.Target, len(res.Targets))}
	for id, t := range res.Targets {
		out.Targets[id] = freeze.Target{Done: t.Done, Outcome: trimEnum(t.Outcome, "OUTCOME_"), Error: t.Error}
	}
	return out
}
