// Package freeze is the interface between the guest kubelet and whatever stops and restarts a
// mirror pod's processes.
//
// M3 (VK-A3) froze the pod's cgroup from the guest kubelet itself. M4 (VK-A4) replaced that
// with the node's snapshot agent (internal/hostcmd implements Agent over gRPC), so the guest
// kubelet never touches cgroups. The agent is addressed by job id, the mirror's
// timeslice.io/job-id label.
package freeze

import (
	"context"
	"errors"
	"time"
)

// Agent is the snapshot agent's Q6 API as the guest kubelet uses it. Every call carries an
// absolute deadline for the agent; ctx bounds how long the caller waits for the answer. An
// implementation retries lost calls with the same epoch (the agent answers a repeat with the
// same operation) and returns once the operation has finished, failed, or ctx ended.
type Agent interface {
	// Jobs returns the jobs the agent knows on its node, by job id.
	Jobs(ctx context.Context) (map[string]Job, error)
	// Suspend checkpoints and freezes one job. It returns the outcome (Suspended or Released).
	Suspend(ctx context.Context, jobID string, epoch int64, deadline time.Time) (string, error)
	// Resume restores and thaws one job.
	Resume(ctx context.Context, jobID string, epoch int64, deadline time.Time) error
	// Kill kills every process of one job and returns once the agent confirmed it.
	Kill(ctx context.Context, jobID string, deadline time.Time, reason string) error
	// SuspendAll suspends every job of the role on the node (D-NS-5 ns-host), in one operation.
	SuspendAll(ctx context.Context, role string, epoch int64, deadline time.Time) (*HostResult, error)
	// ResumeAll resumes them the same way.
	ResumeAll(ctx context.Context, role string, epoch int64, deadline time.Time) (*HostResult, error)
}

// Job is one entry of the agent's Status.
type Job struct {
	// State is the JobState name without its prefix: IDLE, RUNNING, TRANSITIONING, SAVED,
	// FAULTED or SUSPENDED.
	State string
	// Epoch is the last epoch the agent saw for the job.
	Epoch int64
}

// Job states the guest kubelet acts on.
const (
	JobSuspended     = "SUSPENDED"
	JobTransitioning = "TRANSITIONING"
)

// Outcomes of a finished operation.
const (
	OutcomeSuspended = "SUSPENDED"
	OutcomeReleased  = "RELEASED"
	OutcomeResumed   = "RESUMED"
	OutcomeKilled    = "KILLED"
)

// HostResult is a finished host-level operation.
type HostResult struct {
	// Complete: every target reached the wanted state.
	Complete bool
	Error    string
	// Targets by job id: one per job the agent found on the node when the call arrived.
	Targets map[string]Target
}

// Target is one job of a host-level operation.
type Target struct {
	// Done: the job reached the wanted state (SUSPENDED or RELEASED for SuspendAll, RESUMED for
	// ResumeAll).
	Done    bool
	Outcome string
	Error   string
}

// ErrUnimplemented is wrapped by an Agent error when the agent does not implement the call.
// The guest kubelet treats it like any other failure: the kill sequence.
var ErrUnimplemented = errors.New("snapshot agent does not implement the call")

// LastEpoch returns the agent's last epoch from a STALE_EPOCH refusal, and whether err is one
// that says so. A host-level call retries once with that epoch plus one.
func LastEpoch(err error) (int64, bool) {
	var se interface{ LastEpoch() (int64, bool) }
	if !errors.As(err, &se) {
		return 0, false
	}
	return se.LastEpoch()
}
