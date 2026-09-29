package statemachine

import (
	"fmt"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc/codes"
)

// Unknown-job Suspend policies: how a Suspend is answered for a job ID the
// agent has no record of (for example right after an agent restart, before
// the watcher has listed the mirror pods again).
//
// PENDING LEAD DECISION D-AGENT-4: the default completes the Suspend at once
// with RELEASED; "precondition" refuses it with FailedPrecondition.
const (
	// UnknownJobSuspendReleased completes the Suspend at once with
	// OUTCOME_RELEASED. Default.
	UnknownJobSuspendReleased = "released"
	// UnknownJobSuspendPrecondition refuses the Suspend with
	// codes.FailedPrecondition and creates no operation.
	UnknownJobSuspendPrecondition = "precondition"
)

// ValidateUnknownJobSuspend returns an error unless policy is
// UnknownJobSuspendReleased or UnknownJobSuspendPrecondition.
func ValidateUnknownJobSuspend(policy string) error {
	switch policy {
	case UnknownJobSuspendReleased, UnknownJobSuspendPrecondition:
		return nil
	}
	return fmt.Errorf("unknown unknown-job-suspend policy %q (want %q or %q)",
		policy, UnknownJobSuspendReleased, UnknownJobSuspendPrecondition)
}

// WithUnknownJobSuspend selects the unknown-job Suspend policy. Without it
// the StateManager uses UnknownJobSuspendReleased. Callers validate policy
// with ValidateUnknownJobSuspend first; any value other than
// UnknownJobSuspendPrecondition behaves as UnknownJobSuspendReleased.
func WithUnknownJobSuspend(policy string) Option {
	return func(sm *StateManager) {
		sm.unknownJobSuspend = policy
	}
}

// releaseUnknownLocked is the UnknownJobSuspendReleased path: the Suspend
// completes at once with RELEASED. No job record is created.
func (sm *StateManager) releaseUnknownLocked(jobID string, epoch int64) string {
	op := sm.newCompletedOpLocked(jobID, OpTypeSuspend, pb.Outcome_OUTCOME_RELEASED)
	op.Epoch = epoch
	return op.ID
}

// refuseUnknownSuspend is the UnknownJobSuspendPrecondition path: the
// Suspend is refused and no operation or job record is created. The epoch
// is not recorded (there is no job to hold it); once the watcher registers
// the job, SeedEpoch restores the fence from the mirror's guest-epoch
// annotation.
func refuseUnknownSuspend(jobID string) error {
	return refuse(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		"cannot suspend job %s: job unknown to this agent", jobID)
}
