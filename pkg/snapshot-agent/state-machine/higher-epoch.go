package statemachine

import (
	"fmt"
	"log/slog"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc/codes"
)

// Higher-epoch policies: what a Suspend or Resume with a higher epoch does
// while a lower-epoch Suspend or Resume of the same job runs.
//
// PENDING LEAD DECISION D-AGENT-3: the default aborts the running operation
// (the shared contract); "aborted" refuses the new call with Aborted instead.
const (
	// HigherEpochAbort aborts the running operation (FAILED, STALE_EPOCH,
	// context cancelled) and starts the new call's worker. Default.
	HigherEpochAbort = "abort"
	// HigherEpochAborted refuses the new call with codes.Aborted and leaves
	// the running operation untouched. The caller retries when it finishes.
	HigherEpochAborted = "aborted"
)

// ValidateHigherEpoch returns an error unless policy is HigherEpochAbort or
// HigherEpochAborted.
func ValidateHigherEpoch(policy string) error {
	switch policy {
	case HigherEpochAbort, HigherEpochAborted:
		return nil
	}
	return fmt.Errorf("unknown higher-epoch policy %q (want %q or %q)",
		policy, HigherEpochAbort, HigherEpochAborted)
}

// WithHigherEpoch selects the higher-epoch policy. Without it the
// StateManager uses HigherEpochAbort. Callers validate policy with
// ValidateHigherEpoch first; any value other than HigherEpochAborted
// behaves as HigherEpochAbort.
func WithHigherEpoch(policy string) Option {
	return func(sm *StateManager) {
		sm.higherEpoch = policy
	}
}

// abortRunningLocked is the HigherEpochAbort path: it supersedes the
// running guest operation and starts this call's worker.
func (sm *StateManager) abortRunningLocked(
	job *Job, intent OpType, epoch int64, deadline time.Time, worker GuestWorker,
) string {
	running := job.current.op
	slog.Warn("Aborting running guest operation for a higher epoch",
		"jobID", job.ID, "aborted", running.Type, "abortedEpoch", running.Epoch,
		"intent", intent, "epoch", epoch)
	sm.supersedeLocked(job, pb.ErrorReason_STALE_EPOCH,
		fmt.Sprintf("aborted by %s with epoch %d", intent, epoch))
	return sm.startGuestLocked(job, intent, epoch, deadline, worker)
}

// refuseHigherEpochLocked is the HigherEpochAborted path: it refuses this
// call with Aborted and leaves the running operation alone. The epoch the
// call carried stays recorded (StartGuestOp raised it before this point).
func (sm *StateManager) refuseHigherEpochLocked(job *Job, intent OpType, epoch int64) error {
	running := job.current.op
	slog.Info("Refusing higher-epoch call: an operation is running",
		"jobID", job.ID, "running", running.Type, "runningEpoch", running.Epoch,
		"intent", intent, "epoch", epoch)
	return refuse(codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		"%s of job %s with epoch %d: a %s operation with epoch %d is running",
		intent, job.ID, epoch, running.Type, running.Epoch)
}
