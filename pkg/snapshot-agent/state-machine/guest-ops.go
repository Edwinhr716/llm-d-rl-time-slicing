package statemachine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GuestResult is what a successful Suspend or Resume worker reports.
type GuestResult struct {
	// Outcome must be OUTCOME_SUSPENDED or OUTCOME_RELEASED for a Suspend.
	// It is ignored for a Resume, whose outcome the StateManager sets.
	Outcome pb.Outcome
	// DeviceBytes is the device memory released (Suspend) or restored
	// (Resume).
	DeviceBytes int64
	// HostBytesPinned is the host memory the job holds afterwards.
	HostBytesPinned int64
	// StorageBytes is the size of the checkpoint image, if known.
	StorageBytes int64
}

// GuestWorker runs a Suspend or Resume pipeline. Its context carries the
// call's absolute deadline and is cancelled when the operation is aborted
// or superseded.
type GuestWorker func(ctx context.Context) (GuestResult, error)

// KillWorker kills a job's processes and confirms they are gone. Its
// context carries the Kill's absolute deadline.
type KillWorker func(ctx context.Context) error

// OpError is a worker failure classified with an ErrorReason.
type OpError struct {
	Reason pb.ErrorReason
	Err    error
}

// NewOpError wraps err with reason.
func NewOpError(reason pb.ErrorReason, err error) *OpError {
	return &OpError{Reason: reason, Err: err}
}

func (e *OpError) Error() string {
	return fmt.Sprintf("%s: %v", e.Reason, e.Err)
}

func (e *OpError) Unwrap() error {
	return e.Err
}

// RefusalError is returned when a Suspend, Resume or Kill call is refused
// before any operation starts. It converts to a gRPC status with Code, and
// the status message starts with the ErrorReason name when Reason is set.
type RefusalError struct {
	Code   codes.Code
	Reason pb.ErrorReason
	Msg    string
}

func (e *RefusalError) Error() string {
	if e.Reason == pb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return e.Msg
	}
	return e.Reason.String() + ": " + e.Msg
}

// GRPCStatus lets status.FromError and status.Code read the refusal.
func (e *RefusalError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Error())
}

func refuse(code codes.Code, reason pb.ErrorReason, format string, args ...any) error {
	return &RefusalError{Code: code, Reason: reason, Msg: fmt.Sprintf(format, args...)}
}

// ErrorReasonOf returns the ErrorReason carried by err (a RefusalError or
// an OpError), or ERROR_REASON_UNSPECIFIED.
func ErrorReasonOf(err error) pb.ErrorReason {
	var refusal *RefusalError
	if errors.As(err, &refusal) {
		return refusal.Reason
	}
	var opErr *OpError
	if errors.As(err, &opErr) {
		return opErr.Reason
	}
	return pb.ErrorReason_ERROR_REASON_UNSPECIFIED
}

// StartGuestOp starts a Suspend or Resume of a background (guest) job and
// returns the operation ID to poll. deadline is absolute and required.
//
// Epoch fencing comes first:
//   - an epoch lower than the job's last epoch is refused with STALE_EPOCH;
//   - the same epoch and the same call returns that call's operation ID,
//     running or finished, until the operation expires (a re-issue after a
//     lost reply);
//   - the same epoch and a different call is refused with STALE_EPOCH;
//   - a higher epoch while a Suspend or Resume runs aborts the running
//     operation (FAILED with STALE_EPOCH, context cancelled) and starts
//     this call's worker, which observes the node and continues from there.
//
// A call that passes fencing is then answered by job state:
//   - Suspend runs the worker from RUNNING, SAVED, SUSPENDED and IDLE (the
//     worker reports RELEASED when no process is left). It completes at
//     once with RELEASED for an unknown job or an IDLE job whose last
//     outcome is KILLED.
//   - Resume runs the worker from SAVED and SUSPENDED and completes at once
//     from RUNNING.
//   - Anything else is refused with FAILED_PRECONDITION, and a running
//     Kill, Snapshot or Restore with Aborted.
//
// A worker failure, or a worker that returns after the deadline, fails the
// operation and leaves the job FAULTED; the kill path follows.
func (sm *StateManager) StartGuestOp(
	jobID string, intent OpType, epoch int64, deadline time.Time, worker GuestWorker,
) (string, error) {
	if intent != OpTypeSuspend && intent != OpTypeResume {
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"unsupported guest operation %q", intent)
	}
	if deadline.IsZero() {
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"%s of job %s: a deadline is required", intent, jobID)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.gcLocked()

	job, ok := sm.jobs[jobID]
	if !ok {
		if intent == OpTypeResume {
			return "", refuse(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
				"cannot resume job %s: the job is unknown to this agent", jobID)
		}
		op := sm.newCompletedOpLocked(jobID, intent, pb.Outcome_OUTCOME_RELEASED)
		op.Epoch = epoch
		return op.ID, nil
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if epoch < job.LastEpoch {
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH,
			"%s of job %s: epoch %d is lower than the last epoch %d", intent, jobID, epoch, job.LastEpoch)
	}
	if rec := sm.guestRecordLocked(job); rec != nil && rec.Epoch == epoch {
		if rec.Type == intent {
			return rec.ID, nil
		}
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH,
			"%s of job %s: epoch %d was already used for %s", intent, jobID, epoch, rec.Type)
	}
	// The epoch is used from here on even if the call is refused below: the
	// caller wrote it on the mirror before calling, and any lower call is
	// late.
	job.LastEpoch = epoch

	if job.current != nil {
		return sm.preemptLocked(job, intent, epoch, deadline, worker)
	}
	return sm.answerByStateLocked(job, intent, epoch, deadline, worker)
}

// preemptLocked answers a guest call that passed fencing while the job has
// a running operation. The running operation has a lower epoch: fencing
// already answered an equal or higher one.
func (sm *StateManager) preemptLocked(
	job *Job, intent OpType, epoch int64, deadline time.Time, worker GuestWorker,
) (string, error) {
	running := job.current.op
	if running.Type != OpTypeSuspend && running.Type != OpTypeResume {
		// A Kill is never aborted by a guest call, and a foreground
		// Snapshot or Restore belongs to the RL job.
		return "", refuse(codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"%s of job %s: a %s operation is running", intent, job.ID, running.Type)
	}
	if err := sm.checkDeadline(job.ID, intent, deadline); err != nil {
		return "", err
	}
	slog.Warn("Aborting running guest operation for a higher epoch",
		"jobID", job.ID, "aborted", running.Type, "abortedEpoch", running.Epoch,
		"intent", intent, "epoch", epoch)
	sm.supersedeLocked(job, pb.ErrorReason_STALE_EPOCH,
		fmt.Sprintf("aborted by %s with epoch %d", intent, epoch))
	return sm.startGuestLocked(job, intent, epoch, deadline, worker), nil
}

// answerByStateLocked answers a guest call that passed fencing when the job
// has no running operation.
func (sm *StateManager) answerByStateLocked(
	job *Job, intent OpType, epoch int64, deadline time.Time, worker GuestWorker,
) (string, error) {
	switch job.State {
	case pb.JobState_JOB_STATE_RUNNING:
		if intent == OpTypeResume {
			return sm.completeGuestLocked(job, intent, epoch, sm.resumedOutcome()), nil
		}
	case pb.JobState_JOB_STATE_SAVED, pb.JobState_JOB_STATE_SUSPENDED:
		// Both calls run their pipeline; every step observes first.
	case pb.JobState_JOB_STATE_IDLE:
		if intent == OpTypeResume {
			return "", refuse(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
				"cannot resume job %s in state %s", job.ID, job.State)
		}
		if job.LastOutcome == pb.Outcome_OUTCOME_KILLED {
			return sm.completeGuestLocked(job, intent, epoch, pb.Outcome_OUTCOME_RELEASED), nil
		}
		// An IDLE job may still have processes; the worker decides.
	default:
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"cannot %s job %s in state %s", intent, job.ID, job.State)
	}
	if err := sm.checkDeadline(job.ID, intent, deadline); err != nil {
		return "", err
	}
	return sm.startGuestLocked(job, intent, epoch, deadline, worker), nil
}

// checkDeadline refuses a call whose deadline has already passed.
func (sm *StateManager) checkDeadline(jobID string, intent OpType, deadline time.Time) error {
	if deadline.After(sm.now()) {
		return nil
	}
	return refuse(codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE,
		"%s of job %s: the deadline %s has passed", intent, jobID, deadline.Format(time.RFC3339Nano))
}

// completeGuestLocked records a guest call answered at once, without a
// worker. It does not change the job's state or last outcome.
func (sm *StateManager) completeGuestLocked(job *Job, intent OpType, epoch int64, outcome pb.Outcome) string {
	op := sm.newCompletedOpLocked(job.ID, intent, outcome)
	op.Epoch = epoch
	job.lastGuestOp = op
	return op.ID
}

// startGuestLocked starts a guest operation's worker.
func (sm *StateManager) startGuestLocked(
	job *Job, intent OpType, epoch int64, deadline time.Time, worker GuestWorker,
) string {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	op := &Operation{
		ID:        uuid.New().String(),
		JobID:     job.ID,
		Status:    pb.OperationStatus_OPERATION_STATUS_PENDING,
		Type:      intent,
		StartedAt: sm.now(),
		Epoch:     epoch,
		Deadline:  deadline,
	}
	sm.operations[op.ID] = op
	job.current = &runningOp{op: op, cancel: cancel}
	job.lastGuestOp = op
	job.State = pb.JobState_JOB_STATE_TRANSITIONING
	go sm.runGuest(ctx, cancel, job, op, worker)
	return op.ID
}

func (sm *StateManager) runGuest(
	ctx context.Context, cancel context.CancelFunc, job *Job, op *Operation, worker GuestWorker,
) {
	defer cancel()
	res, err := worker(ctx)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	job.mu.Lock()
	defer job.mu.Unlock()

	if !sm.finishCurrentLocked(job, op) {
		return
	}
	if err == nil && op.FinishedAt.After(op.Deadline) {
		err = NewOpError(pb.ErrorReason_DEADLINE_EXCEEDED,
			fmt.Errorf("finished after the deadline %s", op.Deadline.Format(time.RFC3339Nano)))
	}
	if err == nil && op.Type == OpTypeSuspend &&
		res.Outcome != pb.Outcome_OUTCOME_SUSPENDED && res.Outcome != pb.Outcome_OUTCOME_RELEASED {
		err = NewOpError(pb.ErrorReason_BACKEND_ERROR,
			fmt.Errorf("suspend worker reported outcome %s", res.Outcome))
	}
	if err != nil {
		op.Status = pb.OperationStatus_OPERATION_STATUS_FAILED
		op.Error = err.Error()
		op.ErrorReason = failureReason(ctx, err)
		job.State = pb.JobState_JOB_STATE_FAULTED
		slog.Error("Guest operation failed; job is FAULTED",
			"jobID", job.ID, "type", op.Type, "epoch", op.Epoch, "reason", op.ErrorReason, "error", err)
		return
	}

	op.Status = pb.OperationStatus_OPERATION_STATUS_COMPLETE
	op.StorageBytes = res.StorageBytes
	op.SnapshotDeviceBytes = res.DeviceBytes
	op.HostBytesPinned = res.HostBytesPinned
	job.HostBytesPinned = res.HostBytesPinned
	switch {
	case op.Type == OpTypeResume:
		op.Outcome = sm.resumedOutcome()
		job.State = pb.JobState_JOB_STATE_RUNNING
	case res.Outcome == pb.Outcome_OUTCOME_SUSPENDED:
		op.Outcome = res.Outcome
		job.State = pb.JobState_JOB_STATE_SUSPENDED
		job.DeviceBytes = res.DeviceBytes
	default: // OUTCOME_RELEASED: no process was left.
		op.Outcome = res.Outcome
		job.State = pb.JobState_JOB_STATE_IDLE
		job.PIDs = nil
		job.DeviceBytes = 0
	}
	job.LastOutcome = op.Outcome
}

// failureReason classifies a worker error.
func failureReason(ctx context.Context, err error) pb.ErrorReason {
	if reason := ErrorReasonOf(err); reason != pb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return reason
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return pb.ErrorReason_DEADLINE_EXCEEDED
	}
	return pb.ErrorReason_BACKEND_ERROR
}

// StartKill starts a Kill of a job and returns the operation ID to poll.
// deadline is absolute and required; reason is logged. Kill carries no
// epoch and is accepted in every state:
//   - an unknown job, or an IDLE job whose last outcome is KILLED, completes
//     at once with OUTCOME_KILLED;
//   - a Kill that is already running is returned to the new caller;
//   - any other running operation is superseded first (FAILED, context
//     cancelled); when its worker returns it writes nothing to the job.
//
// A confirmed kill before the deadline leaves the job IDLE with last
// outcome KILLED. A worker error, or a confirmation after the deadline,
// fails the operation with KILL_UNCONFIRMED and leaves the job FAULTED; a
// later Kill may still clear it.
func (sm *StateManager) StartKill(jobID string, deadline time.Time, reason string, worker KillWorker) (string, error) {
	if deadline.IsZero() {
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"Kill of job %s: a deadline is required", jobID)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.gcLocked()

	job, ok := sm.jobs[jobID]
	if !ok {
		op := sm.newCompletedOpLocked(jobID, OpTypeKill, pb.Outcome_OUTCOME_KILLED)
		op.Deadline = deadline
		return op.ID, nil
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if job.current != nil && job.current.op.Type == OpTypeKill {
		return job.current.op.ID, nil
	}
	if job.current == nil && job.State == pb.JobState_JOB_STATE_IDLE && job.LastOutcome == pb.Outcome_OUTCOME_KILLED {
		op := sm.newCompletedOpLocked(jobID, OpTypeKill, pb.Outcome_OUTCOME_KILLED)
		op.Deadline = deadline
		return op.ID, nil
	}

	slog.Warn("Kill requested", "jobID", jobID, "state", job.State, "reason", reason,
		"deadline", deadline.Format(time.RFC3339Nano))
	if job.current != nil {
		sm.supersedeLocked(job, pb.ErrorReason_ERROR_REASON_UNSPECIFIED, "superseded by Kill: "+reason)
	}

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	op := &Operation{
		ID:        uuid.New().String(),
		JobID:     jobID,
		Status:    pb.OperationStatus_OPERATION_STATUS_PENDING,
		Type:      OpTypeKill,
		StartedAt: sm.now(),
		Deadline:  deadline,
	}
	sm.operations[op.ID] = op
	job.current = &runningOp{op: op, cancel: cancel}
	job.State = pb.JobState_JOB_STATE_TRANSITIONING
	go sm.runKill(ctx, cancel, job, op, worker)
	return op.ID, nil
}

func (sm *StateManager) runKill(
	ctx context.Context, cancel context.CancelFunc, job *Job, op *Operation, worker KillWorker,
) {
	defer cancel()
	err := worker(ctx)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	job.mu.Lock()
	defer job.mu.Unlock()

	if !sm.finishCurrentLocked(job, op) {
		return
	}
	if err == nil && op.FinishedAt.After(op.Deadline) {
		err = fmt.Errorf("kill confirmed after the deadline %s", op.Deadline.Format(time.RFC3339Nano))
	}
	if err != nil {
		op.Status = pb.OperationStatus_OPERATION_STATUS_FAILED
		op.Error = err.Error()
		op.ErrorReason = pb.ErrorReason_KILL_UNCONFIRMED
		job.State = pb.JobState_JOB_STATE_FAULTED
		slog.Error("Kill unconfirmed; job is FAULTED", "jobID", job.ID, "error", err)
		return
	}
	op.Status = pb.OperationStatus_OPERATION_STATUS_COMPLETE
	op.Outcome = pb.Outcome_OUTCOME_KILLED
	job.State = pb.JobState_JOB_STATE_IDLE
	job.LastOutcome = pb.Outcome_OUTCOME_KILLED
	job.PIDs = nil
	job.Slot = ""
	job.DeviceBytes = 0
	job.HostBytesPinned = 0
}

// SeedEpoch raises a known job's last epoch to epoch if it is higher. The
// watcher calls it with the mirror pod's timeslice.io/guest-epoch
// annotation, which the VK writes before each Suspend or Resume. This keeps
// the fence after an agent restart, and refuses a late call even when it
// arrives before the call that replaced it. It never aborts a running
// operation.
func (sm *StateManager) SeedEpoch(jobID string, epoch int64) {
	sm.mu.RLock()
	job, ok := sm.jobs[jobID]
	sm.mu.RUnlock()
	if !ok {
		return
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if epoch > job.LastEpoch {
		job.LastEpoch = epoch
	}
}

// supersedeLocked fails the job's running operation with reason and msg,
// cancels its context and clears it. The operation's worker, when it
// returns, writes nothing.
func (sm *StateManager) supersedeLocked(job *Job, reason pb.ErrorReason, msg string) {
	running := job.current
	if running == nil {
		return
	}
	running.op.Status = pb.OperationStatus_OPERATION_STATUS_FAILED
	running.op.Error = msg
	running.op.ErrorReason = reason
	running.op.FinishedAt = sm.now()
	if running.cancel != nil {
		running.cancel()
	}
	job.current = nil
}

// finishCurrentLocked is called when op's worker returns. It reports
// whether op is still the job's current operation; if so, it records the
// finish time and clears it, and the caller writes the result. A
// superseded operation already carries its failure.
func (sm *StateManager) finishCurrentLocked(job *Job, op *Operation) bool {
	if job.current == nil || job.current.op != op || op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		return false
	}
	op.FinishedAt = sm.now()
	job.current = nil
	return true
}

// guestRecordLocked returns the job's last Suspend or Resume operation if
// it has not expired.
func (sm *StateManager) guestRecordLocked(job *Job) *Operation {
	rec := job.lastGuestOp
	if rec == nil {
		return nil
	}
	if _, ok := sm.operations[rec.ID]; !ok || sm.expired(rec) {
		return nil
	}
	return rec
}

// newCompletedOpLocked records an operation that completed at once.
func (sm *StateManager) newCompletedOpLocked(jobID string, opType OpType, outcome pb.Outcome) *Operation {
	now := sm.now()
	op := &Operation{
		ID:         uuid.New().String(),
		JobID:      jobID,
		Status:     pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		Type:       opType,
		StartedAt:  now,
		FinishedAt: now,
		Outcome:    outcome,
	}
	sm.operations[op.ID] = op
	return op
}

func (sm *StateManager) resumedOutcome() pb.Outcome {
	if sm.reportResumed {
		return pb.Outcome_OUTCOME_RESUMED
	}
	return pb.Outcome_OUTCOME_UNSPECIFIED
}

// expired reports whether a finished operation is past OperationTTL.
// Must be called with sm.mu held.
func (sm *StateManager) expired(op *Operation) bool {
	return op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING &&
		!op.FinishedAt.IsZero() && sm.now().Sub(op.FinishedAt) > OperationTTL
}

// gcLocked drops finished operations past OperationTTL.
// Must be called with sm.mu held for writing.
func (sm *StateManager) gcLocked() {
	for id, op := range sm.operations {
		if sm.expired(op) {
			delete(sm.operations, id)
		}
	}
}
