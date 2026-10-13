package statemachine

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"google.golang.org/grpc/codes"
)

// Host operation types: one call for every job on the node with a role.
const (
	OpTypeSuspendAll OpType = "SuspendAll"
	OpTypeResumeAll  OpType = "ResumeAll"
)

// TargetLister returns the job IDs of the pods on this node that carry the
// role label, read when it is called. The agent wires it to its node-scoped
// pod informer.
type TargetLister func(role string) []string

// WithTargetLister sets how SuspendAll and ResumeAll find their targets.
// Without it, StartHostOp refuses every call.
func WithTargetLister(lister TargetLister) Option {
	return func(sm *StateManager) {
		sm.targetLister = lister
	}
}

// TargetResult is the result for one target of a host operation.
type TargetResult struct {
	JobID string
	// Status is PENDING while the target runs, then COMPLETE or FAILED.
	Status pb.OperationStatus
	// Outcome is set on COMPLETE.
	Outcome pb.Outcome
	// ErrorReason and Error are set on FAILED.
	ErrorReason pb.ErrorReason
	Error       string
}

// hostTarget is one target of a host operation: the per-job operation it
// started or joined, or the refusal it got instead.
type hostTarget struct {
	jobID   string
	op      *Operation
	refusal *TargetResult
}

// StartHostOp starts a SuspendAll (intent OpTypeSuspend) or ResumeAll
// (intent OpTypeResume) for every job on this node whose pod carries the role
// label, and returns one operation ID to poll. deadline is absolute and
// required.
//
// Fencing is per role first, with the per-job rules: a lower epoch than the
// last one seen for the role is refused with STALE_EPOCH and no target is
// acted on; the same epoch and the same call returns the same operation ID;
// the same epoch and a different call is refused with STALE_EPOCH.
//
// The lister is called once per call, and every target gets exactly what
// StartGuestOp(target, intent, epoch, deadline, workerFor(target)) would
// give it: its own epoch fence, the answer by state, a worker. The workers
// run in parallel. A target that is refused (for example STALE_EPOCH on its
// own fence) is reported FAILED with that reason and is not acted on.
//
// The operation stays PENDING until every target has finished. It is then
// COMPLETE when every target is SUSPENDED or RELEASED (Suspend) or resumed
// (Resume), and when there was no target; otherwise FAILED, with the first
// failing target's reason. Operation.Targets has one result per target.
func (sm *StateManager) StartHostOp(
	role string, intent OpType, epoch int64, deadline time.Time, workerFor func(jobID string) GuestWorker,
) (string, error) {
	hostType, err := hostOpType(intent)
	if err != nil {
		return "", err
	}
	if role == "" {
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"%s: a role is required", hostType)
	}
	if deadline.IsZero() {
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"%s of role %s: a deadline is required", hostType, role)
	}

	if sm.targetLister == nil {
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"%s of role %s: this agent has no target lister", hostType, role)
	}
	// The targets are read now, at call time. The lister and workerFor are
	// outside code, so they run before sm.mu is taken.
	jobIDs := uniqueSorted(sm.targetLister(role))
	workers := make([]GuestWorker, len(jobIDs))
	for i, jobID := range jobIDs {
		workers[i] = workerFor(jobID)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.gcLocked()

	if sm.hostEpochs == nil {
		sm.hostEpochs = make(map[string]int64)
		sm.lastHostOps = make(map[string]*Operation)
	}
	if last := sm.hostEpochs[role]; epoch < last {
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH,
			"%s of role %s: epoch %d is lower than the last epoch %d", hostType, role, epoch, last)
	}
	if rec := sm.lastHostOps[role]; rec != nil && rec.Epoch == epoch && !sm.expired(rec) {
		if rec.Type == hostType {
			return rec.ID, nil
		}
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH,
			"%s of role %s: epoch %d was already used for %s", hostType, role, epoch, rec.Type)
	}
	// As for a per-job call, the epoch is used from here on even if the
	// call is refused below.
	sm.hostEpochs[role] = epoch
	if !deadline.After(sm.now()) {
		return "", refuse(codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE,
			"%s of role %s: the deadline %s has passed", hostType, role, deadline.Format(time.RFC3339Nano))
	}

	host := &Operation{
		ID:        uuid.New().String(),
		Status:    pb.OperationStatus_OPERATION_STATUS_PENDING,
		Type:      hostType,
		StartedAt: sm.now(),
		Epoch:     epoch,
		Deadline:  deadline,
		Role:      role,
	}
	sm.operations[host.ID] = host
	sm.lastHostOps[role] = host

	slog.Info("Host operation started", "type", hostType, "role", role, "epoch", epoch,
		"deadline", deadline.Format(time.RFC3339Nano), "targets", jobIDs)
	for i, jobID := range jobIDs {
		target := hostTarget{jobID: jobID}
		opID, err := sm.startGuestOpLocked(jobID, intent, epoch, deadline, workers[i])
		if err != nil {
			target.refusal = &TargetResult{
				JobID:       jobID,
				Status:      pb.OperationStatus_OPERATION_STATUS_FAILED,
				ErrorReason: ErrorReasonOf(err),
				Error:       err.Error(),
			}
		} else {
			target.op = sm.operations[opID]
			target.op.hostOps = append(target.op.hostOps, host)
		}
		host.targets = append(host.targets, target)
	}
	sm.finishHostOpLocked(host)
	return host.ID, nil
}

func hostOpType(intent OpType) (OpType, error) {
	switch intent {
	case OpTypeSuspend:
		return OpTypeSuspendAll, nil
	case OpTypeResume:
		return OpTypeResumeAll, nil
	default:
		return "", refuse(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED,
			"unsupported host operation %q", intent)
	}
}

func uniqueSorted(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// notifyHostOpsLocked is called when a per-job operation leaves PENDING. It
// finishes each host operation waiting on it whose targets have all
// finished. Must be called with sm.mu held for writing.
func (sm *StateManager) notifyHostOpsLocked(op *Operation) {
	if op.Status == pb.OperationStatus_OPERATION_STATUS_PENDING {
		return
	}
	for _, host := range op.hostOps {
		sm.finishHostOpLocked(host)
	}
}

// finishHostOpLocked finishes host if every target has finished. Must be
// called with sm.mu held for writing.
func (sm *StateManager) finishHostOpLocked(host *Operation) {
	if host.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		return
	}
	results := sm.targetResultsLocked(host)
	for _, r := range results {
		if r.Status == pb.OperationStatus_OPERATION_STATUS_PENDING {
			return
		}
	}

	host.Targets = results
	host.FinishedAt = sm.now()
	var failed []string
	suspended := false
	for _, r := range results {
		ok := r.Status == pb.OperationStatus_OPERATION_STATUS_COMPLETE &&
			(host.Type == OpTypeResumeAll ||
				r.Outcome == pb.Outcome_OUTCOME_SUSPENDED || r.Outcome == pb.Outcome_OUTCOME_RELEASED)
		if r.Outcome == pb.Outcome_OUTCOME_SUSPENDED {
			suspended = true
		}
		if ok {
			continue
		}
		reason := r.ErrorReason
		if len(failed) == 0 {
			host.ErrorReason = reason
		}
		failed = append(failed, fmt.Sprintf("%s: %s: %s", r.JobID, reason, r.Error))
	}

	if len(failed) > 0 {
		host.Status = pb.OperationStatus_OPERATION_STATUS_FAILED
		host.Error = fmt.Sprintf("%d of %d targets failed: %s", len(failed), len(results), strings.Join(failed, "; "))
		slog.Error("Host operation failed", "type", host.Type, "role", host.Role, "epoch", host.Epoch,
			"targets", len(results), "failed", failed)
		return
	}
	host.Status = pb.OperationStatus_OPERATION_STATUS_COMPLETE
	switch {
	case host.Type == OpTypeResumeAll:
		host.Outcome = sm.resumedOutcome()
	case suspended:
		host.Outcome = pb.Outcome_OUTCOME_SUSPENDED
	default:
		host.Outcome = pb.Outcome_OUTCOME_RELEASED
	}
	slog.Info("Host operation complete", "type", host.Type, "role", host.Role, "epoch", host.Epoch,
		"targets", len(results), "outcome", host.Outcome)
}

// targetResultsLocked returns a fresh copy of a host operation's per-target
// results, live while it runs. Nil for other operations. Must be called with
// sm.mu held.
func (sm *StateManager) targetResultsLocked(op *Operation) []TargetResult {
	if op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		if op.Targets == nil {
			return nil
		}
		return append([]TargetResult{}, op.Targets...)
	}
	if op.targets == nil {
		return nil
	}
	results := make([]TargetResult, 0, len(op.targets))
	for _, t := range op.targets {
		if t.refusal != nil {
			results = append(results, *t.refusal)
			continue
		}
		results = append(results, TargetResult{
			JobID:       t.jobID,
			Status:      t.op.Status,
			Outcome:     t.op.Outcome,
			ErrorReason: t.op.ErrorReason,
			Error:       t.op.Error,
		})
	}
	return results
}
