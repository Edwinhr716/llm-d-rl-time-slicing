package statemachine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
)

// TestHostOp_RemoveJobFinishesTarget: when a target's last pod is deleted
// while a SuspendAll runs (the watcher's DeleteFunc calls RemoveJob), the
// target is aborted and reported FAILED, the other targets are untouched,
// and the host operation finishes instead of waiting on a job that is gone.
func TestHostOp_RemoveJobFinishesTarget(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-gone")
	sm := newHostSM(host, "job-a", "job-gone")
	hung := newGuestStub(suspendedResult)

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, perJob(map[string]statemachine.GuestWorker{
		"job-a": instant(suspendedResult, nil), "job-gone": hung.run,
	}))
	hungCtx := waitStarted(t, hung)
	if got := getOp(t, sm, opID).Status; got != pb.OperationStatus_OPERATION_STATUS_PENDING {
		t.Fatalf("host operation finished before its hung target: %s", got)
	}

	sm.RemoveJob("job-gone")

	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if r := targetsByJob(op)["job-gone"]; r.Status != pb.OperationStatus_OPERATION_STATUS_FAILED ||
		!strings.Contains(r.Error, "pod is gone") {
		t.Errorf("unexpected result for the removed target: %+v", r)
	}
	if !errors.Is(hungCtx.Err(), context.Canceled) {
		t.Errorf("removed target's context: expected Canceled, got %v", hungCtx.Err())
	}
	close(hung.release)
	waitReturned(t, hung)
	for _, st := range sm.GetJobStatus() {
		if st.GetJobId() == "job-gone" {
			t.Fatalf("a returning target worker re-created the removed job: %v", st)
		}
	}
}

// TestHostOp_KilledTargetIsReleased: a guest killed after one SuspendAll is
// still listed while its pod lingers. The next SuspendAll reports it
// RELEASED at once, without a worker, and completes.
func TestHostOp_KilledTargetIsReleased(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-killed")
	sm := newHostSM(host, "job-a", "job-killed")

	killID, err := sm.StartKill("job-killed", future(), "T reached", func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("Kill: unexpected error: %v", err)
	}
	checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)

	recorder := &workers{worker: instant(suspendedResult, nil)}
	op := waitForOperation(t, sm, startHost(t, sm, statemachine.OpTypeSuspend, 2, recorder.forJob))
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	checkTarget(t, op, "job-killed", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_RELEASED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if got := jobState(t, sm, "job-killed"); got != pb.JobState_JOB_STATE_IDLE {
		t.Errorf("killed target: expected IDLE, got %s", got)
	}
}
