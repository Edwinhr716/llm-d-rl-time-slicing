package statemachine_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
)

const hostRole = "background"

// fakeHost is the pods on one node: which jobs carry the role label now.
type fakeHost struct {
	mu    sync.Mutex
	jobs  map[string][]string
	calls atomic.Int32
}

func newFakeHost() *fakeHost {
	return &fakeHost{jobs: make(map[string][]string)}
}

func (h *fakeHost) set(role string, jobIDs ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.jobs[role] = jobIDs
}

func (h *fakeHost) list(role string) []string {
	h.calls.Add(1)
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.jobs[role]...)
}

// newHostSM returns a StateManager listing targets from host, with each job
// registered RUNNING.
func newHostSM(host *fakeHost, jobIDs ...string) *statemachine.StateManager {
	sm := statemachine.NewStateManager(statemachine.WithTargetLister(host.list))
	for _, id := range jobIDs {
		addJob(sm, id, pb.JobState_JOB_STATE_RUNNING)
	}
	return sm
}

func addJob(sm *statemachine.StateManager, jobID string, state pb.JobState) {
	sm.InternalMu().Lock()
	defer sm.InternalMu().Unlock()
	job := sm.InternalGetOrCreateJob(jobID, guestGroup)
	job.State = state
	job.PIDs = []int{42}
}

func jobState(t *testing.T, sm *statemachine.StateManager, jobID string) pb.JobState {
	t.Helper()
	for _, st := range sm.GetJobStatus() {
		if st.GetJobId() == jobID {
			return st.GetState()
		}
	}
	t.Fatalf("job %s not found", jobID)
	return pb.JobState_JOB_STATE_UNSPECIFIED
}

// workers hands every target the same worker and records which targets got one.
type workers struct {
	mu     sync.Mutex
	called []string
	worker statemachine.GuestWorker
}

func (w *workers) forJob(jobID string) statemachine.GuestWorker {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.called = append(w.called, jobID)
	return w.worker
}

func perJob(m map[string]statemachine.GuestWorker) func(string) statemachine.GuestWorker {
	return func(jobID string) statemachine.GuestWorker { return m[jobID] }
}

func startHost(
	t *testing.T, sm *statemachine.StateManager, intent statemachine.OpType, epoch int64,
	workerFor func(string) statemachine.GuestWorker,
) string {
	t.Helper()
	opID, err := sm.StartHostOp(hostRole, intent, epoch, future(), workerFor)
	if err != nil {
		t.Fatalf("host %s epoch %d: unexpected error: %v", intent, epoch, err)
	}
	return opID
}

func targetsByJob(op *statemachine.Operation) map[string]statemachine.TargetResult {
	out := make(map[string]statemachine.TargetResult, len(op.Targets))
	for _, r := range op.Targets {
		out[r.JobID] = r
	}
	return out
}

func checkTarget(
	t *testing.T, op *statemachine.Operation, jobID string, status pb.OperationStatus, outcome pb.Outcome, reason pb.ErrorReason,
) {
	t.Helper()
	r, ok := targetsByJob(op)[jobID]
	if !ok {
		t.Fatalf("operation %s: no result for target %s (targets %+v)", op.ID, jobID, op.Targets)
	}
	if r.Status != status || r.Outcome != outcome || r.ErrorReason != reason {
		t.Errorf("target %s: expected %s/%s/%s, got %s/%s/%s (%s)",
			jobID, status, outcome, reason, r.Status, r.Outcome, r.ErrorReason, r.Error)
	}
}

func TestHostOp_SuspendAllComplete(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-b", "job-a", "job-a", "")
	host.set("foreground", "trainer")
	sm := newHostSM(host, "job-a", "job-b", "trainer")
	recorder := &workers{worker: instant(suspendedResult, nil)}

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, recorder.forJob)
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	if op.Type != statemachine.OpTypeSuspendAll || op.Role != hostRole || op.Epoch != 1 {
		t.Errorf("unexpected host operation fields: type %s role %q epoch %d", op.Type, op.Role, op.Epoch)
	}
	if len(op.Targets) != 2 || op.Targets[0].JobID != "job-a" || op.Targets[1].JobID != "job-b" {
		t.Fatalf("expected targets job-a, job-b (deduped, sorted), got %+v", op.Targets)
	}
	for _, id := range []string{"job-a", "job-b"} {
		checkTarget(t, op, id, pb.OperationStatus_OPERATION_STATUS_COMPLETE,
			pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		if got := jobState(t, sm, id); got != pb.JobState_JOB_STATE_SUSPENDED {
			t.Errorf("job %s: expected SUSPENDED, got %s", id, got)
		}
	}
	if got := jobState(t, sm, "trainer"); got != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("a job of another role was acted on: %s", got)
	}
	if len(recorder.called) != 2 {
		t.Errorf("expected one worker per target, got %v", recorder.called)
	}
}

// TestHostOp_ListsTargetsAtCallTime covers freshness: a guest labelled just
// before the call is a target, one labelled after it is not.
func TestHostOp_ListsTargetsAtCallTime(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a")
	sm := newHostSM(host, "job-a", "job-fresh", "job-late")
	host.set(hostRole, "job-a", "job-fresh")

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, func(string) statemachine.GuestWorker {
		return instant(suspendedResult, nil)
	})
	if n := host.calls.Load(); n != 1 {
		t.Errorf("expected the lister to be called once, got %d", n)
	}
	host.set(hostRole, "job-a", "job-fresh", "job-late")
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	if len(op.Targets) != 2 {
		t.Errorf("expected targets job-a and job-fresh, got %+v", op.Targets)
	}
	checkTarget(t, op, "job-fresh", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if got := jobState(t, sm, "job-late"); got != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("a job labelled after the call was acted on: %s", got)
	}
	if n := host.calls.Load(); n != 1 {
		t.Errorf("GetOperation must not list again: %d lister calls", n)
	}
}

// TestHostOp_PerTargetFailure covers attribution: the failing target is
// named with its own reason, the others keep their results.
func TestHostOp_PerTargetFailure(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-b", "job-c")
	sm := newHostSM(host, "job-a", "job-b", "job-c")
	memErr := statemachine.NewOpError(pb.ErrorReason_PRECONDITION_MEMORY, errors.New("not enough host memory"))

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, perJob(map[string]statemachine.GuestWorker{
		"job-a": instant(suspendedResult, nil),
		"job-b": instant(statemachine.GuestResult{}, memErr),
		"job-c": instant(statemachine.GuestResult{Outcome: pb.Outcome_OUTCOME_RELEASED}, nil),
	}))
	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, pb.ErrorReason_PRECONDITION_MEMORY)
	if !strings.Contains(op.Error, "1 of 3 targets failed") || !strings.Contains(op.Error, "job-b") {
		t.Errorf("host error does not name the failing target: %q", op.Error)
	}
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-b", pb.OperationStatus_OPERATION_STATUS_FAILED,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_PRECONDITION_MEMORY)
	checkTarget(t, op, "job-c", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_RELEASED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if r := targetsByJob(op)["job-b"]; !strings.Contains(r.Error, "not enough host memory") {
		t.Errorf("target error lost: %q", r.Error)
	}
}

// TestHostOp_PendingUntilEveryTargetFinishes checks the live per-target view
// and that targets run in parallel.
func TestHostOp_PendingUntilEveryTargetFinishes(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-b")
	sm := newHostSM(host, "job-a", "job-b")
	stubA := newGuestStub(suspendedResult)
	stubB := newGuestStub(suspendedResult)

	start := time.Now()
	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, perJob(map[string]statemachine.GuestWorker{
		"job-a": stubA.run, "job-b": stubB.run,
	}))
	if d := time.Since(start); d > time.Second {
		t.Errorf("StartHostOp must ack at once, took %s", d)
	}
	waitStarted(t, stubA)
	waitStarted(t, stubB) // both run before either is released

	op := getOp(t, sm, opID)
	if op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		t.Fatalf("expected PENDING, got %s", op.Status)
	}
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_PENDING,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	close(stubA.release)
	waitReturned(t, stubA)
	op = getOp(t, sm, opID)
	if op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		t.Fatalf("expected PENDING while job-b runs, got %s", op.Status)
	}
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-b", pb.OperationStatus_OPERATION_STATUS_PENDING,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	// The copy returned by GetOperation is not shared with the manager.
	op.Targets[0].JobID = "changed"
	if getOp(t, sm, opID).Targets[0].JobID != "job-a" {
		t.Error("GetOperation returned shared target results")
	}

	close(stubB.release)
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
}

func TestHostOp_NoTargets(t *testing.T) {
	sm := newHostSM(newFakeHost())
	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, func(string) statemachine.GuestWorker {
		t.Error("no worker must be asked for")
		return nil
	})
	op := getOp(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_RELEASED)
	if len(op.Targets) != 0 || op.FinishedAt.IsZero() {
		t.Errorf("expected a finished operation with no targets, got %+v", op)
	}
}

// TestHostOp_StaleHostEpoch is M4: after SuspendAll at epoch 5, a late
// ResumeAll at epoch 4 acts on no guest; ResumeAll at epoch 6 resumes all.
func TestHostOp_StaleHostEpoch(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-b")
	sm := newHostSM(host, "job-a", "job-b")

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 5, func(string) statemachine.GuestWorker {
		return instant(suspendedResult, nil)
	})
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)

	// A guest labelled after the vacate never saw epoch 5; the host fence
	// still refuses the late call before any target is touched.
	addJob(sm, "job-new", pb.JobState_JOB_STATE_SUSPENDED)
	host.set(hostRole, "job-a", "job-b", "job-new")
	_, err := sm.StartHostOp(hostRole, statemachine.OpTypeResume, 4, future(), func(string) statemachine.GuestWorker {
		return mustNotRun(t)
	})
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	for _, id := range []string{"job-a", "job-b"} {
		if got := jobState(t, sm, id); got != pb.JobState_JOB_STATE_SUSPENDED {
			t.Errorf("stale resume acted on %s: %s", id, got)
		}
	}

	resumeID := startHost(t, sm, statemachine.OpTypeResume, 6, func(string) statemachine.GuestWorker {
		return instant(statemachine.GuestResult{}, nil)
	})
	op := waitForOperation(t, sm, resumeID)
	checkComplete(t, op, pb.Outcome_OUTCOME_RESUMED)
	if op.Type != statemachine.OpTypeResumeAll || len(op.Targets) != 3 {
		t.Errorf("expected ResumeAll over 3 targets, got %s %+v", op.Type, op.Targets)
	}
	for _, id := range []string{"job-a", "job-b", "job-new"} {
		if got := jobState(t, sm, id); got != pb.JobState_JOB_STATE_RUNNING {
			t.Errorf("job %s: expected RUNNING, got %s", id, got)
		}
	}
}

func TestHostOp_SameEpoch(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a")
	sm := newHostSM(host, "job-a")
	stub := newGuestStub(suspendedResult)

	first := startHost(t, sm, statemachine.OpTypeSuspend, 3, func(string) statemachine.GuestWorker { return stub.run })
	waitStarted(t, stub)
	again := startHost(t, sm, statemachine.OpTypeSuspend, 3, func(string) statemachine.GuestWorker { return mustNotRun(t) })
	if again != first {
		t.Errorf("same epoch and same call: expected operation %s, got %s", first, again)
	}
	_, err := sm.StartHostOp(hostRole, statemachine.OpTypeResume, 3, future(), func(string) statemachine.GuestWorker {
		return mustNotRun(t)
	})
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)

	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, first), pb.Outcome_OUTCOME_SUSPENDED)
	if n := stub.calls.Load(); n != 1 {
		t.Errorf("expected one worker run, got %d", n)
	}
}

// TestHostOp_StaleTarget: a target whose own epoch is higher is refused with
// STALE_EPOCH and not acted on; the other targets proceed.
func TestHostOp_StaleTarget(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-b")
	sm := newHostSM(host, "job-a", "job-b")
	sm.SeedEpoch("job-b", 9)

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 2, perJob(map[string]statemachine.GuestWorker{
		"job-a": instant(suspendedResult, nil),
		"job-b": mustNotRun(t),
	}))
	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, pb.ErrorReason_STALE_EPOCH)
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-b", pb.OperationStatus_OPERATION_STATUS_FAILED,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_STALE_EPOCH)
	if got := jobState(t, sm, "job-b"); got != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("stale target was acted on: %s", got)
	}
}

// TestHostOp_UnknownTarget: a labelled pod the agent does not know yet has
// nothing on the accelerator, so Suspend reports it RELEASED; Resume cannot
// resume it and reports it FAILED.
func TestHostOp_UnknownTarget(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-gone")
	sm := newHostSM(host, "job-a")

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, func(string) statemachine.GuestWorker {
		return instant(suspendedResult, nil)
	})
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	checkTarget(t, op, "job-gone", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_RELEASED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

	resumeID := startHost(t, sm, statemachine.OpTypeResume, 2, func(string) statemachine.GuestWorker {
		return instant(statemachine.GuestResult{}, nil)
	})
	op = waitForOperation(t, sm, resumeID)
	checkFailed(t, op, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_RESUMED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	checkTarget(t, op, "job-gone", pb.OperationStatus_OPERATION_STATUS_FAILED,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
}

func TestHostOp_TargetDeadlineExceeded(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-hung")
	sm := newHostSM(host, "job-a", "job-hung")
	hung := func(ctx context.Context) (statemachine.GuestResult, error) {
		<-ctx.Done()
		return statemachine.GuestResult{}, ctx.Err()
	}

	opID, err := sm.StartHostOp(hostRole, statemachine.OpTypeSuspend, 1, time.Now().Add(200*time.Millisecond),
		perJob(map[string]statemachine.GuestWorker{"job-a": instant(suspendedResult, nil), "job-hung": hung}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, pb.ErrorReason_DEADLINE_EXCEEDED)
	checkTarget(t, op, "job-hung", pb.OperationStatus_OPERATION_STATUS_FAILED,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_DEADLINE_EXCEEDED)
	checkTarget(t, op, "job-a", pb.OperationStatus_OPERATION_STATUS_COMPLETE,
		pb.Outcome_OUTCOME_SUSPENDED, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	if got := jobState(t, sm, "job-hung"); got != pb.JobState_JOB_STATE_FAULTED {
		t.Errorf("hung target: expected FAULTED, got %s", got)
	}
}

// TestHostOp_KillSupersedesTarget: Kill stays per job and is the fallback for
// a hung target; the host operation finishes when the target is superseded.
func TestHostOp_KillSupersedesTarget(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a", "job-hung")
	sm := newHostSM(host, "job-a", "job-hung")
	hung := newGuestStub(suspendedResult)
	killStub := newGuestStub(statemachine.GuestResult{})

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 1, perJob(map[string]statemachine.GuestWorker{
		"job-a": instant(suspendedResult, nil), "job-hung": hung.run,
	}))
	hungCtx := waitStarted(t, hung)
	if _, err := sm.StartKill("job-hung", future(), "T reached", killStub.kill); err != nil {
		t.Fatalf("Kill: unexpected error: %v", err)
	}
	waitStarted(t, killStub)

	op := waitForOperation(t, sm, opID)
	checkFailed(t, op, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	r := targetsByJob(op)["job-hung"]
	if r.Status != pb.OperationStatus_OPERATION_STATUS_FAILED || !strings.Contains(r.Error, "superseded by Kill") {
		t.Errorf("unexpected result for the killed target: %+v", r)
	}
	if !errors.Is(hungCtx.Err(), context.Canceled) {
		t.Errorf("superseded target's context: expected Canceled, got %v", hungCtx.Err())
	}
	close(hung.release)
	close(killStub.release)
	waitReturned(t, hung)
	if got := getOp(t, sm, opID); got.Status != pb.OperationStatus_OPERATION_STATUS_FAILED {
		t.Errorf("host operation changed after it finished: %s", got.Status)
	}
}

// TestHostOp_HigherEpochAborts: a higher-epoch host call aborts the running
// targets of the older one, like a per-job call.
func TestHostOp_HigherEpochAborts(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, "job-a")
	sm := newHostSM(host, "job-a")
	suspendStub := newGuestStub(suspendedResult)

	suspendID := startHost(t, sm, statemachine.OpTypeSuspend, 3, func(string) statemachine.GuestWorker { return suspendStub.run })
	suspendCtx := waitStarted(t, suspendStub)
	resumeID := startHost(t, sm, statemachine.OpTypeResume, 4, func(string) statemachine.GuestWorker {
		return instant(statemachine.GuestResult{}, nil)
	})

	aborted := getOp(t, sm, suspendID)
	checkFailed(t, aborted, pb.ErrorReason_STALE_EPOCH)
	checkTarget(t, aborted, "job-a", pb.OperationStatus_OPERATION_STATUS_FAILED,
		pb.Outcome_OUTCOME_UNSPECIFIED, pb.ErrorReason_STALE_EPOCH)
	if !errors.Is(suspendCtx.Err(), context.Canceled) {
		t.Errorf("aborted target's context: expected Canceled, got %v", suspendCtx.Err())
	}
	checkComplete(t, waitForOperation(t, sm, resumeID), pb.Outcome_OUTCOME_RESUMED)
	close(suspendStub.release)
	waitReturned(t, suspendStub)
	if got := jobState(t, sm, "job-a"); got != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("expected RUNNING, got %s", got)
	}
}

// TestHostOp_SharesPerJobFence: a per-job call and a host call use the same
// per-job epoch.
func TestHostOp_SharesPerJobFence(t *testing.T) {
	host := newFakeHost()
	host.set(hostRole, guestJob)
	sm := newHostSM(host, guestJob)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 7, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)

	opID := startHost(t, sm, statemachine.OpTypeSuspend, 7, func(string) statemachine.GuestWorker { return mustNotRun(t) })
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	if len(op.Targets) != 1 {
		t.Fatalf("expected one target, got %+v", op.Targets)
	}
}

func TestHostOp_Validation(t *testing.T) {
	t.Run("no lister", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		_, err := sm.StartHostOp(hostRole, statemachine.OpTypeSuspend, 1, future(), func(string) statemachine.GuestWorker {
			return mustNotRun(t)
		})
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})

	host := newFakeHost()
	host.set(hostRole, "job-a")
	refused := func(
		t *testing.T, role string, intent statemachine.OpType, deadline time.Time, code codes.Code, reason pb.ErrorReason,
	) {
		t.Helper()
		sm := newHostSM(host, "job-a")
		_, err := sm.StartHostOp(role, intent, 1, deadline, func(string) statemachine.GuestWorker {
			return mustNotRun(t)
		})
		requireRefusal(t, err, code, reason)
		if n := len(sm.InternalOperations()); n != 0 {
			t.Errorf("refused call created %d operations", n)
		}
		if got := jobState(t, sm, "job-a"); got != pb.JobState_JOB_STATE_RUNNING {
			t.Errorf("refused call acted on a target: %s", got)
		}
	}

	t.Run("missing deadline", func(t *testing.T) {
		refused(t, hostRole, statemachine.OpTypeSuspend, time.Time{},
			codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})
	t.Run("unsupported intent", func(t *testing.T) {
		refused(t, hostRole, statemachine.OpTypeSnapshot, future(),
			codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})
	t.Run("missing role", func(t *testing.T) {
		refused(t, "", statemachine.OpTypeSuspend, future(),
			codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})
	t.Run("past deadline", func(t *testing.T) {
		refused(t, hostRole, statemachine.OpTypeSuspend, time.Now().Add(-time.Second),
			codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE)
	})
}
