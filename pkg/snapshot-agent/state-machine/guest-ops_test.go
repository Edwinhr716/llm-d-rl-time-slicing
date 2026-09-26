package statemachine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	statemachine "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	guestJob   = "guest-job"
	guestGroup = "group-1"
	waitFor    = 2 * time.Second
)

var suspendedResult = statemachine.GuestResult{
	Outcome:         pb.Outcome_OUTCOME_SUSPENDED,
	DeviceBytes:     22 << 30,
	HostBytesPinned: 23 << 30,
	StorageBytes:    21 << 30,
}

// guestStub is a Suspend, Resume or Kill worker that blocks until released.
type guestStub struct {
	res      statemachine.GuestResult
	started  chan context.Context
	release  chan struct{}
	returned chan struct{}
	calls    atomic.Int32
}

func newGuestStub(res statemachine.GuestResult) *guestStub {
	return &guestStub{
		res:      res,
		started:  make(chan context.Context, 16),
		release:  make(chan struct{}),
		returned: make(chan struct{}, 16),
	}
}

func (g *guestStub) run(ctx context.Context) (statemachine.GuestResult, error) {
	g.calls.Add(1)
	g.started <- ctx
	<-g.release
	g.returned <- struct{}{}
	return g.res, nil
}

func (g *guestStub) kill(ctx context.Context) error {
	_, err := g.run(ctx)
	return err
}

func instant(res statemachine.GuestResult, err error) statemachine.GuestWorker {
	return func(context.Context) (statemachine.GuestResult, error) {
		return res, err
	}
}

func mustNotRun(t *testing.T) statemachine.GuestWorker {
	t.Helper()
	return func(context.Context) (statemachine.GuestResult, error) {
		t.Error("worker must not run")
		return statemachine.GuestResult{}, nil
	}
}

// fakeClock is a settable clock, safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(sm *statemachine.StateManager) *fakeClock {
	clk := &fakeClock{now: time.Now()}
	sm.InternalSetClock(clk.Now)
	return clk
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func future() time.Time {
	return time.Now().Add(time.Minute)
}

// newGuestSM returns a StateManager with guestJob registered in state.
func newGuestSM(t *testing.T, state pb.JobState, opts ...statemachine.Option) *statemachine.StateManager {
	t.Helper()
	sm := statemachine.NewStateManager(opts...)
	setJob(t, sm, state, pb.Outcome_OUTCOME_UNSPECIFIED)
	return sm
}

func setJob(t *testing.T, sm *statemachine.StateManager, state pb.JobState, outcome pb.Outcome) {
	t.Helper()
	sm.InternalMu().Lock()
	defer sm.InternalMu().Unlock()
	job := sm.InternalGetOrCreateJob(guestJob, guestGroup)
	job.State = state
	job.LastOutcome = outcome
	job.PIDs = []int{42}
}

func startGuest(
	t *testing.T, sm *statemachine.StateManager, intent statemachine.OpType, epoch int64, worker statemachine.GuestWorker,
) string {
	t.Helper()
	opID, err := sm.StartGuestOp(guestJob, intent, epoch, future(), worker)
	if err != nil {
		t.Fatalf("%s epoch %d: unexpected error: %v", intent, epoch, err)
	}
	return opID
}

func startKill(t *testing.T, sm *statemachine.StateManager, worker statemachine.KillWorker) string {
	t.Helper()
	opID, err := sm.StartKill(guestJob, future(), "T reached", worker)
	if err != nil {
		t.Fatalf("Kill: unexpected error: %v", err)
	}
	return opID
}

func requireRefusal(t *testing.T, err error, code codes.Code, reason pb.ErrorReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a %s refusal (%s), got nil", code, reason)
	}
	if got := status.Code(err); got != code {
		t.Errorf("expected code %s, got %s (%v)", code, got, err)
	}
	if got := statemachine.ErrorReasonOf(err); got != reason {
		t.Errorf("expected reason %s, got %s (%v)", reason, got, err)
	}
	if reason != pb.ErrorReason_ERROR_REASON_UNSPECIFIED &&
		!strings.HasPrefix(status.Convert(err).Message(), reason.String()+": ") {
		t.Errorf("status message %q does not start with %s", status.Convert(err).Message(), reason)
	}
}

func jobStatus(t *testing.T, sm *statemachine.StateManager) *pb.JobStatus {
	t.Helper()
	for _, st := range sm.GetJobStatus() {
		if st.GetJobId() == guestJob {
			return st
		}
	}
	t.Fatalf("job %s not found", guestJob)
	return nil
}

func waitStarted(t *testing.T, g *guestStub) context.Context {
	t.Helper()
	select {
	case ctx := <-g.started:
		return ctx
	case <-time.After(waitFor):
		t.Fatal("worker did not start")
		return nil
	}
}

func waitReturned(t *testing.T, g *guestStub) {
	t.Helper()
	select {
	case <-g.returned:
	case <-time.After(waitFor):
		t.Fatal("worker did not return")
	}
	// Let the operation goroutine take the locks after the worker returns.
	time.Sleep(50 * time.Millisecond)
}

func getOp(t *testing.T, sm *statemachine.StateManager, opID string) *statemachine.Operation {
	t.Helper()
	op, ok := sm.GetOperation(opID)
	if !ok {
		t.Fatalf("operation %s not found", opID)
	}
	return op
}

func checkComplete(t *testing.T, op *statemachine.Operation, outcome pb.Outcome) {
	t.Helper()
	if op.Status != pb.OperationStatus_OPERATION_STATUS_COMPLETE {
		t.Errorf("operation %s (%s): expected COMPLETE, got %s (%s: %s)", op.ID, op.Type, op.Status, op.ErrorReason, op.Error)
	}
	if op.Outcome != outcome {
		t.Errorf("operation %s (%s): expected outcome %s, got %s", op.ID, op.Type, outcome, op.Outcome)
	}
}

func checkFailed(t *testing.T, op *statemachine.Operation, reason pb.ErrorReason) {
	t.Helper()
	if op.Status != pb.OperationStatus_OPERATION_STATUS_FAILED {
		t.Errorf("operation %s (%s): expected FAILED, got %s", op.ID, op.Type, op.Status)
	}
	if op.ErrorReason != reason {
		t.Errorf("operation %s (%s): expected reason %s, got %s (%s)", op.ID, op.Type, reason, op.ErrorReason, op.Error)
	}
	if op.FinishedAt.IsZero() {
		t.Errorf("operation %s (%s): FinishedAt not set", op.ID, op.Type)
	}
}

// TestGuestOp_StaleEpochRejected is the late-call case: Suspend with epoch 6
// has run, then the Resume with epoch 5 that it replaced arrives. The Resume
// is refused with STALE_EPOCH, never reaches the worker, and the guest stays
// suspended.
func TestGuestOp_StaleEpochRejected(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 6, instant(suspendedResult, nil))
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)

	opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 5, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	if opID != "" {
		t.Errorf("a refused call must not return an operation ID, got %q", opID)
	}
	var refusal *statemachine.RefusalError
	if !errors.As(fmt.Errorf("wrapped: %w", err), &refusal) {
		t.Errorf("expected a RefusalError, got %T", err)
	}

	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_SUSPENDED || st.GetEpoch() != 6 {
		t.Errorf("expected SUSPENDED at epoch 6, got %s at epoch %d", st.GetState(), st.GetEpoch())
	}
	checkComplete(t, getOp(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)

	// The next real call, with a higher epoch, goes through.
	resumeID := startGuest(t, sm, statemachine.OpTypeResume, 7, instant(statemachine.GuestResult{}, nil))
	checkComplete(t, waitForOperation(t, sm, resumeID), pb.Outcome_OUTCOME_RESUMED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_RUNNING)
}

func TestGuestOp_StaleEpochWhileRunning(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 6, stub.run)
	ctx := waitStarted(t, stub)

	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 5, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	if ctx.Err() != nil {
		t.Errorf("a stale call must not cancel the running operation: %v", ctx.Err())
	}
	if op := getOp(t, sm, suspendID); op.Status != pb.OperationStatus_OPERATION_STATUS_PENDING {
		t.Errorf("running operation changed to %s", op.Status)
	}

	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
}

func TestGuestOp_SameEpochSameCallReturnsSameOperation(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)

	first := startGuest(t, sm, statemachine.OpTypeSuspend, 3, stub.run)
	waitStarted(t, stub)

	if again := startGuest(t, sm, statemachine.OpTypeSuspend, 3, stub.run); again != first {
		t.Errorf("re-issue while running: expected %s, got %s", first, again)
	}

	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, first), pb.Outcome_OUTCOME_SUSPENDED)

	if again := startGuest(t, sm, statemachine.OpTypeSuspend, 3, stub.run); again != first {
		t.Errorf("re-issue after completion: expected %s, got %s", first, again)
	}
	if calls := stub.calls.Load(); calls != 1 {
		t.Errorf("expected the worker to run once, ran %d times", calls)
	}
}

func TestGuestOp_SameEpochConcurrentCallsShareOperation(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)

	const callers = 16
	ids := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			ids[i], errs[i] = sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 7, future(), stub.run)
		})
	}
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: unexpected error: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Errorf("caller %d: expected %s, got %s", i, ids[0], ids[i])
		}
	}
	waitStarted(t, stub)
	close(stub.release)
	waitForOperation(t, sm, ids[0])
	if calls := stub.calls.Load(); calls != 1 {
		t.Errorf("expected the worker to run once, ran %d times", calls)
	}
}

func TestGuestOp_SameEpochDifferentCallIsStale(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	stub := newGuestStub(suspendedResult)

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 3, stub.run)
	waitStarted(t, stub)

	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 3, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)

	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)

	_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 3, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SUSPENDED)
}

func TestGuestOp_HigherEpochAbortsRunningOperation(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	suspendStub := newGuestStub(suspendedResult)
	resumeStub := newGuestStub(statemachine.GuestResult{HostBytesPinned: 1 << 20})

	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 3, suspendStub.run)
	suspendCtx := waitStarted(t, suspendStub)

	resumeID := startGuest(t, sm, statemachine.OpTypeResume, 4, resumeStub.run)
	if resumeID == suspendID {
		t.Fatal("a higher epoch must start a new operation")
	}
	waitStarted(t, resumeStub)

	aborted := getOp(t, sm, suspendID)
	checkFailed(t, aborted, pb.ErrorReason_STALE_EPOCH)
	if !strings.Contains(aborted.Error, "aborted by Resume with epoch 4") {
		t.Errorf("unexpected abort message %q", aborted.Error)
	}
	if !errors.Is(suspendCtx.Err(), context.Canceled) {
		t.Errorf("aborted operation's context: expected Canceled, got %v", suspendCtx.Err())
	}

	// The aborted worker returning success afterwards writes nothing.
	close(suspendStub.release)
	waitReturned(t, suspendStub)
	checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_TRANSITIONING)
	checkFailed(t, getOp(t, sm, suspendID), pb.ErrorReason_STALE_EPOCH)

	close(resumeStub.release)
	checkComplete(t, waitForOperation(t, sm, resumeID), pb.Outcome_OUTCOME_RESUMED)
	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_RUNNING || st.GetLastOutcome() != pb.Outcome_OUTCOME_RESUMED {
		t.Errorf("expected RUNNING/RESUMED, got %s/%s", st.GetState(), st.GetLastOutcome())
	}
	if st.GetEpoch() != 4 {
		t.Errorf("expected epoch 4, got %d", st.GetEpoch())
	}
}

func TestSeedEpoch(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED)

	sm.SeedEpoch(guestJob, 10)
	_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 9, future(), mustNotRun(t))
	requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_STALE_EPOCH)

	sm.SeedEpoch(guestJob, 5)
	if got := jobStatus(t, sm).GetEpoch(); got != 10 {
		t.Errorf("SeedEpoch must never lower the epoch: got %d", got)
	}

	// The call that the annotation announced carries the same epoch.
	resumeID := startGuest(t, sm, statemachine.OpTypeResume, 10, instant(statemachine.GuestResult{}, nil))
	checkComplete(t, waitForOperation(t, sm, resumeID), pb.Outcome_OUTCOME_RESUMED)

	// SeedEpoch never aborts a running operation.
	stub := newGuestStub(suspendedResult)
	suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 11, stub.run)
	ctx := waitStarted(t, stub)
	sm.SeedEpoch(guestJob, 12)
	if ctx.Err() != nil {
		t.Errorf("SeedEpoch cancelled the running operation: %v", ctx.Err())
	}
	close(stub.release)
	checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
	if got := jobStatus(t, sm).GetEpoch(); got != 12 {
		t.Errorf("expected epoch 12, got %d", got)
	}

	// Unknown jobs are ignored and not created.
	sm.SeedEpoch("unknown-job", 3)
	if n := len(sm.GetJobStatus()); n != 1 {
		t.Errorf("SeedEpoch created a job: %d jobs", n)
	}
}

func TestGuestOp_StateTable(t *testing.T) {
	const (
		ranWorker = iota
		completedAtOnce
		refused
	)
	unknown := pb.JobState_JOB_STATE_UNSPECIFIED // the job is not registered
	tests := []struct {
		name        string
		state       pb.JobState
		lastOutcome pb.Outcome
		intent      statemachine.OpType
		want        int
		wantOutcome pb.Outcome
		wantState   pb.JobState
	}{
		{
			"suspend RUNNING", pb.JobState_JOB_STATE_RUNNING, 0, statemachine.OpTypeSuspend, ranWorker,
			pb.Outcome_OUTCOME_SUSPENDED, pb.JobState_JOB_STATE_SUSPENDED,
		},
		{
			"suspend SAVED", pb.JobState_JOB_STATE_SAVED, 0, statemachine.OpTypeSuspend, ranWorker,
			pb.Outcome_OUTCOME_SUSPENDED, pb.JobState_JOB_STATE_SUSPENDED,
		},
		{
			"suspend SUSPENDED", pb.JobState_JOB_STATE_SUSPENDED, 0, statemachine.OpTypeSuspend, ranWorker,
			pb.Outcome_OUTCOME_SUSPENDED, pb.JobState_JOB_STATE_SUSPENDED,
		},
		{
			"suspend IDLE", pb.JobState_JOB_STATE_IDLE, 0, statemachine.OpTypeSuspend, ranWorker,
			pb.Outcome_OUTCOME_SUSPENDED, pb.JobState_JOB_STATE_SUSPENDED,
		},
		{
			"suspend IDLE after kill", pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_KILLED, statemachine.OpTypeSuspend,
			completedAtOnce, pb.Outcome_OUTCOME_RELEASED, pb.JobState_JOB_STATE_IDLE,
		},
		{
			"suspend unknown job", unknown, 0, statemachine.OpTypeSuspend, completedAtOnce,
			pb.Outcome_OUTCOME_RELEASED, unknown,
		},
		{
			"suspend FAULTED", pb.JobState_JOB_STATE_FAULTED, 0, statemachine.OpTypeSuspend, refused,
			0, pb.JobState_JOB_STATE_FAULTED,
		},
		{
			"suspend TRANSITIONING without operation", pb.JobState_JOB_STATE_TRANSITIONING, 0, statemachine.OpTypeSuspend,
			refused, 0, pb.JobState_JOB_STATE_TRANSITIONING,
		},
		{
			"resume RUNNING", pb.JobState_JOB_STATE_RUNNING, 0, statemachine.OpTypeResume, completedAtOnce,
			pb.Outcome_OUTCOME_RESUMED, pb.JobState_JOB_STATE_RUNNING,
		},
		{
			"resume SAVED", pb.JobState_JOB_STATE_SAVED, 0, statemachine.OpTypeResume, ranWorker,
			pb.Outcome_OUTCOME_RESUMED, pb.JobState_JOB_STATE_RUNNING,
		},
		{
			"resume SUSPENDED", pb.JobState_JOB_STATE_SUSPENDED, 0, statemachine.OpTypeResume, ranWorker,
			pb.Outcome_OUTCOME_RESUMED, pb.JobState_JOB_STATE_RUNNING,
		},
		{
			"resume IDLE", pb.JobState_JOB_STATE_IDLE, 0, statemachine.OpTypeResume, refused,
			0, pb.JobState_JOB_STATE_IDLE,
		},
		{
			"resume IDLE after kill", pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_KILLED, statemachine.OpTypeResume,
			refused, 0, pb.JobState_JOB_STATE_IDLE,
		},
		{
			"resume FAULTED", pb.JobState_JOB_STATE_FAULTED, 0, statemachine.OpTypeResume, refused,
			0, pb.JobState_JOB_STATE_FAULTED,
		},
		{"resume unknown job", unknown, 0, statemachine.OpTypeResume, refused, 0, unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := statemachine.NewStateManager()
			if tt.state != unknown {
				setJob(t, sm, tt.state, tt.lastOutcome)
			}
			var calls atomic.Int32
			worker := func(ctx context.Context) (statemachine.GuestResult, error) {
				calls.Add(1)
				return instant(suspendedResult, nil)(ctx)
			}

			opID, err := sm.StartGuestOp(guestJob, tt.intent, 1, future(), worker)
			if tt.want == refused {
				requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				checkComplete(t, waitForOperation(t, sm, opID), tt.wantOutcome)
			}

			wantCalls := int32(0)
			if tt.want == ranWorker {
				wantCalls = 1
			}
			if got := calls.Load(); got != wantCalls {
				t.Errorf("expected %d worker calls, got %d", wantCalls, got)
			}
			if tt.state == unknown {
				if n := len(sm.GetJobStatus()); n != 0 {
					t.Errorf("a call for an unknown job created %d jobs", n)
				}
				return
			}
			checkJobState(t, sm, guestJob, tt.wantState)
		})
	}
}

func TestGuestOp_SuspendReleased(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_IDLE)
	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1,
		instant(statemachine.GuestResult{Outcome: pb.Outcome_OUTCOME_RELEASED}, nil))
	checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_RELEASED)
	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetLastOutcome() != pb.Outcome_OUTCOME_RELEASED {
		t.Errorf("expected IDLE/RELEASED, got %s/%s", st.GetState(), st.GetLastOutcome())
	}
	if _, err := sm.GetJobPIDs(guestJob); status.Code(err) != codes.NotFound {
		t.Errorf("expected no PIDs after RELEASED, got %v", err)
	}
}

func TestGuestOp_JobStatusFields(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	opID := startGuest(t, sm, statemachine.OpTypeSuspend, 9, instant(suspendedResult, nil))
	op := waitForOperation(t, sm, opID)
	checkComplete(t, op, pb.Outcome_OUTCOME_SUSPENDED)
	if op.HostBytesPinned != suspendedResult.HostBytesPinned || op.SnapshotDeviceBytes != suspendedResult.DeviceBytes ||
		op.StorageBytes != suspendedResult.StorageBytes || op.Epoch != 9 {
		t.Errorf("unexpected operation fields: %+v", op)
	}

	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_SUSPENDED ||
		st.GetLastOutcome() != pb.Outcome_OUTCOME_SUSPENDED ||
		st.GetDeviceBytes() != suspendedResult.DeviceBytes ||
		st.GetHostBytesPinned() != suspendedResult.HostBytesPinned ||
		st.GetEpoch() != 9 {
		t.Errorf("unexpected job status: %v", st)
	}
}

func TestGuestOp_ReportResumedOutcome(t *testing.T) {
	tests := []struct {
		name string
		opts []statemachine.Option
		want pb.Outcome
	}{
		{name: "default keeps RESUMED", want: pb.Outcome_OUTCOME_RESUMED},
		{
			name: "explicit true",
			opts: []statemachine.Option{statemachine.WithReportResumedOutcome(true)},
			want: pb.Outcome_OUTCOME_RESUMED,
		},
		{
			name: "false reports no outcome",
			opts: []statemachine.Option{statemachine.WithReportResumedOutcome(false)},
			want: pb.Outcome_OUTCOME_UNSPECIFIED,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED, tt.opts...)
			opID := startGuest(t, sm, statemachine.OpTypeResume, 1, instant(statemachine.GuestResult{}, nil))
			checkComplete(t, waitForOperation(t, sm, opID), tt.want)
			checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_RUNNING)

			// A Resume of a RUNNING job completes at once with the same outcome.
			opID = startGuest(t, sm, statemachine.OpTypeResume, 2, mustNotRun(t))
			checkComplete(t, waitForOperation(t, sm, opID), tt.want)
		})
	}
}

func TestGuestOp_Deadlines(t *testing.T) {
	t.Run("missing deadline", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, time.Time{}, mustNotRun(t))
		requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		_, err = sm.StartKill(guestJob, time.Time{}, "no deadline", func(context.Context) error {
			t.Error("kill worker must not run")
			return nil
		})
		requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		if n := len(sm.InternalOperations()); n != 0 {
			t.Errorf("refused calls created %d operations", n)
		}
	})

	t.Run("unsupported intent", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSnapshot, 1, future(), mustNotRun(t))
		requireRefusal(t, err, codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})

	t.Run("past deadline is infeasible", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, time.Now().Add(-time.Second), mustNotRun(t))
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_RUNNING)
	})

	t.Run("past deadline does not abort the running operation", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(suspendedResult)
		suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
		ctx := waitStarted(t, stub)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 2, time.Now().Add(-time.Second), mustNotRun(t))
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_DEADLINE_INFEASIBLE)
		if ctx.Err() != nil {
			t.Errorf("an infeasible call cancelled the running operation: %v", ctx.Err())
		}
		close(stub.release)
		checkComplete(t, waitForOperation(t, sm, suspendID), pb.Outcome_OUTCOME_SUSPENDED)
	})

	t.Run("worker context carries the deadline", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(suspendedResult)
		deadline := future()
		opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, deadline, stub.run)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx := waitStarted(t, stub)
		if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
			t.Errorf("expected worker deadline %v, got %v (set=%v)", deadline, got, ok)
		}
		close(stub.release)
		if op := waitForOperation(t, sm, opID); !op.Deadline.Equal(deadline) {
			t.Errorf("expected operation deadline %v, got %v", deadline, op.Deadline)
		}
	})

	t.Run("running past the deadline", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		worker := func(ctx context.Context) (statemachine.GuestResult, error) {
			<-ctx.Done()
			return statemachine.GuestResult{}, ctx.Err()
		}
		opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, time.Now().Add(100*time.Millisecond), worker)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		checkFailed(t, waitForOperation(t, sm, opID), pb.ErrorReason_DEADLINE_EXCEEDED)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_FAULTED)
	})

	t.Run("success reported after the deadline", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		clk := newFakeClock(sm)
		stub := newGuestStub(suspendedResult)
		opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, clk.Now().Add(time.Second), stub.run)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		waitStarted(t, stub)
		clk.Advance(2 * time.Second)
		close(stub.release)
		checkFailed(t, waitForOperation(t, sm, opID), pb.ErrorReason_DEADLINE_EXCEEDED)
		st := jobStatus(t, sm)
		if st.GetState() != pb.JobState_JOB_STATE_FAULTED || st.GetLastOutcome() == pb.Outcome_OUTCOME_SUSPENDED {
			t.Errorf("a late suspend must not count as suspended: %v", st)
		}
	})
}

func TestGuestOp_WorkerFailures(t *testing.T) {
	tests := []struct {
		name   string
		intent statemachine.OpType
		res    statemachine.GuestResult
		err    error
		want   pb.ErrorReason
	}{
		{
			name:   "classified precondition",
			intent: statemachine.OpTypeSuspend,
			err:    statemachine.NewOpError(pb.ErrorReason_PRECONDITION_MEMORY, errors.New("not enough host memory")),
			want:   pb.ErrorReason_PRECONDITION_MEMORY,
		},
		{
			name:   "wrapped verify failure",
			intent: statemachine.OpTypeResume,
			err:    fmt.Errorf("resume: %w", statemachine.NewOpError(pb.ErrorReason_VERIFY_FAILED, errors.New("VRAM held"))),
			want:   pb.ErrorReason_VERIFY_FAILED,
		},
		{
			name:   "unclassified error",
			intent: statemachine.OpTypeSuspend,
			err:    errors.New("cuda-checkpoint exited 1"),
			want:   pb.ErrorReason_BACKEND_ERROR,
		},
		{
			name:   "suspend without a suspend outcome",
			intent: statemachine.OpTypeSuspend,
			res:    statemachine.GuestResult{Outcome: pb.Outcome_OUTCOME_RESUMED},
			want:   pb.ErrorReason_BACKEND_ERROR,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED)
			opID := startGuest(t, sm, tt.intent, 1, instant(tt.res, tt.err))
			op := waitForOperation(t, sm, opID)
			checkFailed(t, op, tt.want)
			if op.Error == "" {
				t.Error("a failed operation must carry an error message")
			}
			checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_FAULTED)
		})
	}
}

func TestGuestOp_BlockedByOtherOperations(t *testing.T) {
	t.Run("foreground snapshot", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		release := make(chan struct{})
		snapID, err := sm.StartSnapshot(guestJob, guestGroup, func() error {
			<-release
			return nil
		})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		_, err = sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, future(), mustNotRun(t))
		requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		close(release)
		waitForOperation(t, sm, snapID)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_SAVED)
	})

	t.Run("kill", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(statemachine.GuestResult{})
		killID := startKill(t, sm, stub.kill)
		ctx := waitStarted(t, stub)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, future(), mustNotRun(t))
		requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		if ctx.Err() != nil {
			t.Errorf("a guest call cancelled the Kill: %v", ctx.Err())
		}
		close(stub.release)
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	})

	t.Run("snapshot and restore during a guest operation", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(suspendedResult)
		opID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, stub.run)
		waitStarted(t, stub)
		_, err := sm.StartSnapshot(guestJob, guestGroup, func() error { return nil })
		requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		_, err = sm.StartRestore(guestJob, guestGroup, func() error { return nil })
		requireRefusal(t, err, codes.Aborted, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		close(stub.release)
		checkComplete(t, waitForOperation(t, sm, opID), pb.Outcome_OUTCOME_SUSPENDED)
	})
}

func checkKilled(t *testing.T, sm *statemachine.StateManager) {
	t.Helper()
	st := jobStatus(t, sm)
	if st.GetState() != pb.JobState_JOB_STATE_IDLE || st.GetLastOutcome() != pb.Outcome_OUTCOME_KILLED {
		t.Errorf("expected IDLE/KILLED, got %s/%s", st.GetState(), st.GetLastOutcome())
	}
	if st.GetDeviceBytes() != 0 || st.GetHostBytesPinned() != 0 {
		t.Errorf("a killed job must hold no bytes: %v", st)
	}
	if _, err := sm.GetJobPIDs(guestJob); status.Code(err) != codes.NotFound {
		t.Errorf("expected no PIDs after Kill, got %v", err)
	}
}

func TestKill(t *testing.T) {
	t.Run("from RUNNING", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(statemachine.GuestResult{})
		deadline := future()
		killID, err := sm.StartKill(guestJob, deadline, "T reached", stub.kill)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx := waitStarted(t, stub)
		if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
			t.Errorf("expected kill deadline %v, got %v (set=%v)", deadline, got, ok)
		}
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_TRANSITIONING)
		close(stub.release)
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		checkKilled(t, sm)
	})

	t.Run("supersedes a running suspend", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		suspendStub := newGuestStub(suspendedResult)
		killStub := newGuestStub(statemachine.GuestResult{})

		suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, suspendStub.run)
		suspendCtx := waitStarted(t, suspendStub)
		killID := startKill(t, sm, killStub.kill)
		waitStarted(t, killStub)

		superseded := getOp(t, sm, suspendID)
		checkFailed(t, superseded, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		if superseded.Error != "superseded by Kill: T reached" {
			t.Errorf("unexpected supersede message %q", superseded.Error)
		}
		if !errors.Is(suspendCtx.Err(), context.Canceled) {
			t.Errorf("superseded operation's context: expected Canceled, got %v", suspendCtx.Err())
		}

		// A hung suspend that returns late writes nothing.
		close(suspendStub.release)
		waitReturned(t, suspendStub)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_TRANSITIONING)

		close(killStub.release)
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		checkKilled(t, sm)
	})

	t.Run("supersedes a foreground snapshot", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		release := make(chan struct{})
		returned := make(chan struct{})
		snapID, err := sm.StartSnapshot(guestJob, guestGroup, func() error {
			<-release
			close(returned)
			return nil
		})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		killID := startKill(t, sm, func(context.Context) error { return nil })
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		if !strings.HasPrefix(getOp(t, sm, snapID).Error, "superseded by Kill") {
			t.Errorf("snapshot not superseded: %+v", getOp(t, sm, snapID))
		}

		close(release)
		<-returned
		time.Sleep(50 * time.Millisecond)
		checkKilled(t, sm)
		checkOperationStatus(t, getOp(t, sm, snapID), pb.OperationStatus_OPERATION_STATUS_FAILED, "superseded by Kill: T reached")
	})

	t.Run("unknown job", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		killID := startKill(t, sm, func(context.Context) error {
			t.Error("kill worker must not run for an unknown job")
			return nil
		})
		checkComplete(t, getOp(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		if n := len(sm.GetJobStatus()); n != 0 {
			t.Errorf("Kill created %d jobs", n)
		}
	})

	t.Run("already killed", func(t *testing.T) {
		sm := statemachine.NewStateManager()
		setJob(t, sm, pb.JobState_JOB_STATE_IDLE, pb.Outcome_OUTCOME_KILLED)
		killID := startKill(t, sm, func(context.Context) error {
			t.Error("kill worker must not run for a killed job")
			return nil
		})
		checkComplete(t, getOp(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
	})

	t.Run("second kill joins the first", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		stub := newGuestStub(statemachine.GuestResult{})
		first := startKill(t, sm, stub.kill)
		waitStarted(t, stub)
		if second := startKill(t, sm, stub.kill); second != first {
			t.Errorf("expected %s, got %s", first, second)
		}
		close(stub.release)
		checkComplete(t, waitForOperation(t, sm, first), pb.Outcome_OUTCOME_KILLED)
		if calls := stub.calls.Load(); calls != 1 {
			t.Errorf("expected the kill worker to run once, ran %d times", calls)
		}
	})

	t.Run("unconfirmed kill faults, a later kill clears it", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_SUSPENDED)
		failedID := startKill(t, sm, func(context.Context) error { return errors.New("cgroup.procs not empty") })
		checkFailed(t, waitForOperation(t, sm, failedID), pb.ErrorReason_KILL_UNCONFIRMED)
		st := jobStatus(t, sm)
		if st.GetState() != pb.JobState_JOB_STATE_FAULTED || st.GetLastOutcome() == pb.Outcome_OUTCOME_KILLED {
			t.Errorf("an unconfirmed kill must leave the job FAULTED and not KILLED: %v", st)
		}

		// FAULTED refuses guest calls but not Kill.
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 1, future(), mustNotRun(t))
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)

		killID := startKill(t, sm, func(context.Context) error { return nil })
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		checkKilled(t, sm)
	})

	t.Run("confirmation after the deadline is unconfirmed", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		clk := newFakeClock(sm)
		stub := newGuestStub(statemachine.GuestResult{})
		killID, err := sm.StartKill(guestJob, clk.Now().Add(3*time.Second), "T reached", stub.kill)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		waitStarted(t, stub)
		clk.Advance(4 * time.Second)
		close(stub.release)
		checkFailed(t, waitForOperation(t, sm, killID), pb.ErrorReason_KILL_UNCONFIRMED)
		checkJobState(t, sm, guestJob, pb.JobState_JOB_STATE_FAULTED)
	})

	t.Run("guest calls after kill", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		killID := startKill(t, sm, func(context.Context) error { return nil })
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)

		suspendID := startGuest(t, sm, statemachine.OpTypeSuspend, 1, mustNotRun(t))
		checkComplete(t, getOp(t, sm, suspendID), pb.Outcome_OUTCOME_RELEASED)
		_, err := sm.StartGuestOp(guestJob, statemachine.OpTypeResume, 2, future(), mustNotRun(t))
		requireRefusal(t, err, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_UNSPECIFIED)
		checkKilled(t, sm)
	})

	t.Run("a job that runs again is no longer killed", func(t *testing.T) {
		sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
		killID := startKill(t, sm, func(context.Context) error { return nil })
		checkComplete(t, waitForOperation(t, sm, killID), pb.Outcome_OUTCOME_KILLED)
		if err := sm.TransitionToRunning(guestJob, []int{7}); err != nil {
			t.Fatalf("TransitionToRunning: %v", err)
		}
		if got := jobStatus(t, sm).GetLastOutcome(); got != pb.Outcome_OUTCOME_UNSPECIFIED {
			t.Errorf("a RUNNING job must not report %s", got)
		}
	})
}

func TestOperationTTL(t *testing.T) {
	sm := newGuestSM(t, pb.JobState_JOB_STATE_RUNNING)
	clk := newFakeClock(sm)
	stub := newGuestStub(suspendedResult)
	// The deadline follows the fake clock, which runs past a real-time one.
	deadline := clk.Now().Add(time.Hour)
	suspend := func() string {
		t.Helper()
		opID, err := sm.StartGuestOp(guestJob, statemachine.OpTypeSuspend, 1, deadline, stub.run)
		if err != nil {
			t.Fatalf("Suspend: unexpected error: %v", err)
		}
		return opID
	}

	first := suspend()
	waitStarted(t, stub)

	// A running operation never expires.
	clk.Advance(2 * statemachine.OperationTTL)
	getOp(t, sm, first)

	close(stub.release)
	waitForOperation(t, sm, first)

	clk.Advance(statemachine.OperationTTL - time.Second)
	getOp(t, sm, first)
	if again := suspend(); again != first {
		t.Errorf("re-issue within the TTL: expected %s, got %s", first, again)
	}

	clk.Advance(2 * time.Second)
	if _, ok := sm.GetOperation(first); ok {
		t.Error("operation still readable after the TTL")
	}

	// After the TTL the record is gone: a re-issue runs anew and the expired
	// record is collected.
	second := suspend()
	if second == first {
		t.Error("re-issue after the TTL returned the expired operation")
	}
	waitStarted(t, stub)
	waitForOperation(t, sm, second)
	if calls := stub.calls.Load(); calls != 2 {
		t.Errorf("expected 2 worker calls, got %d", calls)
	}
	sm.InternalMu().RLock()
	_, kept := sm.InternalOperations()[first]
	sm.InternalMu().RUnlock()
	if kept {
		t.Error("expired operation was not collected")
	}
}
