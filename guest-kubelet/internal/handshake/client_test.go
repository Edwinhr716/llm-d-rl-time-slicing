package handshake_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/edwinhr716/guest-kubelet/api/snapshot_agent/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/handshake"
)

// fakeAgent is a scripted snapshot agent. Unset hooks behave like a healthy agent that lists
// the job RUNNING and completes every operation at once.
type fakeAgent struct {
	pb.SnapshotAgentServiceClient // methods the client never calls panic

	mu       sync.Mutex
	calls    []string
	epochs   []int64
	deadline []time.Time
	status   func() (*pb.StatusResponse, error)
	start    func(op string, n int) error // n counts the starts of op, from 1
	getOp    func(id string, n int) (*pb.GetOperationResponse, error)
	starts   map[string]int
	gets     map[string]int
}

func newFake() *fakeAgent {
	return &fakeAgent{starts: map[string]int{}, gets: map[string]int{}}
}

func (f *fakeAgent) record(op string, epoch int64, dl time.Time) error {
	f.mu.Lock()
	f.calls = append(f.calls, op)
	f.epochs = append(f.epochs, epoch)
	f.deadline = append(f.deadline, dl)
	f.starts[op]++
	n, start := f.starts[op], f.start
	f.mu.Unlock()
	if start != nil {
		return start(op, n)
	}
	return nil
}

func (f *fakeAgent) Suspend(ctx context.Context, in *pb.SuspendRequest, _ ...grpc.CallOption) (*pb.SuspendResponse, error) {
	if err := hang(ctx, f, "Suspend"); err != nil {
		return nil, err
	}
	err := f.record("Suspend", in.GetEpoch(), in.GetDeadline().AsTime())
	if err != nil {
		return nil, err
	}
	return &pb.SuspendResponse{OperationId: "Suspend"}, nil
}

func (f *fakeAgent) Resume(ctx context.Context, in *pb.ResumeRequest, _ ...grpc.CallOption) (*pb.ResumeResponse, error) {
	if err := hang(ctx, f, "Resume"); err != nil {
		return nil, err
	}
	err := f.record("Resume", in.GetEpoch(), in.GetDeadline().AsTime())
	if err != nil {
		return nil, err
	}
	return &pb.ResumeResponse{OperationId: "Resume"}, nil
}

func (f *fakeAgent) Kill(ctx context.Context, in *pb.KillRequest, _ ...grpc.CallOption) (*pb.KillResponse, error) {
	if err := hang(ctx, f, "Kill"); err != nil {
		return nil, err
	}
	err := f.record("Kill", 0, in.GetDeadline().AsTime())
	if err != nil {
		return nil, err
	}
	return &pb.KillResponse{OperationId: "Kill"}, nil
}

// errHang, returned by a start hook, makes the call block until its RPC context ends, as a
// SIGSTOPped agent would.
var errHang = errors.New("hang")

func hang(ctx context.Context, f *fakeAgent, op string) error {
	f.mu.Lock()
	start := f.start
	n := f.starts[op] + 1
	f.mu.Unlock()
	if start == nil || !errors.Is(start(op, -n), errHang) {
		return nil
	}
	f.mu.Lock()
	f.starts[op]++
	f.calls = append(f.calls, op+"(hung)")
	f.mu.Unlock()
	<-ctx.Done()
	return status.FromContextError(ctx.Err()).Err()
}

func (f *fakeAgent) GetOperation(
	ctx context.Context, in *pb.GetOperationRequest, _ ...grpc.CallOption,
) (*pb.GetOperationResponse, error) {
	f.mu.Lock()
	f.gets[in.GetOperationId()]++
	n, get := f.gets[in.GetOperationId()], f.getOp
	f.mu.Unlock()
	if get != nil {
		return get(in.GetOperationId(), n)
	}
	return complete(in.GetOperationId()), nil
}

func (f *fakeAgent) Status(context.Context, *pb.StatusRequest, ...grpc.CallOption) (*pb.StatusResponse, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "Status")
	st := f.status
	f.mu.Unlock()
	if st != nil {
		return st()
	}
	return listed(pb.JobState_JOB_STATE_RUNNING), nil
}

func (f *fakeAgent) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func listed(s pb.JobState) *pb.StatusResponse {
	return &pb.StatusResponse{JobStatuses: []*pb.JobStatus{
		{JobId: "other", State: pb.JobState_JOB_STATE_RUNNING},
		{JobId: "job-1", State: s},
	}}
}

func complete(op string) *pb.GetOperationResponse {
	out := map[string]pb.Outcome{
		"Suspend": pb.Outcome_OUTCOME_SUSPENDED,
		"Resume":  pb.Outcome_OUTCOME_RESUMED,
		"Kill":    pb.Outcome_OUTCOME_KILLED,
	}
	return &pb.GetOperationResponse{Status: pb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: out[op]}
}

func pending() *pb.GetOperationResponse {
	return &pb.GetOperationResponse{Status: pb.OperationStatus_OPERATION_STATUS_PENDING}
}

func mirrorPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "vllm-m", Labels: map[string]string{handshake.LabelJobID: "job-1"},
	}}
}

var fastOpts = handshake.Options{
	Poll: 5 * time.Millisecond, RetryInitial: 10 * time.Millisecond, RetryMax: 40 * time.Millisecond,
	RPCTimeout: 50 * time.Millisecond, Grace: 20 * time.Millisecond,
}

func withDeadline(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func wantKind(t *testing.T, err error, kind handshake.Kind) *handshake.Error {
	t.Helper()
	var e *handshake.Error
	if !errors.As(err, &e) || e.Kind != kind {
		t.Fatalf("want a %s error, got %v", kind, err)
	}
	return e
}

func requireKind(t *testing.T, err error, kind handshake.Kind) {
	t.Helper()
	var e *handshake.Error
	if !errors.As(err, &e) || e.Kind != kind {
		t.Fatalf("want a %s error, got %v", kind, err)
	}
}

func TestSuspendResumeKill_Happy(t *testing.T) {
	f := newFake()
	c := handshake.New(f, fastOpts)
	ctx := withDeadline(t, 2*time.Second)
	dl, _ := ctx.Deadline()
	if err := c.Suspend(ctx, mirrorPod(), 3); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(ctx, mirrorPod(), 4); err != nil {
		t.Fatal(err)
	}
	if err := c.Kill(ctx, mirrorPod(), "test"); err != nil {
		t.Fatal(err)
	}
	if got := f.callList(); len(got) != 4 || got[0] != "Status" || got[1] != "Suspend" || got[2] != "Resume" || got[3] != "Kill" {
		t.Fatalf("calls = %v, want Status first (only guests the agent lists are suspended)", got)
	}
	if f.epochs[0] != 3 || f.epochs[1] != 4 {
		t.Fatalf("epochs = %v", f.epochs)
	}
	for i, d := range f.deadline {
		if !d.Equal(dl.Add(-fastOpts.Grace)) {
			t.Fatalf("call %d: agent deadline %v, want the caller's deadline minus the grace %v", i, d, dl.Add(-fastOpts.Grace))
		}
	}
}

func TestOptions_Defaults(t *testing.T) {
	o := handshake.Options{}.WithDefaults()
	if o.Poll != 100*time.Millisecond || o.RetryInitial != time.Second || o.RetryMax != 30*time.Second ||
		o.RPCTimeout != 5*time.Second || o.Grace != time.Second {
		t.Fatalf("Q13 defaults: %+v", o)
	}
}

func TestPrepare_NeedsDeadlineAndJobID(t *testing.T) {
	c := handshake.New(newFake(), fastOpts)
	requireKind(t, c.Suspend(context.Background(), mirrorPod(), 1), handshake.KindInvalid)
	m := mirrorPod()
	m.Labels = nil
	requireKind(t, c.Kill(withDeadline(t, time.Second), m, "x"), handshake.KindInvalid)
	// A deadline inside the grace leaves the agent no time.
	requireKind(t, c.Resume(withDeadline(t, 10*time.Millisecond), mirrorPod(), 1), handshake.KindDeadline)
}

func TestSuspend_AgentHangsEndsAtDeadline(t *testing.T) {
	f := newFake()
	f.start = func(string, int) error { return errHang }
	c := handshake.New(f, fastOpts)
	t0 := time.Now()
	e := wantKind(t, c.Suspend(withDeadline(t, 300*time.Millisecond), mirrorPod(), 1), handshake.KindDeadline)
	if el := time.Since(t0); el > 500*time.Millisecond {
		t.Fatalf("a hung agent held the call %s past its deadline", el)
	}
	if e.Op != "Suspend" {
		t.Fatalf("error %+v", e)
	}
	if n := len(f.callList()); n < 3 {
		t.Fatalf("each hung RPC must time out and be retried: calls = %v", f.callList())
	}
}

func TestSuspend_PendingForeverEndsAtDeadline(t *testing.T) {
	f := newFake()
	f.getOp = func(string, int) (*pb.GetOperationResponse, error) { return pending(), nil }
	c := handshake.New(f, fastOpts)
	e := wantKind(t, c.Suspend(withDeadline(t, 200*time.Millisecond), mirrorPod(), 1), handshake.KindDeadline)
	if e.Msg == "" {
		t.Fatal("the error should say the operation was still pending")
	}
}

func TestResume_UnavailableThenSuccess(t *testing.T) {
	f := newFake()
	f.start = func(_ string, n int) error {
		if n > 0 && n <= 2 {
			return status.Error(codes.Unavailable, "connection refused")
		}
		return nil
	}
	c := handshake.New(f, fastOpts)
	if err := c.Resume(withDeadline(t, 2*time.Second), mirrorPod(), 7); err != nil {
		t.Fatal(err)
	}
	if got := f.callList(); len(got) != 3 {
		t.Fatalf("calls = %v, want two refused starts then one that went through", got)
	}
	for _, e := range f.epochs {
		if e != 7 {
			t.Fatalf("a retry must carry the same epoch: %v", f.epochs)
		}
	}
}

func TestResume_OperationLostAfterRestartIsReissued(t *testing.T) {
	f := newFake()
	f.getOp = func(id string, n int) (*pb.GetOperationResponse, error) {
		switch n {
		case 1:
			return pending(), nil
		case 2:
			return nil, status.Error(codes.Unavailable, "agent restarting")
		case 3:
			return nil, status.Error(codes.NotFound, "no operation Resume")
		}
		return complete(id), nil
	}
	c := handshake.New(f, fastOpts)
	if err := c.Resume(withDeadline(t, 2*time.Second), mirrorPod(), 5); err != nil {
		t.Fatal(err)
	}
	if got := f.callList(); len(got) != 2 || got[0] != "Resume" || got[1] != "Resume" || f.epochs[1] != 5 {
		t.Fatalf("calls = %v epochs = %v, want the Resume re-issued with the same epoch", got, f.epochs)
	}
}

func TestResume_FaultedAfterRestartIsRefused(t *testing.T) {
	// An agent restart mid-resume leaves the job FAULTED; the re-issued Resume is refused
	// and only Kill is left.
	f := newFake()
	f.getOp = func(id string, n int) (*pb.GetOperationResponse, error) {
		if id == "Resume" {
			return nil, status.Error(codes.NotFound, "no such operation")
		}
		return complete(id), nil
	}
	f.start = func(op string, n int) error {
		if op == "Resume" && n == 2 {
			return status.Error(codes.FailedPrecondition, "cannot resume job job-1 in state JOB_STATE_FAULTED")
		}
		return nil
	}
	c := handshake.New(f, fastOpts)
	e := wantKind(t, c.Resume(withDeadline(t, time.Second), mirrorPod(), 2), handshake.KindRefused)
	if e.Code != codes.FailedPrecondition || e.Reason != "" {
		t.Fatalf("error %+v", e)
	}
	if err := c.Kill(withDeadline(t, time.Second), mirrorPod(), "resume refused"); err != nil {
		t.Fatalf("kill after FAULTED: %v", err)
	}
}

func TestSuspend_RefusalWithReasonPrefix(t *testing.T) {
	f := newFake()
	f.start = func(string, int) error {
		return status.Error(codes.FailedPrecondition, "STALE_EPOCH: epoch 2 is lower than 3")
	}
	c := handshake.New(f, fastOpts)
	err := c.Suspend(withDeadline(t, time.Second), mirrorPod(), 2)
	e := wantKind(t, err, handshake.KindRefused)
	if e.Reason != "STALE_EPOCH" || e.Msg != "epoch 2 is lower than 3" || handshake.ReasonOf(err) != "STALE_EPOCH" {
		t.Fatalf("error %+v", e)
	}
	if got := f.callList(); len(got) != 2 {
		t.Fatalf("a refusal is final, no retry: %v", got)
	}
}

func TestParseReason(t *testing.T) {
	for _, tc := range []struct{ in, reason, rest string }{
		{"PRECONDITION_MEMORY: no room", "PRECONDITION_MEMORY", "no room"},
		{"ERROR_REASON_UNSPECIFIED: x", "", "ERROR_REASON_UNSPECIFIED: x"},
		{"NOT_A_REASON: x", "", "NOT_A_REASON: x"},
		{"plain message", "", "plain message"},
	} {
		if r, rest := handshake.ParseReason(tc.in); r != tc.reason || rest != tc.rest {
			t.Errorf("handshake.ParseReason(%q) = %q, %q", tc.in, r, rest)
		}
	}
}

func TestKill_Unimplemented(t *testing.T) {
	f := newFake()
	f.start = func(string, int) error { return status.Error(codes.Unimplemented, "unknown method Kill") }
	c := handshake.New(f, fastOpts)
	requireKind(t, c.Kill(withDeadline(t, time.Second), mirrorPod(), "x"), handshake.KindUnimplemented)
}

func TestSuspend_FailedOperationCarriesReason(t *testing.T) {
	f := newFake()
	f.getOp = func(string, int) (*pb.GetOperationResponse, error) {
		msg := "checkpoint took too long"
		return &pb.GetOperationResponse{
			Status:      pb.OperationStatus_OPERATION_STATUS_FAILED,
			ErrorReason: pb.ErrorReason_DEADLINE_EXCEEDED, Error: &msg,
		}, nil
	}
	c := handshake.New(f, fastOpts)
	e := wantKind(t, c.Suspend(withDeadline(t, time.Second), mirrorPod(), 1), handshake.KindFailed)
	if e.Reason != "DEADLINE_EXCEEDED" || e.Msg != "checkpoint took too long" {
		t.Fatalf("error %+v", e)
	}
}

func TestKill_UnconfirmedIsAnError(t *testing.T) {
	f := newFake()
	f.getOp = func(string, int) (*pb.GetOperationResponse, error) {
		return &pb.GetOperationResponse{
			Status: pb.OperationStatus_OPERATION_STATUS_FAILED, ErrorReason: pb.ErrorReason_KILL_UNCONFIRMED,
		}, nil
	}
	c := handshake.New(f, fastOpts)
	e := wantKind(t, c.Kill(withDeadline(t, time.Second), mirrorPod(), "x"), handshake.KindFailed)
	if e.Reason != "KILL_UNCONFIRMED" {
		t.Fatalf("error %+v", e)
	}
}

func TestSuspend_NotListedUntilDeadline(t *testing.T) {
	f := newFake()
	f.status = func() (*pb.StatusResponse, error) { return &pb.StatusResponse{}, nil }
	c := handshake.New(f, fastOpts)
	requireKind(t, c.Suspend(withDeadline(t, 200*time.Millisecond), mirrorPod(), 1), handshake.KindDeadline)
	for _, call := range f.callList() {
		if call != "Status" {
			t.Fatalf("a job the agent does not list must never be suspended: %v", f.callList())
		}
	}
}

func TestSuspend_ListedAfterWatcherCatchesUp(t *testing.T) {
	f := newFake()
	var n int
	f.status = func() (*pb.StatusResponse, error) {
		n++
		if n < 3 {
			return &pb.StatusResponse{}, nil
		}
		return listed(pb.JobState_JOB_STATE_RUNNING), nil
	}
	c := handshake.New(f, fastOpts)
	if err := c.Suspend(withDeadline(t, 2*time.Second), mirrorPod(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestSuspend_FaultedJobFailsAtOnce(t *testing.T) {
	f := newFake()
	f.status = func() (*pb.StatusResponse, error) { return listed(pb.JobState_JOB_STATE_FAULTED), nil }
	c := handshake.New(f, fastOpts)
	requireKind(t, c.Suspend(withDeadline(t, 5*time.Second), mirrorPod(), 1), handshake.KindFaulted)
	if got := f.callList(); len(got) != 1 {
		t.Fatalf("calls = %v", got)
	}
}

func TestSuspend_ReleasedIsUnexpected(t *testing.T) {
	f := newFake()
	f.getOp = func(string, int) (*pb.GetOperationResponse, error) {
		return &pb.GetOperationResponse{Status: pb.OperationStatus_OPERATION_STATUS_COMPLETE, Outcome: pb.Outcome_OUTCOME_RELEASED}, nil
	}
	c := handshake.New(f, fastOpts)
	requireKind(t, c.Suspend(withDeadline(t, time.Second), mirrorPod(), 1), handshake.KindOutcome)
}

func TestResume_UnspecifiedOutcomeAccepted(t *testing.T) {
	// D-AGENT-7: an agent with --report-resumed-outcome=false completes a Resume without an
	// outcome.
	f := newFake()
	f.getOp = func(string, int) (*pb.GetOperationResponse, error) {
		return &pb.GetOperationResponse{Status: pb.OperationStatus_OPERATION_STATUS_COMPLETE}, nil
	}
	c := handshake.New(f, fastOpts)
	if err := c.Resume(withDeadline(t, time.Second), mirrorPod(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestDial_AgentDownFailsTheCallNotDial(t *testing.T) {
	c, conn, err := handshake.Dial("127.0.0.1:1", fastOpts) // nothing listens on port 1
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	requireKind(t, c.Kill(withDeadline(t, 300*time.Millisecond), mirrorPod(), "x"), handshake.KindDeadline)
}

func TestFrozen_HostFactFromStatus(t *testing.T) {
	cases := []struct {
		name    string
		status  func() (*pb.StatusResponse, error)
		want    bool
		wantErr bool
	}{
		{
			name:   "suspended",
			want:   true,
			status: func() (*pb.StatusResponse, error) { return listed(pb.JobState_JOB_STATE_SUSPENDED), nil },
		},
		{name: "running", status: func() (*pb.StatusResponse, error) { return listed(pb.JobState_JOB_STATE_RUNNING), nil }},
		{name: "not-listed", status: func() (*pb.StatusResponse, error) { return &pb.StatusResponse{}, nil }},
		{
			name:    "rpc-error",
			wantErr: true,
			status:  func() (*pb.StatusResponse, error) { return nil, status.Error(codes.Unavailable, "down") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.status = tc.status
			got, err := handshake.New(f, fastOpts).Frozen(context.Background(), mirrorPod())
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("Frozen = %v, %v; want %v, err %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
