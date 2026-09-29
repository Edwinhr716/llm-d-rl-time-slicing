package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pb "github.com/edwinhr716/guest-kubelet/api/timeslice_orchestrator/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/orchestrator"
)

const (
	testNode  = "real-node"
	testGuest = "g"
)

// events is the ordered record of what the loop did, shared by the fakes.
type events struct {
	mu  sync.Mutex
	all []string
}

func (e *events) add(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.all = append(e.all, fmt.Sprintf(format, args...))
}

func (e *events) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.all)
}

func (e *events) index(name string) int { return slices.Index(e.list(), name) }

func (e *events) count(prefix string) int {
	n := 0
	for _, ev := range e.list() {
		if strings.HasPrefix(ev, prefix) {
			n++
		}
	}
	return n
}

// fakeOrch is a scripted orchestrator.
type fakeOrch struct {
	pb.UnimplementedTimeSliceOrchestratorServiceServer
	ev *events

	mu       sync.Mutex
	protocol int32
	vacateAt time.Time // zero: no notice
	grants   chan struct{}
}

func (f *fakeOrch) setNotice(within time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if within == 0 {
		f.vacateAt = time.Time{}
		return
	}
	f.vacateAt = time.Now().Add(within)
}

func (f *fakeOrch) grant() { f.grants <- struct{}{} }

func (f *fakeOrch) Acquire(ctx context.Context, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	if req.GetRole() != pb.Role_ROLE_BACKGROUND || req.GetNodeName() != testNode || req.GetJobId() != "vk/"+testNode {
		return nil, errors.New("bad background Acquire")
	}
	f.ev.add("acquire")
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.grants:
		f.ev.add("granted")
		return &pb.AcquireResponse{Success: true}, nil
	}
}

func (f *fakeOrch) Yield(_ context.Context, req *pb.YieldRequest) (*pb.YieldResponse, error) {
	if req.GetRole() != pb.Role_ROLE_BACKGROUND {
		return nil, errors.New("bad background Yield")
	}
	f.ev.add("yield")
	return &pb.YieldResponse{Success: true}, nil
}

func (f *fakeOrch) GetGroupStatus(_ context.Context, req *pb.GetGroupStatusRequest) (*pb.GetGroupStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gs := &pb.GroupStatus{GroupId: req.GetGroupId(), BackgroundProtocol: f.protocol}
	if !f.vacateAt.IsZero() {
		gs.VacateWithin = durationpb.New(max(time.Until(f.vacateAt), 0))
	}
	return &pb.GetGroupStatusResponse{Group: gs}, nil
}

// fakeHost is an in-memory mirror backend.
type fakeHost struct {
	ev *events

	mu     sync.Mutex
	guests map[string]*mirror.Guest
	epoch  int64
}

func (h *fakeHost) addGuest() {
	name := testGuest
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guests[name] = &mirror.Guest{Pod: &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name + "-uid")},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}}
}

func (h *fakeHost) guest(name string) mirror.Guest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return *h.guests[name]
}

func (h *fakeHost) SetGroup(group string) { h.ev.add("group %s", group) }

func (h *fakeHost) Guests() ([]mirror.Guest, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]mirror.Guest, 0, len(h.guests))
	for _, g := range h.guests {
		out = append(out, *g)
	}
	return out, nil
}

func (h *fakeHost) Create(_ context.Context, guest *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guests[guest.Name].Mirror = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: guest.Name + "-m", Namespace: "ns", UID: types.UID(guest.Name + "-m-uid")},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		}},
	}
	h.ev.add("create %s", guest.Name)
	return nil
}

func (h *fakeHost) HoldNotReady(guest *corev1.Pod, reason string) {
	h.ev.add("hold %s %s", guest.Name, reason)
}

func (h *fakeHost) ReleaseReady(guest *corev1.Pod) { h.ev.add("ready %s", guest.Name) }

func (h *fakeHost) ConfirmNotReady(_ context.Context, guest *corev1.Pod) error {
	h.ev.add("confirm %s", guest.Name)
	return nil
}

func (h *fakeHost) MirrorNow(_ context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.guests[guest.Name].Mirror
	return m, m != nil, nil
}

func (h *fakeHost) VacateMirror(_ context.Context, guest, _ *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guests[guest.Name].Mirror = nil
	h.ev.add("vacate-delete %s", guest.Name)
	return nil
}

func (h *fakeHost) KillMirror(_ context.Context, m *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, g := range h.guests {
		if g.Mirror != nil && g.Mirror.UID == m.UID {
			g.Mirror = nil
			g.Pod.Status.Phase = corev1.PodFailed
		}
	}
	h.ev.add("kill %s", m.Name)
	return nil
}

func (h *fakeHost) BumpEpoch(_ context.Context, m *corev1.Pod) (*corev1.Pod, int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.epoch++
	h.ev.add("epoch %d", h.epoch)
	return m, h.epoch, nil
}

// testFreezer records suspend and resume in the shared event log.
type testFreezer struct {
	ev         *events
	failNext   error
	suspendFor time.Duration
}

func (f *testFreezer) Suspend(ctx context.Context, m *corev1.Pod, epoch int64) error {
	if err := f.failNext; err != nil {
		f.failNext = nil
		f.ev.add("suspend-failed %s", m.Name)
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(f.suspendFor):
	}
	f.ev.add("suspend %s %d", m.Name, epoch)
	return nil
}

func (f *testFreezer) Resume(_ context.Context, m *corev1.Pod, epoch int64) error {
	f.ev.add("resume %s %d", m.Name, epoch)
	return nil
}

type rig struct {
	ev      *events
	orch    *fakeOrch
	host    *fakeHost
	freezer *testFreezer
	loop    *orchestrator.Loop
}

func newRig(t *testing.T, protocol int32) *rig {
	t.Helper()
	return newRigWith(t, protocol, true)
}

func newRigWith(t *testing.T, protocol int32, withFreezer bool) *rig {
	t.Helper()
	ev := &events{}
	fix := &rig{
		ev:      ev,
		orch:    &fakeOrch{ev: ev, protocol: protocol, grants: make(chan struct{})},
		host:    &fakeHost{ev: ev, guests: map[string]*mirror.Guest{}},
		freezer: &testFreezer{ev: ev, suspendFor: 50 * time.Millisecond},
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterTimeSliceOrchestratorServiceServer(srv, fix.orch)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Log(err)
		}
	}()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Log(err)
		}
	})
	var freezer orchestrator.Freezer
	if withFreezer {
		freezer = fix.freezer
	}
	fix.loop, err = orchestrator.New(&orchestrator.Config{
		Node:          testNode,
		Client:        pb.NewTimeSliceOrchestratorServiceClient(conn),
		Group:         func(context.Context) (string, error) { return "g1", nil },
		Host:          fix.host,
		Freezer:       freezer,
		PollInterval:  20 * time.Millisecond,
		Liveness:      time.Second,
		VacateMargin:  10 * time.Millisecond,
		RetryInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fix
}

func (fix *rig) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := fix.loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func waitFor(t *testing.T, ev *events, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; events: %v", what, ev.list())
}

func waitEvent(t *testing.T, ev *events, name string) {
	t.Helper()
	waitFor(t, ev, name, func() bool { return ev.index(name) >= 0 })
}

// before fails unless event a happened, event b happened, and a came first.
func before(t *testing.T, ev *events, first, second string) {
	t.Helper()
	i, j := ev.index(first), ev.index(second)
	if i < 0 || j < 0 || i > j {
		t.Errorf("want %q before %q; events: %v", first, second, ev.list())
	}
}

func TestLoop_ProtocolNotOneStartsNothing(t *testing.T) {
	fix := newRig(t, 0)
	fix.host.addGuest()
	fix.start(t)
	time.Sleep(500 * time.Millisecond)
	if n := fix.ev.count("acquire") + fix.ev.count("create"); n != 0 {
		t.Fatalf("with background_protocol 0 the loop must not Acquire or create; events: %v", fix.ev.list())
	}
}

func TestLoop_NoCreateWithoutGrant(t *testing.T) {
	fix := newRig(t, 1)
	fix.host.addGuest()
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.loop.GuestWaiting(fix.host.guest(testGuest).Pod)
	time.Sleep(300 * time.Millisecond)
	if fix.ev.count("create") != 0 {
		t.Fatalf("mirror created without a grant; events: %v", fix.ev.list())
	}
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")
	before(t, fix.ev, "granted", "create g")
	before(t, fix.ev, "create g", "ready g")
}

func TestLoop_VacateNotReadyThenSuspendThenYield(t *testing.T) {
	fix := newRig(t, 1)
	fix.host.addGuest()
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")

	fix.orch.setNotice(2 * time.Second)
	waitEvent(t, fix.ev, "yield")
	before(t, fix.ev, "hold g "+mirror.ReasonSuspending, "confirm g")
	before(t, fix.ev, "confirm g", "epoch 1")
	before(t, fix.ev, "epoch 1", "suspend g-m 1")
	before(t, fix.ev, "suspend g-m 1", "hold g "+mirror.ReasonSuspended)
	before(t, fix.ev, "suspend g-m 1", "yield")
	if fix.ev.count("create") != 1 {
		t.Errorf("want exactly one create; events: %v", fix.ev.list())
	}
}

func TestLoop_ResumeBeforeReady(t *testing.T) {
	fix := newRig(t, 1)
	fix.host.addGuest()
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")
	fix.orch.setNotice(2 * time.Second)
	waitEvent(t, fix.ev, "yield")
	fix.orch.setNotice(0)
	waitFor(t, fix.ev, "second acquire", func() bool { return fix.ev.count("acquire") >= 2 })
	fix.orch.grant()
	waitFor(t, fix.ev, "second ready", func() bool { return fix.ev.count("ready g") >= 2 })
	before(t, fix.ev, "resume g-m 2", "hold g "+mirror.ReasonWaitingForGrant)
	evs := fix.ev.list()
	lastReady := -1
	for i, ev := range evs {
		if ev == "ready g" {
			lastReady = i
		}
	}
	if resume := fix.ev.index("resume g-m 2"); resume < 0 || resume > lastReady {
		t.Errorf("want resume before the second Ready; events: %v", evs)
	}
	if fix.ev.count("create") != 1 {
		t.Errorf("a suspended guest must be resumed, not re-created; events: %v", evs)
	}
}

func TestLoop_NothingToSuspendYieldsAtOnce(t *testing.T) {
	fix := newRig(t, 1)
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "granted")
	start := time.Now()
	fix.orch.setNotice(5 * time.Second)
	waitEvent(t, fix.ev, "yield")
	if took := time.Since(start); took > time.Second {
		t.Errorf("Yield took %v with nothing to suspend", took)
	}
}

func TestLoop_SuspendFailureKillsThenYields(t *testing.T) {
	fix := newRig(t, 1)
	fix.freezer.failNext = errors.New("agent unavailable")
	fix.host.addGuest()
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")
	fix.orch.setNotice(2 * time.Second)
	waitEvent(t, fix.ev, "yield")
	before(t, fix.ev, "suspend-failed g-m", "kill g-m")
	before(t, fix.ev, "kill g-m", "yield")
}

func TestLoop_NoFreezerVacatesByDeleteAndRecreates(t *testing.T) {
	fix := newRigWith(t, 1, false)
	fix.host.addGuest()
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")
	fix.orch.setNotice(2 * time.Second)
	waitEvent(t, fix.ev, "yield")
	before(t, fix.ev, "confirm g", "vacate-delete g")
	before(t, fix.ev, "vacate-delete g", "yield")
	fix.orch.setNotice(0)
	waitFor(t, fix.ev, "second acquire", func() bool { return fix.ev.count("acquire") >= 2 })
	fix.orch.grant()
	waitFor(t, fix.ev, "second create", func() bool { return fix.ev.count("create g") >= 2 })
}

func TestNew_RequiresClientGroupAndHost(t *testing.T) {
	if _, err := orchestrator.New(&orchestrator.Config{Node: testNode}); err == nil {
		t.Fatal("New must refuse a config without Client, Group and Host")
	}
}

// withMirror gives the test guest a running mirror without a create event, as a restarted
// guest kubelet finds it (M5).
func (h *fakeHost) withMirror() types.UID {
	h.mu.Lock()
	defer h.mu.Unlock()
	g := h.guests[testGuest]
	g.Pod.Status.Phase = corev1.PodRunning
	g.Mirror = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testGuest + "-m", Namespace: "ns", UID: types.UID(testGuest + "-m-uid")},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		}},
	}
	return g.Pod.UID
}

func TestLoop_AdoptedSuspendedGuestIsResumedNotCreated(t *testing.T) {
	fix := newRig(t, 1)
	fix.host.addGuest()
	fix.loop.Adopt(fix.host.withMirror(), true, false)
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "ready g")
	before(t, fix.ev, "granted", "resume g-m 1")
	before(t, fix.ev, "resume g-m 1", "ready g")
	if fix.ev.count("create") != 0 {
		t.Errorf("an adopted suspended guest must be resumed, not re-created; events: %v", fix.ev.list())
	}
}

func TestLoop_AdoptedReleasedGuestIsNotReleasedAgain(t *testing.T) {
	fix := newRig(t, 1)
	fix.host.addGuest()
	fix.loop.Adopt(fix.host.withMirror(), false, true)
	fix.start(t)
	waitEvent(t, fix.ev, "acquire")
	fix.orch.grant()
	waitEvent(t, fix.ev, "granted")
	time.Sleep(300 * time.Millisecond)
	if n := fix.ev.count("create") + fix.ev.count("ready") + fix.ev.count("resume"); n != 0 {
		t.Errorf("an adopted released guest needs no create, resume or release; events: %v", fix.ev.list())
	}
	// It is still vacated at the next notice.
	fix.orch.setNotice(2 * time.Second)
	waitEvent(t, fix.ev, "yield")
	before(t, fix.ev, "confirm g", "suspend g-m 1")
}
