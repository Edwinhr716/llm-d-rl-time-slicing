package hostcmd_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	hcpb "github.com/edwinhr716/guest-kubelet/api/hostcommand/v1alpha1"
	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

const (
	testNode  = "real-node"
	testGroup = "g1"
)

// events is the ordered record of what the server did, shared by the fakes.
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

// fakeHost is an in-memory mirror backend.
type fakeHost struct {
	ev *events

	mu        sync.Mutex
	guests    map[string]*mirror.Guest
	gone      chan struct{} // if set, deleted mirrors disappear only when it is closed
	neverUp   bool          // mirrors never become ready
	failState error         // if set, SetMirrorSuspendState fails with it
}

func newHost(ev *events) *fakeHost { return &fakeHost{ev: ev, guests: map[string]*mirror.Guest{}} }

func (h *fakeHost) addGuest(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guests[name] = &mirror.Guest{Pod: &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name + "-uid")},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}}
}

func (h *fakeHost) mirrorOf(name string) *corev1.Pod {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.guests[name].Mirror
}

func (h *fakeHost) Guests() ([]mirror.Guest, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]mirror.Guest, 0, len(h.guests))
	for _, g := range h.guests {
		out = append(out, *g)
	}
	slices.SortFunc(out, func(a, b mirror.Guest) int { return strings.Compare(a.Pod.Name, b.Pod.Name) })
	return out, nil
}

func (h *fakeHost) Create(_ context.Context, guest *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	ready := corev1.ConditionTrue
	if h.neverUp {
		ready = corev1.ConditionFalse
	}
	h.guests[guest.Name].Mirror = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: guest.Name + "-m", Namespace: "ns", UID: types.UID(guest.Name + "-m-uid"),
			Labels: map[string]string{mirror.LabelJobID: guest.Name + "-job"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: ready},
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

func (h *fakeHost) remove(m *corev1.Pod, failGuest bool) {
	h.mu.Lock()
	gone := h.gone
	for _, g := range h.guests {
		if g.Mirror != nil && g.Mirror.UID == m.UID {
			cp := g.Mirror.DeepCopy()
			cp.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			g.Mirror = cp
			if failGuest {
				g.Pod = g.Pod.DeepCopy()
				g.Pod.Status.Phase = corev1.PodFailed
			}
		}
	}
	h.mu.Unlock()
	finish := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, g := range h.guests {
			if g.Mirror != nil && g.Mirror.UID == m.UID {
				g.Mirror = nil
			}
		}
	}
	if gone == nil {
		finish()
		return
	}
	go func() {
		<-gone
		finish()
	}()
}

func (h *fakeHost) VacateMirror(_ context.Context, guest, m *corev1.Pod) error {
	h.ev.add("vacate-delete %s", guest.Name)
	h.remove(m, false)
	return nil
}

func (h *fakeHost) KillMirror(_ context.Context, m *corev1.Pod) error {
	h.ev.add("kill %s", m.Name)
	h.remove(m, true)
	return nil
}

func (h *fakeHost) WaitMirrorGone(ctx context.Context, m *corev1.Pod) error {
	for {
		h.mu.Lock()
		present := false
		for _, g := range h.guests {
			if g.Mirror != nil && g.Mirror.UID == m.UID {
				present = true
			}
		}
		h.mu.Unlock()
		if !present {
			h.ev.add("gone %s", m.Name)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// SetMirrorSuspendState records the state and epoch on the mirror, as the real backend does.
// Events: "epoch N" for a bump, "annotate <mirror> guest-epoch=N" for a given value, and
// "state <mirror> <state>" ("running" for a cleared state).
func (h *fakeHost) SetMirrorSuspendState(
	_ context.Context, m *corev1.Pod, state string, epoch int64,
) (*corev1.Pod, int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failState != nil {
		return nil, 0, h.failState
	}
	var owner *mirror.Guest
	for _, x := range h.guests {
		if x.Mirror != nil && x.Mirror.UID == m.UID {
			owner = x
		}
	}
	if owner == nil {
		return nil, 0, mirror.ErrMirrorReplaced
	}
	cur := owner.Mirror.DeepCopy()
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	_, out := mirror.SuspendState(cur)
	switch {
	case epoch > 0:
		out = epoch
		h.ev.add("annotate %s %s=%d", cur.Name, mirror.AnnotationGuestEpoch, epoch)
	case epoch == mirror.EpochBump:
		out++
		h.ev.add("epoch %d", out)
	}
	cur.Annotations[mirror.AnnotationGuestEpoch] = strconv.FormatInt(out, 10)
	shown := state
	if state == "" {
		delete(cur.Annotations, mirror.AnnotationSuspendState)
		shown = "running"
	} else {
		cur.Annotations[mirror.AnnotationSuspendState] = state
	}
	h.ev.add("state %s %s", cur.Name, shown)
	owner.Mirror = cur
	return cur.DeepCopy(), out, nil
}

// testFreezer records suspend and resume in the shared event log.
type testFreezer struct {
	ev *events

	mu          sync.Mutex
	failSuspend error
	suspendFor  time.Duration
	resumeFor   time.Duration
	kills       int
	frozen      map[string]bool // mirror name -> stopped now
}

func (f *testFreezer) setFrozen(name string, frozen bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.frozen == nil {
		f.frozen = map[string]bool{}
	}
	f.frozen[name] = frozen
}

// reportingFreezer is testFreezer that also answers Frozen, as freeze.Backend and FakeFreezer do.
type reportingFreezer struct{ *testFreezer }

func (f reportingFreezer) Frozen(m *corev1.Pod) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frozen[m.Name], nil
}

func (f *testFreezer) Suspend(ctx context.Context, m *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	fail, delay := f.failSuspend, f.suspendFor
	f.failSuspend = nil
	f.mu.Unlock()
	if fail != nil {
		f.ev.add("suspend-failed %s", m.Name)
		return fail
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}
	f.setFrozen(m.Name, true)
	f.ev.add("suspend %s %d", m.Name, epoch)
	return nil
}

func (f *testFreezer) Resume(ctx context.Context, m *corev1.Pod, epoch int64) error {
	f.mu.Lock()
	delay := f.resumeFor
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}
	f.setFrozen(m.Name, false)
	f.ev.add("resume %s %d", m.Name, epoch)
	return nil
}

func (f *testFreezer) Kill(_ context.Context, m *corev1.Pod, _ string) error {
	f.mu.Lock()
	f.kills++
	f.mu.Unlock()
	f.ev.add("agent-kill %s", m.Name)
	return nil
}

type rig struct {
	ev      *events
	host    *fakeHost
	freezer *testFreezer
	srv     *hostcmd.Server
	group   string
	cancel  context.CancelFunc // stops srv
}

type rigOpt func(*hostcmd.Config, *rig)

func withoutFreezer() rigOpt { return func(c *hostcmd.Config, _ *rig) { c.Freezer = nil } }

func newRig(t *testing.T, opts ...rigOpt) *rig {
	t.Helper()
	ev := &events{}
	fix := &rig{ev: ev, host: newHost(ev), freezer: &testFreezer{ev: ev}, group: testGroup}
	fix.start(t, opts...)
	return fix
}

// start creates the server on the rig's host and freezer. Called again after stop, it is a
// guest-kubelet restart: the host (the API) and the freezer (the node) keep their state.
func (r *rig) start(t *testing.T, opts ...rigOpt) {
	t.Helper()
	cfg := &hostcmd.Config{
		Node:          testNode,
		Group:         func() (string, bool) { return r.group, r.group != "" },
		Host:          r.host,
		Freezer:       r.freezer,
		VacateMargin:  10 * time.Millisecond,
		ServeInterval: 20 * time.Millisecond,
		PollInterval:  5 * time.Millisecond,
		KillTimeout:   time.Second,
	}
	for _, o := range opts {
		o(cfg, r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := hostcmd.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.srv, r.cancel = srv, cancel
	t.Cleanup(func() {
		cancel()
		srv.Wait()
	})
}

// stop kills the server as a crash would: its detached work ends with it.
func (r *rig) stop() {
	r.cancel()
	r.srv.Wait()
}

func (r *rig) vacate(t *testing.T, epoch int64, within time.Duration) *hcpb.HostAck {
	t.Helper()
	ack, err := r.srv.Vacate(context.Background(), &hcpb.VacateRequest{
		GroupId: testGroup, NodeName: testNode, Epoch: epoch, Deadline: timestamppb.New(time.Now().Add(within)),
	})
	if err != nil {
		t.Fatalf("Vacate(%d): %v", epoch, err)
	}
	return ack
}

func (r *rig) resume(t *testing.T, epoch int64) *hcpb.HostAck {
	t.Helper()
	ack, err := r.srv.Resume(context.Background(), &hcpb.ResumeRequest{
		GroupId: testGroup, NodeName: testNode, Epoch: epoch, Deadline: timestamppb.New(time.Now().Add(2 * time.Second)),
	})
	if err != nil {
		t.Fatalf("Resume(%d): %v", epoch, err)
	}
	return ack
}

func wantOutcome(t *testing.T, ack *hcpb.HostAck, want hcpb.Outcome) {
	t.Helper()
	if ack.GetOutcome() != want {
		t.Fatalf("outcome = %s (error %q), want %s", ack.GetOutcome(), ack.GetError(), want)
	}
}

func before(t *testing.T, ev *events, a, b string) {
	t.Helper()
	// The first a must come before the last b.
	all := ev.list()
	ia, ib := slices.Index(all, a), -1
	for i, e := range all {
		if e == b {
			ib = i
		}
	}
	if ia < 0 || ib < 0 || ia >= ib {
		t.Fatalf("want %q before %q; events %v", a, b, ev.list())
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestResume_NoMirrorBeforeFirstResume(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	r.srv.GuestWaiting(nil)
	time.Sleep(100 * time.Millisecond)
	if n := r.ev.count("create"); n != 0 {
		t.Fatalf("created %d mirrors before any Resume (fail closed)", n)
	}
	ack := r.resume(t, 1)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_RESUMED)
	if ack.GetEpoch() != 1 || ack.GetCommand() != hcpb.Command_COMMAND_RESUME || ack.GetNodeName() != testNode {
		t.Fatalf("ack = %v", ack)
	}
	before(t, r.ev, "create a", "ready a")
}

func TestResume_WhileLentNewGuestGetsMirror(t *testing.T) {
	r := newRig(t)
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.host.addGuest("late")
	r.srv.GuestWaiting(nil)
	eventually(t, func() bool { return r.ev.index("ready late") >= 0 })
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	if r.srv.Lent() {
		t.Fatal("still lent after Vacate")
	}
	r.host.addGuest("later")
	r.srv.GuestWaiting(nil)
	time.Sleep(100 * time.Millisecond)
	if r.ev.index("create later") >= 0 {
		t.Fatal("mirror created after Vacate")
	}
}

func TestVacate_NotReadyConfirmedBeforeSuspend(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	ack := r.vacate(t, 2, time.Second)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_VACATED)
	before(t, r.ev, "hold a "+mirror.ReasonSuspending, "confirm a")
	before(t, r.ev, "confirm a", "epoch 1")
	before(t, r.ev, "epoch 1", "suspend a-m 1")
	before(t, r.ev, "suspend a-m 1", "hold a "+mirror.ReasonSuspended)
	if len(ack.GetGuests()) != 1 || ack.GetGuests()[0].GetOutcome() != hcpb.Outcome_OUTCOME_VACATED {
		t.Fatalf("guests = %v", ack.GetGuests())
	}
}

func TestResume_ResumeBeforeReady(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	n := len(r.ev.list())
	wantOutcome(t, r.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	after := r.ev.list()[n:]
	iResume, iReady := slices.Index(after, "resume a-m 2"), slices.Index(after, "ready a")
	if iResume < 0 || iReady < 0 || iResume > iReady || slices.Contains(after, "create a") {
		t.Fatalf("want resume then ready, no create; events %v", after)
	}
}

func TestVacate_Idempotent(t *testing.T) {
	r := newRig(t)
	wantOutcome(t, r.vacate(t, 1, time.Second), hcpb.Outcome_OUTCOME_VACATED) // nothing to vacate
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 2), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, r.vacate(t, 3, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	wantOutcome(t, r.vacate(t, 3, time.Second), hcpb.Outcome_OUTCOME_VACATED) // retry, same epoch
	wantOutcome(t, r.vacate(t, 4, time.Second), hcpb.Outcome_OUTCOME_VACATED) // already suspended
	if n := r.ev.count("suspend "); n != 1 {
		t.Fatalf("suspended %d times, want 1; events %v", n, r.ev.list())
	}
}

func TestVacate_DeleteWaitsUntilMirrorGone(t *testing.T) {
	fix := newRig(t, withoutFreezer())
	fix.host.addGuest("a")
	wantOutcome(t, fix.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	gone := make(chan struct{})
	fix.host.mu.Lock()
	fix.host.gone = gone
	fix.host.mu.Unlock()
	done := make(chan *hcpb.HostAck, 1)
	go func() { done <- fix.vacate(t, 2, 2*time.Second) }()
	eventually(t, func() bool { return fix.ev.index("vacate-delete a") >= 0 })
	select {
	case ack := <-done:
		t.Fatalf("acked %s while the mirror was still terminating", ack.GetOutcome())
	case <-time.After(100 * time.Millisecond):
	}
	close(gone)
	wantOutcome(t, <-done, hcpb.Outcome_OUTCOME_VACATED)
	before(t, fix.ev, "confirm a", "vacate-delete a")
	before(t, fix.ev, "vacate-delete a", "gone a-m")
	// The next Resume creates a new mirror.
	wantOutcome(t, fix.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	if n := fix.ev.count("create a"); n != 2 {
		t.Fatalf("creates = %d, want 2", n)
	}
}

func TestVacate_KillWhenSuspendFails(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.freezer.failSuspend = errors.New("boom")
	ack := r.vacate(t, 2, time.Second)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_VACATED) // killed and gone: the accelerator is clear
	before(t, r.ev, "suspend-failed a-m", "agent-kill a-m")
	before(t, r.ev, "agent-kill a-m", "kill a-m")
	before(t, r.ev, "kill a-m", "gone a-m")
}

func TestVacate_FailedWhenKilledMirrorStays(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.host.mu.Lock()
	r.host.gone = make(chan struct{}) // never closed
	r.host.mu.Unlock()
	r.freezer.failSuspend = errors.New("boom")
	ack := r.vacate(t, 2, time.Second)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_FAILED)
}

func TestFencing_StaleEpoch(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.vacate(t, 5, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	ack := r.resume(t, 3)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_STALE_EPOCH)
	if ack.GetCurrentEpoch() != 5 {
		t.Fatalf("current_epoch = %d, want 5", ack.GetCurrentEpoch())
	}
	wantOutcome(t, r.resume(t, 5), hcpb.Outcome_OUTCOME_STALE_EPOCH) // same epoch, other command
	if r.ev.count("create") != 0 {
		t.Fatal("a stale Resume acted")
	}
	wantOutcome(t, r.resume(t, 6), hcpb.Outcome_OUTCOME_RESUMED)
}

func TestFencing_VacateAbortsResume(t *testing.T) {
	r := newRig(t)
	r.host.neverUp = true // the Resume waits for an engine that never serves
	r.host.addGuest("a")
	res := make(chan *hcpb.HostAck, 1)
	go func() { res <- r.resume(t, 1) }()
	eventually(t, func() bool { return r.ev.index("create a") >= 0 })
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	wantOutcome(t, <-res, hcpb.Outcome_OUTCOME_ABORTED)
	if r.ev.index("ready a") >= 0 {
		t.Fatal("guest released Ready")
	}
}

func TestFencing_HigherVacateJoins(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.freezer.suspendFor = 150 * time.Millisecond
	acks := make(chan *hcpb.HostAck, 2)
	go func() { acks <- r.vacate(t, 2, time.Second) }()
	eventually(t, func() bool { return r.ev.index("epoch 1") >= 0 })
	go func() { acks <- r.vacate(t, 3, time.Second) }()
	for range 2 {
		ack := <-acks
		wantOutcome(t, ack, hcpb.Outcome_OUTCOME_VACATED)
		if ack.GetCurrentEpoch() != 3 {
			t.Fatalf("current_epoch = %d, want 3", ack.GetCurrentEpoch())
		}
	}
	if n := r.ev.count("suspend "); n != 1 {
		t.Fatalf("suspended %d times, want 1 (joined)", n)
	}
}

func TestRefusesOtherNode(t *testing.T) {
	r := newRig(t)
	_, err := r.srv.Vacate(context.Background(), &hcpb.VacateRequest{
		GroupId: testGroup, NodeName: "other", Epoch: 9, Deadline: timestamppb.New(time.Now().Add(time.Second)),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want FailedPrecondition", err)
	}
	wantOutcome(t, r.vacate(t, 1, time.Second), hcpb.Outcome_OUTCOME_VACATED) // epoch 9 not recorded
}

func TestResume_RefusedForOtherGroup(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	r.group = "other"
	ack := r.resume(t, 1)
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_FAILED)
	if r.ev.count("create") != 0 {
		t.Fatal("created a mirror for another group")
	}
	r.group = ""
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_FAILED) // no group: fail closed
	r.group = testGroup
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED) // epoch 1 was not recorded
}

func TestVacate_ActsForOtherGroup(t *testing.T) {
	r := newRig(t)
	r.host.addGuest("a")
	wantOutcome(t, r.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	r.group = "other"
	wantOutcome(t, r.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	if r.ev.count("suspend ") != 1 {
		t.Fatal("did not vacate")
	}
}

func TestResume_RepliesBeforeCallDeadline(t *testing.T) {
	fix := newRig(t, func(c *hostcmd.Config, _ *rig) { c.ReplyMargin = 50 * time.Millisecond })
	fix.host.addGuest("a")
	wantOutcome(t, fix.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, fix.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	fix.freezer.resumeFor = 600 * time.Millisecond // a resume longer than one attempt

	req := &hcpb.ResumeRequest{
		GroupId: testGroup, NodeName: testNode, Epoch: 3, Deadline: timestamppb.New(time.Now().Add(5 * time.Second)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	start := time.Now()
	ack, err := fix.srv.Resume(ctx, req)
	cancel()
	if err != nil {
		t.Fatalf("Resume past the call deadline: %v (want an ack before it)", err)
	}
	if took := time.Since(start); took >= 200*time.Millisecond {
		t.Fatalf("ack after %s, want before the 200ms call deadline", took)
	}
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_UNSPECIFIED)
	if ack.GetEpoch() != 3 || ack.GetCommand() != hcpb.Command_COMMAND_RESUME {
		t.Fatalf("ack = %v", ack)
	}

	// Calling again with the same epoch joins the running resume.
	ack, err = fix.srv.Resume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, ack, hcpb.Outcome_OUTCOME_RESUMED)
	if n := fix.ev.count("resume a-m"); n != 1 {
		t.Fatalf("resume ran %d times, want 1 (joined)", n)
	}
}
