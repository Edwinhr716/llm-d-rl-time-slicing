package hostcmd_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

// memJournal is a hostcmd.Journal in memory. crash makes it refuse writes, as a killed process
// writes nothing more.
type memJournal struct {
	mu     sync.Mutex
	rec    *hostcmd.Record
	dead   bool
	writes []string
}

func (j *memJournal) Load(context.Context) (*hostcmd.Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.rec == nil {
		return nil, nil //nolint:nilnil // no journal
	}
	cp := *j.rec
	return &cp, nil
}

func (j *memJournal) Save(_ context.Context, rec *hostcmd.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.dead {
		return errors.New("process is dead")
	}
	cp := *rec
	j.rec = &cp
	j.writes = append(j.writes, rec.Command+" "+rec.Phase+" "+rec.Outcome)
	return nil
}

func (j *memJournal) crash() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.dead = true
}

func (j *memJournal) revive() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.dead = false
}

func (j *memJournal) record() hostcmd.Record {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.rec == nil {
		return hostcmd.Record{}
	}
	return *j.rec
}

func withJournal(j *memJournal) rigOpt {
	return func(c *hostcmd.Config, _ *rig) { c.Journal = j }
}

func withFrozenReport() rigOpt {
	return func(c *hostcmd.Config, r *rig) { c.Freezer = reportingFreezer{r.freezer} }
}

// restart kills the server mid-whatever and starts a new one that restores from the journal
// and the host, as guest-kubelet does on start (provider.Recover).
func (r *rig) restart(t *testing.T, j *memJournal, opts ...rigOpt) hostcmd.Restored {
	t.Helper()
	j.crash()
	r.stop()
	j.revive()
	r.ev.add("restart")
	r.start(t, append([]rigOpt{withJournal(j)}, opts...)...)
	rec, err := j.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	guests, err := r.host.Guests()
	if err != nil {
		t.Fatal(err)
	}
	return r.srv.Restore(context.Background(), rec, guests)
}

// sinceRestart returns the events after the last restart.
func sinceRestart(ev *events) []string {
	all := ev.list()
	for i := len(all) - 1; i >= 0; i-- {
		if all[i] == "restart" {
			return all[i+1:]
		}
	}
	return all
}

func countIn(evs []string, prefix string) int {
	n := 0
	for _, e := range evs {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (h *fakeHost) setMirrorState(guest, state, epoch string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.guests[guest].Mirror.DeepCopy()
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	if state == "" {
		delete(m.Annotations, mirror.AnnotationSuspendState)
	} else {
		m.Annotations[mirror.AnnotationSuspendState] = state
	}
	m.Annotations[mirror.AnnotationGuestEpoch] = epoch
	h.guests[guest].Mirror = m
}

func (h *fakeHost) setGuestReady(guest string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := h.guests[guest].Pod.DeepCopy()
	p.Status.Phase = corev1.PodRunning
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	h.guests[guest].Pod = p
}

func (h *fakeHost) mirrorState(guest string) string { //nolint:unparam // generic over guests; the tests use one
	m := h.mirrorOf(guest)
	if m == nil {
		return "<none>"
	}
	s, _ := mirror.SuspendState(m)
	if s == "" {
		return "running"
	}
	return s
}

func TestRestore_JournalWritten(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j))
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	rec := j.record()
	if rec.Epoch != 2 || rec.Command != "COMMAND_VACATE" || rec.Phase != hostcmd.PhaseDone || rec.Outcome != "OUTCOME_VACATED" {
		t.Fatalf("journal = %+v", rec)
	}
	want := []string{
		"COMMAND_RESUME running ", "COMMAND_RESUME done OUTCOME_RESUMED",
		"COMMAND_VACATE running ", "COMMAND_VACATE done OUTCOME_VACATED",
	}
	j.mu.Lock()
	got := append([]string(nil), j.writes...)
	j.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("journal writes = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("journal writes = %q, want %q", got, want)
		}
	}
	if s := rg.host.mirrorState("a"); s != mirror.StateSuspended {
		t.Fatalf("mirror state = %s, want Suspended", s)
	}
}

func TestRestore_NoJournalFailsClosed(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j))
	rg.host.addGuest("a")
	got := rg.restart(t, j)
	if got.Epoch != 0 || got.Lent || got.Restarted {
		t.Fatalf("restored = %+v", got)
	}
	time.Sleep(100 * time.Millisecond)
	if rg.ev.count("create") != 0 || rg.srv.Lent() {
		t.Fatal("mirror created without a Resume after a start with no journal")
	}
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
}

// Killed while the freeze runs: the restarted server runs the vacate again with its epoch and
// the orchestrator's retry joins it. The guest is suspended once.
func TestRestore_MidVacate_Joins(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j), withFrozenReport())
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	rg.freezer.delays(300*time.Millisecond, 0)
	srv := rg.srv
	lost := make(chan error, 1) // the first try's answer, lost with the process
	go func() {
		lost <- ignoreAck(srv.Vacate(context.Background(), &hcpb.VacateRequest{
			GroupId: testGroup, NodeName: testNode, Epoch: 2, Deadline: timestampIn(5 * time.Second),
		}))
	}()
	eventually(t, func() bool { return rg.ev.index("state a-m "+mirror.StateSuspending) >= 0 })
	rg.freezer.delays(0, 0)
	got := rg.restart(t, j, withFrozenReport())
	if !got.Restarted || got.Epoch != 2 || got.Suspending != 1 {
		t.Fatalf("restored = %+v", got)
	}
	wantOutcome(t, rg.vacate(t, 2, 5*time.Second), hcpb.Outcome_OUTCOME_VACATED)
	if n := rg.ev.count("suspend a-m"); n != 1 {
		t.Fatalf("suspended %d times, want 1; events %v", n, rg.ev.list())
	}
	if s := rg.host.mirrorState("a"); s != mirror.StateSuspended {
		t.Fatalf("mirror state = %s, want Suspended", s)
	}
	if rg.ev.count("kill") != 0 {
		t.Fatal("guest killed")
	}
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_STALE_EPOCH)
}

// Killed after the freeze and before Suspended was written: the host wins, no second freeze.
func TestRestore_MidVacate_AlreadyFrozen(t *testing.T) {
	j := &memJournal{rec: &hostcmd.Record{
		Epoch: 2, Command: "COMMAND_VACATE", Phase: hostcmd.PhaseRunning,
		Deadline: time.Now().Add(5 * time.Second),
	}}
	rg := newRig(t, withJournal(j), withFrozenReport())
	rg.host.addGuest("a")
	if err := rg.host.Create(context.Background(), rg.host.guests["a"].Pod); err != nil {
		t.Fatal(err)
	}
	rg.host.setMirrorState("a", mirror.StateSuspending, "1")
	rg.freezer.setFrozen("a-m", true)
	got := rg.restart(t, j, withFrozenReport())
	if !got.Restarted {
		t.Fatalf("restored = %+v", got)
	}
	wantOutcome(t, rg.vacate(t, 2, 5*time.Second), hcpb.Outcome_OUTCOME_VACATED)
	evs := sinceRestart(rg.ev)
	if countIn(evs, "suspend ") != 0 || countIn(evs, "confirm") != 0 {
		t.Fatalf("frozen guest suspended again; events %v", evs)
	}
	if s := rg.host.mirrorState("a"); s != mirror.StateSuspended {
		t.Fatalf("mirror state = %s, want Suspended", s)
	}
}

// Killed while the thaw runs (the thaw finished on the node): the retry joins the restarted
// resume; no second thaw, the guest gets Ready, the node is lent.
func TestRestore_MidResume_AlreadyThawed(t *testing.T) {
	j := &memJournal{rec: &hostcmd.Record{
		Epoch: 3, Command: "COMMAND_RESUME", Phase: hostcmd.PhaseRunning,
		Deadline: time.Now().Add(time.Second),
	}}
	rg := newRig(t, withJournal(j), withFrozenReport())
	rg.host.addGuest("a")
	if err := rg.host.Create(context.Background(), rg.host.guests["a"].Pod); err != nil {
		t.Fatal(err)
	}
	rg.host.setMirrorState("a", mirror.StateResuming, "2")
	got := rg.restart(t, j, withFrozenReport())
	if !got.Restarted || !got.Lent || got.Resuming != 1 {
		t.Fatalf("restored = %+v", got)
	}
	wantOutcome(t, rg.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	evs := sinceRestart(rg.ev)
	if countIn(evs, "resume ") != 0 {
		t.Fatalf("thawed again; events %v", evs)
	}
	if countIn(evs, "ready a") != 1 {
		t.Fatalf("guest not released Ready once; events %v", evs)
	}
	if s := rg.host.mirrorState("a"); s != "running" {
		t.Fatalf("mirror state = %s, want running", s)
	}
	eventually(t, rg.srv.Lent)
}

// Killed while the thaw runs (the node still frozen): the restarted resume thaws it once.
func TestRestore_MidResume_StillFrozen(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j), withFrozenReport())
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	rg.freezer.delays(0, 300*time.Millisecond)
	srv := rg.srv
	lost := make(chan error, 1) // the first try's answer, lost with the process
	go func() {
		lost <- ignoreAck(srv.Resume(context.Background(), &hcpb.ResumeRequest{
			GroupId: testGroup, NodeName: testNode, Epoch: 3, Deadline: timestampIn(2 * time.Second),
		}))
	}()
	eventually(t, func() bool { return rg.ev.index("state a-m "+mirror.StateResuming) >= 0 })
	rg.freezer.delays(0, 0)
	got := rg.restart(t, j, withFrozenReport())
	if !got.Restarted || got.Resuming != 1 {
		t.Fatalf("restored = %+v", got)
	}
	wantOutcome(t, rg.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	if n := countIn(sinceRestart(rg.ev), "resume a-m"); n != 1 {
		t.Fatalf("thawed %d times after the restart, want 1; events %v", n, rg.ev.list())
	}
	if s := rg.host.mirrorState("a"); s != "running" {
		t.Fatalf("mirror state = %s, want running", s)
	}
}

// Killed while held: the guest stays suspended and NotReady, a guest that arrives gets no
// mirror, the retry of the last vacate gets its ack without acting, and the next Resume works.
func TestRestore_Held_StaysHeld(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j))
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	rg.host.addGuest("b")
	got := rg.restart(t, j)
	if got.Lent || got.Restarted || got.Suspended != 1 || got.Epoch != 2 {
		t.Fatalf("restored = %+v", got)
	}
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
	time.Sleep(100 * time.Millisecond)
	evs := sinceRestart(rg.ev)
	for _, bad := range []string{"create", "confirm", "suspend ", "resume ", "ready", "epoch"} {
		if countIn(evs, bad) != 0 {
			t.Fatalf("held node acted after a restart (%s); events %v", bad, evs)
		}
	}
	if countIn(evs, "restore-gate a false "+mirror.ReasonSuspended) != 1 {
		t.Fatalf("suspended guest not held; events %v", evs)
	}
	wantOutcome(t, rg.resume(t, 2), hcpb.Outcome_OUTCOME_STALE_EPOCH)
	wantOutcome(t, rg.resume(t, 3), hcpb.Outcome_OUTCOME_RESUMED)
	evs = sinceRestart(rg.ev)
	if countIn(evs, "resume a-m 2") != 1 || countIn(evs, "create b") != 1 {
		t.Fatalf("resume after the restart: events %v", evs)
	}
}

// Killed while lent: the node is lent again at once (the orchestrator never resends Resume), a
// Ready guest keeps Ready without a flap, and a guest that arrives gets its mirror.
func TestRestore_Lent_Relends(t *testing.T) {
	j := &memJournal{}
	rg := newRig(t, withJournal(j))
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	rg.host.setGuestReady("a")
	got := rg.restart(t, j)
	if !got.Lent || got.Restarted || got.Released != 1 {
		t.Fatalf("restored = %+v", got)
	}
	eventually(t, rg.srv.Lent)
	rg.host.addGuest("b")
	rg.srv.GuestWaiting(nil)
	eventually(t, func() bool { return rg.ev.index("create b") >= 0 })
	evs := sinceRestart(rg.ev)
	if countIn(evs, "hold a") != 0 || countIn(evs, "ready a") != 0 || countIn(evs, "create a") != 0 {
		t.Fatalf("lent guest flapped or was re-created; events %v", evs)
	}
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED) // the retry: recorded ack
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
}

// A lent node restarted while a Resume had left a mirror marked suspended (the resume of that
// guest was cut off and its retry never came): the serve loop resumes it.
func TestRestore_Lent_ResumesLeftoverSuspended(t *testing.T) {
	j := &memJournal{rec: &hostcmd.Record{
		Epoch: 5, Command: "COMMAND_RESUME", Phase: hostcmd.PhaseDone,
		Outcome: "OUTCOME_RESUMED",
	}}
	rg := newRig(t, withJournal(j), withFrozenReport())
	rg.host.addGuest("a")
	if err := rg.host.Create(context.Background(), rg.host.guests["a"].Pod); err != nil {
		t.Fatal(err)
	}
	rg.host.setMirrorState("a", mirror.StateSuspended, "4")
	rg.freezer.setFrozen("a-m", true)
	got := rg.restart(t, j, withFrozenReport())
	if !got.Lent || got.Suspended != 1 {
		t.Fatalf("restored = %+v", got)
	}
	eventually(t, func() bool { return rg.ev.index("ready a") >= 0 })
	before(t, rg.ev, "resume a-m 5", "ready a")
}

// No command acts before Ready is closed (the guest informer has synced).
func TestRestore_ReadyGatesCommands(t *testing.T) {
	ready := make(chan struct{})
	rg := newRig(t, func(c *hostcmd.Config, _ *rig) { c.Ready = ready })
	rg.host.addGuest("a")
	acks := make(chan *hcpb.HostAck, 1)
	go func() { acks <- rg.resume(t, 1) }()
	time.Sleep(100 * time.Millisecond)
	if rg.ev.count("create") != 0 {
		t.Fatal("a command acted before Ready")
	}
	close(ready)
	wantOutcome(t, <-acks, hcpb.Outcome_OUTCOME_RESUMED)
}

// A retry of a failed command runs again, as before M5; only a success is answered from memory.
func TestRestore_FailedCommandRunsAgain(t *testing.T) {
	rg := newRig(t)
	rg.host.addGuest("a")
	wantOutcome(t, rg.resume(t, 1), hcpb.Outcome_OUTCOME_RESUMED)
	rg.host.mu.Lock()
	rg.host.failState = errors.New("conflict storm") // the suspend state cannot be written: kill
	rg.host.gone = make(chan struct{})               // and the killed mirror stays: FAILED
	rg.host.mu.Unlock()
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_FAILED)
	rg.host.mu.Lock()
	rg.host.failState = nil
	close(rg.host.gone)
	rg.host.mu.Unlock()
	wantOutcome(t, rg.vacate(t, 2, time.Second), hcpb.Outcome_OUTCOME_VACATED)
}

func timestampIn(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(time.Now().Add(d)) }

func (f *testFreezer) delays(suspend, resume time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspendFor, f.resumeFor = suspend, resume
}

// RestoreGate records the gate Restore re-derived.
func (h *fakeHost) RestoreGate(guest *corev1.Pod, released bool, reason string) {
	h.ev.add("restore-gate %s %t %s", guest.Name, released, reason)
}

func ignoreAck(_ *hcpb.HostAck, err error) error { return err }
