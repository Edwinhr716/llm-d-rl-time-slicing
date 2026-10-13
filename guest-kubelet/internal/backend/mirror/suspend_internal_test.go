package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

// fakeAgent is a snapshot agent in memory. It records each call and, at each Suspend, whether
// the guest was already NotReady in the guest lister.
type fakeAgent struct {
	mu          sync.Mutex
	calls       []string
	jobs        map[string]freeze.Job
	deadlines   []time.Time
	readyAtCall []bool
	guestReady  func() bool

	jobsErr, suspendErr, resumeErr, killErr error
	// hang makes the named call ("suspend", "resume", "kill", "suspendall", "resumeall") block
	// until its ctx ends, as an agent that never answers.
	hang string
	// hostTargets overrides the target of a job in SuspendAll/ResumeAll.
	hostTargets map[string]freeze.Target
	// hostErrs is returned by successive SuspendAll/ResumeAll calls (nil: success).
	hostErrs []error
	// onKill runs when Kill succeeds, before it answers (the agent stops the mirror first).
	onKill func()
}

func newFakeAgent() *fakeAgent { return &fakeAgent{jobs: map[string]freeze.Job{}} }

func (f *fakeAgent) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeAgent) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeAgent) setJob(id, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[id] = freeze.Job{State: state}
}

func (f *fakeAgent) block(ctx context.Context, call string) error {
	f.mu.Lock()
	h := f.hang
	f.mu.Unlock()
	if h != call {
		return nil
	}
	<-ctx.Done()
	return fmt.Errorf("no answer: %w", ctx.Err())
}

func (f *fakeAgent) Jobs(context.Context) (map[string]freeze.Job, error) {
	f.record("status")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobsErr != nil {
		return nil, f.jobsErr
	}
	out := make(map[string]freeze.Job, len(f.jobs))
	for k, v := range f.jobs {
		out[k] = v
	}
	return out, nil
}

func (f *fakeAgent) Suspend(ctx context.Context, jobID string, epoch int64, deadline time.Time) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("suspend:%d", epoch))
	f.deadlines = append(f.deadlines, deadline)
	if f.guestReady != nil {
		f.readyAtCall = append(f.readyAtCall, f.guestReady())
	}
	err := f.suspendErr
	f.mu.Unlock()
	if berr := f.block(ctx, "suspend"); berr != nil {
		return "", berr
	}
	if err != nil {
		return "", err
	}
	f.setJob(jobID, freeze.JobSuspended)
	return freeze.OutcomeSuspended, nil
}

func (f *fakeAgent) Resume(ctx context.Context, jobID string, epoch int64, deadline time.Time) error {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("resume:%d", epoch))
	f.deadlines = append(f.deadlines, deadline)
	err := f.resumeErr
	f.mu.Unlock()
	if berr := f.block(ctx, "resume"); berr != nil {
		return berr
	}
	if err != nil {
		return err
	}
	f.setJob(jobID, "RUNNING")
	return nil
}

func (f *fakeAgent) Kill(ctx context.Context, jobID string, deadline time.Time, _ string) error {
	f.mu.Lock()
	f.calls = append(f.calls, "kill")
	f.deadlines = append(f.deadlines, deadline)
	err := f.killErr
	f.mu.Unlock()
	if berr := f.block(ctx, "kill"); berr != nil {
		return berr
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	delete(f.jobs, jobID)
	onKill := f.onKill
	f.mu.Unlock()
	if onKill != nil {
		onKill()
	}
	return nil
}

func (f *fakeAgent) host(ctx context.Context, call string, epoch int64, outcome string) (*freeze.HostResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("%s:%d", call, epoch))
	var err error
	if len(f.hostErrs) > 0 {
		err, f.hostErrs = f.hostErrs[0], f.hostErrs[1:]
	}
	f.mu.Unlock()
	if berr := f.block(ctx, call); berr != nil {
		return nil, berr
	}
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	res := &freeze.HostResult{Complete: true, Targets: map[string]freeze.Target{}}
	for id := range f.jobs {
		t := freeze.Target{Done: true, Outcome: outcome}
		if o, ok := f.hostTargets[id]; ok {
			t = o
		}
		if !t.Done {
			res.Complete, res.Error = false, "a target failed"
		} else {
			state := freeze.JobSuspended
			if outcome == freeze.OutcomeResumed {
				state = "RUNNING"
			}
			f.jobs[id] = freeze.Job{State: state, Epoch: epoch}
		}
		res.Targets[id] = t
	}
	return res, nil
}

func (f *fakeAgent) SuspendAll(ctx context.Context, _ string, epoch int64, _ time.Time) (*freeze.HostResult, error) {
	return f.host(ctx, "suspendall", epoch, freeze.OutcomeSuspended)
}

func (f *fakeAgent) ResumeAll(ctx context.Context, _ string, epoch int64, _ time.Time) (*freeze.HostResult, error) {
	return f.host(ctx, "resumeall", epoch, freeze.OutcomeResumed)
}

// staleEpochError is a STALE_EPOCH refusal as freeze.LastEpoch reads it.
type staleEpochError struct{ last int64 }

func (e staleEpochError) Error() string { return fmt.Sprintf("STALE_EPOCH: last epoch %d", e.last) }

func (e staleEpochError) LastEpoch() (int64, bool) { return e.last, true }

func (f *fakeAgent) String() string { return strings.Join(f.callList(), ",") }

func (f *fakeAgent) lastDeadline() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadlines[len(f.deadlines)-1]
}

func agentOptions(fa *fakeAgent) Options {
	opts := testOptions()
	opts.Suspend = SuspendOptions{
		Agent: fa, NotReadyTimeout: 2 * time.Second, ReadyTimeout: time.Second,
		NoticeWindow: 30 * time.Second, KillBudget: 3 * time.Second,
		ReadyCheck: func(context.Context, *corev1.Pod, *corev1.Pod) error { fa.record("readycheck"); return nil },
	}
	return opts
}

// suspendHarness runs a Ready CPU guest "vllm" with a running mirror and a fake agent that
// lists its job.
func suspendHarness(t *testing.T) (*harness, *fakeAgent) {
	t.Helper()
	fa := newFakeAgent()
	hrn := newHarness(t, ref(agentOptions(fa)))
	hrn.startGuest("vllm", "g1", fa)
	fa.guestReady = func() bool {
		cur, err := hrn.b.guests.Pods("ns").Get("vllm")
		return err == nil && IsReady(cur)
	}
	return hrn, fa
}

// startGuest creates a Ready CPU guest with a running mirror, and registers the mirror's job
// with the fake agent (as the agent's pod watcher would).
func (h *harness) startGuest(name, uid string, fa *fakeAgent) {
	h.t.Helper()
	guest := cpuGuest(uid)
	guest.Name = name
	guest.Spec.ReadinessGates = nil // an unset gate would hold Ready false throughout
	h.mu.Lock()
	h.addGuest(guest)
	h.mu.Unlock()
	if err := h.b.Create(context.Background(), guest); err != nil {
		h.t.Fatal(err)
	}
	mirrorPod := h.mirror(name + Suffix)
	if fa != nil {
		fa.setJob(mirrorPod.Labels[LabelJobID], "RUNNING")
	}
	mirrorPod.Status = runningMirror().Status
	if _, err := h.client.CoreV1().Pods("ns").UpdateStatus(context.Background(), mirrorPod, metav1.UpdateOptions{}); err != nil {
		h.t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cur, err := h.b.mirrors.Pods("ns").Get(name + Suffix); err == nil && cur.Status.PodIP != "" {
			break
		}
		if time.Now().After(deadline) {
			h.t.Fatal("informer never saw the running mirror")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// From here on the guest lister follows what the backend emits. Start from Ready, as the
	// library last wrote it.
	h.mu.Lock()
	defer h.mu.Unlock()
	ready := guest.DeepCopy()
	ready.Status = runningMirror().Status
	if err := h.guests.Update(ready); err != nil {
		h.t.Fatal(err)
	}
	h.writeBack = true
}

// settled waits until the last emitted status of guest "vllm" satisfies ok and returns it.
func (h *harness) settled(what string, ok func(*corev1.Pod) bool) *corev1.Pod {
	h.t.Helper()
	return h.settledFor("vllm", what, ok)
}

func (h *harness) settledFor(name, what string, ok func(*corev1.Pod) bool) *corev1.Pod {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var last *corev1.Pod
		for _, p := range h.emittedPods() {
			if p.Name == name {
				last = p
			}
		}
		if last != nil && ok(last) {
			return last
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("last emitted %s never became %s", name, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func notReady(p *corev1.Pod) bool { return !IsReady(p) }

func failedWith(reason string) func(*corev1.Pod) bool {
	return func(p *corev1.Pod) bool { return p.Status.Phase == corev1.PodFailed && p.Status.Reason == reason }
}

// neverReadyWhileSuspended checks every status emitted so far: a guest in a suspend state
// (condition timeslice.io/suspended present) or Failed is never Ready.
func (h *harness) neverReadyWhileSuspended() {
	h.t.Helper()
	for i, p := range h.emittedPods() {
		suspended := findCondition(p.Status.Conditions, ConditionSuspended) != nil
		if (suspended || p.Status.Phase == corev1.PodFailed) && IsReady(p) {
			h.t.Fatalf("emitted status %d of %s is Ready while %s: %+v", i, p.Name, p.Status.Phase, p.Status.Conditions)
		}
	}
}

func (h *harness) mirrorGone(name string) {
	h.t.Helper()
	if m := h.mirror(name); m != nil {
		h.t.Fatalf("mirror %s must be deleted: %v", name, m.Annotations)
	}
}

func TestAgentSuspendResume_NotReadyBeforeAgentCall(t *testing.T) {
	hrn, fa := suspendHarness(t)
	m := hrn.mirror("vllm-m")
	if m.Labels[LabelJobID] != "g1-0" || m.Labels[LabelRole] != RoleBackground || m.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("mirror labels %v, restartPolicy %s", m.Labels, m.Spec.RestartPolicy)
	}
	start := time.Now()
	res, err := hrn.b.Suspend(context.Background(), "ns", "vllm", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if res.State != StateSuspended || res.Epoch != 1 || res.Outcome != freeze.OutcomeSuspended {
		t.Fatalf("suspend result: %+v", res)
	}
	if want := start.Add(27 * time.Second); fa.lastDeadline().Before(want) || fa.lastDeadline().After(want.Add(2*time.Second)) {
		t.Fatalf("deadline %s, want now + N - K = %s", fa.lastDeadline(), want)
	}
	if len(fa.readyAtCall) != 1 || fa.readyAtCall[0] {
		t.Fatalf("the agent call must come after NotReady is visible: ready at call = %v", fa.readyAtCall)
	}
	m = hrn.mirror("vllm-m")
	if m.Annotations[AnnotationSuspendState] != StateSuspended || m.Annotations[AnnotationGuestEpoch] != "1" {
		t.Fatalf("mirror annotations: %v", m.Annotations)
	}
	st := hrn.settled("suspended", func(p *corev1.Pod) bool {
		c := findCondition(p.Status.Conditions, ConditionSuspended)
		return c != nil && c.Status == corev1.ConditionTrue
	}).Status
	cs := st.ContainerStatuses[0]
	if st.Phase != corev1.PodRunning || cs.Ready || cs.State.Waiting == nil || cs.State.Waiting.Reason != StateSuspended {
		t.Fatalf("suspended guest status: %+v", st)
	}
	if res, err := hrn.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil || !res.Noop {
		t.Fatalf("second suspend: %+v %v", res, err)
	}
	within := time.Now().Add(10 * time.Second)
	res, err = hrn.b.Resume(context.Background(), "ns", "vllm", within)
	if err != nil {
		t.Fatal(err)
	}
	if res.Epoch != 2 || res.State != "Running" || !fa.lastDeadline().Equal(within) {
		t.Fatalf("resume result: %+v, deadline %s", res, fa.lastDeadline())
	}
	if got, want := fa.String(), "status,suspend:1,resume:2,readycheck"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	hrn.settled("ready", IsReady)
	hrn.neverReadyWhileSuspended()
}

func TestAgentSuspend_NotReadyUnconfirmedRevertsWithoutAgentCall(t *testing.T) {
	rig, fa := suspendHarness(t)
	rig.mu.Lock()
	rig.writeBack = false // the guest's NotReady never reaches the API
	rig.mu.Unlock()
	rig.b.opts.Suspend.NotReadyTimeout = 200 * time.Millisecond
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil {
		t.Fatal("want an error")
	}
	if len(fa.callList()) != 0 {
		t.Fatalf("no agent call may be made before NotReady is confirmed: %s", fa)
	}
	if _, ok := rig.mirror("vllm-m").Annotations[AnnotationSuspendState]; ok {
		t.Fatal("state must be cleared")
	}
}

func TestAgentSuspend_FailureRunsKillSequence(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.suspendErr = errors.New("BACKEND_ERROR: checkpoint failed")
	res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{})
	if err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,suspend:1,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm-m")
	p := rig.settled("Failed", failedWith(ReasonAgentKilled))
	if !strings.Contains(p.Status.Message, "checkpoint failed") {
		t.Fatalf("message = %q", p.Status.Message)
	}
	rig.neverReadyWhileSuspended()
}

func TestAgentSuspend_HangEndsAtDeadlineWithKill(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.hang = "suspend"
	start := time.Now()
	res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Now().Add(300*time.Millisecond))
	if err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("a hung agent must end at the deadline plus the answer grace; took %s", d)
	}
	if got, want := fa.String(), "status,suspend:1,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
	rig.neverReadyWhileSuspended()
}

func TestAgentSuspend_UnimplementedKills(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.suspendErr = fmt.Errorf("Suspend: %w", freeze.ErrUnimplemented)
	if res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
}

func TestAgentSuspend_KillUnconfirmedStillDeletes(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.suspendErr, fa.hang = errors.New("crash"), "kill"
	start := time.Now()
	res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{})
	if err == nil || !strings.Contains(res.Killed, "not confirmed") {
		t.Fatalf("want an unconfirmed kill: %+v %v", res, err)
	}
	if d := time.Since(start); d > 3*time.Second+2*time.Second {
		t.Fatalf("the kill must be bounded by K plus the grace; took %s", d)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
}

func TestAgentSuspend_StatusFailureKills(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.jobsErr = errors.New("unavailable")
	if res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm-m")
}

func TestAgentSuspend_UnlistedJobIsDeletedNotSuspended(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.mu.Lock()
	fa.jobs = map[string]freeze.Job{}
	fa.mu.Unlock()
	res, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{})
	if err != nil || !strings.HasPrefix(res.Killed, ReasonNoContext) {
		t.Fatalf("want a no-context delete: %+v %v", res, err)
	}
	if got, want := fa.String(), "status"; got != want {
		t.Fatalf("calls = %s, want %s (no Suspend, no Kill)", got, want)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonNoContext))
}

func TestAgentResume_FailureRunsKillSequence(t *testing.T) {
	rig, fa := suspendHarness(t)
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	fa.resumeErr = errors.New("VERIFY_FAILED: restore")
	if res, err := rig.b.Resume(context.Background(), "ns", "vllm", time.Time{}); err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,suspend:1,resume:2,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
	rig.neverReadyWhileSuspended()
}

func TestAgentResume_ReadyCheckFailureKills(t *testing.T) {
	rig, fa := suspendHarness(t)
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	rig.b.opts.Suspend.ReadyCheck = func(context.Context, *corev1.Pod, *corev1.Pod) error { return errors.New("503") }
	if res, err := rig.b.Resume(context.Background(), "ns", "vllm", time.Time{}); err == nil || res.Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,suspend:1,resume:2,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
	rig.neverReadyWhileSuspended()
}

func TestAgentSuspend_NoAgentConfigured(t *testing.T) {
	rig := newHarness(t, ref(testOptions()))
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil {
		t.Fatal("want an error without a snapshot agent")
	}
}

func TestAgentDelete_KillsSuspendedMirrorFirst(t *testing.T) {
	rig, fa := suspendHarness(t)
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err != nil {
		t.Fatal(err)
	}
	g, err := rig.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.b.Delete(context.Background(), g.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if got, want := fa.String(), "status,suspend:1,kill"; got != want {
		t.Fatalf("calls = %s, want the delete to kill through the agent first", got)
	}
}

func TestAgentSuspendAll_PerTargetKill(t *testing.T) {
	rig, fa := suspendHarness(t)
	rig.startGuest("vllm2", "g2", fa)
	fa.hostTargets = map[string]freeze.Target{"g2-0": {Error: "DEADLINE_EXCEEDED: checkpoint"}}
	res, err := rig.b.SuspendAll(context.Background(), time.Time{})
	if err == nil {
		t.Fatal("want the host error")
	}
	if res.Epoch != 1 || res.Guests["ns/vllm"].State != StateSuspended || res.Guests["ns/vllm2"].Killed == "" {
		t.Fatalf("host result: %+v", res)
	}
	if got, want := fa.String(), "status,suspendall:1,kill"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.mirrorGone("vllm2-m")
	rig.settledFor("vllm2", "Failed", failedWith(ReasonAgentKilled))
	if m := rig.mirror("vllm-m"); m.Annotations[AnnotationSuspendState] != StateSuspended ||
		m.Annotations[AnnotationGuestEpoch] != "1" {
		t.Fatalf("vllm-m annotations: %v", m.Annotations)
	}

	fa.hostTargets = nil
	res, err = rig.b.ResumeAll(context.Background(), time.Time{})
	if err != nil || res.Guests["ns/vllm"].State != "Running" || res.Epoch != 2 {
		t.Fatalf("resume-all: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,suspendall:1,kill,resumeall:2,readycheck"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	rig.settled("ready", IsReady)
	rig.neverReadyWhileSuspended()
}

func TestAgentSuspendAll_StaleEpochRetriesOnce(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.hostErrs = []error{fmt.Errorf("SuspendAll: %w", staleEpochError{last: 10})}
	res, err := rig.b.SuspendAll(context.Background(), time.Time{})
	if err != nil || res.Epoch != 11 {
		t.Fatalf("host result: %+v %v", res, err)
	}
	if got, want := fa.String(), "status,suspendall:1,suspendall:11"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	if m := rig.mirror("vllm-m"); m.Annotations[AnnotationGuestEpoch] != "11" ||
		m.Annotations[AnnotationSuspendState] != StateSuspended {
		t.Fatalf("mirror annotations: %v", m.Annotations)
	}
}

func TestAgentSuspendAll_CallFailureKillsAll(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.hang = "suspendall"
	res, err := rig.b.SuspendAll(context.Background(), time.Now().Add(200*time.Millisecond))
	if err == nil || res.Guests["ns/vllm"].Killed == "" {
		t.Fatalf("want the kill sequence: %+v %v", res, err)
	}
	rig.mirrorGone("vllm-m")
	rig.settled("Failed", failedWith(ReasonAgentKilled))
	rig.neverReadyWhileSuspended()
}

func TestAgentAdmission_OneRestorePlusCheckpointsFitNMinusK(t *testing.T) {
	rig, _ := suspendHarness(t)
	rig.b.opts.Suspend.CheckpointEstimate, rig.b.opts.Suspend.RestoreEstimate = 13*time.Second, 6500*time.Millisecond
	g2 := cpuGuest("g2")
	g2.Name = "vllm2"
	if err := rig.b.Create(context.Background(), g2); err == nil || !strings.Contains(err.Error(), "not admitted") {
		t.Fatalf("two mirrors need 32.5s > 27s: want a refusal, got %v", err)
	}
	if rig.mirror("vllm2-m") != nil {
		t.Fatal("no mirror may be created")
	}
	rig.b.opts.Suspend.NoticeWindow = 60 * time.Second
	if err := rig.b.Create(context.Background(), g2); err != nil {
		t.Fatalf("with N = 60s it fits: %v", err)
	}
}

func TestAgentJobID_NewAttemptPerIncarnation(t *testing.T) {
	rig, _ := suspendHarness(t)
	g, err := rig.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.CoreV1().Pods("ns").Delete(context.Background(), "vllm-m", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := rig.b.mirrorOf(g); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("informer never saw the delete")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := rig.b.Create(context.Background(), g.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if id := rig.mirror("vllm-m").Labels[LabelJobID]; id != "g1-1" {
		t.Fatalf("job id = %q, want g1-1", id)
	}
}

// The agent's Kill can stop the mirror before the kill sequence records the kill. The library
// never updates a Failed guest again, so a stopped mirror of a running kill sequence must already
// translate to the kill reason, not to the mirror's own Failed status. Ported from STACK-TODAY
// VK-A4 (same name); here the running kill is the timeslice.io/killing annotation.
func TestTranslate_StoppedKillingMirrorIsKilled(t *testing.T) {
	rig, _ := suspendHarness(t)
	ctx := context.Background()
	guest, err := rig.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.b.setSuspendState(ctx, guest, StateSuspending, bumpEpoch); err != nil {
		t.Fatal(err)
	}
	mirrorPod, err := rig.b.mutateMirror(ctx, guest, func(_ *corev1.Pod, a map[string]string) {
		a[AnnotationKilling] = "agent Suspend: BACKEND_ERROR"
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := rig.b.translate(guest, mirrorPod); p.Status.Phase == corev1.PodFailed || IsReady(p) {
		t.Fatalf("a running Killing mirror must translate to a live NotReady guest: %+v", p.Status)
	}
	stopped := mirrorPod.DeepCopy()
	stopped.Status.Phase = corev1.PodFailed
	killed := rig.b.translate(guest, stopped)
	if !failedWith(ReasonAgentKilled)(killed) || !strings.Contains(killed.Status.Message, "BACKEND_ERROR") {
		t.Fatalf("a stopped Killing mirror must translate to a killed guest: %+v", killed.Status)
	}
	deleting := mirrorPod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if p := rig.b.translate(guest, deleting); !failedWith(ReasonAgentKilled)(p) {
		t.Fatalf("a deleted Killing mirror must translate to a killed guest: %+v", p.Status)
	}
}

// A mirror that stops while Suspended was killed (a frozen process cannot exit by itself): the
// orchestrator's Kill at T, or the agent's. One that stops while Resuming may have crashed on its
// own and keeps the mirror's status.
func TestTranslate_StoppedSuspendedMirrorIsKilled(t *testing.T) {
	rig, _ := suspendHarness(t)
	ctx := context.Background()
	g, err := rig.b.guests.Pods("ns").Get("vllm")
	if err != nil {
		t.Fatal(err)
	}
	for state, killed := range map[string]bool{StateSuspended: true, StateResuming: false} {
		m, err := rig.b.setSuspendState(ctx, g, state, bumpEpoch)
		if err != nil {
			t.Fatal(err)
		}
		stopped := m.DeepCopy()
		stopped.Status.Phase = corev1.PodFailed
		if got := failedWith(ReasonAgentKilled)(rig.b.translate(g, stopped)); got != killed {
			t.Errorf("stopped while %s: killed = %v, want %v", state, got, killed)
		}
	}
}

// End to end: the agent's Kill stops the mirror (phase Failed, seen by the informer) before it
// answers. Every Failed status the guest ever gets carries the kill reason.
func TestAgentSuspend_KillStopsMirrorFirstGuestStillKilled(t *testing.T) {
	rig, fa := suspendHarness(t)
	fa.suspendErr = errors.New("BACKEND_ERROR: checkpoint failed")
	fa.onKill = func() {
		m := rig.mirror("vllm-m").DeepCopy()
		m.Status.Phase = corev1.PodFailed
		if _, err := rig.client.CoreV1().Pods("ns").UpdateStatus(context.Background(), m, metav1.UpdateOptions{}); err != nil {
			t.Error(err)
			return
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cur, err := rig.b.mirrors.Pods("ns").Get("vllm-m"); err == nil && cur.Status.Phase == corev1.PodFailed {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Error("the informer never saw the stopped mirror")
	}
	if _, err := rig.b.Suspend(context.Background(), "ns", "vllm", time.Time{}); err == nil {
		t.Fatal("want the kill sequence")
	}
	rig.settled("Failed", failedWith(ReasonAgentKilled))
	for i, p := range rig.emittedPods() {
		if p.Status.Phase == corev1.PodFailed && p.Status.Reason != ReasonAgentKilled {
			t.Fatalf("emitted status %d is Failed with reason %q, want %s first", i, p.Status.Reason, ReasonAgentKilled)
		}
	}
}
