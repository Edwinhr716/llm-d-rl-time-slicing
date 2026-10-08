package mirror_test

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

func TestJobAttempt(t *testing.T) {
	for _, tc := range []struct {
		uid, id string
		want    int
		ok      bool
	}{
		{"u", "u-0", 0, true},
		{"u", "u-12", 12, true},
		{"u", "v-3", 0, false},
		{"u", "u-x", 0, false},
		{"", "u-1", 0, false},
	} {
		m := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			mirror.LabelMirrorOf: tc.uid, mirror.LabelJobID: tc.id,
		}}}
		if got, ok := mirror.JobAttempt(m); got != tc.want || ok != tc.ok {
			t.Errorf("JobAttempt(%s, %s) = %d, %t; want %d, %t", tc.uid, tc.id, got, ok, tc.want, tc.ok)
		}
	}
}

// After a restart, a guest without a mirror gets one more attempt than the journal recorded, so
// no job id repeats; a guest with a mirror keeps the mirror's.
func TestRestoreAttempts_NoRepeatedJobID(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	fix.b.RestoreAttempts([]mirror.Guest{{Pod: fix.guest}}, map[types.UID]int{fix.guest.UID: 2})
	if got := fix.b.Attempts()[fix.guest.UID]; got != 3 {
		t.Fatalf("attempt %d, want 3", got)
	}
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m, err := fix.client.CoreV1().Pods("ns").Get(ctx, mirror.Name(fix.guest.Name), metav1.GetOptions{})
	if err != nil || m.Labels[mirror.LabelJobID] != "guest-uid-3" {
		t.Fatalf("mirror after restore: %v, %v", m, err)
	}
	// A second restart finds the mirror: its attempt wins over an older journal.
	fix.b.RestoreAttempts([]mirror.Guest{{Pod: fix.guest, Mirror: m}}, map[types.UID]int{fix.guest.UID: 1})
	if got := fix.b.Attempts()[fix.guest.UID]; got != 3 {
		t.Fatalf("attempt %d, want the mirror's 3", got)
	}
}

func TestSetMirrorSuspendState_EpochAndState(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	cur := fix.mirrorRunningReady(t)
	steps := []struct {
		state     string
		epoch     int64
		wantEpoch int64
	}{
		{mirror.StateSuspending, mirror.EpochBump, 1},
		{mirror.StateSuspended, mirror.EpochKeep, 1},
		{mirror.StateResuming, 7, 7},
		{"", mirror.EpochKeep, 7},
	}
	for _, s := range steps {
		upd, epoch, err := fix.b.SetMirrorSuspendState(ctx, cur, s.state, s.epoch)
		if err != nil {
			t.Fatal(err)
		}
		state, onMirror := mirror.SuspendState(upd)
		if epoch != s.wantEpoch || onMirror != s.wantEpoch || state != s.state {
			t.Fatalf("after %q/%d: state %q epoch %d (returned %d); want %q/%d",
				s.state, s.epoch, state, onMirror, epoch, s.state, s.wantEpoch)
		}
		if _, ok := upd.Annotations[mirror.AnnotationSuspendStateSince]; ok == (s.state == "") {
			t.Fatalf("since annotation after %q: %v", s.state, upd.Annotations)
		}
		cur = upd
	}
	// A replaced mirror is not written.
	other := cur.DeepCopy()
	other.UID = "someone-else"
	if _, _, err := fix.b.SetMirrorSuspendState(ctx, other, mirror.StateSuspending, mirror.EpochBump); err == nil {
		t.Fatal("a write to a replaced mirror must fail")
	}
}

// A mirror deleted to vacate reports the guest vacated even when this process did not delete it
// (the delete was seen only after a restart).
func TestVacatedMark_SurvivesRestart(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m := fix.mirrorRunningReady(t)
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[mirror.AnnotationVacated] = "true"
	if _, err := fix.client.CoreV1().Pods("ns").Update(ctx, m, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fix.client.CoreV1().Pods("ns").Delete(ctx, m.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := fix.last(); p != nil && p.Status.Reason == mirror.ReasonVacated {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("a mirror marked vacated must report the guest vacated, not failed; last: %+v", fix.last())
}

// Without the mark, the same delete is a failure, as before M5.
func TestVacatedMark_UnmarkedDeleteFails(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m := fix.mirrorRunningReady(t)
	if err := fix.client.CoreV1().Pods("ns").Delete(ctx, m.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := fix.last(); p != nil && p.Status.Reason == mirror.ReasonMirrorDeleted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("an unmarked mirror delete must fail the guest; last: %+v", fix.last())
}

// markKilling puts the M4 kill sequence's first mark on the guest's running mirror, as a restart
// right after markKilling leaves it.
func markKilling(t *testing.T, fix *gatedRig) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m := fix.mirrorRunningReady(t)
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[mirror.AnnotationKilling] = "agent Suspend: BACKEND_ERROR"
	m, err := fix.client.CoreV1().Pods("ns").Update(ctx, m, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// killedWritten reports whether the mirror was updated with the kill message before its delete.
func killedWritten(fix *gatedRig) string {
	for _, a := range fix.client.Actions() {
		if ua, ok := a.(k8stesting.UpdateAction); ok {
			if p, ok := ua.GetObject().(*corev1.Pod); ok && p.Annotations[mirror.AnnotationKilled] != "" {
				return p.Annotations[mirror.AnnotationKilled]
			}
		}
	}
	return ""
}

// A kill sequence cut short between the killing mark and the delete is finished on relist:
// agent Kill again, the kill recorded, the mirror deleted (normal grace), the guest failed.
func TestFinishKills_InterruptedKillFinished(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	m := markKilling(t, fix)
	var jobs []string
	kill := func(_ context.Context, job, _ string) error { jobs = append(jobs, job); return nil }
	out, err := fix.b.FinishKills(ctx, []mirror.Guest{{Pod: fix.guest, Mirror: m}}, kill, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Agent != "ok" || out[0].Cause != "agent Suspend: BACKEND_ERROR" {
		t.Fatalf("finished %+v", out)
	}
	if len(jobs) != 1 || jobs[0] != m.Labels[mirror.LabelJobID] {
		t.Fatalf("agent Kill calls %v, want the mirror's job", jobs)
	}
	if got := killedWritten(fix); got != "killed after agent Suspend: BACKEND_ERROR" {
		t.Fatalf("kill message written %q", got)
	}
	if _, err := fix.client.CoreV1().Pods("ns").Get(ctx, m.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("the mirror must be deleted")
	}
	for _, a := range fix.client.Actions() {
		if da, ok := a.(k8stesting.DeleteAction); ok && da.GetName() == m.Name {
			if g := da.GetDeleteOptions().GracePeriodSeconds; g != nil && *g == 0 {
				t.Fatal("never force-delete a mirror")
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := fix.last(); p != nil && p.Status.Phase == corev1.PodFailed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the guest must fail once its killed mirror is gone; last: %+v", fix.last())
}

// Without an agent, or when the agent's Kill fails, the kill still finishes by the delete.
func TestFinishKills_NoAgentOrKillErrorStillDeletes(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill mirror.KillFunc
		want string
	}{
		{"no agent", nil, "skipped"},
		{"kill error", func(context.Context, string, string) error { return errors.New("agent down") }, "agent down"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix := newGatedRig(t)
			ctx := context.Background()
			m := markKilling(t, fix)
			out, err := fix.b.FinishKills(ctx, []mirror.Guest{{Pod: fix.guest, Mirror: m}}, tc.kill, time.Second)
			if err != nil || len(out) != 1 || out[0].Agent != tc.want {
				t.Fatalf("finished %+v, %v; want agent %q", out, err, tc.want)
			}
			if _, err := fix.client.CoreV1().Pods("ns").Get(ctx, m.Name, metav1.GetOptions{}); err == nil {
				t.Fatal("the mirror must be deleted")
			}
		})
	}
}

// Mirrors without the mark, and marked mirrors already being deleted (the last step ran), are
// left alone.
func TestFinishKills_LeavesOthersAlone(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m := fix.mirrorRunningReady(t)
	going := m.DeepCopy()
	now := metav1.Now()
	going.DeletionTimestamp = &now
	going.Annotations = map[string]string{mirror.AnnotationKilling: "x", mirror.AnnotationKilled: "killed after x"}
	called := false
	kill := func(context.Context, string, string) error { called = true; return nil }
	for _, g := range []mirror.Guest{{Pod: fix.guest, Mirror: m}, {Pod: fix.guest, Mirror: going}, {Pod: fix.guest}} {
		out, err := fix.b.FinishKills(ctx, []mirror.Guest{g}, kill, time.Second)
		if err != nil || len(out) != 0 {
			t.Fatalf("finished %+v, %v", out, err)
		}
	}
	if called {
		t.Fatal("no agent Kill for a mirror without an interrupted kill")
	}
	if _, err := fix.client.CoreV1().Pods("ns").Get(ctx, m.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unmarked mirror must stay: %v", err)
	}
}
