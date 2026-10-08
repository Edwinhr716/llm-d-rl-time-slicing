package mirror_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// verdicts is a fixed Readiness: containers not in the map have no verdict.
type verdicts map[string]bool

//nolint:gocritic // unnamedResult: the interface's (ready, has) pair; nonamedreturns forbids naming them
func (v verdicts) ContainerReady(_ *corev1.Pod, container string) (bool, bool) {
	ready, has := v[container]
	return ready, has
}

func readinessGuest() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "ns", UID: "uid-1"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "server"},
			{Name: "sidecar"},
		}},
	}
}

func readinessMirror(phase corev1.PodPhase, running ...string) *corev1.Pod {
	now := metav1.Now()
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: phase, PodIP: "10.1.2.3",
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		},
	}}
	for _, name := range []string{"server", "sidecar"} {
		status := corev1.ContainerStatus{Name: name, Ready: true}
		for _, run := range running {
			if run == name {
				status.State.Running = &corev1.ContainerStateRunning{StartedAt: now}
			}
		}
		if status.State.Running == nil {
			status.Ready = false
			status.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		}
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, status)
	}
	return pod
}

func condition(pod *corev1.Pod, ct corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == ct {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

func TestTranslateStatusWithVerdicts(t *testing.T) {
	t.Parallel()
	both := []string{"server", "sidecar"}
	cases := []struct {
		name       string
		rd         verdicts
		phase      corev1.PodPhase
		running    []string
		wantReady  bool
		wantServer bool
	}{
		{"probe not yet passed", verdicts{"server": false}, corev1.PodRunning, both, false, false},
		{"probe passed", verdicts{"server": true}, corev1.PodRunning, both, true, true},
		{"verdict but not running", verdicts{"server": true}, corev1.PodRunning, []string{"sidecar"}, false, false},
		{"no verdict keeps mirror flag", verdicts{}, corev1.PodRunning, both, true, true},
		{"pending pod never ready", verdicts{"server": true, "sidecar": true}, corev1.PodPending, both, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := mirror.TranslateStatusWith(readinessGuest(), readinessMirror(tc.phase, tc.running...), tc.rd)
			ready := condition(out, corev1.PodReady)
			if ready == nil || (ready.Status == corev1.ConditionTrue) != tc.wantReady {
				t.Errorf("Ready: got %+v, want %t", ready, tc.wantReady)
			}
			if got := out.Status.ContainerStatuses[0].Ready; got != tc.wantServer {
				t.Errorf("server ready: got %t, want %t", got, tc.wantServer)
			}
		})
	}
}

func TestTranslateStatusWithMessage(t *testing.T) {
	t.Parallel()
	out := mirror.TranslateStatusWith(readinessGuest(), readinessMirror(corev1.PodRunning, "server", "sidecar"),
		verdicts{"server": false})
	for _, ct := range []corev1.PodConditionType{corev1.PodReady, corev1.ContainersReady} {
		cond := condition(out, ct)
		if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != "ContainersNotReady" ||
			cond.Message != "containers with unready status: [server]" {
			t.Errorf("%s: %+v", ct, cond)
		}
	}
}

func TestTranslateStatusWithKeepsTransitionTime(t *testing.T) {
	t.Parallel()
	guest := readinessGuest()
	old := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	guest.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: old},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: old},
	}
	running := readinessMirror(corev1.PodRunning, "server", "sidecar")
	out := mirror.TranslateStatusWith(guest, running, verdicts{"server": true})
	if got := condition(out, corev1.PodReady).LastTransitionTime; !got.Equal(&old) {
		t.Errorf("unchanged Ready must keep its transition time: got %v, want %v", got, old)
	}
	out = mirror.TranslateStatusWith(guest, running, verdicts{"server": false})
	if got := condition(out, corev1.PodReady).LastTransitionTime; got.Equal(&old) {
		t.Error("a Ready flip must get a new transition time")
	}
}

func TestTranslateStatusNilReadinessIsM1(t *testing.T) {
	t.Parallel()
	running := readinessMirror(corev1.PodRunning, "server", "sidecar")
	withNil := mirror.TranslateStatusWith(readinessGuest(), running, nil)
	plain := mirror.TranslateStatus(readinessGuest(), running)
	if condition(withNil, corev1.PodReady).Status != condition(plain, corev1.PodReady).Status {
		t.Error("nil Readiness must behave as TranslateStatus")
	}
}
