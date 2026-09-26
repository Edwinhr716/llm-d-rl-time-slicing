package mirror

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func runningMirror() *corev1.Pod {
	now := metav1.Now()
	return &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning, PodIP: "10.1.2.3", PodIPs: []corev1.PodIP{{IP: "10.1.2.3"}},
		HostIP: "10.0.0.9", StartTime: &now, QOSClass: corev1.PodQOSBurstable,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "vllm", Ready: true, RestartCount: 2,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		}},
	}}
}

func TestTranslateStatus(t *testing.T) {
	g := testGuest()
	g.Spec.ReadinessGates = nil
	g.Status.QOSClass = corev1.PodQOSGuaranteed
	out := TranslateStatus(g, runningMirror())
	s := out.Status
	if s.Phase != corev1.PodRunning || s.PodIP != "10.1.2.3" || s.HostIP != "10.0.0.9" || s.StartTime == nil {
		t.Errorf("status: %+v", s)
	}
	if s.ContainerStatuses[0].RestartCount != 2 || s.ContainerStatuses[0].State.Running == nil {
		t.Errorf("container status: %+v", s.ContainerStatuses)
	}
	if s.QOSClass != corev1.PodQOSGuaranteed {
		t.Errorf("qosClass must stay the guest's, got %s", s.QOSClass)
	}
	if g.Status.PodIP != "" {
		t.Error("TranslateStatus modified the guest")
	}
}

func TestReadinessGateHoldsReady(t *testing.T) {
	g := testGuest() // has gate timeslice.io/serving, not set
	s := TranslateStatus(g, runningMirror()).Status
	if c := findCondition(s.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionFalse {
		t.Errorf("Ready must be false while a gate is unset: %+v", s.Conditions)
	}
	g.Status.Conditions = []corev1.PodCondition{{Type: "timeslice.io/serving", Status: corev1.ConditionTrue}}
	s = TranslateStatus(g, runningMirror()).Status
	if c := findCondition(s.Conditions, corev1.PodReady); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("Ready must be true once the gate is true: %+v", s.Conditions)
	}
	if findCondition(s.Conditions, "timeslice.io/serving") == nil {
		t.Error("gate condition must be kept")
	}
}

func TestTerminalStatus(t *testing.T) {
	g := testGuest()
	out := TerminalStatus(g, runningMirror(), ReasonGuestDeleted).Status
	if out.Phase != corev1.PodSucceeded || out.ContainerStatuses[0].State.Terminated == nil {
		t.Errorf("guest delete: %+v", out)
	}
	out = TerminalStatus(g, nil, ReasonMirrorDeleted).Status
	if out.Phase != corev1.PodFailed || len(out.ContainerStatuses) != 1 || out.ContainerStatuses[0].State.Terminated == nil {
		t.Errorf("mirror gone: %+v", out)
	}
}
