package mirror_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Tests for readinessGates: they are honoured and a status write never reverts a gate.

const gateType corev1.PodConditionType = "example.com/gate"

func gatedPod(gate corev1.ConditionStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "g", UID: "uid-1"},
		Spec:       corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: gateType}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: gateType, Status: gate},
		}},
	}
}

// gateIs is the API's conditions with only the gate, at status.
func gateIs(status corev1.ConditionStatus) []corev1.PodCondition {
	return []corev1.PodCondition{{Type: gateType, Status: status}}
}

func condOf(st *corev1.PodStatus, ct corev1.PodConditionType) *corev1.PodCondition {
	for i := range st.Conditions {
		if st.Conditions[i].Type == ct {
			return &st.Conditions[i]
		}
	}
	return nil
}

func TestKeepReadinessGates(t *testing.T) {
	t.Parallel()
	gates := []corev1.PodReadinessGate{{ConditionType: gateType}}
	cases := []struct {
		name      string
		current   []corev1.PodCondition
		wantGate  string // "" = no gate condition
		wantReady corev1.ConditionStatus
	}{
		{"gate true in the API", gateIs(corev1.ConditionTrue), "True", corev1.ConditionTrue},
		{"gate false in the API", gateIs(corev1.ConditionFalse), "False", corev1.ConditionFalse},
		{"gate not set in the API", nil, "", corev1.ConditionFalse},
	}
	for _, tc := range cases {
		st := gatedPod(corev1.ConditionFalse).Status // a stale write: gate false
		mirror.KeepReadinessGates(&st, tc.current, gates)
		gate := condOf(&st, gateType)
		switch {
		case tc.wantGate == "" && gate != nil:
			t.Errorf("%s: gate kept (%+v), want dropped", tc.name, gate)
		case tc.wantGate != "" && (gate == nil || string(gate.Status) != tc.wantGate):
			t.Errorf("%s: gate %+v, want %s", tc.name, gate, tc.wantGate)
		}
		ready := condOf(&st, corev1.PodReady)
		if ready == nil || ready.Status != tc.wantReady {
			t.Errorf("%s: Ready %+v, want %s", tc.name, ready, tc.wantReady)
		}
		if tc.wantReady == corev1.ConditionFalse && ready != nil && ready.Reason != mirror.ReasonReadinessGatesNotReady {
			t.Errorf("%s: Ready reason %q", tc.name, ready.Reason)
		}
	}

	// Ready turning true keeps the API's transition time when the API already had it true.
	then := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	st := gatedPod(corev1.ConditionFalse).Status
	condOf(&st, corev1.PodReady).Status = corev1.ConditionFalse
	mirror.KeepReadinessGates(&st, []corev1.PodCondition{
		{Type: gateType, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: then},
	}, gates)
	if ready := condOf(&st, corev1.PodReady); ready.Status != corev1.ConditionTrue || !ready.LastTransitionTime.Equal(&then) {
		t.Errorf("Ready %+v, want true since %v", ready, then)
	}

	// Not running: never ready, whatever the gates say.
	st = gatedPod(corev1.ConditionTrue).Status
	st.Phase = corev1.PodPending
	mirror.KeepReadinessGates(&st, []corev1.PodCondition{{Type: gateType, Status: corev1.ConditionTrue}}, gates)
	if ready := condOf(&st, corev1.PodReady); ready.Status != corev1.ConditionFalse {
		t.Errorf("pending pod: Ready %+v", ready)
	}
}

func TestGateKeepingClient(t *testing.T) {
	t.Parallel()
	inAPI := gatedPod(corev1.ConditionTrue) // the gate owner already set the gate true
	inAPI.ResourceVersion = "7"
	client := fake.NewClientset(inAPI)
	pods := mirror.GateKeepingClient(client).CoreV1().Pods("ns")

	stale := gatedPod(corev1.ConditionFalse) // the library's translation predates the gate write
	stale.ResourceVersion = "3"
	if _, err := pods.UpdateStatus(t.Context(), stale, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Pods("ns").Get(t.Context(), "g", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if gate := condOf(&got.Status, gateType); gate == nil || gate.Status != corev1.ConditionTrue {
		t.Errorf("the write reverted the gate: %+v", got.Status.Conditions)
	}
	if ready := condOf(&got.Status, corev1.PodReady); ready == nil || ready.Status != corev1.ConditionTrue {
		t.Errorf("Ready: %+v", got.Status.Conditions)
	}

	// A pod with another UID under the same name is written as given (the server decides).
	other := gatedPod(corev1.ConditionFalse)
	other.UID = "uid-2"
	if _, err := pods.UpdateStatus(t.Context(), other, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err = client.CoreV1().Pods("ns").Get(t.Context(), "g", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if gate := condOf(&got.Status, gateType); gate == nil || gate.Status != corev1.ConditionFalse {
		t.Errorf("a different pod must pass through unchanged: %+v", got.Status.Conditions)
	}

	// A missing pod is an error, not a blind write.
	missing := gatedPod(corev1.ConditionTrue)
	missing.Name = "gone"
	if _, err := pods.UpdateStatus(t.Context(), missing, metav1.UpdateOptions{}); err == nil {
		t.Error("a status write for a missing gated pod must fail")
	}
}

func TestGatesChanged(t *testing.T) {
	t.Parallel()
	falsePod, truePod := gatedPod(corev1.ConditionFalse), gatedPod(corev1.ConditionTrue)
	unset := gatedPod(corev1.ConditionTrue)
	unset.Status.Conditions = unset.Status.Conditions[:2]
	if !mirror.GatesChanged(falsePod, truePod) || !mirror.GatesChanged(unset, truePod) || !mirror.GatesChanged(truePod, unset) {
		t.Error("a gate status change or a gate appearing or going must count")
	}
	moved := gatedPod(corev1.ConditionTrue)
	condOf(&moved.Status, corev1.ContainersReady).Status = corev1.ConditionFalse
	if mirror.GatesChanged(truePod, moved) {
		t.Error("a change outside the gate conditions must not count")
	}
}
