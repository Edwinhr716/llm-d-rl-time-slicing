package infrastructure_test

import (
	"context"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// The same vector is pinned in guest-kubelet internal/group TestTokenVector.
func TestGroupTokenVector(t *testing.T) {
	const want = "gt-eb06e970a8b68e2de029dbb2ffc471b7"
	if got := infrastructure.GroupToken("team-a.trainer.workers"); got != want {
		t.Fatalf("GroupToken = %q, want %q", got, want)
	}
	if !infrastructure.IsGroupToken(want) || infrastructure.IsGroupToken("group-1") {
		t.Fatal("IsGroupToken")
	}
}

func TestGroupSelectorMatchesNameAndToken(t *testing.T) {
	sel := infrastructure.GroupSelector("group-1", labels.Set{infrastructure.JobLabelKey: "j"})
	for _, v := range []string{"group-1", infrastructure.GroupToken("group-1")} {
		if !sel.Matches(labels.Set{infrastructure.PodLabelKey: v, infrastructure.JobLabelKey: "j"}) {
			t.Errorf("selector %s does not match %s", sel, v)
		}
	}
	if sel.Matches(labels.Set{infrastructure.PodLabelKey: infrastructure.GroupToken("group-2"), infrastructure.JobLabelKey: "j"}) {
		t.Error("matches another group's token")
	}
	if sel.Matches(labels.Set{infrastructure.PodLabelKey: "group-1", infrastructure.JobLabelKey: "k"}) {
		t.Error("ignores the extra term")
	}
}

// A mirror labelled with the token is still the group's guest: the unconfirmed-kill event finds it.
func TestKubeActions_TokenMirrorIsFound(t *testing.T) {
	m := kubeActionsPod("mirror-1", "guest", "node-1", true)
	m.Labels[infrastructure.PodLabelKey] = infrastructure.GroupToken("group-1")
	actions, clientset := newKubeActions(t,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "node-uid"}}, m)
	ctx := context.Background()
	if err := actions.GuestEvent(ctx, "group-1", "guest", "node-1", "KillUnconfirmed", "m"); err != nil {
		t.Fatal(err)
	}
	events, err := clientset.CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Items) != 1 || events.Items[0].InvolvedObject.Name != "mirror-1" {
		t.Fatalf("events = %+v, want one on mirror-1", events.Items)
	}
}
