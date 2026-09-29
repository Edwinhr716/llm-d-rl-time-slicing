package infrastructure_test

import (
	"context"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Tests for the Kubernetes side of decision D-NS-6 (--unconfirmed-kill):
// Warning events for every option, and the graceful mirror pod delete of
// escalation step 1.

func kubeActionsPod(name, ns, job, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			Labels: map[string]string{
				infrastructure.PodLabelKey:  "group-1",
				infrastructure.JobLabelKey:  job,
				infrastructure.RoleLabelKey: infrastructure.RoleBackground,
			},
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

func newKubeActionsClient() *fake.Clientset {
	return fake.NewClientset(
		kubeActionsPod("mirror-a", "demo", "guest-1", "node-1"),
		kubeActionsPod("mirror-b", "demo", "guest-1", "node-2"),
		kubeActionsPod("mirror-c", "demo", "guest-2", "node-1"),
		kubeActionsPod("mirror-d", "other", "guest-1", "node-1"),
	)
}

// TestUnconfirmedKill_Escalate_KubeActionsDeleteIsGraceful: step 1 deletes
// only the guest's mirror pod on the node, in the watched namespaces, with a
// UID precondition and never a grace period; no Node is touched.
func TestUnconfirmedKill_Escalate_KubeActionsDeleteIsGraceful(t *testing.T) {
	ctx := context.Background()
	cs := newKubeActionsClient()
	var actions controller.KubeActions = infrastructure.NewKubeActions(cs, []string{"demo"})

	n, err := actions.DeletePodsGracefully(ctx, "group-1", "guest-1", "node-1")
	if err != nil {
		t.Fatalf("DeletePodsGracefully: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted %d pods, want 1", n)
	}
	var deletes int
	for _, action := range cs.Actions() {
		if action.GetResource().Resource == "nodes" {
			t.Errorf("unexpected action on nodes: %v", action)
		}
		del, ok := action.(k8stesting.DeleteAction)
		if !ok {
			continue
		}
		deletes++
		if del.GetName() != "mirror-a" || del.GetNamespace() != "demo" {
			t.Errorf("deleted %s/%s, want demo/mirror-a", del.GetNamespace(), del.GetName())
		}
		opts := del.GetDeleteOptions()
		if opts.GracePeriodSeconds != nil {
			t.Errorf("delete with grace period %d, want the pod's own", *opts.GracePeriodSeconds)
		}
		if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "uid-mirror-a" {
			t.Errorf("delete preconditions = %+v, want the pod UID", opts.Preconditions)
		}
	}
	if deletes != 1 {
		t.Errorf("delete actions = %d, want 1", deletes)
	}
	for _, name := range []string{"mirror-b", "mirror-c"} {
		if _, err := cs.CoreV1().Pods("demo").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("pod %s: %v, want it kept", name, err)
		}
	}
	if _, err := cs.CoreV1().Pods("other").Get(ctx, "mirror-d", metav1.GetOptions{}); err != nil {
		t.Errorf("pod other/mirror-d outside the watched namespaces: %v, want it kept", err)
	}

	// Nothing left to delete: no error, nothing deleted.
	if n, err := actions.DeletePodsGracefully(ctx, "group-1", "guest-1", "node-1"); err != nil || n != 0 {
		t.Errorf("second DeletePodsGracefully = %d, %v, want 0, nil", n, err)
	}
}

// TestUnconfirmedKill_Grant_KubeActionsWarnPods: a Warning event goes on each
// pod of the job (every node when node is empty), across all namespaces when
// none are watched.
func TestUnconfirmedKill_Grant_KubeActionsWarnPods(t *testing.T) {
	ctx := context.Background()
	cs := newKubeActionsClient()
	actions := infrastructure.NewKubeActions(cs, nil)

	if err := actions.WarnPods(ctx, "group-1", "guest-1", "", controller.EventKillUnconfirmed, "not confirmed"); err != nil {
		t.Fatalf("WarnPods: %v", err)
	}
	got := map[string]bool{}
	for _, ns := range []string{"demo", "other"} {
		events, err := cs.CoreV1().Events(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		for i := range events.Items {
			e := &events.Items[i]
			if e.Type != corev1.EventTypeWarning || e.Reason != controller.EventKillUnconfirmed ||
				e.Message != "not confirmed" || e.InvolvedObject.Kind != "Pod" {
				t.Errorf("event = %+v", e)
			}
			if e.Source.Component != "timeslice-orchestrator" {
				t.Errorf("event source = %q", e.Source.Component)
			}
			got[ns+"/"+e.InvolvedObject.Name] = true
		}
	}
	want := map[string]bool{"demo/mirror-a": true, "demo/mirror-b": true, "other/mirror-d": true}
	if len(got) != len(want) {
		t.Errorf("events on %v, want %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Errorf("no event on %s", name)
		}
	}
	for _, action := range cs.Actions() {
		if action.GetVerb() == "delete" {
			t.Errorf("WarnPods deleted something: %v", action)
		}
	}
}
