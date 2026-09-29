package infrastructure_test

import (
	"context"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func kubeActionsPod(name, job, node string, background bool) *corev1.Pod {
	podLabels := map[string]string{
		infrastructure.PodLabelKey: "group-1",
		infrastructure.JobLabelKey: job,
	}
	if background {
		podLabels[infrastructure.RoleLabelKey] = infrastructure.RoleBackground
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "guests", Labels: podLabels, UID: types.UID("uid-" + name),
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

// newKubeActions starts informers over objs and returns KubeActions on them.
func newKubeActions(t *testing.T, objs ...runtime.Object) (*infrastructure.KubeActions, *fake.Clientset) {
	t.Helper()
	clientset := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	infraOrch := infrastructure.NewKubernetesOrchestrator(factory.Core().V1().Nodes(), factory.Core().V1().Pods(),
		store.NewGroupStore(store.NewMemLockStore()), store.NewJobStore(), &fakeSnapshotAgentStore{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			t.Fatalf("cache of %v not synced", typ)
		}
	}
	return infrastructure.NewKubeActions(clientset, infraOrch), clientset
}

func TestKubeActions_DeleteGuestMirrorIsGraceful(t *testing.T) {
	gone := kubeActionsPod("mirror-gone", "guest", "node-3", true)
	now := metav1.Now()
	gone.DeletionTimestamp = &now
	gone.Finalizers = []string{"test/hold"}
	actions, clientset := newKubeActions(t,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "node-uid"}},
		kubeActionsPod("mirror-1", "guest", "node-1", true),
		kubeActionsPod("mirror-2", "guest", "node-2", true),
		kubeActionsPod("trainer-0", "guest", "node-1", false),
		gone,
	)
	ctx := context.Background()

	n, err := actions.DeleteGuestMirror(ctx, "group-1", "guest", "node-1")
	if err != nil || n != 1 {
		t.Fatalf("DeleteGuestMirror(node-1) = %d, %v; want 1, nil", n, err)
	}
	if n, err := actions.DeleteGuestMirror(ctx, "group-1", "guest", "node-3"); err != nil || n != 0 {
		t.Errorf("DeleteGuestMirror of a pod already being deleted = %d, %v; want 0, nil", n, err)
	}

	var deletes []k8stesting.DeleteActionImpl
	for _, a := range clientset.Actions() {
		if a.GetVerb() != "delete" {
			continue
		}
		if a.GetResource().Resource != "pods" {
			t.Errorf("deleted a %s", a.GetResource().Resource)
			continue
		}
		del, ok := a.(k8stesting.DeleteActionImpl)
		if !ok {
			t.Fatalf("delete action %T", a)
		}
		deletes = append(deletes, del)
	}
	if len(deletes) != 1 || deletes[0].GetName() != "mirror-1" || deletes[0].GetNamespace() != "guests" {
		t.Fatalf("deletes = %+v, want only guests/mirror-1", deletes)
	}
	opts := deletes[0].GetDeleteOptions()
	if opts.GracePeriodSeconds != nil {
		t.Errorf("delete with grace period %d, want the pod's own", *opts.GracePeriodSeconds)
	}
	if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "uid-mirror-1" {
		t.Errorf("delete preconditions = %+v, want the mirror's UID", opts.Preconditions)
	}
	for _, name := range []string{"mirror-2", "trainer-0"} {
		if _, err := clientset.CoreV1().Pods("guests").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("%s deleted: %v", name, err)
		}
	}
	if _, err := clientset.CoreV1().Nodes().Get(ctx, "node-1", metav1.GetOptions{}); err != nil {
		t.Errorf("node deleted: %v", err)
	}
}

func TestKubeActions_Events(t *testing.T) {
	actions, clientset := newKubeActions(t,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "node-uid"}},
		kubeActionsPod("mirror-1", "guest", "node-1", true),
		kubeActionsPod("trainer-0", "trainer", "node-1", false),
	)
	ctx := context.Background()
	if err := actions.GuestEvent(ctx, "group-1", "guest", "node-1", "KillUnconfirmed", "m1"); err != nil {
		t.Fatal(err)
	}
	if err := actions.ForegroundEvent(ctx, "group-1", "trainer", "GrantBlocked", "m2"); err != nil {
		t.Fatal(err)
	}
	if err := actions.NodeEvent(ctx, "node-1", "NodeNotLendable", "m3"); err != nil {
		t.Fatal(err)
	}
	events, err := clientset.CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]corev1.ObjectReference{
		"KillUnconfirmed": {Kind: "Pod", Namespace: "guests", Name: "mirror-1", UID: "uid-mirror-1"},
		"GrantBlocked":    {Kind: "Pod", Namespace: "guests", Name: "trainer-0", UID: "uid-trainer-0"},
		"NodeNotLendable": {Kind: "Node", Name: "node-1", UID: "node-uid"},
	}
	if len(events.Items) != len(want) {
		t.Fatalf("events = %+v, want %d", events.Items, len(want))
	}
	for _, ev := range events.Items {
		ref, ok := want[ev.Reason]
		if !ok {
			t.Errorf("unexpected event %s", ev.Reason)
			continue
		}
		got := ev.InvolvedObject
		if got.Kind != ref.Kind || got.Namespace != ref.Namespace || got.Name != ref.Name || got.UID != ref.UID {
			t.Errorf("%s on %+v, want %+v", ev.Reason, got, ref)
		}
		if ev.Type != corev1.EventTypeWarning || ev.Source.Component != infrastructure.EventComponent {
			t.Errorf("%s: type %s source %s, want Warning from %s", ev.Reason, ev.Type, ev.Source.Component,
				infrastructure.EventComponent)
		}
	}
}
