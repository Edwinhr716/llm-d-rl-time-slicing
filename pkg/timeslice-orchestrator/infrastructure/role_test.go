package infrastructure_test

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// TestObserveGroupState_JobRoleFromPodLabel checks that a job is a guest
// (background) only when every one of its pods carries the background role
// label; anything else is foreground, so a guest's fault can never be mistaken
// for the trainer's and vice versa.
func TestObserveGroupState_JobRoleFromPodLabel(t *testing.T) {
	type podSpec struct {
		name string
		job  string
		role string // "" means no role label
	}
	pods := []podSpec{
		{name: "guest-0", job: "guest", role: infrastructure.RoleBackground},
		{name: "guest-1", job: "guest", role: infrastructure.RoleBackground},
		{name: "mixed-0", job: "mixed", role: infrastructure.RoleBackground},
		{name: "mixed-1", job: "mixed"},
		{name: "trainer-0", job: "trainer"},
		{name: "odd-0", job: "odd", role: "foreground"},
	}
	want := map[string]store.Role{
		"guest":   store.RoleBackground,
		"mixed":   store.RoleForeground,
		"trainer": store.RoleForeground,
		"odd":     store.RoleForeground,
	}

	clientset := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)
	nodeInformer := informerFactory.Core().V1().Nodes()
	podInformer := informerFactory.Core().V1().Pods()
	jobStore := store.NewJobStore()
	infraOrch := infrastructure.NewKubernetesOrchestrator(nodeInformer, podInformer,
		store.NewGroupStore(store.NewMemLockStore()), jobStore, &fakeSnapshotAgentStore{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informerFactory.Start(ctx.Done())
	if err := infraOrch.Init(ctx); err != nil {
		t.Fatalf("Failed to initialize infra orchestrator: %v", err)
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-1",
		Labels: map[string]string{"group.timeslice.io/group-1": "true"},
	}}
	if _, err := clientset.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	for _, spec := range pods {
		podLabels := map[string]string{
			infrastructure.PodLabelKey: "group-1",
			infrastructure.JobLabelKey: spec.job,
		}
		if spec.role != "" {
			podLabels[infrastructure.RoleLabelKey] = spec.role
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: spec.name, Namespace: "default", Labels: podLabels}}
		if _, err := clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create pod %s: %v", spec.name, err)
		}
	}

	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 3*time.Second, true, func(context.Context) (bool, error) {
		nodes, err := nodeInformer.Lister().List(labels.Everything())
		if err != nil {
			return false, err
		}
		listed, err := podInformer.Lister().List(labels.Everything())
		if err != nil {
			return false, err
		}
		return len(nodes) == 1 && len(listed) == len(pods), nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for caches to sync: %v", err)
	}

	if err := infraOrch.ObserveGroupState(ctx, "group-1"); err != nil {
		t.Fatalf("ObserveGroupState failed: %v", err)
	}
	for jobID, wantRole := range want {
		job, err := jobStore.Get(ctx, "group-1", jobID)
		if err != nil {
			t.Fatalf("job %s not in store: %v", jobID, err)
		}
		if got := job.Role(); got != wantRole {
			t.Errorf("job %s role = %v, want %v", jobID, got, wantRole)
		}
	}
}
