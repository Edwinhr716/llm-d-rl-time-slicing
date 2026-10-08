package mirror_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/group"
)

func orchGuest() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "vllm", Namespace: "ns", UID: "guest-uid"},
		Spec: corev1.PodSpec{
			NodeName:      "vk-x",
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{{Name: "c", Image: "img"}},
		},
	}
}

func TestBuild_ContractLabelsOnlyWithBackground(t *testing.T) {
	cfg := mirror.Config{HostNode: "real", VirtualNode: "vk-x", Group: "rl"}
	plain, err := mirror.Build(orchGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, hasJob := plain.Labels[mirror.LabelJobID]
	_, hasRole := plain.Labels[mirror.LabelRole]
	if hasJob || hasRole || plain.Labels[mirror.LabelGroup] != "rl" || plain.Spec.RestartPolicy != corev1.RestartPolicyAlways {
		t.Fatalf("outside host-command mode the mirror carries only the group label: labels %v, restartPolicy %s",
			plain.Labels, plain.Spec.RestartPolicy)
	}

	cfg.Background, cfg.Attempt = true, 2
	m, err := mirror.Build(orchGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		mirror.LabelGroup: "rl", mirror.LabelJobID: "guest-uid-2", mirror.LabelRole: mirror.RoleBackground,
	}
	for k, v := range want {
		if m.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, m.Labels[k], v)
		}
	}
	if m.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy %s, want Never", m.Spec.RestartPolicy)
	}
}

type gatedRig struct {
	client *fake.Clientset
	b      *mirror.Backend
	guest  *corev1.Pod

	mu      sync.Mutex
	emitted []*corev1.Pod
}

func newGatedRig(t *testing.T) *gatedRig {
	t.Helper()
	guest := orchGuest()
	client := fake.NewClientset(guest)
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if ca, ok := a.(k8stesting.CreateAction); ok {
			if p, ok := ca.GetObject().(*corev1.Pod); ok && p.UID == "" {
				p.UID = uuid.NewUUID()
			}
		}
		return false, nil, nil
	})
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := idx.Add(guest); err != nil {
		t.Fatal(err)
	}
	fix := &gatedRig{client: client, guest: guest}
	fix.b = mirror.New(client, corev1listers.NewPodLister(idx), &mirror.Options{
		Config: mirror.Config{HostNode: "real", VirtualNode: "vk-x"}, Gated: true,
		Group: func() group.Result { return group.Result{Groups: []string{"rl"}, Reason: group.ReasonOK} },
	})
	fix.b.SetStatusCallback(func(p *corev1.Pod) {
		fix.mu.Lock()
		defer fix.mu.Unlock()
		fix.emitted = append(fix.emitted, p)
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := fix.b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return fix
}

func (fix *gatedRig) last() *corev1.Pod {
	fix.mu.Lock()
	defer fix.mu.Unlock()
	if len(fix.emitted) == 0 {
		return nil
	}
	return fix.emitted[len(fix.emitted)-1]
}

// readyOf returns the Ready condition, or Unknown when there is none.
func readyOf(p *corev1.Pod) corev1.PodCondition {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c
		}
	}
	return corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionUnknown}
}

// waitReady waits until the last emitted guest status has Ready = want (and reason, if set).
func (fix *gatedRig) waitReady(t *testing.T, want corev1.ConditionStatus, reason string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := fix.last(); p != nil {
			cond := readyOf(p)
			if cond.Status == want && (reason == "" || cond.Reason == reason) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("guest Ready never became %s (%s); last: %+v", want, reason, fix.last())
}

func (fix *gatedRig) mirrorRunningReady(t *testing.T) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	m, err := fix.client.CoreV1().Pods("ns").Get(ctx, mirror.Name(fix.guest.Name), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
	}}
	upd, err := fix.client.CoreV1().Pods("ns").UpdateStatus(ctx, m, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return upd
}

func TestGated_HoldReleaseEpochVacate(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	m := fix.mirrorRunningReady(t)
	if m.Labels[mirror.LabelJobID] != "guest-uid-0" || m.Labels[mirror.LabelGroup] != "rl" {
		t.Fatalf("mirror labels %v", m.Labels)
	}
	// A running, ready mirror does not make the guest Ready until the server releases it.
	fix.waitReady(t, corev1.ConditionFalse, mirror.ReasonWaitingForGrant)
	fix.b.ReleaseReady(fix.guest)
	fix.waitReady(t, corev1.ConditionTrue, "")
	fix.b.HoldNotReady(fix.guest, mirror.ReasonSuspending)
	fix.waitReady(t, corev1.ConditionFalse, mirror.ReasonSuspending)

	for want := int64(1); want <= 2; want++ {
		upd, epoch, err := fix.b.BumpEpoch(ctx, m)
		if err != nil || epoch != want || upd.Annotations[mirror.AnnotationGuestEpoch] != strconv.FormatInt(want, 10) {
			t.Fatalf("epoch %d, %v, annotations %v; want %d", epoch, err, upd.Annotations, want)
		}
	}

	if err := fix.b.VacateMirror(ctx, fix.guest, m); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := fix.last(); p != nil && p.Status.Reason == mirror.ReasonVacated {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p := fix.last(); p == nil || p.Status.Phase != corev1.PodPending || p.Status.Reason != mirror.ReasonVacated {
		t.Fatalf("a vacated guest must be Pending, not Failed; last: %+v", p)
	}
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	again, err := fix.client.CoreV1().Pods("ns").Get(ctx, mirror.Name(fix.guest.Name), metav1.GetOptions{})
	if err != nil || again.Labels[mirror.LabelJobID] != "guest-uid-1" {
		t.Fatalf("re-created mirror must get attempt 1: %v, %v", again, err)
	}
}

func TestGated_WaitMirrorGone(t *testing.T) {
	fix := newGatedRig(t)
	ctx := context.Background()
	if err := fix.b.Create(ctx, fix.guest); err != nil {
		t.Fatal(err)
	}
	mirrorPod := fix.mirrorRunningReady(t)

	// A mirror that is still there is not gone: the wait ends with the context.
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := fix.b.WaitMirrorGone(short, mirrorPod); err == nil {
		t.Fatal("a running mirror must not count as gone")
	}

	// A terminal mirror is gone.
	mirrorPod.Status.Phase = corev1.PodFailed
	if _, err := fix.client.CoreV1().Pods("ns").UpdateStatus(ctx, mirrorPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fix.b.WaitMirrorGone(ctx, mirrorPod); err != nil {
		t.Fatalf("a Failed mirror is gone: %v", err)
	}

	// A deleted mirror is gone.
	if err := fix.client.CoreV1().Pods("ns").Delete(ctx, mirrorPod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fix.b.WaitMirrorGone(ctx, mirrorPod); err != nil {
		t.Fatalf("a deleted mirror is gone: %v", err)
	}
}
