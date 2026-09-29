package provider

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const testVKNode = "vk-test"

// ownedDSPod is a pod created by a DaemonSet controller and bound to the virtual Node.
func ownedDSPod(name string) *corev1.Pod {
	p := dsPod(name)
	p.UID = types.UID("uid-" + name)
	p.Spec.NodeName = testVKNode
	p.Spec.InitContainers = []corev1.Container{{Name: "init", Image: "busybox"}}
	p.Status.QOSClass = corev1.PodQOSBurstable
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}
	p.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds-a", UID: "ds-uid", Controller: new(true),
	}}
	return p
}

type deleteRec struct {
	mu   sync.Mutex
	done chan struct{}
	got  []string
}

func (d *deleteRec) fn(_ context.Context, ns, name string, uid types.UID) error {
	d.mu.Lock()
	d.got = append(d.got, ns+"/"+name+"/"+string(uid))
	d.mu.Unlock()
	close(d.done)
	return nil
}

func fakeReadyProvider(t *testing.T) (*Provider, *[]*corev1.Pod, *deleteRec) {
	t.Helper()
	rec := &deleteRec{done: make(chan struct{})}
	f := NewFakeReady(testVKNode, "10.0.0.1", rec.fn)
	f.settle = 0
	f.now = func() time.Time { return time.Unix(1000, 0) }
	p := New(&fakeBackend{}).WithFakeReady(f)
	var notified []*corev1.Pod
	p.NotifyPods(context.Background(), func(pod *corev1.Pod) { notified = append(notified, pod) })
	return p, &notified, rec
}

func condStatus(st *corev1.PodStatus, ctype corev1.PodConditionType) corev1.ConditionStatus {
	for i := range st.Conditions {
		if st.Conditions[i].Type == ctype {
			return st.Conditions[i].Status
		}
	}
	return ""
}

func TestDaemonSetPolicy_Parse(t *testing.T) {
	for _, s := range []string{"rule", "fake-ready"} {
		if p, err := ParseDaemonSetPolicy(s); err != nil || string(p) != s {
			t.Errorf("%s: %v %v", s, p, err)
		}
	}
	if _, err := ParseDaemonSetPolicy("both"); err == nil {
		t.Error("unknown policy must be rejected")
	}
}

// The default policy (rule) leaves DaemonSet pods alone: nothing is created, no status is
// written, and GetPod stays NotFound, exactly as before D-VK-7.
func TestDaemonSetPolicy_Rule_ProviderIgnoresDaemonSetPods(t *testing.T) {
	b := &fakeBackend{}
	p := New(b)
	var notified int
	p.NotifyPods(context.Background(), func(*corev1.Pod) { notified++ })
	pod := ownedDSPod("ds-a-x")
	if err := p.CreatePod(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if len(b.created) != 0 || notified != 0 {
		t.Errorf("rule: created=%v notified=%d", b.created, notified)
	}
	if _, err := p.GetPod(context.Background(), pod.Namespace, pod.Name); !errdefs.IsNotFound(err) {
		t.Errorf("rule: GetPod want NotFound, got %v", err)
	}
	if err := p.DeletePod(context.Background(), pod); !errdefs.IsNotFound(err) {
		t.Errorf("rule: DeletePod want NotFound, got %v", err)
	}
}

func TestDaemonSetPolicy_FakeReady_HandlesOnlyDaemonSetNonGuestsOnThisNode(t *testing.T) {
	f := NewFakeReady(testVKNode, "10.0.0.1", nil)
	if !f.Handles(ownedDSPod("ds")) {
		t.Error("a DaemonSet pod on this node must be faked")
	}
	bare := ownedDSPod("bare")
	bare.OwnerReferences = nil
	rs := ownedDSPod("rs")
	rs.OwnerReferences[0].Kind = "ReplicaSet"
	notCtl := ownedDSPod("notctl")
	notCtl.OwnerReferences[0].Controller = nil
	other := ownedDSPod("other")
	other.Spec.NodeName = "vk-other"
	guest := ownedDSPod("guest")
	guest.Spec.Tolerations = append(guest.Spec.Tolerations,
		corev1.Toleration{Key: GuestTaintKey, Operator: corev1.TolerationOpExists})
	for _, p := range []*corev1.Pod{bare, rs, notCtl, other, guest} {
		if f.Handles(p) {
			t.Errorf("%s must not be faked", p.Name)
		}
	}
}

func TestDaemonSetPolicy_FakeReady_CreateReportsRunningReady(t *testing.T) {
	var buf bytes.Buffer
	ctx := log.WithLogger(context.Background(), vkslog.FromSlog(slog.New(slog.NewJSONHandler(&buf, nil))))
	prov, notified, _ := fakeReadyProvider(t)
	pod := ownedDSPod("ds-a-x")
	if err := prov.CreatePod(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if len(*notified) != 1 {
		t.Fatalf("want one status, got %d", len(*notified))
	}
	st := (*notified)[0].Status
	if st.Phase != corev1.PodRunning || st.PodIP != "" || len(st.PodIPs) != 0 || st.HostIP != "10.0.0.1" {
		t.Errorf("phase=%s podIP=%q hostIP=%q", st.Phase, st.PodIP, st.HostIP)
	}
	for _, c := range []corev1.PodConditionType{
		corev1.PodScheduled, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady,
	} {
		if condStatus(&st, c) != corev1.ConditionTrue {
			t.Errorf("condition %s = %q", c, condStatus(&st, c))
		}
	}
	if st.QOSClass != corev1.PodQOSBurstable {
		t.Errorf("qosClass must be kept, got %q", st.QOSClass)
	}
	if len(st.ContainerStatuses) != 1 || st.ContainerStatuses[0].State.Running == nil || !st.ContainerStatuses[0].Ready {
		t.Errorf("container statuses: %+v", st.ContainerStatuses)
	}
	if len(st.InitContainerStatuses) != 1 || st.InitContainerStatuses[0].State.Terminated == nil {
		t.Errorf("init container statuses: %+v", st.InitContainerStatuses)
	}
	got, err := prov.GetPod(ctx, pod.Namespace, pod.Name)
	if err != nil || got.Status.Phase != corev1.PodRunning {
		t.Errorf("GetPod after fake: %v %v", got, err)
	}
	if pods, err := prov.GetPods(ctx); err != nil || len(pods) != 1 {
		t.Errorf("GetPods: %d %v", len(pods), err)
	}
	line := buf.String()
	if strings.Count(line, "ready without running") != 1 || !strings.Contains(line, "ns/ds-a-x") {
		t.Errorf("want one 'ready without running' line with ns/name, got %q", line)
	}
}

func TestDaemonSetPolicy_FakeReady_UpdateKeepsFakeStatus(t *testing.T) {
	p, _, _ := fakeReadyProvider(t)
	pod := ownedDSPod("ds-a-x")
	if err := p.CreatePod(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	upd := pod.DeepCopy()
	upd.Annotations = map[string]string{"rev": "2"}
	if err := p.UpdatePod(context.Background(), upd); err != nil {
		t.Fatal(err)
	}
	got, err := p.GetPod(context.Background(), pod.Namespace, pod.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations["rev"] != "2" || got.Status.Phase != corev1.PodRunning {
		t.Errorf("update: annotations=%v phase=%s", got.Annotations, got.Status.Phase)
	}
}

func TestDaemonSetPolicy_FakeReady_DeleteReportsTerminatedAndRemoves(t *testing.T) {
	prov, notified, rec := fakeReadyProvider(t)
	pod := ownedDSPod("ds-a-x")
	if err := prov.CreatePod(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	del := pod.DeepCopy()
	del.DeletionTimestamp = &metav1.Time{Time: time.Unix(2000, 0)}
	if err := prov.DeletePod(context.Background(), del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	last := (*notified)[len(*notified)-1].Status
	if last.Phase != corev1.PodSucceeded || condStatus(&last, corev1.PodReady) != corev1.ConditionFalse {
		t.Errorf("terminal status: phase=%s ready=%s", last.Phase, condStatus(&last, corev1.PodReady))
	}
	for i := range last.ContainerStatuses {
		if last.ContainerStatuses[i].State.Terminated == nil {
			t.Errorf("container %s not terminated", last.ContainerStatuses[i].Name)
		}
	}
	select {
	case <-rec.done:
	case <-time.After(5 * time.Second):
		t.Fatal("pod was not removed")
	}
	if len(rec.got) != 1 || rec.got[0] != "ns/ds-a-x/uid-ds-a-x" {
		t.Errorf("removed %v", rec.got)
	}
	if _, err := prov.GetPod(context.Background(), pod.Namespace, pod.Name); !errdefs.IsNotFound(err) {
		t.Errorf("after delete GetPod want NotFound, got %v", err)
	}
}

// Guests keep the mirror path under fake-ready.
func TestDaemonSetPolicy_FakeReady_GuestsStillMirrored(t *testing.T) {
	b := &fakeBackend{}
	p := New(b).WithFakeReady(NewFakeReady(testVKNode, "10.0.0.1", nil))
	g := guestPod("g")
	g.Spec.NodeName = testVKNode
	if err := p.CreatePod(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if len(b.created) != 1 {
		t.Errorf("guest must reach the backend, got %v", b.created)
	}
}
