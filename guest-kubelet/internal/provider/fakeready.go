package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// DaemonSetPolicy is how DaemonSet pods that land on the virtual Node are handled
// (--daemonset-policy). PENDING LEAD DECISION D-VK-7: both options are built; the default is
// the behaviour before the decision.
type DaemonSetPolicy string

const (
	// DaemonSetPolicyRule (default): the VK ignores non-guest pods, which stay Pending. A
	// platform rule outside the VK (deploy/daemonset-rule, internal/dsrule) keeps DaemonSets
	// off virtual nodes, so their rollouts do not wait for a pod that never starts.
	DaemonSetPolicyRule DaemonSetPolicy = "rule"
	// DaemonSetPolicyFakeReady: the VK reports DaemonSet pods bound to its Node as Running and
	// Ready without running anything (FakeReady).
	DaemonSetPolicyFakeReady DaemonSetPolicy = "fake-ready"
)

// ParseDaemonSetPolicy validates a --daemonset-policy value.
func ParseDaemonSetPolicy(s string) (DaemonSetPolicy, error) {
	switch p := DaemonSetPolicy(s); p {
	case DaemonSetPolicyRule, DaemonSetPolicyFakeReady:
		return p, nil
	}
	return "", fmt.Errorf("--daemonset-policy: %q is not one of rule, fake-ready", s)
}

// ReasonFakeReady is written in the pod status message of every faked pod.
const ReasonFakeReady = "GuestKubeletFakeReady"

// FakeReady is the fake-ready option. It claims DaemonSet-owned, non-guest pods bound to this
// virtual Node, reports them Running and Ready with no process behind them, and on delete
// reports them terminated and removes them at once, so the DaemonSet controller never waits on
// the virtual Node. It keeps the faked pods in memory only; after a restart the library offers
// them to CreatePod again and they are faked again.
type FakeReady struct {
	nodeName string
	hostIP   string
	// deleteNow removes a pod from the API with grace 0 (UID precondition). Called after the
	// terminal status has been handed to the library.
	deleteNow DeleteNowFunc
	now       func() time.Time
	settle    time.Duration

	mu   sync.Mutex
	pods map[string]*corev1.Pod
}

// DeleteNowFunc removes a pod from the API with grace 0 and a UID precondition.
type DeleteNowFunc func(ctx context.Context, namespace, name string, uid types.UID) error

// NewFakeReady returns the fake-ready handler for the virtual Node nodeName on the host hostIP.
func NewFakeReady(nodeName, hostIP string, deleteNow DeleteNowFunc) *FakeReady {
	return &FakeReady{
		nodeName: nodeName, hostIP: hostIP, deleteNow: deleteNow,
		now: time.Now, settle: time.Second, pods: map[string]*corev1.Pod{},
	}
}

// Handles reports whether the pod is faked: owned by a DaemonSet, bound to this virtual Node,
// and not a guest. A guest (toleration of the guest taint by key) keeps running as a mirror
// even when a DaemonSet owns it.
func (f *FakeReady) Handles(pod *corev1.Pod) bool {
	if IsGuest(pod) || pod.Spec.NodeName != f.nodeName {
		return false
	}
	c := metav1.GetControllerOf(pod)
	return c != nil && c.Kind == "DaemonSet" && (c.APIVersion == "apps/v1" || c.APIVersion == "extensions/v1beta1")
}

// ReadyStatus is the status written for a faked pod: phase Running, every condition True,
// every container running and ready, init containers completed. hostIP is the host's IP;
// podIP is left empty, so no EndpointSlice ever points at the pod.
func ReadyStatus(pod *corev1.Pod, hostIP string, at time.Time) corev1.PodStatus {
	now := metav1.NewTime(at)
	st := corev1.PodStatus{
		Phase:     corev1.PodRunning,
		Reason:    ReasonFakeReady,
		Message:   "guest-kubelet --daemonset-policy=fake-ready: reported ready without running",
		HostIP:    hostIP,
		StartTime: &now,
		QOSClass:  pod.Status.QOSClass,
	}
	if hostIP != "" {
		st.HostIPs = []corev1.HostIP{{IP: hostIP}}
	}
	// Keep the scheduler's PodScheduled (and anything else not ours), then set ours to True.
	ours := map[corev1.PodConditionType]bool{
		corev1.PodReadyToStartContainers: true, corev1.PodInitialized: true,
		corev1.ContainersReady: true, corev1.PodReady: true,
	}
	for _, c := range pod.Status.Conditions {
		if !ours[c.Type] {
			st.Conditions = append(st.Conditions, c)
		}
	}
	owned := []corev1.PodConditionType{
		corev1.PodReadyToStartContainers, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady,
	}
	for _, t := range owned {
		st.Conditions = append(st.Conditions, corev1.PodCondition{
			Type: t, Status: corev1.ConditionTrue, Reason: ReasonFakeReady, LastTransitionTime: now,
		})
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		st.InitContainerStatuses = append(st.InitContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, Ready: true, Started: new(false),
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0, Reason: "Completed", StartedAt: now, FinishedAt: now,
			}},
		})
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, Ready: true, Started: new(true),
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		})
	}
	return st
}

// Create fakes the pod and hands its Running/Ready status to notify. One log line per faked
// pod says so.
func (f *FakeReady) Create(ctx context.Context, pod *corev1.Pod, notify func(*corev1.Pod)) {
	out := pod.DeepCopy()
	out.Status = ReadyStatus(pod, f.hostIP, f.now())
	f.mu.Lock()
	f.pods[key(pod)] = out
	f.mu.Unlock()
	owner := ""
	if c := metav1.GetControllerOf(pod); c != nil {
		owner = c.Name
	}
	log.G(ctx).WithField("pod", key(pod)).WithField("daemonset", owner).WithField("node", f.nodeName).
		Warn("fake-ready: DaemonSet pod " + key(pod) + " reported ready without running")
	if notify != nil {
		notify(out.DeepCopy())
	}
}

// Update keeps the stored copy's metadata and spec current, so the library's comparison sees
// no pending change. The faked status is kept.
func (f *FakeReady) Update(pod *corev1.Pod) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.pods[key(pod)]
	if !ok {
		return
	}
	upd := pod.DeepCopy()
	upd.Status = cur.Status
	f.pods[key(pod)] = upd
}

// Get returns the faked pod, if this handler faked it.
func (f *FakeReady) Get(namespace, name string) (*corev1.Pod, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pods[namespace+"/"+name]
	if !ok {
		return nil, false
	}
	return p.DeepCopy(), true
}

// List returns every faked pod.
func (f *FakeReady) List() []*corev1.Pod {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*corev1.Pod, 0, len(f.pods))
	for _, p := range f.pods {
		out = append(out, p.DeepCopy())
	}
	return out
}

// Delete reports the pod terminated (phase Succeeded, Ready False, containers terminated) and
// then removes it from the API with grace 0, as the real kubelet does once a pod's containers
// have stopped. Nothing runs, so there is nothing to wait for.
func (f *FakeReady) Delete(ctx context.Context, pod *corev1.Pod, notify func(*corev1.Pod)) {
	f.mu.Lock()
	last, ok := f.pods[key(pod)]
	delete(f.pods, key(pod))
	f.mu.Unlock()
	if !ok {
		last = pod.DeepCopy()
		last.Status = ReadyStatus(pod, f.hostIP, f.now())
	}
	if notify != nil {
		notify(mirror.TerminalStatus(last, nil, mirror.ReasonGuestDeleted))
	}
	if f.deleteNow == nil {
		return
	}
	ns, name, uid := pod.Namespace, pod.Name, pod.UID
	go func() {
		time.Sleep(f.settle) // let the library write the terminal status first
		if err := f.deleteNow(context.WithoutCancel(ctx), ns, name, uid); err != nil {
			log.G(ctx).WithError(err).WithField("pod", ns+"/"+name).Debug("fake-ready: remove after delete")
		}
	}()
}
