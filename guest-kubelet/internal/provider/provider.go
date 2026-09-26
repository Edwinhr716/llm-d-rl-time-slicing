// Package provider is the M0 guest-kubelet provider: a copy of the upstream mock
// provider's idea (pods live in memory and are reported Running with a fake IP),
// trimmed to what M0 needs and made safe for concurrent calls.
package provider

import (
	"context"
	"io"
	"net/netip"
	"sync"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// fakePodCIDRStart is the first fake pod IP. 198.18.0.0/15 is reserved for benchmarking
// (RFC 2544), so it never collides with real pod or node IPs.
var fakePodCIDRStart = netip.MustParseAddr("198.18.0.1")

// Provider keeps guest pods in memory. The library calls it; it never talks to the API server.
type Provider struct {
	hostIP string

	mu     sync.Mutex
	pods   map[string]*corev1.Pod // key: namespace/name
	nextIP netip.Addr
	notify func(*corev1.Pod) // set once by NotifyPods before any other call
}

// New returns an empty provider. hostIP is reported as status.hostIP on every pod.
func New(hostIP string) *Provider {
	return &Provider{hostIP: hostIP, pods: map[string]*corev1.Pod{}, nextIP: fakePodCIDRStart}
}

// IsGuest reports whether a pod is meant for this node: it must tolerate the guest taint
// by key. System DaemonSets that tolerate everything ({operator: Exists}, no key) do not count.
func IsGuest(pod *corev1.Pod) bool {
	for _, t := range pod.Spec.Tolerations {
		if t.Key == GuestTaintKey {
			return true
		}
	}
	return false
}

func key(namespace, name string) string { return namespace + "/" + name }

// NotifyPods is called once by the pod controller at startup. After that, every status
// change we want written to the API goes through this callback.
func (p *Provider) NotifyPods(_ context.Context, cb func(*corev1.Pod)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notify = cb
}

// CreatePod "starts" a guest: it records the pod and reports it Running and Ready.
// Non-guest pods are ignored, so they stay Pending and nothing churns.
func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if !IsGuest(pod) {
		log.G(ctx).WithField("pod", key(pod.Namespace, pod.Name)).Info("ignoring non-guest pod")
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	k := key(pod.Namespace, pod.Name)
	podIP := p.nextIP.String()
	if old, ok := p.pods[k]; ok && old.Status.PodIP != "" {
		podIP = old.Status.PodIP // keep the IP stable if the library retries
	} else {
		p.nextIP = p.nextIP.Next()
	}

	now := metav1.Now()
	pod.Status = corev1.PodStatus{
		Phase:     corev1.PodRunning,
		HostIP:    p.hostIP,
		HostIPs:   []corev1.HostIP{{IP: p.hostIP}},
		PodIP:     podIP,
		PodIPs:    []corev1.PodIP{{IP: podIP}},
		StartTime: &now,
		QOSClass:  pod.Status.QOSClass, // set by the API server at admission; keep it
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
		},
	}
	for _, c := range pod.Spec.Containers {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			Ready:   true,
			Started: new(true),
			State:   corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		})
	}

	p.pods[k] = pod
	log.G(ctx).WithField("pod", k).WithField("podIP", podIP).Info("guest pod running (fake)")
	p.notify(pod.DeepCopy())
	return nil
}

// UpdatePod stores the new spec (labels, annotations, image). Status is unchanged.
func (p *Provider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	k := key(pod.Namespace, pod.Name)
	old, ok := p.pods[k]
	if !ok {
		return errdefs.NotFoundf("pod %q is not known to the provider", k)
	}
	pod.Status = old.Status
	p.pods[k] = pod
	p.notify(pod.DeepCopy())
	return nil
}

// DeletePod "stops" a guest: it forgets the pod and reports every container terminated.
// The library then deletes the pod object from the API once the grace period ends.
func (p *Provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	k := key(pod.Namespace, pod.Name)
	stored, ok := p.pods[k]
	if !ok {
		return errdefs.NotFoundf("pod %q is not known to the provider", k)
	}
	delete(p.pods, k)

	now := metav1.Now()
	final := pod.DeepCopy()
	final.Status = *stored.Status.DeepCopy()
	final.Status.Phase = corev1.PodSucceeded
	final.Status.Reason = "GuestKubeletPodDeleted"
	for i := range final.Status.Conditions {
		if t := final.Status.Conditions[i].Type; t == corev1.PodReady || t == corev1.ContainersReady {
			final.Status.Conditions[i].Status = corev1.ConditionFalse
			final.Status.Conditions[i].LastTransitionTime = now
		}
	}
	for i := range final.Status.ContainerStatuses {
		cs := &final.Status.ContainerStatuses[i]
		var started metav1.Time
		if cs.State.Running != nil {
			started = cs.State.Running.StartedAt
		}
		cs.Ready = false
		cs.Started = new(false)
		cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "GuestKubeletPodDeleted", StartedAt: started, FinishedAt: now,
		}}
	}

	log.G(ctx).WithField("pod", k).Info("guest pod deleted (fake)")
	p.notify(final)
	return nil
}

// GetPod returns the stored pod, or errdefs.NotFound. The library calls this before
// every create/update to decide between CreatePod and UpdatePod.
func (p *Provider) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pod, ok := p.pods[key(namespace, name)]; ok {
		return pod.DeepCopy(), nil
	}
	return nil, errdefs.NotFoundf("pod %q is not known to the provider", key(namespace, name))
}

// GetPodStatus returns the stored status, or errdefs.NotFound.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

// GetPods lists every stored pod. At startup the library deletes any of these that the
// API server does not know about ("dangling" pods).
func (p *Provider) GetPods(context.Context) ([]*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]*corev1.Pod, 0, len(p.pods))
	for _, pod := range p.pods {
		out = append(out, pod.DeepCopy())
	}
	return out, nil
}

// The methods below back kubectl logs/exec/attach/port-forward and the stats endpoints.
// M0 runs no HTTP server and no containers, so they all say "not implemented".

var errNotImplemented = errdefs.InvalidInput("not implemented in M0: guest-kubelet runs no containers")

func (p *Provider) GetContainerLogs(context.Context, string, string, string, api.ContainerLogOpts) (io.ReadCloser, error) {
	return nil, errNotImplemented
}

func (p *Provider) RunInContainer(context.Context, string, string, string, []string, api.AttachIO) error {
	return errNotImplemented
}

func (p *Provider) AttachToContainer(context.Context, string, string, string, api.AttachIO) error {
	return errNotImplemented
}

func (p *Provider) GetStatsSummary(context.Context) (*statsv1alpha1.Summary, error) {
	return nil, errNotImplemented
}

func (p *Provider) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, errNotImplemented
}

func (p *Provider) PortForward(context.Context, string, string, int32, io.ReadWriteCloser) error {
	return errNotImplemented
}
