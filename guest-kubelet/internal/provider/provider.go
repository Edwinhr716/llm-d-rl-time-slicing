// Package provider is the guest-kubelet's pod provider. M1: guests run for real, as mirror pods
// on the host node (internal/backend/mirror). The provider only decides which pods are guests
// and hands them to the backend; it holds no pod state, so a restart loses nothing.
package provider

import (
	"context"
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Backend is what the provider needs from a runtime backend. The mirror backend implements it.
type Backend interface {
	Create(ctx context.Context, guest *corev1.Pod) error
	Delete(ctx context.Context, guest *corev1.Pod) error
	Get(namespace, name string) (*corev1.Pod, error)
	List() ([]*corev1.Pod, error)
	SetStatusCallback(func(*corev1.Pod))
}

// Provider implements nodeutil.Provider and node.PodNotifier on top of a Backend.
type Provider struct {
	backend Backend
}

// New returns a provider backed by b.
func New(b Backend) *Provider { return &Provider{backend: b} }

// IsGuest reports whether a pod is meant for this node: it must tolerate the guest taint
// by key. System DaemonSets that tolerate everything ({operator: Exists}, no key) do not count,
// so they never get a mirror.
func IsGuest(pod *corev1.Pod) bool {
	for _, t := range pod.Spec.Tolerations {
		if t.Key == GuestTaintKey {
			return true
		}
	}
	return false
}

func key(p *corev1.Pod) string { return p.Namespace + "/" + p.Name }

// NotifyPods is called once by the pod controller at startup. From then on, every mirror change
// the backend sees is translated and written to the guest through cb.
func (p *Provider) NotifyPods(_ context.Context, cb func(*corev1.Pod)) {
	p.backend.SetStatusCallback(func(pod *corev1.Pod) {
		if IsGuest(pod) {
			cb(pod)
		}
	})
}

// CreatePod creates the guest's mirror. Non-guests are ignored and stay Pending.
func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if !IsGuest(pod) {
		log.G(ctx).WithField("pod", key(pod)).Debug("ignoring non-guest pod")
		return nil
	}
	return p.backend.Create(ctx, pod)
}

// UpdatePod is called when the guest's labels, annotations, tolerations or images change.
// Labels and annotations are not copied to the mirror, so there is nothing to do; changing a
// running guest's image is not supported in M1 (a real kubelet would restart the container).
func (p *Provider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	if IsGuest(pod) {
		log.G(ctx).WithField("pod", key(pod)).Debug("guest spec update ignored (M1)")
	}
	return nil
}

// DeletePod deletes the guest's mirror with the guest's grace period. The mirror informer then
// reports the containers terminating, and once they have stopped the library removes the
// guest object. If there is no mirror, the guest is reported terminated at once.
func (p *Provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	if !IsGuest(pod) {
		return errdefs.NotFoundf("pod %q is not a guest", key(pod))
	}
	return p.backend.Delete(ctx, pod)
}

// GetPod returns the guest with its mirror's status, or errdefs.NotFound. Non-guests are
// always NotFound, so the library keeps offering them to CreatePod, which ignores them.
func (p *Provider) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	return p.backend.Get(namespace, name)
}

// GetPodStatus returns the translated status, or errdefs.NotFound.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

// GetPods lists the guests that have a mirror. At startup the library deletes (through
// DeletePod) any of these that the API no longer has.
func (p *Provider) GetPods(context.Context) ([]*corev1.Pod, error) { return p.backend.List() }

// The methods below back kubectl logs/exec/attach/port-forward and the stats endpoints.
// Until M2 proxies them to the mirror, use kubectl logs <guest>-m.

var errNotImplemented = errdefs.InvalidInput("not implemented in M1: use kubectl logs/exec on the mirror pod (<guest>" + mirror.Suffix + ")")

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
