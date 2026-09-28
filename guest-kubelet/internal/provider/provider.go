// Package provider is the guest-kubelet's pod provider. M1: guests run for real, as mirror pods
// on the host node (internal/backend/mirror). The provider only decides which pods are guests
// and hands them to the backend; it holds no pod state, so a restart loses nothing.
package provider

import (
	"context"
	"io"
	"sync"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
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
	backend   Backend
	admission *Admission

	mu     sync.Mutex
	notify func(*corev1.Pod) // the library's status callback, set by NotifyPods
}

// Admission is the check CreatePod runs before a guest gets a mirror.
type Admission struct {
	Policy AdmissionPolicy
	// Recorder gets one Warning event (reason GuestRejected) per refused guest.
	Recorder record.EventRecorder
	// Rejected is shared with GuestOnlyRecorder, which then drops the library's
	// "ProviderCreateSuccess" for refused guests.
	Rejected *RejectedSet
}

// New returns a provider backed by b, with no admission check.
func New(b Backend) *Provider { return &Provider{backend: b} }

// WithAdmission turns on the admission check.
func (p *Provider) WithAdmission(a *Admission) *Provider {
	if a.Rejected == nil {
		a.Rejected = NewRejectedSet()
	}
	p.admission = a
	return p
}

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
	p.mu.Lock()
	p.notify = cb
	p.mu.Unlock()
	p.backend.SetStatusCallback(func(pod *corev1.Pod) {
		if IsGuest(pod) {
			cb(pod)
		}
	})
}

// CreatePod creates the guest's mirror. Non-guests are ignored and stay Pending. A guest that
// admission refuses gets no mirror: it gets a Warning event and goes Failed with reason
// GuestRejected, as a pod the real kubelet refuses at admission does.
func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if !IsGuest(pod) {
		log.G(ctx).WithField("pod", key(pod)).Debug("ignoring non-guest pod")
		return nil
	}
	if p.admission != nil {
		if r := Admit(pod, p.admission.Policy); r != nil {
			p.reject(ctx, pod, r)
			return nil
		}
	}
	return p.backend.Create(ctx, pod)
}

// reject records the event once per guest and reports the guest Failed through the library's
// status callback. Returning nil (not an error) keeps the library from retrying the create;
// once the guest is Failed the library never offers it again.
func (p *Provider) reject(ctx context.Context, pod *corev1.Pod, r *Rejection) {
	log.G(ctx).WithField("pod", key(pod)).WithField("rule", r.Rule).Warn("guest rejected: " + r.Message)
	if p.admission.Rejected.Add(pod.UID) && p.admission.Recorder != nil {
		p.admission.Recorder.Event(pod, corev1.EventTypeWarning, ReasonGuestRejected, r.String())
	}
	out := pod.DeepCopy()
	out.Status.Phase = corev1.PodFailed
	out.Status.Reason = ReasonGuestRejected
	out.Status.Message = "Pod was rejected by the guest kubelet: " + r.String()
	p.mu.Lock()
	cb := p.notify
	p.mu.Unlock()
	if cb != nil {
		// The callback may wait for the library's pod cache; never block the create worker.
		go cb(out)
	}
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
	if p.admission != nil {
		p.admission.Rejected.Remove(pod.UID)
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
