package provider

import (
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ReasonGuestRejected is the event reason and the pod status reason of a guest that admission
// refused. Like a real kubelet's admission failure (OutOfcpu, NodeAffinity), the guest goes
// Failed at once and is never started; its controller decides whether to make a new one.
const ReasonGuestRejected = "GuestRejected"

// GPU model labels on the host Node, most specific first. GKE sets the first, the NVIDIA GPU
// feature discovery the second.
var GPUModelLabels = []string{"cloud.google.com/gke-accelerator", "nvidia.com/gpu.product"}

// AdmissionPolicy is what CreatePod checks before a guest gets a mirror.
type AdmissionPolicy struct {
	// GPUAllowlist holds the GPU models guests may use, normalized (NormalizeGPUModel). A guest
	// asking for a GPU is refused unless the host's model is on it. Empty refuses every GPU guest.
	GPUAllowlist []string
	// HostGPUModel is the host Node's GPU model, normalized. Empty (no model label) refuses
	// every GPU guest: fail closed.
	HostGPUModel string
}

// Rejection says which rule refused a guest and why, in words for the event.
type Rejection struct {
	Rule    string
	Message string
}

// String is the rule and the reason, as the event and the pod status message show them.
func (r *Rejection) String() string { return r.Rule + ": " + r.Message }

// NormalizeGPUModel lower-cases a model and turns spaces and underscores into dashes, so
// "NVIDIA L4", "NVIDIA-L4" and "nvidia-l4" compare equal.
func NormalizeGPUModel(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.NewReplacer(" ", "-", "_", "-").Replace(s)
}

// ParseGPUAllowlist splits a comma-separated list and normalizes each entry.
func ParseGPUAllowlist(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = NormalizeGPUModel(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// HostGPUModel reads the GPU model from a Node's labels, normalized, or "".
func HostGPUModel(n *corev1.Node) string {
	for _, l := range GPUModelLabels {
		if v := n.Labels[l]; v != "" {
			return NormalizeGPUModel(v)
		}
	}
	return ""
}

// Admit returns why a guest must be refused, or nil. It is a pure function of the pod and
// the policy.
func Admit(pod *corev1.Pod, pol AdmissionPolicy) *Rejection {
	if r := checkProbes(pod); r != nil {
		return r
	}
	return checkGPU(pod, pol)
}

func allContainers(pod *corev1.Pod) []corev1.Container {
	return append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
}

// checkProbes is the probe rule: a guest carries no probes at all. The mirror never gets
// probes (the real kubelet must not restart or un-ready a guest the orchestrator froze on
// purpose), so rather than silently dropping them, any livenessProbe, readinessProbe or
// startupProbe, on any container (init containers included), is refused, and so are
// readinessGates. The guest's Ready rests on the guest kubelet's own checks.
func checkProbes(pod *corev1.Pod) *Rejection {
	containers := allContainers(pod)
	for i := range containers {
		ctr := &containers[i]
		for _, pr := range []struct {
			field, rule string
			probe       *corev1.Probe
		}{
			{"livenessProbe", "liveness-probe", ctr.LivenessProbe},
			{"readinessProbe", "readiness-probe", ctr.ReadinessProbe},
			{"startupProbe", "startup-probe", ctr.StartupProbe},
		} {
			if pr.probe != nil {
				return &Rejection{pr.rule, fmt.Sprintf(
					"container %q has a %s; guests may not have probes", ctr.Name, pr.field)}
			}
		}
	}
	if n := len(pod.Spec.ReadinessGates); n > 0 {
		return &Rejection{"readiness-gates", fmt.Sprintf(
			"pod has %d readinessGates (first %q); guests may not have readiness gates",
			n, pod.Spec.ReadinessGates[0].ConditionType)}
	}
	return nil
}

// isGPUResource reports whether a resource name is a GPU or GPU slice (nvidia.com/*,
// amd.com/gpu, anything named */gpu).
func isGPUResource(n corev1.ResourceName) bool {
	s := string(n)
	return strings.HasPrefix(s, "nvidia.com/") || strings.HasSuffix(s, "/gpu")
}

// checkGPU refuses a GPU guest the host cannot give: a GPU resource other than plain
// nvidia.com/gpu (a MIG slice, another vendor), a nodeSelector naming another GPU model (a
// guest bound with spec.nodeName skips the scheduler that would have caught it), or a host
// whose model is not on the allowlist.
func checkGPU(pod *corev1.Pod, pol AdmissionPolicy) *Rejection {
	wantsGPU := false
	containers := allContainers(pod)
	for i := range containers {
		ctr := &containers[i]
		for _, list := range []corev1.ResourceList{ctr.Resources.Requests, ctr.Resources.Limits} {
			for name := range list {
				if !isGPUResource(name) {
					continue
				}
				if name != GPUResource {
					return &Rejection{"gpu-resource", fmt.Sprintf(
						"container %q asks for %s; guests may only ask for %s", ctr.Name, name, GPUResource)}
				}
				wantsGPU = true
			}
		}
	}
	allowed := strings.Join(pol.GPUAllowlist, ",")
	for _, l := range GPUModelLabels {
		if v, ok := pod.Spec.NodeSelector[l]; ok && !contains(pol.GPUAllowlist, NormalizeGPUModel(v)) {
			return &Rejection{"gpu-allowlist", fmt.Sprintf(
				"nodeSelector %s=%s names a GPU model off the allowlist [%s]", l, v, allowed)}
		}
	}
	if !wantsGPU {
		return nil
	}
	if pol.HostGPUModel == "" {
		return &Rejection{"gpu-allowlist", fmt.Sprintf(
			"guest asks for %s but the host Node has no GPU model label (%s)",
			GPUResource, strings.Join(GPUModelLabels, ", "))}
	}
	if !contains(pol.GPUAllowlist, pol.HostGPUModel) {
		return &Rejection{"gpu-allowlist", fmt.Sprintf(
			"guest asks for %s but the host GPU %q is not on the allowlist [%s]", GPUResource, pol.HostGPUModel, allowed)}
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// RejectedSet remembers the UIDs of guests admission refused, so the event recorder can drop
// the library's "ProviderCreateSuccess" for them (CreatePod returns nil after a rejection) and a
// repeated CreatePod does not repeat the event. It is bounded: at maxRejected entries it starts
// over, which at worst repeats one event.
type RejectedSet struct {
	mu   sync.Mutex
	uids map[types.UID]struct{}
}

const maxRejected = 4096

// NewRejectedSet returns an empty set.
func NewRejectedSet() *RejectedSet { return &RejectedSet{uids: map[types.UID]struct{}{}} }

// Add records a UID and reports whether it was new.
func (s *RejectedSet) Add(uid types.UID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uids[uid]; ok {
		return false
	}
	if len(s.uids) >= maxRejected {
		s.uids = map[types.UID]struct{}{}
	}
	s.uids[uid] = struct{}{}
	return true
}

// Has reports whether a UID was rejected. A nil set has nothing.
func (s *RejectedSet) Has(uid types.UID) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.uids[uid]
	return ok
}

// Remove forgets a UID (the guest was deleted).
func (s *RejectedSet) Remove(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.uids, uid)
}
