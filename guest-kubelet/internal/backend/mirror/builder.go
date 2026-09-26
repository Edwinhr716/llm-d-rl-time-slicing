// Package mirror is backend c1 from the plan: every guest pod bound to the virtual node gets a
// "mirror" pod, an ordinary pod pinned to the real node that the real kubelet runs. This file
// turns a guest pod into its mirror. It is a pure function, so it is unit-tested without a cluster.
package mirror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// LabelMirrorOf holds the guest pod's UID. It is how a mirror names its guest.
	LabelMirrorOf = "timeslice.io/mirror-of"
	// LabelMirrorNode holds the virtual node's name. The backend's informer selects on it,
	// so two guest kubelets never adopt each other's mirrors.
	LabelMirrorNode = "timeslice.io/mirror-node"
	// AnnotationGuestName is the guest pod's name (labels cannot hold every pod name).
	AnnotationGuestName = "timeslice.io/guest-name"
	// AnnotationGuestSpecHash is a hash of the guest's containers. A guest re-created with the
	// same name may adopt an orphaned mirror only if the hash matches.
	AnnotationGuestSpecHash = "timeslice.io/guest-spec-hash"

	// Suffix is appended to the guest's name to get the mirror's name. The name is
	// deterministic, so a create is idempotent (AlreadyExists), like LWS's StatefulSet names.
	Suffix = "-m"

	// GPUResource is what guests request on the virtual node.
	GPUResource corev1.ResourceName = "nvidia.com/gpu"
	// ClaimRefName is the name of the pod-level resourceClaims entry the mirror uses.
	ClaimRefName = "guest-gpu"
)

// Config is everything the builder needs besides the guest itself.
type Config struct {
	HostNode    string // real node the mirror is pinned to (spec.nodeName)
	VirtualNode string // our virtual node; stored in LabelMirrorNode
	// Headroom caps each container's requests on the real node. The kubelet re-runs the fit
	// check on pods that skip the scheduler, so a mirror asking for the guest's full requests
	// is rejected (OutOfcpu/OutOfmemory) on a full trainer node. Zero means "do not cap".
	CPUHeadroom    resource.Quantity
	MemoryHeadroom resource.Quantity
	// GPUClaim is the ResourceClaim (in the guest's namespace) that replaces nvidia.com/gpu.
	// Empty means guests asking for a GPU are refused.
	GPUClaim string
	// HostTaints are the real node's taints. The mirror tolerates each one, as the approach-7
	// tenant tolerated nvidia.com/gpu and timeslice.io/shared.
	HostTaints []corev1.Taint
	// GuestTaintKey is the virtual node's taint. Its toleration is dropped from the mirror.
	GuestTaintKey string
	// OwnerRef makes the guest the mirror's owner, so deleting the guest garbage-collects the
	// mirror. With false, the mirror outlives a guest that is force-deleted (for example after
	// the virtual Node is deleted) and a guest re-created with the same name re-adopts it.
	OwnerRef bool
}

// Name returns the mirror's name for a guest.
func Name(guestName string) string { return guestName + Suffix }

// SpecHash hashes the fields of the guest that decide what runs: its containers, minus the
// service-account token mount. Admission adds that mount as "kube-api-access-<random>", so it
// differs between two pods from the same template and would block every re-adoption (found
// in the M1 outage test with a StatefulSet).
func SpecHash(guest *corev1.Pod) string {
	cs := make([]corev1.Container, len(guest.Spec.Containers))
	for i, c := range guest.Spec.Containers {
		c = *c.DeepCopy()
		mounts := c.VolumeMounts[:0]
		for _, m := range c.VolumeMounts {
			if !strings.HasPrefix(m.Name, "kube-api-access-") {
				mounts = append(mounts, m)
			}
		}
		c.VolumeMounts = mounts
		cs[i] = c
	}
	b, _ := json.Marshal(cs)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// RequestsGPU reports whether any container asks for nvidia.com/gpu.
func RequestsGPU(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Requests[GPUResource]; ok {
			return true
		}
		if _, ok := c.Resources.Limits[GPUResource]; ok {
			return true
		}
	}
	return false
}

// Build returns the mirror pod for a guest. The guest is not modified.
func Build(guest *corev1.Pod, cfg Config) (*corev1.Pod, error) {
	if cfg.HostNode == "" {
		return nil, fmt.Errorf("mirror: HostNode is required")
	}
	gpu := RequestsGPU(guest)
	if gpu && cfg.GPUClaim == "" {
		return nil, fmt.Errorf("guest %s/%s requests %s but no GPU claim is configured", guest.Namespace, guest.Name, GPUResource)
	}

	spec := *guest.Spec.DeepCopy()

	// Placement: the real node, directly. The scheduler never sees the mirror, so every
	// scheduling field the guest used to reach the virtual node must go, or the kubelet's own
	// admission (it re-checks nodeSelector and affinity) rejects the pod.
	spec.NodeName = cfg.HostNode
	spec.NodeSelector = nil
	spec.Affinity = nil
	spec.TopologySpreadConstraints = nil
	spec.SchedulingGates = nil
	spec.Tolerations = mirrorTolerations(guest.Spec.Tolerations, cfg)
	// Admission fills these from the PriorityClass/RuntimeClass; copying the values makes it
	// reject the create ("must not be set"), so let admission fill them again.
	spec.Priority = nil
	spec.PreemptionPolicy = nil
	spec.Overhead = nil
	spec.Resources = nil // pod-level resources would bypass the per-container headroom cap
	// The guest kubelet owns readiness (plan option c1). The real kubelet must not probe the
	// mirror, and the guest's readiness gates belong to the guest, not the mirror.
	spec.ReadinessGates = nil
	// Keep the guest's hostname inside the container (it would default to "<guest>-m").
	if spec.Hostname == "" {
		spec.Hostname = guest.Name
	}

	for i := range spec.Containers {
		c := &spec.Containers[i]
		c.LivenessProbe, c.ReadinessProbe, c.StartupProbe = nil, nil, nil
		c.Resources = mirrorResources(c.Resources, cfg, gpu)
		c.Env = rewriteDownwardEnv(c.Env, guest)
	}
	if gpu {
		spec.ResourceClaims = append(spec.ResourceClaims, corev1.PodResourceClaim{
			Name: ClaimRefName, ResourceClaimName: &cfg.GPUClaim,
		})
	}

	m := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      Name(guest.Name),
			Namespace: guest.Namespace,
			// None of the guest's labels: Services, Jobs and ReplicaSets select the guest, and a
			// mirror carrying its labels would be a second endpoint for one process.
			Labels: map[string]string{
				LabelMirrorOf:   string(guest.UID),
				LabelMirrorNode: cfg.VirtualNode,
			},
			Annotations: map[string]string{
				AnnotationGuestName:     guest.Name,
				AnnotationGuestSpecHash: SpecHash(guest),
			},
		},
		Spec: spec,
	}
	if cfg.OwnerRef {
		m.OwnerReferences = []metav1.OwnerReference{OwnerRef(guest)}
	}
	return m, nil
}

// OwnerRef is the reference from a mirror to its guest. blockOwnerDeletion is left unset: it
// needs extra RBAC (pods/finalizers) and nothing waits on foreground deletion of guests.
func OwnerRef(guest *corev1.Pod) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "v1", Kind: "Pod", Name: guest.Name, UID: guest.UID,
		Controller: new(true),
	}
}

func mirrorTolerations(guestTols []corev1.Toleration, cfg Config) []corev1.Toleration {
	var out []corev1.Toleration
	for _, t := range guestTols {
		if cfg.GuestTaintKey != "" && t.Key == cfg.GuestTaintKey {
			continue // the virtual node's taint means nothing on the real node
		}
		out = append(out, t)
	}
	for _, taint := range cfg.HostTaints {
		tol := corev1.Toleration{Key: taint.Key, Operator: corev1.TolerationOpExists, Effect: taint.Effect}
		if !tolerated(out, tol) {
			out = append(out, tol)
		}
	}
	return out
}

func tolerated(tols []corev1.Toleration, want corev1.Toleration) bool {
	for _, t := range tols {
		if t.Key == want.Key && t.Operator == corev1.TolerationOpExists && (t.Effect == "" || t.Effect == want.Effect) {
			return true
		}
	}
	return false
}

// mirrorResources caps requests at the headroom and swaps the GPU for the claim. Limits are
// kept (a memory limit is what protects the trainer from the guest), except that a capped
// request never exceeds its limit.
func mirrorResources(in corev1.ResourceRequirements, cfg Config, gpu bool) corev1.ResourceRequirements {
	out := *in.DeepCopy()
	delete(out.Requests, GPUResource)
	delete(out.Limits, GPUResource)
	// A container with a limit but no request gets request=limit from API defaulting, which
	// would dodge the cap. Make the request explicit so the cap applies.
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if _, hasReq := out.Requests[name]; hasReq {
			continue
		}
		if lim, hasLim := out.Limits[name]; hasLim {
			if out.Requests == nil {
				out.Requests = corev1.ResourceList{}
			}
			out.Requests[name] = lim.DeepCopy()
		}
	}
	capAt(out.Requests, corev1.ResourceCPU, cfg.CPUHeadroom)
	capAt(out.Requests, corev1.ResourceMemory, cfg.MemoryHeadroom)
	if gpu {
		out.Claims = append(out.Claims, corev1.ResourceClaim{Name: ClaimRefName})
	}
	if len(out.Requests) == 0 {
		out.Requests = nil
	}
	if len(out.Limits) == 0 {
		out.Limits = nil
	}
	return out
}

func capAt(list corev1.ResourceList, name corev1.ResourceName, headroom resource.Quantity) {
	if headroom.IsZero() || list == nil {
		return
	}
	if q, ok := list[name]; ok && q.Cmp(headroom) > 0 {
		list[name] = headroom.DeepCopy()
	}
}

// rewriteDownwardEnv replaces downward-API env vars that would otherwise describe the mirror
// (its name, UID, labels) with the guest's values. Other fieldRefs (podIP, nodeName) are left
// to the real kubelet: they are the same for both pods, or the real value is the useful one.
func rewriteDownwardEnv(env []corev1.EnvVar, guest *corev1.Pod) []corev1.EnvVar {
	for i := range env {
		vf := env[i].ValueFrom
		if vf == nil || vf.FieldRef == nil {
			continue
		}
		path := vf.FieldRef.FieldPath
		var val string
		switch {
		case path == "metadata.name":
			val = guest.Name
		case path == "metadata.uid":
			val = string(guest.UID)
		case len(path) > len("metadata.labels['") && path[:len("metadata.labels['")] == "metadata.labels['":
			val = guest.Labels[path[len("metadata.labels['"):len(path)-2]]
		case len(path) > len("metadata.annotations['") && path[:len("metadata.annotations['")] == "metadata.annotations['":
			val = guest.Annotations[path[len("metadata.annotations['"):len(path)-2]]
		default:
			continue
		}
		env[i].Value, env[i].ValueFrom = val, nil
	}
	return env
}
