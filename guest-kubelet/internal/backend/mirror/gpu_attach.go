package mirror

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// GPUMode is how a guest's nvidia.com/gpu reaches the real node (--gpu-mode).
type GPUMode string

const (
	// GPUModeClaim swaps nvidia.com/gpu for the shared ResourceClaim named by --gpu-claim. The
	// donor holds the same claim (DRA). This is the default and the M1 behaviour.
	GPUModeClaim GPUMode = "claim"
	// GPUModeDevicePlugin (D-NS-10 option ns-deviceplugin) uses no DRA object. The donor books
	// plain nvidia.com/gpu through the cluster's device plugin; the guest kubelet learns which
	// GPU the donor holds (the shadow plugin's pod-resources view) and asks the real kubelet
	// for that GPU's shadow resource, timeslice.io/gpu-shadow-<minor>. The shadow plugin's
	// Allocate hands out the same device nodes, so the kubelet sets the device cgroup and the
	// mirror needs no privilege, no host /dev and no hostPath.
	GPUModeDevicePlugin GPUMode = "deviceplugin"
)

// ParseGPUMode validates a --gpu-mode value.
func ParseGPUMode(s string) (GPUMode, error) {
	switch m := GPUMode(s); m {
	case GPUModeClaim, GPUModeDevicePlugin:
		return m, nil
	}
	return "", fmt.Errorf("unknown GPU mode %q (want %q or %q)", s, GPUModeClaim, GPUModeDevicePlugin)
}

const (
	// AnnotationGPUUUID is the UUID of the GPU the mirror got (deviceplugin mode).
	AnnotationGPUUUID = "timeslice.io/gpu-uuid"
	// AnnotationGPUDonorUID is the UID of the donor pod whose GPU the mirror shares. When that
	// pod goes away, the mirror is fenced and stopped (fence.go).
	AnnotationGPUDonorUID = "timeslice.io/gpu-donor-uid"
	// AttachShadowDevicePlugin is the attach mechanism logged in "mirror gpu attached".
	AttachShadowDevicePlugin = "shadow-device-plugin"
)

// GPUAttachment is the donor GPU a mirror gets in deviceplugin mode.
type GPUAttachment struct {
	// Resource is the GPU's shadow resource (timeslice.io/gpu-shadow-<minor>).
	Resource corev1.ResourceName
	UUID     string
	DonorUID types.UID
	// Donor is "<namespace>/<name>" of the donor pod, for logs and events.
	Donor string
}

func containerRequestsGPU(c *corev1.Container) bool {
	_, req := c.Resources.Requests[GPUResource]
	_, lim := c.Resources.Limits[GPUResource]
	return req || lim
}

// CheckDevicePluginGuest refuses guests the deviceplugin mode cannot serve: one physical GPU
// is shared per mirror, so exactly one container may ask for it, and for exactly one.
func CheckDevicePluginGuest(guest *corev1.Pod) error {
	n := 0
	for i := range guest.Spec.Containers {
		c := &guest.Spec.Containers[i]
		if !containerRequestsGPU(c) {
			continue
		}
		n++
		for _, l := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
			if q, ok := l[GPUResource]; ok && q.Cmp(resource.MustParse("1")) != 0 {
				return fmt.Errorf("guest %s/%s container %s asks for %s %s; --gpu-mode=deviceplugin shares exactly one GPU",
					guest.Namespace, guest.Name, c.Name, q.String(), GPUResource)
			}
		}
	}
	if n > 1 {
		return fmt.Errorf("guest %s/%s asks for %s in %d containers; --gpu-mode=deviceplugin shares one GPU with one container",
			guest.Namespace, guest.Name, GPUResource, n)
	}
	for i := range guest.Spec.InitContainers {
		c := &guest.Spec.InitContainers[i]
		if containerRequestsGPU(c) {
			return fmt.Errorf("guest %s/%s init container %s asks for %s; not supported with --gpu-mode=deviceplugin",
				guest.Namespace, guest.Name, c.Name, GPUResource)
		}
	}
	return nil
}

// attachShadow gives a container the donor GPU's shadow resource (request = limit = 1, as
// extended resources require). mirrorResources has already removed nvidia.com/gpu.
func attachShadow(r *corev1.ResourceRequirements, att *GPUAttachment) {
	one := resource.MustParse("1")
	if r.Requests == nil {
		r.Requests = corev1.ResourceList{}
	}
	if r.Limits == nil {
		r.Limits = corev1.ResourceList{}
	}
	r.Requests[att.Resource] = one.DeepCopy()
	r.Limits[att.Resource] = one.DeepCopy()
}

// ShadowResourceOf returns the shadow resource a mirror holds, or "" if none.
func ShadowResourceOf(m *corev1.Pod) corev1.ResourceName {
	for i := range m.Spec.Containers {
		for name := range m.Spec.Containers[i].Resources.Limits {
			if api.IsShadowResource(name) {
				return name
			}
		}
	}
	return ""
}
