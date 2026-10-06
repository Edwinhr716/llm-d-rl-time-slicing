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
	// GPUModePooled is deviceplugin
	// with one pooled shadow resource, timeslice.io/gpu-shadow, whose devices are all the GPUs
	// the node's single donor holds. The guest kubelet picks no GPU: the mirror asks for
	// gpu-shadow: k in place of nvidia.com/gpu: k and the real kubelet chooses. The guest
	// kubelet only checks that the mirrors on the node fit the host's gpu-shadow allocatable.
	GPUModePooled GPUMode = "pooled"
)

// SharesDonorGPU reports whether the mode hands the donor's own GPUs to mirrors through a
// shadow resource (deviceplugin or pooled).
func (m GPUMode) SharesDonorGPU() bool { return m == GPUModeDevicePlugin || m == GPUModePooled }

// ParseGPUMode validates a --gpu-mode value.
func ParseGPUMode(s string) (GPUMode, error) {
	switch m := GPUMode(s); m {
	case GPUModeClaim, GPUModeDevicePlugin, GPUModePooled:
		return m, nil
	}
	return "", fmt.Errorf("unknown GPU mode %q (want %q, %q or %q)", s, GPUModeClaim, GPUModeDevicePlugin, GPUModePooled)
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
	// Pooled: each GPU container asks for api.PooledResource with its own nvidia.com/gpu
	// quantity; Resource and UUID are unused (the real kubelet picks the devices).
	Pooled bool
	// Qty is the total GPUs the mirror asks for (pooled mode).
	Qty int64
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
	// A privileged container sees every host device and ignores the capability drop, so the
	// MKNOD guard (dropMknod) would mean nothing: refuse it on the shared-GPU path.
	if err := refusePrivileged(guest, GPUModeDevicePlugin); err != nil {
		return err
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

// refusePrivileged refuses privileged containers on the shared-GPU paths: a privileged
// container sees every host device and ignores the capability drop.
func refusePrivileged(guest *corev1.Pod, mode GPUMode) error {
	for _, list := range [][]corev1.Container{guest.Spec.InitContainers, guest.Spec.Containers} {
		for i := range list {
			if sc := list[i].SecurityContext; sc != nil && sc.Privileged != nil && *sc.Privileged {
				return fmt.Errorf("guest %s/%s container %s is privileged; not allowed with --gpu-mode=%s",
					guest.Namespace, guest.Name, list[i].Name, mode)
			}
		}
	}
	return nil
}

// CheckPooledGuest refuses guests pooled mode cannot serve and returns how many GPUs the
// mirror needs (the sum over its containers). Requests and limits of nvidia.com/gpu must be
// whole and equal, as for any extended resource; init containers may not ask for GPUs.
func CheckPooledGuest(guest *corev1.Pod) (int64, error) {
	if err := refusePrivileged(guest, GPUModePooled); err != nil {
		return 0, err
	}
	for i := range guest.Spec.InitContainers {
		if containerRequestsGPU(&guest.Spec.InitContainers[i]) {
			return 0, fmt.Errorf("guest %s/%s init container %s asks for %s; not supported with --gpu-mode=pooled",
				guest.Namespace, guest.Name, guest.Spec.InitContainers[i].Name, GPUResource)
		}
	}
	var total int64
	for i := range guest.Spec.Containers {
		c := &guest.Spec.Containers[i]
		if !containerRequestsGPU(c) {
			continue
		}
		q := containerGPUQty(c)
		if q <= 0 {
			return 0, fmt.Errorf("guest %s/%s container %s asks for a non-positive %s", guest.Namespace, guest.Name, c.Name, GPUResource)
		}
		total += q
	}
	return total, nil
}

// containerGPUQty is the container's nvidia.com/gpu quantity (limit, else request).
func containerGPUQty(c *corev1.Container) int64 {
	if q, ok := c.Resources.Limits[GPUResource]; ok {
		return q.Value()
	}
	if q, ok := c.Resources.Requests[GPUResource]; ok {
		return q.Value()
	}
	return 0
}

// attachShadowQty gives a container qty of a shadow resource (request = limit).
func attachShadowQty(r *corev1.ResourceRequirements, res corev1.ResourceName, qty int64) {
	q := *resource.NewQuantity(qty, resource.DecimalSI)
	if r.Requests == nil {
		r.Requests = corev1.ResourceList{}
	}
	if r.Limits == nil {
		r.Limits = corev1.ResourceList{}
	}
	r.Requests[res] = q.DeepCopy()
	r.Limits[res] = q.DeepCopy()
}

// PooledQtyOf returns how many pooled shadow devices a mirror holds.
func PooledQtyOf(m *corev1.Pod) int64 {
	var n int64
	for i := range m.Spec.Containers {
		if q, ok := m.Spec.Containers[i].Resources.Limits[api.PooledResource]; ok {
			n += q.Value()
		}
	}
	return n
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
