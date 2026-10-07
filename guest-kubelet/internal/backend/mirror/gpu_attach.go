package mirror

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// GPUMode is how a guest's nvidia.com/gpu reaches the real node (--gpu-mode). Pooled is the
// only mode: the mirror asks for timeslice.io/gpu-shadow, whose devices are all the GPUs the
// node's single donor holds (the GPU shadow device plugin, --mode=pooled). The guest kubelet
// picks no GPU: the mirror asks for gpu-shadow: k in place of nvidia.com/gpu: k and the real
// kubelet chooses. The guest kubelet only checks that the mirrors on the node fit the host's
// gpu-shadow allocatable.
type GPUMode string

// GPUModePooled is the pooled shadow resource (see GPUMode).
const GPUModePooled GPUMode = "pooled"

// ParseGPUMode validates a --gpu-mode value.
func ParseGPUMode(s string) (GPUMode, error) {
	if m := GPUMode(s); m == GPUModePooled {
		return m, nil
	}
	return "", fmt.Errorf("unknown GPU mode %q (want %q)", s, GPUModePooled)
}

const (
	// AnnotationGPUDonorUID is the UID of the donor pod whose GPUs the mirror shares. When that
	// pod goes away, the mirror is fenced and stopped (fence.go).
	AnnotationGPUDonorUID = "timeslice.io/gpu-donor-uid"
	// AttachShadowDevicePlugin is the attach mechanism logged in "mirror gpu attached".
	AttachShadowDevicePlugin = "shadow-device-plugin"
)

// GPUAttachment is the donor GPUs a mirror gets: each GPU container asks for
// api.PooledResource with its own nvidia.com/gpu quantity (the real kubelet picks the devices).
type GPUAttachment struct {
	DonorUID types.UID
	// Donor is "<namespace>/<name>" of the donor pod, for logs and events.
	Donor string
	// Qty is the total GPUs the mirror asks for.
	Qty int64
}

// refusePrivileged refuses privileged containers on the shared-GPU path: a privileged
// container sees every host device and ignores the capability drop.
func refusePrivileged(guest *corev1.Pod) error {
	for _, list := range [][]corev1.Container{guest.Spec.InitContainers, guest.Spec.Containers} {
		for i := range list {
			if sc := list[i].SecurityContext; sc != nil && sc.Privileged != nil && *sc.Privileged {
				return fmt.Errorf("guest %s/%s container %s is privileged; not allowed with --gpu-mode=%s",
					guest.Namespace, guest.Name, list[i].Name, GPUModePooled)
			}
		}
	}
	return nil
}

// CheckPooledGuest refuses guests pooled mode cannot serve and returns how many GPUs the
// mirror needs (the sum over its containers). Requests and limits of nvidia.com/gpu must be
// whole and equal, as for any extended resource; init containers may not ask for GPUs.
func CheckPooledGuest(guest *corev1.Pod) (int64, error) {
	if err := refusePrivileged(guest); err != nil {
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
