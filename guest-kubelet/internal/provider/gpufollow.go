package provider

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// HostAllocatable returns the host's allocatable of res (0 if absent).
func HostAllocatable(host *corev1.Node, res corev1.ResourceName) int64 {
	if q, ok := host.Status.Allocatable[res]; ok {
		return q.Value()
	}
	return 0
}

// FollowHostGPUs keeps the virtual node's nvidia.com/gpu capacity equal to the host's
// allocatable of res, read every interval (pooled mode). A read error keeps
// the last value.
func FollowHostGPUs(
	ctx context.Context, client kubernetes.Interface, hostNode string, res corev1.ResourceName,
	interval time.Duration, np *NodeProvider,
) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		host, err := client.CoreV1().Nodes().Get(ctx, hostNode, metav1.GetOptions{})
		if err != nil {
			log.G(ctx).WithError(err).Warn("pooled: read host allocatable failed; keeping the last GPU capacity")
		} else if n := HostAllocatable(host, res); np.SetGPUs(n) {
			log.G(ctx).WithField("resource", string(res)).WithField("gpus", n).Info("virtual node GPU capacity follows host")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
