// Package freeze stops and restarts every process of a mirror pod.
//
// M3 (VK-A3) freezes the pod's cgroup from the guest kubelet itself (Cgroup). That is an
// interim step: it sits behind Backend so that M4 (VK-A4) can replace it with the
// snapshot-agent's Suspend and Resume RPCs, after which the guest kubelet never touches cgroups.
package freeze

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// Backend suspends and resumes one pod. The deadline is the context's. epoch is the guest's
// epoch for this call (timeslice.io/guest-epoch); the snapshot agent fences on it, Cgroup
// ignores it.
type Backend interface {
	// Suspend returns once every process of the pod is stopped, or with an error.
	Suspend(ctx context.Context, pod *corev1.Pod, epoch int64) error
	// Resume returns once the pod's processes run again, or with an error.
	Resume(ctx context.Context, pod *corev1.Pod, epoch int64) error
	// Frozen reports whether the pod is stopped now.
	Frozen(pod *corev1.Pod) (bool, error)
}
