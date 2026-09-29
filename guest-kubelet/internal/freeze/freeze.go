// Package freeze is the interface through which the guest kubelet suspends, resumes and kills
// the processes of a mirror pod.
//
// M3 (VK-A3) implemented it with the cgroup freezer inside the guest kubelet. Since M4 (VK-A4)
// the only implementation is the snapshot agent (internal/handshake): the guest kubelet never
// touches cgroups or the GPU itself.
package freeze

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// Backend suspends, resumes and kills one mirror pod. Every call's deadline is the context's
// deadline, and a call without one is refused. epoch is the guest's epoch for this call
// (timeslice.io/guest-epoch), already written on the mirror; the agent fences on it.
type Backend interface {
	// Suspend returns nil once the pod is checkpointed and frozen and holds no device
	// memory. Any error means the pod's state is unknown: the caller must kill it.
	Suspend(ctx context.Context, pod *corev1.Pod, epoch int64) error
	// Resume returns nil once the pod's processes run again with their device memory back.
	// Any error means the pod's state is unknown: the caller must kill it.
	Resume(ctx context.Context, pod *corev1.Pod, epoch int64) error
	// Kill kills every process of the pod, from any state, and returns nil once the kill is
	// confirmed. reason is recorded by the agent.
	Kill(ctx context.Context, pod *corev1.Pod, reason string) error
}
