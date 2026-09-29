package provider

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// GuestOnlyRecorder drops events about pods that are not guests (by the active --guest-marker,
// the same predicate as the provider). The library records
// "ProviderCreateSuccess" whenever CreatePod returns nil, including for the DaemonSet pods our
// CreatePod ignores, which made them look started (an M0 finding). Events about other objects
// (the Node) pass through. With Rejected set, it also drops "ProviderCreateSuccess" for guests
// admission refused (CreatePod returns nil for them too).
type GuestOnlyRecorder struct {
	record.EventRecorder
	Rejected *RejectedSet
}

// podEventCreateSuccess is the library's reason for a CreatePod that returned nil.
const podEventCreateSuccess = "ProviderCreateSuccess"

func (r GuestOnlyRecorder) keep(obj runtime.Object, reason string) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return true
	}
	if !isGuest(pod) {
		return false
	}
	return reason != podEventCreateSuccess || !r.Rejected.Has(pod.UID)
}

func (r GuestOnlyRecorder) Event(obj runtime.Object, eventtype, reason, message string) {
	if r.keep(obj, reason) {
		r.EventRecorder.Event(obj, eventtype, reason, message)
	}
}

func (r GuestOnlyRecorder) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	if r.keep(obj, reason) {
		r.EventRecorder.Eventf(obj, eventtype, reason, messageFmt, args...)
	}
}

func (r GuestOnlyRecorder) AnnotatedEventf(obj runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...any) {
	if r.keep(obj, reason) {
		r.EventRecorder.AnnotatedEventf(obj, annotations, eventtype, reason, messageFmt, args...)
	}
}
