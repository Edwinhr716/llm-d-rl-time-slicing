package provider

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// GuestOnlyRecorder drops events about pods that are not guests. The library records
// "ProviderCreateSuccess" whenever CreatePod returns nil, including for the DaemonSet pods our
// CreatePod ignores, which made them look started (an M0 finding). Events about other objects
// (the Node) pass through.
type GuestOnlyRecorder struct {
	record.EventRecorder
}

func keep(obj runtime.Object) bool {
	pod, ok := obj.(*corev1.Pod)
	return !ok || IsGuest(pod)
}

func (r GuestOnlyRecorder) Event(obj runtime.Object, eventtype, reason, message string) {
	if keep(obj) {
		r.EventRecorder.Event(obj, eventtype, reason, message)
	}
}

func (r GuestOnlyRecorder) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	if keep(obj) {
		r.EventRecorder.Eventf(obj, eventtype, reason, messageFmt, args...)
	}
}

func (r GuestOnlyRecorder) AnnotatedEventf(obj runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...any) {
	if keep(obj) {
		r.EventRecorder.AnnotatedEventf(obj, annotations, eventtype, reason, messageFmt, args...)
	}
}
