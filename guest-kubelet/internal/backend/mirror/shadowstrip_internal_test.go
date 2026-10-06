package mirror

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Second guard: a shadow resource a guest lists never reaches the mirror.
func TestMirrorResourcesStripsShadow(t *testing.T) {
	one := resource.MustParse("1")
	in := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{"timeslice.io/gpu-shadow-1": one, corev1.ResourceCPU: resource.MustParse("500m")},
		Limits:   corev1.ResourceList{"timeslice.io/gpu-shadow-1": one, GPUResource: one},
	}
	out := mirrorResources(in, &Config{RealRequests: true}, false, false)
	for _, l := range []corev1.ResourceList{out.Requests, out.Limits} {
		if _, ok := l["timeslice.io/gpu-shadow-1"]; ok {
			t.Fatalf("shadow resource copied to mirror: %+v", out)
		}
	}
	if _, ok := out.Requests[corev1.ResourceCPU]; !ok {
		t.Fatalf("cpu request lost: %+v", out)
	}
}
