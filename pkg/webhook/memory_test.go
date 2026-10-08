package webhook_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

func gpuDonor(mem string, selector map[string]string) *corev1.Pod {
	p := kuberayDonor()
	p.Spec.NodeSelector = selector
	p.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(mem)},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory:   resource.MustParse(mem),
			webhook.GPUResourceName: resource.MustParse("1"),
		},
	}
	return p
}

func TestDonorMemory_Raise(t *testing.T) {
	cases := []struct {
		name, flag string
		selector   map[string]string
		want       string
	}{
		{"auto H100", "auto", map[string]string{"cloud.google.com/gke-accelerator": "nvidia-h100-80gb"}, "200Gi"},
		{"auto L4", "auto", map[string]string{"cloud.google.com/gke-accelerator": "nvidia-l4"}, "144Gi"},
		{"auto unknown falls back", "auto", nil, "200Gi"},
		{"fixed", "16Gi", nil, "136Gi"},
		{"off", "0", nil, "120Gi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustConfig(t, append(append([]string{}, todayFlags...), "--donor-gpu-memory="+tc.flag))
			pod := mustAllow(t, create(t, cfg, gpuDonor("120Gi", tc.selector)))
			c := pod.Spec.Containers[0]
			if got := c.Resources.Limits[corev1.ResourceMemory]; got.Cmp(resource.MustParse(tc.want)) != 0 {
				t.Errorf("memory limit = %s, want %s", got.String(), tc.want)
			}
			if got := c.Resources.Requests[corev1.ResourceMemory]; got.Cmp(resource.MustParse("120Gi")) != 0 {
				t.Errorf("memory request changed to %s", got.String())
			}
			_, annotated := pod.Annotations[webhook.AnnotationMemoryRaised]
			if annotated != (tc.flag != "0") {
				t.Errorf("annotation present = %v", annotated)
			}
			// Reinvocation adds nothing.
			if res := create(t, cfg, pod); res.patchOps(t) != 0 {
				t.Errorf("reinvocation patched %d ops", res.patchOps(t))
			}
		})
	}
}

func TestDonorMemory_NoLimitUnchanged(t *testing.T) {
	cfg := mustConfig(t, todayFlags)
	pod := kuberayDonor()
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{webhook.GPUResourceName: resource.MustParse("1")}
	got := mustAllow(t, create(t, cfg, pod))
	if _, ok := got.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]; ok {
		t.Errorf("memory limit added to a container without one")
	}
	if _, ok := got.Annotations[webhook.AnnotationMemoryRaised]; ok {
		t.Errorf("annotation without a raise")
	}
}

func TestDonorMemory_InvalidFlag(t *testing.T) {
	if _, err := webhook.ConfigFromFlags([]string{"--donor-gpu-memory=lots"}); err == nil {
		t.Error("--donor-gpu-memory=lots accepted")
	}
}
