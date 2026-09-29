package mirror

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

func testAttachment() *GPUAttachment {
	return &GPUAttachment{Resource: api.ShadowResource(1), UUID: "GPU-1111", DonorUID: "donor-uid", Donor: "ns/donor"}
}

func TestParseGPUMode(t *testing.T) {
	for in, want := range map[string]GPUMode{"claim": GPUModeClaim, "deviceplugin": GPUModeDevicePlugin} {
		if got, err := ParseGPUMode(in); err != nil || got != want {
			t.Errorf("ParseGPUMode(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := ParseGPUMode("timeslicing"); err == nil {
		t.Error("want an error for an unknown mode")
	}
}

// Claim mode is the default and must be exactly what Build did before --gpu-mode.
func TestGPUModeClaim_BuildIsBuildWithGPUNil(t *testing.T) {
	built, err := Build(testGuest(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	withNil, err := BuildWithGPU(testGuest(), &cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(built, withNil) {
		t.Error("Build and BuildWithGPU(nil) differ")
	}
	if _, ok := built.Annotations[AnnotationGPUUUID]; ok {
		t.Error("claim mode must not add the GPU UUID annotation")
	}
	for name := range built.Spec.Containers[0].Resources.Limits {
		if api.IsShadowResource(name) {
			t.Errorf("claim mode must not ask for %s", name)
		}
	}
}

func TestGPUModeDevicePlugin_AttachesShadowResource(t *testing.T) {
	guest := testGuest()
	cfg := testConfig()
	cfg.GPUClaim = "" // deviceplugin mode runs without a claim
	mir, err := BuildWithGPU(guest, &cfg, testAttachment())
	if err != nil {
		t.Fatal(err)
	}
	ctr := &mir.Spec.Containers[0]
	if len(mir.Spec.ResourceClaims) != 0 || len(ctr.Resources.Claims) != 0 {
		t.Errorf("no DRA claim in deviceplugin mode: pod %v container %v", mir.Spec.ResourceClaims, ctr.Resources.Claims)
	}
	for _, list := range []corev1.ResourceList{ctr.Resources.Requests, ctr.Resources.Limits} {
		if _, ok := list[GPUResource]; ok {
			t.Errorf("nvidia.com/gpu must be removed: %v", list)
		}
		if q, ok := list["timeslice.io/gpu-shadow-1"]; !ok || q.Cmp(resource.MustParse("1")) != 0 {
			t.Errorf("want timeslice.io/gpu-shadow-1: 1, got %v", list)
		}
	}
	if q := ctr.Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("cpu request must still be capped: %s", q.String())
	}
	if mir.Annotations[AnnotationGPUUUID] != "GPU-1111" || mir.Annotations[AnnotationGPUDonorUID] != "donor-uid" {
		t.Errorf("annotations: %v", mir.Annotations)
	}
	if ShadowResourceOf(mir) != "timeslice.io/gpu-shadow-1" {
		t.Errorf("ShadowResourceOf = %q", ShadowResourceOf(mir))
	}
	// Unprivileged: the builder adds no securityContext, volume or host access.
	if ctr.SecurityContext != nil || len(mir.Spec.Volumes) != len(guest.Spec.Volumes) || mir.Spec.HostPID || mir.Spec.HostIPC {
		t.Errorf("mirror must not gain privilege: sc=%v volumes=%v", ctr.SecurityContext, mir.Spec.Volumes)
	}
	if _, ok := guest.Spec.Containers[0].Resources.Limits["timeslice.io/gpu-shadow-1"]; ok {
		t.Error("BuildWithGPU modified the guest")
	}
}

func TestGPUModeDevicePlugin_OnlyTheGPUContainerGetsIt(t *testing.T) {
	guest := testGuest()
	guest.Spec.Containers = append(guest.Spec.Containers, corev1.Container{Name: "sidecar", Image: "busybox"})
	cfg := testConfig()
	mir, err := BuildWithGPU(guest, &cfg, testAttachment())
	if err != nil {
		t.Fatal(err)
	}
	if ShadowResourceOf(&corev1.Pod{Spec: corev1.PodSpec{Containers: mir.Spec.Containers[1:]}}) != "" {
		t.Errorf("sidecar must not get the GPU: %v", mir.Spec.Containers[1].Resources)
	}
}

func TestGPUModeDevicePlugin_CPUGuestHasNoAttachment(t *testing.T) {
	cfg := testConfig()
	mir, err := BuildWithGPU(cpuGuest("g1"), &cfg, testAttachment())
	if err != nil {
		t.Fatal(err)
	}
	if ShadowResourceOf(mir) != "" || mir.Annotations[AnnotationGPUUUID] != "" {
		t.Errorf("a CPU guest gets no GPU: %v %v", mir.Spec.Containers[0].Resources, mir.Annotations)
	}
}

func TestGPUModeDevicePlugin_RefusesUnsupportedGuests(t *testing.T) {
	two := testGuest()
	two.Spec.Containers[0].Resources.Limits[GPUResource] = resource.MustParse("2")
	two.Spec.Containers[0].Resources.Requests[GPUResource] = resource.MustParse("2")

	multi := testGuest()
	second := *multi.Spec.Containers[0].DeepCopy()
	second.Name = "second"
	multi.Spec.Containers = append(multi.Spec.Containers, second)

	initGPU := testGuest()
	initGPU.Spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{GPUResource: resource.MustParse("1")},
	}}}

	cfg := testConfig()
	for name, guest := range map[string]*corev1.Pod{"two GPUs": two, "two containers": multi, "init container": initGPU} {
		if _, err := BuildWithGPU(guest, &cfg, testAttachment()); err == nil {
			t.Errorf("%s: want an error", name)
		}
		if err := CheckDevicePluginGuest(guest); err == nil {
			t.Errorf("%s: CheckDevicePluginGuest wants an error", name)
		}
	}
	if err := CheckDevicePluginGuest(testGuest()); err != nil {
		t.Errorf("one container, one GPU is fine: %v", err)
	}
}
