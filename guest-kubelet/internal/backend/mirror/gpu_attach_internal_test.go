package mirror

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

func testAttachment() *GPUAttachment {
	return &GPUAttachment{DonorUID: "donor-uid", Donor: "ns/donor", Qty: 1}
}

func TestParseGPUMode(t *testing.T) {
	if got, err := ParseGPUMode("pooled"); err != nil || got != GPUModePooled {
		t.Errorf("ParseGPUMode(pooled) = %q, %v", got, err)
	}
	for _, in := range []string{"claim", "deviceplugin", "timeslicing", ""} {
		if _, err := ParseGPUMode(in); err == nil {
			t.Errorf("ParseGPUMode(%q): want an error", in)
		}
	}
}

// Build is BuildWithGPU without a donor: the same mirror, minus the donor annotation.
func TestBuildIsBuildWithGPUNil(t *testing.T) {
	tc := testConfig()
	built, err := Build(testGuest(), &tc)
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
	if _, ok := built.Annotations[AnnotationGPUDonorUID]; ok {
		t.Error("no donor annotation without a donor")
	}
}

func TestBuildWithGPU_PooledResourceAndDonor(t *testing.T) {
	guest := testGuest()
	cfg := testConfig()
	mir, err := BuildWithGPU(guest, &cfg, &GPUAttachment{DonorUID: "donor-uid", Donor: "ns/donor", Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctr := &mir.Spec.Containers[0]
	if len(mir.Spec.ResourceClaims) != 0 || len(ctr.Resources.Claims) != 0 {
		t.Errorf("no DRA claim: pod %v container %v", mir.Spec.ResourceClaims, ctr.Resources.Claims)
	}
	for _, list := range []corev1.ResourceList{ctr.Resources.Requests, ctr.Resources.Limits} {
		if _, ok := list[GPUResource]; ok {
			t.Errorf("nvidia.com/gpu must be removed: %v", list)
		}
		if q, ok := list[api.PooledResource]; !ok || q.Cmp(resource.MustParse("1")) != 0 {
			t.Errorf("want %s: 1, got %v", api.PooledResource, list)
		}
	}
	if q := ctr.Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("cpu request must still be capped: %s", q.String())
	}
	if mir.Annotations[AnnotationGPUDonorUID] != "donor-uid" {
		t.Errorf("annotations: %v", mir.Annotations)
	}
	// Unprivileged: the builder adds no privilege, volume or host access; its only
	// securityContext change is the MKNOD drop.
	sc := ctr.SecurityContext
	if sc == nil || sc.Privileged != nil || sc.Capabilities == nil || len(sc.Capabilities.Add) != 0 ||
		!reflect.DeepEqual(sc.Capabilities.Drop, []corev1.Capability{CapMknod}) || len(mir.Spec.Volumes) != len(guest.Spec.Volumes) || mir.Spec.HostPID || mir.Spec.HostIPC {
		t.Errorf("mirror must not gain privilege: sc=%v volumes=%v", ctr.SecurityContext, mir.Spec.Volumes)
	}
	if _, ok := guest.Spec.Containers[0].Resources.Limits[api.PooledResource]; ok {
		t.Error("BuildWithGPU modified the guest")
	}
}

func TestBuildWithGPU_OnlyTheGPUContainerGetsIt(t *testing.T) {
	guest := testGuest()
	guest.Spec.Containers = append(guest.Spec.Containers, corev1.Container{Name: "sidecar", Image: "busybox"})
	cfg := testConfig()
	mir, err := BuildWithGPU(guest, &cfg, &GPUAttachment{Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if PooledQtyOf(&corev1.Pod{Spec: corev1.PodSpec{Containers: mir.Spec.Containers[1:]}}) != 0 {
		t.Errorf("sidecar must not get the GPU: %v", mir.Spec.Containers[1].Resources)
	}
}

func TestBuildWithGPU_CPUGuestHasNoAttachment(t *testing.T) {
	cfg := testConfig()
	mir, err := BuildWithGPU(cpuGuest("g1"), &cfg, &GPUAttachment{DonorUID: "donor-uid", Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if PooledQtyOf(mir) != 0 || mir.Annotations[AnnotationGPUDonorUID] != "" {
		t.Errorf("a CPU guest gets no GPU: %v %v", mir.Spec.Containers[0].Resources, mir.Annotations)
	}
}
