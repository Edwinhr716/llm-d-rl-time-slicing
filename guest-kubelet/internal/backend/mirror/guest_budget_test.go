package mirror_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// budgetConfig is the deploy/deployment.yaml headroom (1 CPU, 4Gi) with the mode's request rule.
func budgetConfig(realRequests bool) mirror.Config {
	return mirror.Config{
		HostNode: "host", VirtualNode: "vk", GPUClaim: "claim",
		CPUHeadroom: resource.MustParse("1"), MemoryHeadroom: resource.MustParse("4Gi"),
		RealRequests: realRequests,
	}
}

func budgetGuest(res ...corev1.ResourceRequirements) *corev1.Pod {
	guest := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "g", UID: "uid"}}
	for _, r := range res {
		guest.Spec.Containers = append(guest.Spec.Containers, corev1.Container{Name: "c", Resources: r})
	}
	return guest
}

func bigGuest() *corev1.Pod {
	return budgetGuest(corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("20Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("30Gi")},
	})
}

func build(t *testing.T, guest *corev1.Pod, realRequests bool) *corev1.Pod {
	t.Helper()
	m, err := mirror.Build(guest, budgetConfig(realRequests))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func checkReq(t *testing.T, got corev1.ResourceList, cpu, mem string) {
	t.Helper()
	if got.Cpu().Cmp(resource.MustParse(cpu)) != 0 || got.Memory().Cmp(resource.MustParse(mem)) != 0 {
		t.Errorf("requests = %s / %s, want %s / %s", got.Cpu().String(), got.Memory().String(), cpu, mem)
	}
}

func TestGuestBudget_Static_RequestsCappedLimitsKept(t *testing.T) {
	guest := bigGuest()
	m := build(t, guest, false)
	res := m.Spec.Containers[0].Resources
	checkReq(t, res.Requests, "1", "4Gi")
	if res.Limits.Memory().Cmp(resource.MustParse("30Gi")) != 0 {
		t.Errorf("memory limit = %s, want 30Gi", res.Limits.Memory().String())
	}
	if sum := mirror.ResourceSummary(guest, m); !sum.Capped {
		t.Error("static summary must report capped")
	}
}

func TestGuestBudget_Computed_RealRequestsSameLimits(t *testing.T) {
	guest := bigGuest()
	m := build(t, guest, true)
	res := m.Spec.Containers[0].Resources
	checkReq(t, res.Requests, "6", "20Gi")
	static := build(t, guest, false).Spec.Containers[0].Resources
	if res.Limits.Memory().Cmp(*static.Limits.Memory()) != 0 {
		t.Errorf("memory limit = %s, want the static mode's %s", res.Limits.Memory().String(), static.Limits.Memory().String())
	}
	sum := mirror.ResourceSummary(guest, m)
	if sum.Capped || sum.ReqCPU.Cmp(resource.MustParse("6")) != 0 || sum.LimMemory.Cmp(resource.MustParse("30Gi")) != 0 {
		t.Errorf("summary = %+v, want uncapped 6 CPU, limit 30Gi", sum)
	}
}

func TestGuestBudget_Computed_LimitOnlyGetsRequestEqualLimit(t *testing.T) {
	guest := budgetGuest(corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("8Gi")},
	})
	checkReq(t, build(t, guest, true).Spec.Containers[0].Resources.Requests, "2", "8Gi")
	checkReq(t, build(t, guest, false).Spec.Containers[0].Resources.Requests, "1", "4Gi")
}

func TestGuestBudget_Computed_GPUStillBecomesClaim(t *testing.T) {
	guest := budgetGuest(corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3"), mirror.GPUResource: resource.MustParse("1")},
		Limits:   corev1.ResourceList{mirror.GPUResource: resource.MustParse("1")},
	})
	res := build(t, guest, true).Spec.Containers[0].Resources
	if _, ok := res.Requests[mirror.GPUResource]; ok {
		t.Error("the mirror must not request nvidia.com/gpu")
	}
	if len(res.Claims) != 1 || res.Claims[0].Name != mirror.ClaimRefName {
		t.Errorf("claims = %v, want the guest-gpu claim", res.Claims)
	}
	checkReq(t, res.Requests, "3", "0")
}

func TestGuestBudget_Static_UncappedGuestNotReportedCapped(t *testing.T) {
	guest := budgetGuest(corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
	})
	if sum := mirror.ResourceSummary(guest, build(t, guest, false)); sum.Capped {
		t.Error("a guest under the headroom is not capped")
	}
}
