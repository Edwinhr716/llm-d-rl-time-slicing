package api_test

import (
	"testing"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

func testGPUs() []api.GPU {
	return []api.GPU{
		{Minor: 0, UUID: "GPU-aaa", Device: "nvidia0", Resource: api.ShadowResource(0)},
		{Minor: 1, UUID: "GPU-bbb", Device: "nvidia1", Resource: api.ShadowResource(1)},
	}
}

func TestShadowResource(t *testing.T) {
	if got := api.ShadowResource(3); got != "timeslice.io/gpu-shadow-3" {
		t.Errorf("ShadowResource(3) = %q", got)
	}
	if !api.IsShadowResource(api.ShadowResource(0)) || api.IsShadowResource("nvidia.com/gpu") {
		t.Error("IsShadowResource")
	}
}

func TestMinorFromDeviceID(t *testing.T) {
	for id, want := range map[string]int{"nvidia0": 0, "nvidia1": 1, "nvidia7": 7, "GPU-bbb": 1} {
		if got, err := api.MinorFromDeviceID(id, testGPUs()); err != nil || got != want {
			t.Errorf("MinorFromDeviceID(%q) = %d, %v; want %d", id, got, err, want)
		}
	}
	for _, id := range []string{"nvidia", "nvidia-1", "nvidiactl", "GPU-zzz", ""} {
		if _, err := api.MinorFromDeviceID(id, testGPUs()); err == nil {
			t.Errorf("MinorFromDeviceID(%q): want an error", id)
		}
	}
}

func TestHeldBy(t *testing.T) {
	holders := &api.Holders{
		GPUs: testGPUs(),
		Holders: []api.Holder{
			{Namespace: "ns", Name: "neighbor", Container: "c", Resource: "nvidia.com/gpu", Minors: []int{0}},
			{Namespace: "ns", Name: "donor", Container: "a", Resource: "nvidia.com/gpu", Minors: []int{1}},
			{Namespace: "ns", Name: "donor", Container: "b", Resource: "nvidia.com/gpu", Minors: []int{1}},
			{Namespace: "ns", Name: "donor", Container: "c", Resource: "example.com/other", Minors: []int{0}},
			{Namespace: "other", Name: "donor", Container: "c", Resource: "nvidia.com/gpu", Minors: []int{0}},
		},
	}
	got := holders.HeldBy("ns", "donor", "nvidia.com/gpu")
	if len(got) != 1 || got[0].UUID != "GPU-bbb" || got[0].Resource != api.ShadowResource(1) {
		t.Errorf("HeldBy = %+v, want only GPU-bbb once", got)
	}
	if got := holders.HeldBy("ns", "nobody", "nvidia.com/gpu"); len(got) != 0 {
		t.Errorf("HeldBy(nobody) = %+v", got)
	}
	if _, ok := holders.GPUByMinor(5); ok {
		t.Error("GPUByMinor(5) found a GPU")
	}
}
