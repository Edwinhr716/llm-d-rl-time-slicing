package provider

import "testing"

func TestResolveGPUMemory(t *testing.T) {
	cases := []struct {
		setting, model, want string
		fallback             bool
	}{
		{"auto", "nvidia-l4", "23034Mi", false},
		{"auto", "nvidia-h100-80gb", "81559Mi", false},
		{"auto", "NVIDIA-H100-80GB-HBM3", "81559Mi", false},
		{"auto", "", FallbackGPUMemory, true},
		{"auto", "nvidia-new-gpu", FallbackGPUMemory, true},
		{"40Gi", "nvidia-h100-80gb", "40Gi", false},
	}
	for _, tc := range cases {
		got, fb := ResolveGPUMemory(tc.setting, tc.model)
		if got != tc.want || fb != tc.fallback {
			t.Errorf("ResolveGPUMemory(%q, %q) = %q, %v; want %q, %v", tc.setting, tc.model, got, fb, tc.want, tc.fallback)
		}
	}
}

func TestDefaultGPUAllowlist_H100(t *testing.T) {
	allow := ParseGPUAllowlist(DefaultGPUAllowlist)
	for _, m := range []string{"nvidia-l4", "nvidia-h100-80gb", "nvidia-h100-mega-80gb"} {
		if !contains(allow, m) {
			t.Errorf("default allowlist %v lacks %s", allow, m)
		}
	}
}
