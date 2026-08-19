package features

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestParse(t *testing.T) {
	type testCase struct {
		name    string
		spec    string
		wantErr string
		check   func(t *testing.T, g Gates)
	}

	run := func(t *testing.T, tc testCase) {
		t.Helper()
		gates, err := Parse(tc.spec)
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
			return
		}
		assert.NilError(t, err)
		tc.check(t, gates)
	}

	testCases := []testCase{
		{
			name: "empty spec yields defaults",
			spec: "",
			check: func(t *testing.T, g Gates) {
				assert.Assert(t, !g.Enabled(MemoryRegionsBackend))
				assert.Assert(t, !g.Enabled(DirectMemoryBackend))
			},
		},
		{
			name: "single gate",
			spec: "MemoryRegionsBackend=true",
			check: func(t *testing.T, g Gates) {
				assert.Assert(t, g.Enabled(MemoryRegionsBackend))
				assert.Assert(t, !g.Enabled(DirectMemoryBackend))
			},
		},
		{
			name: "multiple gates with spaces",
			spec: "MemoryRegionsBackend=true, DirectMemoryBackend=true",
			check: func(t *testing.T, g Gates) {
				assert.Assert(t, g.Enabled(MemoryRegionsBackend))
				assert.Assert(t, g.Enabled(DirectMemoryBackend))
			},
		},
		{
			name: "explicit false overrides nothing but is valid",
			spec: "MemoryRegionsBackend=false",
			check: func(t *testing.T, g Gates) {
				assert.Assert(t, !g.Enabled(MemoryRegionsBackend))
			},
		},
		{name: "unknown gate rejected", spec: "NoSuchGate=true", wantErr: "unknown feature gate"},
		{name: "missing value rejected", spec: "MemoryRegionsBackend", wantErr: "expected Name=bool"},
		{name: "non-bool value rejected", spec: "MemoryRegionsBackend=yes-please", wantErr: "expected bool"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestNilGatesUseDefaults(t *testing.T) {
	var gates Gates
	assert.Assert(t, !gates.Enabled(MemoryRegionsBackend))
	assert.Assert(t, !gates.Enabled(DirectMemoryBackend))
}

func TestString(t *testing.T) {
	gates, err := Parse("DirectMemoryBackend=false,MemoryRegionsBackend=true")
	assert.NilError(t, err)
	assert.Equal(t, gates.String(), "DirectMemoryBackend=false,MemoryRegionsBackend=true")
}
