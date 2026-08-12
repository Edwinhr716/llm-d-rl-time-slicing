package features

import (
	"strings"
	"testing"
)

// testGate is a second gate registered for the duration of a test so that
// multi-gate parsing and independence can be exercised even while only one
// real gate exists.
const testGate Feature = "TestGate"

// withGate registers an extra gate in the shared defaults registry for the
// duration of a test.
func withGate(t *testing.T, name Feature, defaultValue bool) {
	t.Helper()
	defaults[name] = defaultValue
	t.Cleanup(func() { delete(defaults, name) })
}

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		spec string
		// want is the exact raw map Parse must return: asserting on the
		// parsed entries (not just Enabled, which falls back to defaults)
		// catches a pair being silently dropped.
		want    map[Feature]bool
		wantErr []string
	}{
		{
			name: "empty spec yields no explicit gates",
			spec: "",
			want: map[Feature]bool{},
		},
		{
			name: "whitespace-only spec yields no explicit gates",
			spec: "   ",
			want: map[Feature]bool{},
		},
		{
			name: "single gate",
			spec: "DirectMemoryBackend=true",
			want: map[Feature]bool{DirectMemoryBackend: true},
		},
		{
			name: "multiple gates with spaces",
			spec: "DirectMemoryBackend=true, TestGate=false",
			want: map[Feature]bool{DirectMemoryBackend: true, testGate: false},
		},
		{
			name: "whitespace around name and value tolerated",
			spec: " DirectMemoryBackend = true ",
			want: map[Feature]bool{DirectMemoryBackend: true},
		},
		{
			name: "explicit false",
			spec: "DirectMemoryBackend=false",
			want: map[Feature]bool{DirectMemoryBackend: false},
		},
		{
			name: "duplicate gate takes the last value, as in Kubernetes",
			spec: "DirectMemoryBackend=false,DirectMemoryBackend=true",
			want: map[Feature]bool{DirectMemoryBackend: true},
		},
		{
			name: "trailing comma tolerated",
			spec: "DirectMemoryBackend=true,",
			want: map[Feature]bool{DirectMemoryBackend: true},
		},
		{
			name: "unknown gate rejected, error lists known gates",
			spec: "NoSuchGate=true",
			wantErr: []string{
				"unknown feature gate",
				string(DirectMemoryBackend),
			},
		},
		{
			name:    "missing = rejected",
			spec:    "DirectMemoryBackend",
			wantErr: []string{"must be Name=bool"},
		},
		{
			name:    "non-bool value rejected",
			spec:    "DirectMemoryBackend=yes",
			wantErr: []string{"must be a boolean"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withGate(t, testGate, false)

			gates, err := Parse(tc.spec)
			if len(tc.wantErr) > 0 {
				if err == nil {
					t.Fatalf("Expected error containing %q, got nil", tc.wantErr)
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Expected error containing %q, got: %v", want, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Expected success, got error: %v", err)
			}
			if len(gates) != len(tc.want) {
				t.Errorf("Parse(%q) = %v, want %v", tc.spec, gates, tc.want)
			}
			for feature, want := range tc.want {
				got, ok := gates[feature]
				if !ok {
					t.Errorf("Parse(%q) dropped gate %s", tc.spec, feature)
					continue
				}
				if got != want {
					t.Errorf("Parse(%q)[%s] = %t, want %t", tc.spec, feature, got, want)
				}
			}
		})
	}
}

func TestGates_Enabled(t *testing.T) {
	withGate(t, testGate, true)

	tests := []struct {
		name    string
		gates   Gates
		feature Feature
		want    bool
	}{
		{
			name:    "nil gates fall back to default off",
			gates:   nil,
			feature: DirectMemoryBackend,
			want:    false,
		},
		{
			name:    "nil gates fall back to default on",
			gates:   nil,
			feature: testGate,
			want:    true,
		},
		{
			name:    "explicit value overrides default",
			gates:   Gates{DirectMemoryBackend: true},
			feature: DirectMemoryBackend,
			want:    true,
		},
		{
			name:    "explicit false overrides default on",
			gates:   Gates{testGate: false},
			feature: testGate,
			want:    false,
		},
		{
			name:    "setting one gate leaves others at their default",
			gates:   Gates{testGate: false},
			feature: DirectMemoryBackend,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.gates.Enabled(tc.feature); got != tc.want {
				t.Errorf("Enabled(%s) = %t, want %t", tc.feature, got, tc.want)
			}
		})
	}
}

func TestGates_String(t *testing.T) {
	// "AAA" sorts before DirectMemoryBackend, so a sorted String() must
	// list it first even though it was registered last.
	withGate(t, "AAATestGate", false)

	got := Gates{DirectMemoryBackend: true}.String()
	want := "AAATestGate=false,DirectMemoryBackend=true"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	if got := Gates(nil).String(); !strings.Contains(got, "DirectMemoryBackend=false") {
		t.Errorf("nil Gates String() should show defaults, got %q", got)
	}
}
