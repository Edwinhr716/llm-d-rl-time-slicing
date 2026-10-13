package expectedidle_test

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/integrations/verl/expectedidle"
)

// parityFixture is tests/testdata/expected_idle_parity.json, shared with the
// Python estimator's tests.
type parityFixture struct {
	Alpha float64 `json:"alpha"`
	Steps []struct {
		Op    string   `json:"op"`
		Point string   `json:"point"`
		Gap   float64  `json:"gap"`
		Want  *float64 `json:"want"`
	} `json:"steps"`
	Modes []struct {
		Raw     string  `json:"raw"`
		Mode    string  `json:"mode"`
		Seconds float64 `json:"seconds"`
	} `json:"modes"`
}

func loadFixture(t *testing.T) parityFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "tests", "testdata", "expected_idle_parity.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fx parityFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(fx.Steps) == 0 || len(fx.Modes) == 0 {
		t.Fatal("fixture has no steps or no modes")
	}
	return fx
}

// TestExpectedIdle_ParityWithPython replays the shared fixture; the Python
// test test_expected_idle_parity_fixture replays the same file.
func TestExpectedIdle_ParityWithPython(t *testing.T) {
	fx := loadFixture(t)
	if fx.Alpha != expectedidle.Alpha {
		t.Fatalf("fixture alpha = %v, Alpha = %v", fx.Alpha, expectedidle.Alpha)
	}
	st := expectedidle.New()
	for i, step := range fx.Steps {
		switch step.Op {
		case "observe":
			st.Observe(step.Point, step.Gap)
		case "next":
			got, ok := st.Next(step.Point)
			switch {
			case step.Want == nil && ok:
				t.Errorf("step %d: Next(%q) = %v, want no hint", i, step.Point, got)
			case step.Want != nil && !ok:
				t.Errorf("step %d: Next(%q) = no hint, want %v", i, step.Point, *step.Want)
			case step.Want != nil && got != *step.Want:
				t.Errorf("step %d: Next(%q) = %v, want %v", i, step.Point, got, *step.Want)
			}
		default:
			t.Fatalf("step %d: unknown op %q", i, step.Op)
		}
	}
}

func TestExpectedIdle_ParseModeParityWithPython(t *testing.T) {
	for _, tc := range loadFixture(t).Modes {
		mode, secs, err := expectedidle.ParseMode(tc.Raw)
		if tc.Mode == "invalid" {
			if err == nil || mode != expectedidle.ModeOff {
				t.Errorf("ParseMode(%q) = %v, %v, %v; want ModeOff and an error", tc.Raw, mode, secs, err)
			}
			continue
		}
		if err != nil || string(mode) != tc.Mode || secs != tc.Seconds {
			t.Errorf("ParseMode(%q) = %v, %v, %v; want %s, %v", tc.Raw, mode, secs, err, tc.Mode, tc.Seconds)
		}
	}
}

func TestExpectedIdle_IgnoresNaN(t *testing.T) {
	st := expectedidle.New()
	st.Observe("p", math.NaN())
	if got, ok := st.Next("p"); ok {
		t.Fatalf("Next after a NaN gap = %v, want no hint", got)
	}
	st.Observe("p", 8)
	st.Observe("p", math.NaN())
	if got, ok := st.Next("p"); !ok || got != 8 {
		t.Fatalf("Next = %v, %v; want 8", got, ok)
	}
}
