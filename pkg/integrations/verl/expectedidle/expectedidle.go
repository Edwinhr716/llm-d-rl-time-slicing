// Package expectedidle is the Go mirror of the verl hooks' expected_idle
// estimator (pkg/integrations/verl/timeslice_verl/estimator.py). Evaluation
// drivers that stand in for the trainer use it to send the same hint the verl
// hooks would. Both implementations are checked against the same fixture,
// pkg/integrations/verl/tests/testdata/expected_idle_parity.json.
//
// The rule, per yield point: the hint is an exponentially weighted moving
// average (alpha 0.5) of the measured gaps between this point's Yield and the
// trainer's next Acquire. The first observed gap is taken as is. A point with
// no observed gap yet gives no hint.
package expectedidle

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Alpha is the weight of the newest gap.
const Alpha = 0.5

// State holds the estimate per yield point. The zero value is not usable;
// call New.
type State struct {
	estimates map[string]float64
}

// New returns an empty estimator state.
func New() *State {
	return &State{estimates: make(map[string]float64)}
}

// Next returns the hint in seconds for the next Yield at point, and false
// when there is no hint (no gap observed at point yet).
func (s *State) Next(point string) (float64, bool) {
	est, ok := s.estimates[point]
	return est, ok
}

// Observe records a measured gap in seconds between a Yield at point and the
// next Acquire. Negative and NaN gaps are ignored.
func (s *State) Observe(point string, gapSeconds float64) {
	if gapSeconds < 0 || math.IsNaN(gapSeconds) {
		return
	}
	prev, ok := s.estimates[point]
	if !ok {
		s.estimates[point] = gapSeconds
		return
	}
	s.estimates[point] = Alpha*gapSeconds + (1-Alpha)*prev
}

// EnvMode is the environment variable that selects the hint source, read by
// the verl hooks and by drivers that stand in for them.
const EnvMode = "TIMESLICE_EXPECTED_IDLE"

// Mode is a hint source.
type Mode string

// Hint sources (TIMESLICE_EXPECTED_IDLE).
const (
	// ModeOff sends no hint: today's behaviour and the default.
	ModeOff Mode = "off"
	// ModeAuto sends the estimate (no hint until a gap was observed).
	ModeAuto Mode = "auto"
	// ModeFixed sends the same number of seconds on every Yield.
	ModeFixed Mode = "fixed"
)

// ParseMode parses a TIMESLICE_EXPECTED_IDLE value: "off", "auto" or a
// non-negative number of seconds (ModeFixed). Case and surrounding spaces
// are ignored; empty means ModeOff. An invalid value returns ModeOff and an
// error.
func ParseMode(raw string) (Mode, float64, error) {
	val := strings.ToLower(strings.TrimSpace(raw))
	switch val {
	case "", string(ModeOff):
		return ModeOff, 0, nil
	case string(ModeAuto):
		return ModeAuto, 0, nil
	default:
	}
	secs, err := strconv.ParseFloat(val, 64)
	if err != nil || secs < 0 || math.IsNaN(secs) || math.IsInf(secs, 0) {
		return ModeOff, 0, fmt.Errorf("invalid %s %q: want off, auto or a non-negative number of seconds", EnvMode, raw)
	}
	return ModeFixed, secs, nil
}
