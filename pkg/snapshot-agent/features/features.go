// Package features implements Kubernetes-style feature gates for the
// snapshot agent: experimental capabilities are explicit per-agent opt-ins
// selected with --feature-gates=Name=bool,... (or the FEATURE_GATES env
// var), with alpha gates off by default.
package features

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Feature is the name of a feature gate.
type Feature string

// DirectMemoryBackend gates the direct_memory backend. It is driven by
// GPU-CR, which is itself experimental and carries deployment requirements
// and operational caveats.
const DirectMemoryBackend Feature = "DirectMemoryBackend"

// MemoryRegionsBackend gates the memory_regions backend. Like
// direct_memory it is driven by GPU-CR and additionally requires the
// target workload to run under the GPU-CR preloader with a shared
// checkpoint directory.
const MemoryRegionsBackend Feature = "MemoryRegionsBackend"

// defaults registers every known gate and its default state. Alpha gates
// default to false; flipping a default here is the promotion path
// (alpha → beta → GA), as in Kubernetes. Adding a gate is one const plus
// one entry here.
var defaults = map[Feature]bool{
	DirectMemoryBackend:  false,
	MemoryRegionsBackend: false,
}

// Gates holds the explicitly configured gate values. The zero value (nil)
// is valid and yields every gate's default — callers that don't care pass
// nil.
type Gates map[Feature]bool

// Parse parses a comma-separated list of Name=bool pairs, tolerating
// whitespace around names, values, and separators. An empty spec yields
// the defaults. Unknown gate names and non-boolean values are errors so
// that a typo never runs silently ignored. A gate repeated within the
// spec takes its last value, matching Kubernetes --feature-gates
// behavior.
func Parse(spec string) (Gates, error) {
	gates := Gates{}
	if strings.TrimSpace(spec) == "" {
		return gates, nil
	}
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		name, value, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("invalid feature gate %q: must be Name=bool", pair)
		}
		feature := Feature(strings.TrimSpace(name))
		if _, known := defaults[feature]; !known {
			return nil, fmt.Errorf("unknown feature gate %q; known gates: %s", strings.TrimSpace(name), knownGates())
		}
		enabled, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid value %q for feature gate %q: must be a boolean", strings.TrimSpace(value), feature)
		}
		gates[feature] = enabled
	}
	return gates, nil
}

// Enabled reports whether the gate is on: the explicitly configured value
// if set, else the registry default.
func (g Gates) Enabled(f Feature) bool {
	if enabled, ok := g[f]; ok {
		return enabled
	}
	return defaults[f]
}

// String returns every known gate with its effective value as a sorted,
// comma-separated Name=bool list, for startup logging.
func (g Gates) String() string {
	pairs := make([]string, 0, len(defaults))
	for feature := range defaults {
		pairs = append(pairs, fmt.Sprintf("%s=%t", feature, g.Enabled(feature)))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func knownGates() string {
	names := make([]string, 0, len(defaults))
	for feature := range defaults {
		names = append(names, string(feature))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
