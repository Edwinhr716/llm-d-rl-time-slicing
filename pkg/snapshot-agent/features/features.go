// Package features implements Kubernetes-style feature gates for the
// snapshot agent: a comma-separated list of Name=bool pairs
// (--feature-gates=MemoryRegionsBackend=true,DirectMemoryBackend=true).
// Experimental (alpha) features default to off and must be explicitly
// enabled per agent, mirroring
// https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/.
package features

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Feature names a gated capability of the agent.
type Feature string

const (
	// MemoryRegionsBackend gates the memory-regions backend, which is
	// driven by the experimental GPU-CR cr_client. Alpha, off by default.
	MemoryRegionsBackend Feature = "MemoryRegionsBackend"
	// DirectMemoryBackend gates the direct-memory backend, which is driven
	// by experimental GPU-CR tooling. Alpha, off by default.
	DirectMemoryBackend Feature = "DirectMemoryBackend"
)

// defaults registers the known gates and their default state. Alpha
// features are off by default; flipping a default here is the promotion
// path (alpha -> beta -> GA), as in Kubernetes.
var defaults = map[Feature]bool{
	MemoryRegionsBackend: false,
	DirectMemoryBackend:  false,
}

// Gates holds resolved feature-gate values. The zero value (nil) is valid
// and yields every gate's default.
type Gates map[Feature]bool

// Parse builds Gates from a Kubernetes-style spec: comma-separated
// Name=bool pairs, e.g. "MemoryRegionsBackend=true,DirectMemoryBackend=false".
// Unknown gate names and non-boolean values are errors; an empty spec
// yields the defaults.
func Parse(spec string) (Gates, error) {
	gates := Gates{}
	if strings.TrimSpace(spec) == "" {
		return gates, nil
	}
	for _, pair := range strings.Split(spec, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return nil, fmt.Errorf("invalid feature gate %q: expected Name=bool", pair)
		}
		feature := Feature(strings.TrimSpace(name))
		if _, known := defaults[feature]; !known {
			return nil, fmt.Errorf("unknown feature gate %q (known gates: %s)", name, knownGates())
		}
		enabled, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid value %q for feature gate %q: expected bool", value, name)
		}
		gates[feature] = enabled
	}
	return gates, nil
}

// Enabled reports whether the feature is enabled, falling back to its
// registered default when the gate was not set explicitly.
func (g Gates) Enabled(feature Feature) bool {
	if enabled, ok := g[feature]; ok {
		return enabled
	}
	return defaults[feature]
}

// String renders the explicitly-set gates as a stable Name=bool list, for
// startup logging.
func (g Gates) String() string {
	pairs := make([]string, 0, len(g))
	for feature, enabled := range g {
		pairs = append(pairs, fmt.Sprintf("%s=%t", feature, enabled))
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
