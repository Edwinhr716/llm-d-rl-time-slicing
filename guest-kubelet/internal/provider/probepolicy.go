package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// ProbePolicy is which probes a guest may carry: lead decision D-VK-5, one value per option,
// selected with --guest-probe-policy. The mirror never carries probes under any policy.
type ProbePolicy string

// The D-VK-5 options.
const (
	// ProbePolicyA (default): reject liveness and startup probes, exec and grpc readiness
	// probes and readinessGates; allow an httpGet or tcpSocket readinessProbe.
	ProbePolicyA ProbePolicy = "a"
	// ProbePolicyB: reject every probe (and readinessGates).
	ProbePolicyB ProbePolicy = "b"
	// ProbePolicyC: accept every probe. Liveness and startup probes are dropped from the mirror;
	// the guest kubelet runs the readiness probe (any handler), gates readiness on the startup
	// probe and honours readinessGates.
	ProbePolicyC ProbePolicy = "c"
)

// ParseProbePolicy checks a --guest-probe-policy value. Empty is ProbePolicyA.
func ParseProbePolicy(s string) (ProbePolicy, error) {
	switch p := ProbePolicy(s); p {
	case "":
		return ProbePolicyA, nil
	case ProbePolicyA, ProbePolicyB, ProbePolicyC:
		return p, nil
	}
	return "", fmt.Errorf("unknown guest probe policy %q (want a, b or c)", s)
}

// checkProbes applies the probe rule of the selected option.
//
// Option c refuses nothing, so a stock chart's pod runs unchanged. What makes that safe is
// elsewhere: the mirror builder strips every probe and readinessGates from the mirror (all
// options), so the real kubelet never restarts or un-readies a frozen guest; the livenessProbe
// is dropped (a hung, not frozen, engine is not restarted); and the guest kubelet's prober runs
// the readinessProbe with any handler (exec through pods/exec on the mirror, grpc health),
// holds Ready false until the startupProbe has passed, and keeps Ready false until every
// readinessGate is true (internal/probe, internal/backend/mirror).
func checkProbes(pod *corev1.Pod, pol ProbePolicy) *Rejection {
	switch pol {
	case ProbePolicyB:
		return checkProbesOptionB(pod)
	case ProbePolicyC:
		return nil
	default:
		return checkProbesOptionA(pod)
	}
}
