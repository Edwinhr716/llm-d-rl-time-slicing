package server

import (
	"fmt"
	"log/slog"
	"os"
	"time"
)

// Lend policies for a foreground Yield (--lend-policy). PENDING LEAD
// DECISION D-NS-17 (and D-ORCH-8): lend only when the Yield's expected_idle
// reaches --min-bubble, or on every Yield.
const (
	// LendPolicyHint lends only when the Yield carries expected_idle >=
	// --min-bubble, and never when --min-bubble is 0. It is the default and
	// today's behaviour.
	LendPolicyHint = "hint"
	// LendPolicyAlways lends on every foreground Yield, with or without
	// expected_idle. --min-bubble is ignored.
	LendPolicyAlways = "always"
)

// EnvLendPolicy overrides the default lend policy of a Server built without
// WithLendPolicy, for embedders that do not wire the flag. The command line
// reads it as the default of --lend-policy.
const EnvLendPolicy = "TIMESLICE_LEND_POLICY"

// ValidateLendPolicy checks a --lend-policy value.
func ValidateLendPolicy(policy string) error {
	switch policy {
	case LendPolicyHint, LendPolicyAlways:
		return nil
	default:
		return fmt.Errorf("unknown lend policy %q: must be %q or %q", policy, LendPolicyHint, LendPolicyAlways)
	}
}

// defaultLendPolicy is the lend policy of a Server built without
// WithLendPolicy: TIMESLICE_LEND_POLICY when it holds a valid value, else
// LendPolicyHint.
func defaultLendPolicy() string {
	raw, ok := os.LookupEnv(EnvLendPolicy)
	if !ok || raw == "" {
		return LendPolicyHint
	}
	if err := ValidateLendPolicy(raw); err != nil {
		slog.Warn("Ignoring "+EnvLendPolicy, "error", err, "policy", LendPolicyHint)
		return LendPolicyHint
	}
	return raw
}

// lendDecision is the whole lend rule: whether a foreground Yield records a
// lend hint. expectedIdle is nil when the Yield carries no expected_idle.
// The reconcile loop still decides whether the node is actually lent.
func lendDecision(policy string, minBubble time.Duration, expectedIdle *time.Duration) bool {
	if policy == LendPolicyAlways {
		return true
	}
	return expectedIdle != nil && minBubble > 0 && *expectedIdle >= minBubble
}

// WithLendPolicy sets the lend policy (LendPolicyHint or LendPolicyAlways).
// An empty value keeps the default. The caller validates the value with
// ValidateLendPolicy; an unknown value behaves as LendPolicyHint.
func WithLendPolicy(policy string) Option {
	return func(s *Server) {
		if policy != "" {
			s.lendPolicy = policy
		}
	}
}
