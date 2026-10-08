package server

import (
	"testing"
	"time"
)

func durationPtr(d time.Duration) *time.Duration { return &d }

// lendCase is one row of the lend rule: a Yield with or without expected_idle
// under a --min-bubble.
type lendCase struct {
	name      string
	minBubble time.Duration
	idle      *time.Duration
	want      bool
}

func runLendCases(t *testing.T, policy string, cases []lendCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lendDecision(policy, tc.minBubble, tc.idle); got != tc.want {
				t.Errorf("lendDecision(%q, %v, %v) = %v, want %v", policy, tc.minBubble, tc.idle, got, tc.want)
			}
		})
	}
}

func TestLendPolicy_Hint_Decision(t *testing.T) {
	runLendCases(t, LendPolicyHint, []lendCase{
		{name: "no hint", minBubble: 30 * time.Second},
		{name: "hint 10s below min-bubble", minBubble: 30 * time.Second, idle: durationPtr(10 * time.Second)},
		{name: "hint 30s at min-bubble", minBubble: 30 * time.Second, idle: durationPtr(30 * time.Second), want: true},
		{name: "hint 60s above min-bubble", minBubble: 30 * time.Second, idle: durationPtr(60 * time.Second), want: true},
		{name: "min-bubble 0 never lends", idle: durationPtr(time.Hour)},
		{name: "min-bubble 0 without hint", minBubble: 0},
		{name: "zero hint", minBubble: 30 * time.Second, idle: durationPtr(0)},
	})
}

func TestLendPolicy_Always_Decision(t *testing.T) {
	runLendCases(t, LendPolicyAlways, []lendCase{
		{name: "no hint", minBubble: 30 * time.Second, want: true},
		{name: "no hint and min-bubble 0", want: true},
		{name: "hint 10s below min-bubble", minBubble: 30 * time.Second, idle: durationPtr(10 * time.Second), want: true},
		{name: "hint 60s", minBubble: 30 * time.Second, idle: durationPtr(60 * time.Second), want: true},
		{name: "zero hint", idle: durationPtr(0), want: true},
	})
}

func TestLendPolicy_Hint_UnknownPolicyBehavesAsHint(t *testing.T) {
	runLendCases(t, "bogus", []lendCase{
		{name: "no hint", minBubble: 30 * time.Second},
		{name: "hint 60s", minBubble: 30 * time.Second, idle: durationPtr(60 * time.Second), want: true},
	})
}

func TestLendPolicy_Validate(t *testing.T) {
	for _, ok := range []string{LendPolicyHint, LendPolicyAlways} {
		if err := ValidateLendPolicy(ok); err != nil {
			t.Errorf("ValidateLendPolicy(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "never", "Always", "min-bubble"} {
		if err := ValidateLendPolicy(bad); err == nil {
			t.Errorf("ValidateLendPolicy(%q) = nil, want an error", bad)
		}
	}
}

func TestLendPolicy_Hint_IsTheDefault(t *testing.T) {
	t.Setenv(EnvLendPolicy, "")
	if got := NewServer(nil, nil, nil).lendPolicy; got != LendPolicyHint {
		t.Errorf("default lend policy = %q, want %q", got, LendPolicyHint)
	}
	t.Setenv(EnvLendPolicy, "bogus")
	if got := NewServer(nil, nil, nil).lendPolicy; got != LendPolicyHint {
		t.Errorf("lend policy with an invalid %s = %q, want %q", EnvLendPolicy, got, LendPolicyHint)
	}
}

func TestLendPolicy_Always_FromEnvAndOption(t *testing.T) {
	t.Setenv(EnvLendPolicy, LendPolicyAlways)
	if got := NewServer(nil, nil, nil).lendPolicy; got != LendPolicyAlways {
		t.Errorf("lend policy from %s = %q, want %q", EnvLendPolicy, got, LendPolicyAlways)
	}
	// The option wins over the environment, as the flag does on the command line.
	if got := NewServer(nil, nil, nil, WithLendPolicy(LendPolicyHint)).lendPolicy; got != LendPolicyHint {
		t.Errorf("lend policy with WithLendPolicy(hint) = %q, want %q", got, LendPolicyHint)
	}
	t.Setenv(EnvLendPolicy, "")
	if got := NewServer(nil, nil, nil, WithLendPolicy(LendPolicyAlways)).lendPolicy; got != LendPolicyAlways {
		t.Errorf("lend policy with WithLendPolicy(always) = %q, want %q", got, LendPolicyAlways)
	}
	if got := NewServer(nil, nil, nil, WithLendPolicy("")).lendPolicy; got != LendPolicyHint {
		t.Errorf("lend policy with WithLendPolicy(\"\") = %q, want the default %q", got, LendPolicyHint)
	}
}
