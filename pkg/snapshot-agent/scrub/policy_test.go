// Copyright 2025 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrub_test

import (
	"testing"

	apiv1alpha1 "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

type decideCase struct {
	name      string
	mode      scrub.Mode
	boundary  scrub.Boundary
	qualified bool
	want      scrub.Action
	reason    apiv1alpha1.ErrorReason
}

func runDecideCases(t *testing.T, policy scrub.Policy, cases []decideCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scrub.Decide(policy, tc.mode, tc.boundary, tc.qualified)
			if got.Action != tc.want || got.Reason != tc.reason {
				t.Fatalf("Decide(%s, %q, %s, qualified=%v) = %+v, want action %s reason %s",
					policy, tc.mode, tc.boundary, tc.qualified, got, tc.want, tc.reason)
			}
		})
	}
}

const noReason = apiv1alpha1.ErrorReason_ERROR_REASON_UNSPECIFIED

func TestScrub_Keep_DefaultIsKeep(t *testing.T) {
	if scrub.DefaultPolicy != scrub.PolicyKeep {
		t.Fatalf("DefaultPolicy = %s, want keep", scrub.DefaultPolicy)
	}
	got, err := scrub.ParsePolicy("")
	if err != nil || got != scrub.PolicyKeep {
		t.Fatalf(`ParsePolicy("") = %s, %v; want keep`, got, err)
	}
}

func TestScrub_Keep_DecisionTable(t *testing.T) {
	runDecideCases(t, scrub.PolicyKeep, []decideCase{
		{"suspend qualified skips", "", scrub.BoundarySuspend, true, scrub.ActionSkip, noReason},
		{
			"suspend unqualified refuses", "", scrub.BoundarySuspend, false, scrub.ActionRefuse,
			apiv1alpha1.ErrorReason_PRECONDITION_NODE,
		},
		{"kill qualified skips", "", scrub.BoundaryKill, true, scrub.ActionSkip, noReason},
		{"kill unqualified skips", "", scrub.BoundaryKill, false, scrub.ActionSkip, noReason},
		// keep ignores the flag option's mode.
		{"mode always ignored", scrub.ModeAlways, scrub.BoundaryKill, false, scrub.ActionSkip, noReason},
	})
}

func TestScrub_Keep_UnknownPolicyFallsBackToKeep(t *testing.T) {
	got := scrub.Decide("", "", scrub.BoundarySuspend, false)
	if got.Action != scrub.ActionRefuse || got.ReasonName() != "PRECONDITION_NODE" {
		t.Fatalf("empty policy = %+v, want keep's refusal", got)
	}
}

func TestScrub_NsScrub_DecisionTable(t *testing.T) {
	runDecideCases(t, scrub.PolicyNsScrub, []decideCase{
		{"suspend qualified scrubs", "", scrub.BoundarySuspend, true, scrub.ActionScrub, noReason},
		{"suspend unqualified scrubs", "", scrub.BoundarySuspend, false, scrub.ActionScrub, noReason},
		{"kill qualified scrubs", "", scrub.BoundaryKill, true, scrub.ActionScrub, noReason},
		{"kill unqualified scrubs", "", scrub.BoundaryKill, false, scrub.ActionScrub, noReason},
		{"mode never ignored", scrub.ModeNever, scrub.BoundaryKill, true, scrub.ActionScrub, noReason},
	})
}

func TestScrub_NsScrub_ParsePolicy(t *testing.T) {
	got, err := scrub.ParsePolicy("ns-scrub")
	if err != nil || got != scrub.PolicyNsScrub {
		t.Fatalf(`ParsePolicy("ns-scrub") = %s, %v`, got, err)
	}
}

func TestScrub_Flag_DefaultModeIsUnqualified(t *testing.T) {
	got, err := scrub.ParseMode("")
	if err != nil || got != scrub.ModeUnqualified || scrub.DefaultMode != scrub.ModeUnqualified {
		t.Fatalf(`ParseMode("") = %s, %v; want unqualified`, got, err)
	}
}

func TestScrub_Flag_UnqualifiedScrubsOnlyWhenUnqualified(t *testing.T) {
	for _, mode := range []scrub.Mode{scrub.ModeUnqualified, ""} {
		runDecideCases(t, scrub.PolicyFlag, []decideCase{
			{"suspend qualified skips", mode, scrub.BoundarySuspend, true, scrub.ActionSkip, noReason},
			{"suspend unqualified scrubs", mode, scrub.BoundarySuspend, false, scrub.ActionScrub, noReason},
			{"kill qualified skips", mode, scrub.BoundaryKill, true, scrub.ActionSkip, noReason},
			{"kill unqualified scrubs", mode, scrub.BoundaryKill, false, scrub.ActionScrub, noReason},
		})
	}
}

func TestScrub_Flag_AlwaysScrubs(t *testing.T) {
	runDecideCases(t, scrub.PolicyFlag, []decideCase{
		{"suspend qualified", scrub.ModeAlways, scrub.BoundarySuspend, true, scrub.ActionScrub, noReason},
		{"suspend unqualified", scrub.ModeAlways, scrub.BoundarySuspend, false, scrub.ActionScrub, noReason},
		{"kill qualified", scrub.ModeAlways, scrub.BoundaryKill, true, scrub.ActionScrub, noReason},
		{"kill unqualified", scrub.ModeAlways, scrub.BoundaryKill, false, scrub.ActionScrub, noReason},
	})
}

func TestScrub_Flag_NeverBehavesLikeKeep(t *testing.T) {
	runDecideCases(t, scrub.PolicyFlag, []decideCase{
		{"suspend qualified skips", scrub.ModeNever, scrub.BoundarySuspend, true, scrub.ActionSkip, noReason},
		{
			"suspend unqualified refuses", scrub.ModeNever, scrub.BoundarySuspend, false, scrub.ActionRefuse,
			apiv1alpha1.ErrorReason_PRECONDITION_NODE,
		},
		{"kill qualified skips", scrub.ModeNever, scrub.BoundaryKill, true, scrub.ActionSkip, noReason},
		{"kill unqualified skips", scrub.ModeNever, scrub.BoundaryKill, false, scrub.ActionSkip, noReason},
	})
}

func TestScrub_Flag_ParseMode(t *testing.T) {
	for _, value := range []string{"always", "unqualified", "never"} {
		got, err := scrub.ParseMode(value)
		if err != nil || string(got) != value {
			t.Fatalf("ParseMode(%q) = %s, %v", value, got, err)
		}
	}
	if _, err := scrub.ParseMode("sometimes"); err == nil {
		t.Fatal(`ParseMode("sometimes") succeeded, want an error`)
	}
}

func TestScrub_ParsePolicyAndBoundary(t *testing.T) {
	for _, value := range []string{"keep", "ns-scrub", "flag"} {
		if got, err := scrub.ParsePolicy(value); err != nil || string(got) != value {
			t.Fatalf("ParsePolicy(%q) = %s, %v", value, got, err)
		}
	}
	if _, err := scrub.ParsePolicy("scrub"); err == nil {
		t.Fatal(`ParsePolicy("scrub") succeeded, want an error`)
	}
	for _, value := range []string{"suspend", "kill"} {
		if got, err := scrub.ParseBoundary(value); err != nil || string(got) != value {
			t.Fatalf("ParseBoundary(%q) = %s, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "resume"} {
		if _, err := scrub.ParseBoundary(value); err == nil {
			t.Fatalf("ParseBoundary(%q) succeeded, want an error", value)
		}
	}
}
