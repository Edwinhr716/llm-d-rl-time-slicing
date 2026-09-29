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
	boundary  scrub.Boundary
	qualified bool
	want      scrub.Action
	reason    apiv1alpha1.ErrorReason
}

func runDecideCases(t *testing.T, mode scrub.Mode, cases []decideCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scrub.Decide(mode, tc.boundary, tc.qualified)
			if got.Action != tc.want || got.Reason != tc.reason {
				t.Fatalf("Decide(%q, %s, qualified=%v) = %+v, want action %s reason %s",
					mode, tc.boundary, tc.qualified, got, tc.want, tc.reason)
			}
		})
	}
}

const noReason = apiv1alpha1.ErrorReason_ERROR_REASON_UNSPECIFIED

func TestScrub_DefaultModeIsUnqualified(t *testing.T) {
	got, err := scrub.ParseMode("")
	if err != nil || got != scrub.ModeUnqualified || scrub.DefaultMode != scrub.ModeUnqualified {
		t.Fatalf(`ParseMode("") = %s, %v; want unqualified`, got, err)
	}
}

func TestScrub_UnqualifiedScrubsOnlyWhenUnqualified(t *testing.T) {
	// "" is what an unset mode decides: the default.
	for _, mode := range []scrub.Mode{scrub.ModeUnqualified, ""} {
		runDecideCases(t, mode, []decideCase{
			{"suspend qualified skips", scrub.BoundarySuspend, true, scrub.ActionSkip, noReason},
			{"suspend unqualified scrubs", scrub.BoundarySuspend, false, scrub.ActionScrub, noReason},
			{"kill qualified skips", scrub.BoundaryKill, true, scrub.ActionSkip, noReason},
			{"kill unqualified scrubs", scrub.BoundaryKill, false, scrub.ActionScrub, noReason},
		})
	}
}

func TestScrub_AlwaysScrubs(t *testing.T) {
	runDecideCases(t, scrub.ModeAlways, []decideCase{
		{"suspend qualified", scrub.BoundarySuspend, true, scrub.ActionScrub, noReason},
		{"suspend unqualified", scrub.BoundarySuspend, false, scrub.ActionScrub, noReason},
		{"kill qualified", scrub.BoundaryKill, true, scrub.ActionScrub, noReason},
		{"kill unqualified", scrub.BoundaryKill, false, scrub.ActionScrub, noReason},
	})
}

func TestScrub_NeverRefusesUnqualifiedSuspend(t *testing.T) {
	runDecideCases(t, scrub.ModeNever, []decideCase{
		{"suspend qualified skips", scrub.BoundarySuspend, true, scrub.ActionSkip, noReason},
		{
			"suspend unqualified refuses", scrub.BoundarySuspend, false, scrub.ActionRefuse,
			apiv1alpha1.ErrorReason_PRECONDITION_NODE,
		},
		{"kill qualified skips", scrub.BoundaryKill, true, scrub.ActionSkip, noReason},
		{"kill unqualified skips", scrub.BoundaryKill, false, scrub.ActionSkip, noReason},
	})
}

func TestScrub_ParseMode(t *testing.T) {
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

func TestScrub_ParseBoundary(t *testing.T) {
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
