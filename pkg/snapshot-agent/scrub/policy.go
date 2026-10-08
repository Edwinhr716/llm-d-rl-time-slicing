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

// Package scrub decides whether GPU memory is scrubbed at a handoff boundary
// (after a guest's checkpoint in Suspend, or after a confirmed Kill) and runs
// the scrub.
//
// PENDING LEAD DECISION D-NS-7. The options sit behind one policy value:
//   - keep (default, today): rely on the driver zeroing freed VRAM, qualified
//     by the GPU/driver allowlist. The agent never scrubs; a Suspend on an
//     unqualified GPU is refused with PRECONDITION_NODE; Kill is unaffected.
//   - ns-scrub: scrub at every handoff boundary, whatever the allowlist says.
//   - flag: scrub according to a mode (always, unqualified, never); the
//     default mode, unqualified, scrubs only when the GPU is not qualified.
package scrub

import (
	"fmt"

	apiv1alpha1 "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Policy selects the D-NS-7 option.
type Policy string

const (
	// PolicyKeep relies on driver zeroing plus the allowlist (today).
	PolicyKeep Policy = "keep"
	// PolicyNsScrub scrubs at every handoff boundary.
	PolicyNsScrub Policy = "ns-scrub"
	// PolicyFlag scrubs according to a Mode.
	PolicyFlag Policy = "flag"

	// DefaultPolicy keeps today's behaviour.
	DefaultPolicy = PolicyKeep
)

// Mode is the --scrub value, used only by PolicyFlag.
type Mode string

const (
	// ModeAlways scrubs at every boundary.
	ModeAlways Mode = "always"
	// ModeUnqualified scrubs only when the GPU is not on the allowlist.
	ModeUnqualified Mode = "unqualified"
	// ModeNever never scrubs. An unqualified Suspend is then refused, as in keep.
	ModeNever Mode = "never"

	// DefaultMode is the --scrub default.
	DefaultMode = ModeUnqualified
)

// Boundary is the handoff boundary at which the scrub may run.
type Boundary string

const (
	// BoundarySuspend is after the guest's checkpoint in Suspend.
	BoundarySuspend Boundary = "suspend"
	// BoundaryKill is after the Kill is confirmed.
	BoundaryKill Boundary = "kill"
)

// Action is the outcome of a scrub decision.
type Action string

const (
	// ActionScrub runs the scrub.
	ActionScrub Action = "scrub"
	// ActionSkip hands off without a scrub.
	ActionSkip Action = "skip"
	// ActionRefuse refuses the operation (Suspend only).
	ActionRefuse Action = "refuse"
)

// Decision is what the agent does at one boundary.
type Decision struct {
	Action Action
	// Reason is set when Action is ActionRefuse.
	Reason apiv1alpha1.ErrorReason
}

// ReasonName is the ErrorReason name, or "" when there is none.
func (d Decision) ReasonName() string {
	if d.Reason == apiv1alpha1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return ""
	}
	return d.Reason.String()
}

// ParsePolicy parses a --scrub-policy value. "" means DefaultPolicy.
func ParsePolicy(value string) (Policy, error) {
	switch Policy(value) {
	case "":
		return DefaultPolicy, nil
	case PolicyKeep, PolicyNsScrub, PolicyFlag:
		return Policy(value), nil
	default:
		return "", fmt.Errorf("invalid scrub policy %q: want keep, ns-scrub or flag", value)
	}
}

// ParseMode parses a --scrub value. "" means DefaultMode.
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case "":
		return DefaultMode, nil
	case ModeAlways, ModeUnqualified, ModeNever:
		return Mode(value), nil
	default:
		return "", fmt.Errorf("invalid scrub mode %q: want always, unqualified or never", value)
	}
}

// ParseBoundary parses a --boundary value.
func ParseBoundary(value string) (Boundary, error) {
	switch Boundary(value) {
	case BoundarySuspend, BoundaryKill:
		return Boundary(value), nil
	default:
		return "", fmt.Errorf("invalid boundary %q: want suspend or kill", value)
	}
}

// Decide is the single decision point for D-NS-7: what the agent does at a
// boundary under a policy, given whether the GPU and driver are qualified.
// mode is used only by PolicyFlag.
func Decide(policy Policy, mode Mode, boundary Boundary, qualified bool) Decision {
	switch policy {
	case PolicyNsScrub:
		return decideNsScrub()
	case PolicyFlag:
		return decideFlag(mode, boundary, qualified)
	default:
		return decideKeep(boundary, qualified)
	}
}

// decideKeep is option keep: never scrub. Suspend on an unqualified GPU is
// refused, since nothing then guarantees the next tenant sees zeroed memory.
// Kill always proceeds: refusing it would leave the guest holding the GPU.
func decideKeep(boundary Boundary, qualified bool) Decision {
	if boundary == BoundarySuspend && !qualified {
		return Decision{Action: ActionRefuse, Reason: apiv1alpha1.ErrorReason_PRECONDITION_NODE}
	}
	return Decision{Action: ActionSkip}
}

// decideNsScrub is option ns-scrub: scrub at every boundary.
func decideNsScrub() Decision {
	return Decision{Action: ActionScrub}
}

// decideFlag is option flag: scrub per mode. ModeNever falls back to keep, so
// an unqualified Suspend is still refused rather than handed off unscrubbed.
func decideFlag(mode Mode, boundary Boundary, qualified bool) Decision {
	switch mode {
	case ModeAlways:
		return Decision{Action: ActionScrub}
	case ModeNever:
		return decideKeep(boundary, qualified)
	default:
		if qualified {
			return Decision{Action: ActionSkip}
		}
		return Decision{Action: ActionScrub}
	}
}
