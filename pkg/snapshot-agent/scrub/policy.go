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
// The --scrub mode picks the rule:
//   - unqualified (default): scrub only when the GPU and driver are not on the
//     --vram-zeroing-qualified allowlist; a qualified driver zeroes freed VRAM
//     itself.
//   - always: scrub at every boundary, whatever the allowlist says.
//   - never: rely on the driver alone. A Suspend on an unqualified GPU is then
//     refused with PRECONDITION_NODE; Kill is never refused.
package scrub

import (
	"fmt"

	apiv1alpha1 "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Mode is the --scrub value.
type Mode string

const (
	// ModeAlways scrubs at every boundary.
	ModeAlways Mode = "always"
	// ModeUnqualified scrubs only when the GPU is not on the allowlist.
	ModeUnqualified Mode = "unqualified"
	// ModeNever never scrubs. An unqualified Suspend is then refused.
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

// Decide is what the agent does at a boundary under a mode, given whether the
// GPU and driver are qualified.
func Decide(mode Mode, boundary Boundary, qualified bool) Decision {
	switch mode {
	case ModeAlways:
		return Decision{Action: ActionScrub}
	case ModeNever:
		return decideNever(boundary, qualified)
	default:
		if qualified {
			return Decision{Action: ActionSkip}
		}
		return Decision{Action: ActionScrub}
	}
}

// decideNever never scrubs. Suspend on an unqualified GPU is refused, since
// nothing then guarantees the next tenant sees zeroed memory. Kill always
// proceeds: refusing it would leave the guest holding the GPU.
func decideNever(boundary Boundary, qualified bool) Decision {
	if boundary == BoundarySuspend && !qualified {
		return Decision{Action: ActionRefuse, Reason: apiv1alpha1.ErrorReason_PRECONDITION_NODE}
	}
	return Decision{Action: ActionSkip}
}
