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

// Package vramzero decides whether a GPU handoff (after a guest's checkpoint
// in Suspend, or after a confirmed Kill) may proceed without the agent
// touching GPU memory. The agent relies on the driver zeroing freed VRAM
// before the next tenant can read it, and trusts that only on the GPU and
// driver combinations listed in --vram-zeroing-qualified. The agent never
// scrubs VRAM itself.
package vramzero

import (
	apiv1alpha1 "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
)

// Boundary is the handoff boundary being decided.
type Boundary string

const (
	// BoundarySuspend is after the guest's checkpoint in Suspend.
	BoundarySuspend Boundary = "suspend"
	// BoundaryKill is after the Kill is confirmed.
	BoundaryKill Boundary = "kill"
)

// Action is the outcome of a handoff decision.
type Action string

const (
	// ActionProceed hands off; the driver zeroes the freed VRAM.
	ActionProceed Action = "proceed"
	// ActionRefuse refuses the operation (Suspend only).
	ActionRefuse Action = "refuse"
)

// Decision is what the agent does at one boundary.
type Decision struct {
	Action Action
	// Reason is set when Action is ActionRefuse.
	Reason apiv1alpha1.ErrorReason
}

// Decide is what the agent does at a boundary, given whether the GPU and
// driver are qualified. Suspend on an unqualified GPU is refused with
// PRECONDITION_NODE, since nothing then guarantees the next tenant sees
// zeroed memory. Kill always proceeds: refusing it would leave the guest
// holding the GPU.
func Decide(boundary Boundary, qualified bool) Decision {
	if boundary == BoundarySuspend && !qualified {
		return Decision{Action: ActionRefuse, Reason: apiv1alpha1.ErrorReason_PRECONDITION_NODE}
	}
	return Decision{Action: ActionProceed}
}
