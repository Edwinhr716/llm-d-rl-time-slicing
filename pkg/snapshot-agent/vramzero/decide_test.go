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

package vramzero_test

import (
	"testing"

	apiv1alpha1 "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/vramzero"
)

func TestVRAMZero_DecisionTable(t *testing.T) {
	noReason := apiv1alpha1.ErrorReason_ERROR_REASON_UNSPECIFIED
	for _, tc := range []struct {
		name      string
		boundary  vramzero.Boundary
		qualified bool
		want      vramzero.Action
		reason    apiv1alpha1.ErrorReason
	}{
		{"suspend qualified proceeds", vramzero.BoundarySuspend, true, vramzero.ActionProceed, noReason},
		{
			"suspend unqualified refuses", vramzero.BoundarySuspend, false, vramzero.ActionRefuse,
			apiv1alpha1.ErrorReason_PRECONDITION_NODE,
		},
		{"kill qualified proceeds", vramzero.BoundaryKill, true, vramzero.ActionProceed, noReason},
		{"kill unqualified proceeds", vramzero.BoundaryKill, false, vramzero.ActionProceed, noReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := vramzero.Decide(tc.boundary, tc.qualified)
			if got.Action != tc.want || got.Reason != tc.reason {
				t.Fatalf("Decide(%s, qualified=%v) = %+v, want action %s reason %s",
					tc.boundary, tc.qualified, got, tc.want, tc.reason)
			}
		})
	}
}

func TestVRAMZero_PickIdentity(t *testing.T) {
	l4 := vramzero.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-l4"}
	other := vramzero.Identity{GPUName: "NVIDIA L4", UUID: "GPU-other"}
	if got, err := vramzero.PickIdentity([]vramzero.Identity{l4}, ""); err != nil || got != l4 {
		t.Fatalf("single GPU: %+v, %v", got, err)
	}
	if _, err := vramzero.PickIdentity([]vramzero.Identity{l4, other}, ""); err == nil {
		t.Fatal("two GPUs without a UUID: want an error")
	}
	if got, err := vramzero.PickIdentity([]vramzero.Identity{l4, other}, "GPU-other"); err != nil || got != other {
		t.Fatalf("by UUID: %+v, %v", got, err)
	}
	if _, err := vramzero.PickIdentity(nil, ""); err == nil {
		t.Fatal("no GPU: want an error")
	}
	if _, err := vramzero.PickIdentity([]vramzero.Identity{l4}, "GPU-missing"); err == nil {
		t.Fatal("missing UUID: want an error")
	}
}
