// Copyright 2026 The llm-d Authors.
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

package hostcmd_test

import (
	"testing"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/hostcmd"
)

func TestNS4_Hybrid_ClientAddress(t *testing.T) {
	resolve := func(node string) string {
		if node == "node-a" {
			return "10.0.0.7"
		}
		return ""
	}
	client := hostcmd.NewClient(9101, resolve)
	defer func() { _ = client.Close() }()
	if got, want := client.Address("node-a"), "10.0.0.7:9101"; got != want {
		t.Fatalf("Address(node-a) = %q, want %q", got, want)
	}
	if got, want := client.Address("node-b"), "node-b:9101"; got != want {
		t.Fatalf("Address(node-b) = %q, want %q (fallback to the node name)", got, want)
	}
	if got, want := hostcmd.NewClient(9101, nil).Address("node-c"), "node-c:9101"; got != want {
		t.Fatalf("Address with a nil resolver = %q, want %q", got, want)
	}
}

func TestNS4_Hybrid_OutcomeName(t *testing.T) {
	for outcome, want := range map[hcpb.Outcome]string{
		hcpb.Outcome_OUTCOME_VACATED:     "vacated",
		hcpb.Outcome_OUTCOME_RESUMED:     "resumed",
		hcpb.Outcome_OUTCOME_FAILED:      "failed",
		hcpb.Outcome_OUTCOME_STALE_EPOCH: "stale_epoch",
	} {
		if got := hostcmd.OutcomeName(outcome); got != want {
			t.Errorf("OutcomeName(%v) = %q, want %q", outcome, got, want)
		}
	}
}
