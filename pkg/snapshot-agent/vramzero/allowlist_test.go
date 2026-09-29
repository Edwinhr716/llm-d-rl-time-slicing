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

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/vramzero"
)

func TestVRAMZero_Allowlist_Default(t *testing.T) {
	list, err := vramzero.ParseAllowlist(vramzero.DefaultQualified)
	if err != nil {
		t.Fatalf("ParseAllowlist(default): %v", err)
	}
	if !list.Qualified("NVIDIA L4", "580.173.02") {
		t.Fatal("L4 on 580.173.02 is not qualified by the default list")
	}
	for _, tc := range []struct{ name, driver string }{
		{"NVIDIA L4", "575.57.08"},
		{"NVIDIA H100 80GB HBM3", "580.173.02"},
		{"NVIDIA L40", "580.173.02"},
		{"", "580.173.02"},
		{"NVIDIA L4", ""},
	} {
		if list.Qualified(tc.name, tc.driver) {
			t.Fatalf("Qualified(%q, %q) = true, want false", tc.name, tc.driver)
		}
	}
	if list.String() != vramzero.DefaultQualified {
		t.Fatalf("String() = %q, want %q", list.String(), vramzero.DefaultQualified)
	}
}

func TestVRAMZero_Allowlist_Parse(t *testing.T) {
	list, err := vramzero.ParseAllowlist(" NVIDIA L4:580 , NVIDIA H100 80GB HBM3:570,")
	if err != nil {
		t.Fatalf("ParseAllowlist: %v", err)
	}
	if len(list) != 2 || !list.Qualified("NVIDIA H100 80GB HBM3", "570.1") {
		t.Fatalf("ParseAllowlist = %+v", list)
	}
	empty, err := vramzero.ParseAllowlist("")
	if err != nil || len(empty) != 0 || empty.Qualified("NVIDIA L4", "580.1") {
		t.Fatalf(`ParseAllowlist("") = %+v, %v; want an empty list that qualifies nothing`, empty, err)
	}
	for _, bad := range []string{"NVIDIA L4", ":580", "NVIDIA L4:", "NVIDIA L4:580.1"} {
		if _, err := vramzero.ParseAllowlist(bad); err == nil {
			t.Fatalf("ParseAllowlist(%q) succeeded, want an error", bad)
		}
	}
}

func TestVRAMZero_Allowlist_DriverBranch(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"580.173.02", "580"}, {"580", "580"}, {"\t575.1\n", "575"}, {"", ""},
	} {
		if got := vramzero.DriverBranch(tc.in); got != tc.want {
			t.Fatalf("DriverBranch(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestVRAMZero_Allowlist_AllQualified(t *testing.T) {
	list, err := vramzero.ParseAllowlist(vramzero.DefaultQualified)
	if err != nil {
		t.Fatal(err)
	}
	l4 := vramzero.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.173.02"}
	h100 := vramzero.Identity{GPUName: "NVIDIA H100", DriverVersion: "580.173.02"}
	if !vramzero.AllQualified(list, []vramzero.Identity{l4, l4}) {
		t.Fatal("two L4s are not qualified")
	}
	if vramzero.AllQualified(list, []vramzero.Identity{l4, h100}) {
		t.Fatal("a node with an H100 is qualified")
	}
	if vramzero.AllQualified(list, nil) {
		t.Fatal("a node with no GPU is qualified")
	}
}
