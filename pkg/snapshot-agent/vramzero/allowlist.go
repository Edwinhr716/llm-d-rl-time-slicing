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

package vramzero

import (
	"fmt"
	"strings"
)

// DefaultQualified is the --vram-zeroing-qualified default.
const DefaultQualified = "NVIDIA L4:580"

// QualifiedEntry is one GPU name and driver branch on which the driver is
// known to zero freed VRAM before the next tenant can read it.
type QualifiedEntry struct {
	GPUName      string
	DriverBranch string
}

// Allowlist is the parsed --vram-zeroing-qualified value.
type Allowlist []QualifiedEntry

// ParseAllowlist parses a comma-separated list of "<GPU name>:<driver branch>"
// entries, for example "NVIDIA L4:580,NVIDIA H100 80GB HBM3:580". An empty
// value is an empty list: no GPU is qualified.
func ParseAllowlist(value string) (Allowlist, error) {
	list := Allowlist{}
	for raw := range strings.SplitSeq(value, ",") {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		idx := strings.LastIndex(item, ":")
		if idx <= 0 || idx == len(item)-1 {
			return nil, fmt.Errorf("invalid qualified entry %q: want <GPU name>:<driver branch>", item)
		}
		name := strings.TrimSpace(item[:idx])
		branch := strings.TrimSpace(item[idx+1:])
		if name == "" || branch == "" || strings.Contains(branch, ".") {
			return nil, fmt.Errorf("invalid qualified entry %q: want <GPU name>:<driver branch>", item)
		}
		list = append(list, QualifiedEntry{GPUName: name, DriverBranch: branch})
	}
	return list, nil
}

// DriverBranch is the major part of an NVML driver version ("580.173.02" -> "580").
func DriverBranch(driverVersion string) string {
	branch, _, _ := strings.Cut(strings.TrimSpace(driverVersion), ".")
	return branch
}

// Qualified reports whether the GPU name and driver version match an entry.
// The GPU name must match exactly; the driver matches on its branch.
func (a Allowlist) Qualified(gpuName, driverVersion string) bool {
	name := strings.TrimSpace(gpuName)
	branch := DriverBranch(driverVersion)
	if name == "" || branch == "" {
		return false
	}
	for _, entry := range a {
		if entry.GPUName == name && entry.DriverBranch == branch {
			return true
		}
	}
	return false
}

// String renders the list in the flag's format.
func (a Allowlist) String() string {
	parts := make([]string, 0, len(a))
	for _, entry := range a {
		parts = append(parts, entry.GPUName+":"+entry.DriverBranch)
	}
	return strings.Join(parts, ",")
}
