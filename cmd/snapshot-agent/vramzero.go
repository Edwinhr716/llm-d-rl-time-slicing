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

package main

import (
	"context"
	"log/slog"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/vramzero"
)

// logVRAMZeroing matches the allowlist against NVML at startup and logs the
// result. It never fails startup: the pipelines decide per handoff.
func logVRAMZeroing(ctx context.Context, list vramzero.Allowlist) {
	ids, err := vramzero.NVMLIdentities()
	if err != nil {
		slog.WarnContext(ctx, "VRAM zeroing qualification unknown", "error", err,
			"vramZeroingQualified", list.String())
		return
	}
	slog.InfoContext(ctx, "VRAM zeroing qualification", "qualified", vramzero.AllQualified(list, ids),
		"gpus", ids, "vramZeroingQualified", list.String())
}
