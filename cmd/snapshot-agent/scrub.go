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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

// Exit codes of the scrub subcommand.
const (
	scrubExitOK    = 0
	scrubExitError = 1
)

// scrubIdentify and scrubExec replace NVML and CUDA in tests; nil means the defaults.
var (
	scrubIdentify scrub.IdentifyFunc
	scrubExec     scrub.ExecFunc
)

// runScrubCommand is "snapshot-agent scrub": one scrub at one boundary,
// printed as a single SCRUBJSON line on stdout. Logs go to stderr. It exits 0
// when the scrub (or dry run) succeeded and 1 on error.
func runScrubCommand(args []string, stdout, stderr io.Writer) int {
	slog.SetDefault(slog.New(logging.NewContextHandler(slog.NewJSONHandler(stderr, nil))))

	fs := flag.NewFlagSet("scrub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	boundary := fs.String("boundary", "", "Handoff boundary: suspend or kill")
	gpuUUID := fs.String("gpu-uuid", "", "GPU to scrub (NVML UUID); default: the only visible GPU")
	marginMiB := fs.Uint64("margin-mib", scrub.DefaultMarginMiB, "VRAM left unscrubbed, in MiB")
	dryRun := fs.Bool("dry-run", false, "Identify the GPU without scrubbing it")
	if err := fs.Parse(args); err != nil {
		return scrubExitError
	}

	res := scrub.Result{}
	bnd, err := scrub.ParseBoundary(*boundary)
	if err == nil {
		res, err = scrub.Run(context.Background(), &scrub.Options{
			Boundary: bnd, GPUUUID: *gpuUUID, MarginMiB: *marginMiB, DryRun: *dryRun,
			Identify: scrubIdentify, Exec: scrubExec,
		})
	}
	if err != nil {
		res.Error = err.Error()
	}
	line, jerr := json.Marshal(res)
	if jerr != nil {
		slog.Error("Failed to encode SCRUBJSON", "error", jerr)
		return scrubExitError
	}
	if _, werr := fmt.Fprintf(stdout, "SCRUBJSON %s\n", line); werr != nil {
		return scrubExitError
	}
	if err != nil {
		return scrubExitError
	}
	return scrubExitOK
}
