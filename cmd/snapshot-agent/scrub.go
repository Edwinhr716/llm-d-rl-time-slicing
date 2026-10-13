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
	scrubExitOK     = 0
	scrubExitError  = 1
	scrubExitRefuse = 3
)

// scrubPolicyFlags registers the D-NS-7 flags shared by the server and the
// scrub subcommand.
type scrubPolicyFlags struct {
	policy    *string
	mode      *string
	qualified *string
}

func addScrubPolicyFlags(fs *flag.FlagSet) scrubPolicyFlags {
	return scrubPolicyFlags{
		// PENDING LEAD DECISION D-NS-7: keep is today's behaviour.
		policy: fs.String("scrub-policy", string(scrub.DefaultPolicy),
			"VRAM handling at handoff (pending decision D-NS-7): keep (driver zeroing plus "+
				"--vram-zeroing-qualified), ns-scrub (scrub at every handoff) or flag (per --scrub)"),
		mode: fs.String("scrub", string(scrub.DefaultMode),
			"With --scrub-policy=flag: always, unqualified (scrub only on a GPU not on "+
				"--vram-zeroing-qualified) or never"),
		qualified: fs.String("vram-zeroing-qualified", scrub.DefaultQualified,
			"Comma-separated <GPU name>:<driver branch> entries whose driver zeroes freed VRAM"),
	}
}

// scrubConfig is the parsed form of scrubPolicyFlags.
type scrubConfig struct {
	policy    scrub.Policy
	mode      scrub.Mode
	allowlist scrub.Allowlist
}

func (f scrubPolicyFlags) parse() (scrubConfig, error) {
	var cfg scrubConfig
	var err error
	if cfg.policy, err = scrub.ParsePolicy(*f.policy); err != nil {
		return cfg, err
	}
	if cfg.mode, err = scrub.ParseMode(*f.mode); err != nil {
		return cfg, err
	}
	if cfg.allowlist, err = scrub.ParseAllowlist(*f.qualified); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// runScrubCommand is "snapshot-agent scrub": one decision (and scrub) at one
// boundary, printed as a single SCRUBJSON line on stdout. Logs go to stderr.
// It exits 0 on scrub or skip, 3 on refuse and 1 on error.
func runScrubCommand(args []string, stdout, stderr io.Writer) int {
	slog.SetDefault(slog.New(logging.NewContextHandler(slog.NewJSONHandler(stderr, nil))))

	fs := flag.NewFlagSet("scrub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	policyFlags := addScrubPolicyFlags(fs)
	boundary := fs.String("boundary", "", "Handoff boundary: suspend or kill")
	gpuUUID := fs.String("gpu-uuid", "", "GPU to scrub (NVML UUID); default: the only visible GPU")
	marginMiB := fs.Uint64("margin-mib", scrub.DefaultMarginMiB, "VRAM left unscrubbed, in MiB")
	dryRun := fs.Bool("dry-run", false, "Decide without touching a GPU")
	gpuName := fs.String("gpu-name", "", "Dry-run only: GPU name instead of NVML's")
	driverVersion := fs.String("driver-version", "", "Dry-run only: driver version instead of NVML's")
	if err := fs.Parse(args); err != nil {
		return scrubExitError
	}

	res := scrub.Result{}
	cfg, err := policyFlags.parse()
	if err == nil {
		res.Policy = cfg.policy
		var bnd scrub.Boundary
		bnd, err = scrub.ParseBoundary(*boundary)
		if err == nil {
			res, err = scrub.Run(context.Background(), &scrub.Options{
				Policy: cfg.policy, Mode: cfg.mode, Boundary: bnd, Allowlist: cfg.allowlist,
				GPUUUID: *gpuUUID, MarginMiB: *marginMiB, DryRun: *dryRun,
				GPUName: *gpuName, DriverVersion: *driverVersion,
			})
		}
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
	switch {
	case err != nil:
		return scrubExitError
	case res.Decision == scrub.ActionRefuse:
		return scrubExitRefuse
	default:
		return scrubExitOK
	}
}

// logScrubPolicy matches the allowlist against NVML at startup and logs the
// result. It never fails startup: the pipelines decide per handoff.
func logScrubPolicy(ctx context.Context, cfg scrubConfig) {
	ids, err := scrub.NVMLIdentities()
	if err != nil {
		slog.WarnContext(ctx, "VRAM zeroing qualification unknown", "error", err,
			"scrubPolicy", cfg.policy, "scrub", cfg.mode, "vramZeroingQualified", cfg.allowlist.String())
		return
	}
	slog.InfoContext(ctx, "VRAM zeroing qualification", "qualified", scrub.AllQualified(cfg.allowlist, ids),
		"gpus", ids, "scrubPolicy", cfg.policy, "scrub", cfg.mode, "vramZeroingQualified", cfg.allowlist.String())
}
