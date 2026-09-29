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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

// fakeScrubGPU swaps NVML and CUDA for a fake GPU for one test and counts scrubs.
func fakeScrubGPU(t *testing.T, execErr error) *int {
	t.Helper()
	calls := new(int)
	scrubIdentify = func(uuid string) (scrub.Identity, error) {
		if uuid != "" && uuid != "GPU-x" {
			return scrub.Identity{}, errors.New("unknown GPU")
		}
		return scrub.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.173.02", UUID: "GPU-x"}, nil
	}
	scrubExec = func(context.Context, string, uint64) (scrub.ExecResult, error) {
		*calls++
		return scrub.ExecResult{FreeBefore: 1000 << 20, BytesScrubbed: 990 << 20}, execErr
	}
	t.Cleanup(func() { scrubIdentify, scrubExec = nil, nil })
	return calls
}

// scrubRun is one subcommand run: its exit code, stdout and parsed SCRUBJSON line.
type scrubRun struct {
	code int
	out  string
	res  scrub.Result
}

// runScrub runs the subcommand.
func runScrub(t *testing.T, args ...string) scrubRun {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runScrubCommand(args, &stdout, &stderr)
	out := stdout.String()
	if strings.Count(out, "\n") != 1 || !strings.HasPrefix(out, "SCRUBJSON {") {
		t.Fatalf("stdout = %q, want exactly one SCRUBJSON line (stderr %q)", out, stderr.String())
	}
	var res scrub.Result
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(out), "SCRUBJSON ")), &res); err != nil {
		t.Fatalf("SCRUBJSON does not parse: %v", err)
	}
	return scrubRun{code: code, out: out, res: res}
}

func TestScrub_CommandScrubsAtEveryBoundary(t *testing.T) {
	calls := fakeScrubGPU(t, nil)
	for _, boundary := range []string{"suspend", "kill"} {
		run := runScrub(t, "--boundary="+boundary)
		code, res := run.code, run.res
		if code != scrubExitOK || string(res.Boundary) != boundary || res.BytesScrubbed != 990<<20 || res.DryRun {
			t.Fatalf("%s: exit %d, %+v", boundary, code, res)
		}
	}
	if *calls != 2 {
		t.Fatalf("scrubs = %d, want 2", *calls)
	}
}

func TestScrub_CommandDryRunNeverScrubs(t *testing.T) {
	calls := fakeScrubGPU(t, nil)
	run := runScrub(t, "--boundary=kill", "--dry-run")
	code, res := run.code, run.res
	if code != scrubExitOK || !res.DryRun || res.GPUUUID != "GPU-x" || res.BytesScrubbed != 0 || *calls != 0 {
		t.Fatalf("dry run: exit %d, %+v, scrubs %d", code, res, *calls)
	}
}

func TestScrub_CommandErrors(t *testing.T) {
	fakeScrubGPU(t, nil)
	for _, args := range [][]string{
		{},
		{"--boundary=resume"},
		{"--boundary=kill", "--gpu-uuid=GPU-missing"},
	} {
		run := runScrub(t, args...)
		code, res := run.code, run.res
		if code != scrubExitError || res.Error == "" {
			t.Fatalf("%v: exit %d, %+v; want exit 1 with an error", args, code, res)
		}
	}
	fakeScrubGPU(t, errors.New("cuMemAlloc failed"))
	if run := runScrub(t, "--boundary=kill"); run.code != scrubExitError || run.res.Error != "cuMemAlloc failed" {
		t.Fatalf("scrub error: exit %d, %+v", run.code, run.res)
	}
}

func TestScrub_CommandSCRUBJSONFields(t *testing.T) {
	fakeScrubGPU(t, nil)
	out := runScrub(t, "--boundary=kill", "--gpu-uuid=GPU-x").out
	var fields map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(out), "SCRUBJSON ")), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"boundary", "gpu_name", "driver", "gpu_uuid", "free_before", "bytes_scrubbed", "coverage",
		"readback_nonzero_words", "t_identify_ms", "t_ctx_ms", "t_alloc_ms", "t_memset_ms", "t_readback_ms",
		"t_free_ms", "t_destroy_ms", "t_total_ms", "dry_run",
	} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("SCRUBJSON has no %q: %s", key, out)
		}
	}
	if fields["gpu_uuid"] != "GPU-x" {
		t.Fatalf("gpu_uuid = %v, want GPU-x", fields["gpu_uuid"])
	}
}
