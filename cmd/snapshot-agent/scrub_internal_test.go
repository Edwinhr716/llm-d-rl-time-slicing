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
	"encoding/json"
	"strings"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

// runScrub runs the subcommand and returns its exit code and parsed SCRUBJSON line.
func runScrub(t *testing.T, args ...string) (int, scrub.Result) {
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
	return code, res
}

func dryRunArgs(extra ...string) []string {
	return append([]string{"--dry-run", "--gpu-name=NVIDIA L4", "--driver-version=580.173.02"}, extra...)
}

const unqualifiedFlag = "--vram-zeroing-qualified=NVIDIA H100:999"

func TestScrub_Keep_CommandDefaultIsKeep(t *testing.T) {
	code, res := runScrub(t, dryRunArgs("--boundary=suspend")...)
	if code != scrubExitOK || res.Policy != scrub.PolicyKeep || res.Decision != scrub.ActionSkip || !res.Qualified {
		t.Fatalf("default qualified suspend: exit %d, %+v", code, res)
	}
	code, res = runScrub(t, dryRunArgs("--boundary=suspend", unqualifiedFlag)...)
	if code != scrubExitRefuse || res.Decision != scrub.ActionRefuse || res.Reason != "PRECONDITION_NODE" {
		t.Fatalf("default unqualified suspend: exit %d, %+v", code, res)
	}
	code, res = runScrub(t, dryRunArgs("--boundary=kill", unqualifiedFlag)...)
	if code != scrubExitOK || res.Decision != scrub.ActionSkip {
		t.Fatalf("default unqualified kill: exit %d, %+v", code, res)
	}
}

func TestScrub_NsScrub_CommandDecides(t *testing.T) {
	for _, boundary := range []string{"--boundary=suspend", "--boundary=kill"} {
		for _, list := range []string{"--vram-zeroing-qualified=NVIDIA L4:580", unqualifiedFlag} {
			code, res := runScrub(t, dryRunArgs("--scrub-policy=ns-scrub", boundary, list)...)
			if code != scrubExitOK || res.Decision != scrub.ActionScrub || !res.DryRun || res.BytesScrubbed != 0 {
				t.Fatalf("ns-scrub %s %s: exit %d, %+v", boundary, list, code, res)
			}
		}
	}
}

func TestScrub_Flag_CommandDecides(t *testing.T) {
	code, res := runScrub(t, dryRunArgs("--scrub-policy=flag", "--boundary=kill")...)
	if code != scrubExitOK || res.Decision != scrub.ActionSkip || res.Mode != scrub.ModeUnqualified {
		t.Fatalf("flag qualified: exit %d, %+v", code, res)
	}
	code, res = runScrub(t, dryRunArgs("--scrub-policy=flag", "--boundary=kill", unqualifiedFlag)...)
	if code != scrubExitOK || res.Decision != scrub.ActionScrub {
		t.Fatalf("flag unqualified: exit %d, %+v", code, res)
	}
	code, res = runScrub(t, dryRunArgs("--scrub-policy=flag", "--scrub=always", "--boundary=suspend")...)
	if code != scrubExitOK || res.Decision != scrub.ActionScrub || res.Mode != scrub.ModeAlways {
		t.Fatalf("flag always: exit %d, %+v", code, res)
	}
	code, res = runScrub(t, dryRunArgs("--scrub-policy=flag", "--scrub=never", "--boundary=suspend", unqualifiedFlag)...)
	if code != scrubExitRefuse || res.Decision != scrub.ActionRefuse {
		t.Fatalf("flag never unqualified: exit %d, %+v", code, res)
	}
}

func TestScrub_CommandErrors(t *testing.T) {
	for _, args := range [][]string{
		dryRunArgs(),
		dryRunArgs("--boundary=resume"),
		dryRunArgs("--boundary=kill", "--scrub-policy=x"),
		dryRunArgs("--boundary=kill", "--scrub=sometimes"),
		dryRunArgs("--boundary=kill", "--vram-zeroing-qualified=NVIDIA L4"),
		{"--boundary=kill", "--gpu-name=NVIDIA L4"},
	} {
		code, res := runScrub(t, args...)
		if code != scrubExitError || res.Error == "" {
			t.Fatalf("%v: exit %d, %+v; want exit 1 with an error", args, code, res)
		}
	}
}

func TestScrub_CommandSCRUBJSONFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	runScrubCommand(dryRunArgs("--boundary=kill", "--gpu-uuid=GPU-x"), &stdout, &stderr)
	var fields map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(stdout.String()), "SCRUBJSON ")), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"policy", "boundary", "qualified", "decision", "reason", "gpu_name", "driver", "gpu_uuid",
		"free_before", "bytes_scrubbed", "coverage", "readback_nonzero_words", "t_decide_ms", "t_ctx_ms",
		"t_alloc_ms", "t_memset_ms", "t_free_ms", "t_destroy_ms", "t_total_ms",
	} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("SCRUBJSON has no %q: %s", key, stdout.String())
		}
	}
	if fields["gpu_uuid"] != "GPU-x" {
		t.Fatalf("gpu_uuid = %v, want GPU-x", fields["gpu_uuid"])
	}
}
