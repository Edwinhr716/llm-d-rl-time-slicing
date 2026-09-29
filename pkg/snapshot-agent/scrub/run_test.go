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

package scrub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/scrub"
)

const testUUID = "GPU-2f3a1b4c-0000-1111-2222-333344445555"

var (
	l4 = scrub.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.173.02", UUID: testUUID}
	// unqualifiedList leaves out the fake L4, so it is not qualified.
	unqualifiedList = scrub.Allowlist{{GPUName: "NVIDIA H100", DriverBranch: "999"}}
	qualifiedList   = scrub.Allowlist{{GPUName: "NVIDIA L4", DriverBranch: "580"}}
)

// fakeGPU records scrub calls and returns a canned result.
type fakeGPU struct {
	calls  int
	uuid   string
	margin uint64
	err    error
}

func (f *fakeGPU) identify(uuid string) (scrub.Identity, error) {
	if uuid != "" && uuid != testUUID {
		return scrub.Identity{}, errors.New("unknown GPU")
	}
	return l4, nil
}

func (f *fakeGPU) exec(_ context.Context, uuid string, margin uint64) (scrub.ExecResult, error) {
	f.calls++
	f.uuid, f.margin = uuid, margin
	return scrub.ExecResult{FreeBefore: 1000 << 20, BytesScrubbed: 990 << 20, MemsetMs: 94}, f.err
}

func runWith(t *testing.T, gpu *fakeGPU, opts *scrub.Options) scrub.Result {
	t.Helper()
	opts.Identify, opts.Exec = gpu.identify, gpu.exec
	if opts.MarginMiB == 0 {
		opts.MarginMiB = scrub.DefaultMarginMiB
	}
	res, err := scrub.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run(%+v): %v", *opts, err)
	}
	return res
}

func TestScrub_Run_NeverDoesNotScrub(t *testing.T) {
	gpu := &fakeGPU{}
	for _, boundary := range []scrub.Boundary{scrub.BoundarySuspend, scrub.BoundaryKill} {
		res := runWith(t, gpu, &scrub.Options{Mode: scrub.ModeNever, Boundary: boundary, Allowlist: qualifiedList})
		if res.Decision != scrub.ActionSkip || !res.Qualified || res.BytesScrubbed != 0 {
			t.Fatalf("never qualified %s = %+v, want skip", boundary, res)
		}
	}
	res := runWith(t, gpu, &scrub.Options{
		Mode: scrub.ModeNever, Boundary: scrub.BoundarySuspend, Allowlist: unqualifiedList,
	})
	if res.Decision != scrub.ActionRefuse || res.Reason != "PRECONDITION_NODE" || res.Qualified {
		t.Fatalf("never unqualified suspend = %+v, want refuse PRECONDITION_NODE", res)
	}
	res = runWith(t, gpu, &scrub.Options{Mode: scrub.ModeNever, Boundary: scrub.BoundaryKill, Allowlist: unqualifiedList})
	if res.Decision != scrub.ActionSkip {
		t.Fatalf("never unqualified kill = %+v, want skip", res)
	}
	if gpu.calls != 0 {
		t.Fatalf("never scrubbed %d times, want 0", gpu.calls)
	}
}

func TestScrub_Run_AlwaysScrubsTheNamedGPU(t *testing.T) {
	gpu := &fakeGPU{}
	res := runWith(t, gpu, &scrub.Options{
		Mode: scrub.ModeAlways, Boundary: scrub.BoundaryKill, Allowlist: qualifiedList, GPUUUID: testUUID,
	})
	if gpu.calls != 1 || gpu.uuid != testUUID || gpu.margin != scrub.DefaultMarginMiB<<20 {
		t.Fatalf("exec calls=%d uuid=%q margin=%d", gpu.calls, gpu.uuid, gpu.margin)
	}
	if res.Decision != scrub.ActionScrub || res.BytesScrubbed != 990<<20 || res.FreeBefore != 1000<<20 {
		t.Fatalf("always result = %+v", res)
	}
	if res.Coverage != 0.99 || res.TMemsetMs != 94 || res.GPUName != "NVIDIA L4" || res.Driver != "580.173.02" {
		t.Fatalf("always result fields = %+v", res)
	}
	if res.Mode != scrub.ModeAlways {
		t.Fatalf("always reports mode %q", res.Mode)
	}
}

func TestScrub_Run_ScrubsOnlyWhenUnqualified(t *testing.T) {
	gpu := &fakeGPU{}
	base := scrub.Options{Mode: scrub.DefaultMode, Boundary: scrub.BoundarySuspend}

	qualified := base
	qualified.Allowlist = qualifiedList
	if res := runWith(t, gpu, &qualified); res.Decision != scrub.ActionSkip || gpu.calls != 0 {
		t.Fatalf("qualified: calls=%d result=%+v", gpu.calls, res)
	}

	unqualified := base
	unqualified.Allowlist = unqualifiedList
	res := runWith(t, gpu, &unqualified)
	if res.Decision != scrub.ActionScrub || gpu.calls != 1 || res.Mode != scrub.ModeUnqualified {
		t.Fatalf("unqualified: calls=%d result=%+v", gpu.calls, res)
	}
}

func TestScrub_Run_DryRunNeverTouchesTheGPU(t *testing.T) {
	for _, mode := range []scrub.Mode{scrub.ModeNever, scrub.ModeAlways, scrub.ModeUnqualified} {
		opts := scrub.Options{
			Mode: mode, Boundary: scrub.BoundarySuspend, Allowlist: qualifiedList,
			DryRun: true, GPUName: "NVIDIA L4", DriverVersion: "580.173.02", GPUUUID: testUUID,
			Identify: func(string) (scrub.Identity, error) { return scrub.Identity{}, errors.New("identify called") },
			Exec: func(context.Context, string, uint64) (scrub.ExecResult, error) {
				return scrub.ExecResult{}, errors.New("exec called")
			},
		}
		res, err := scrub.Run(context.Background(), &opts)
		if err != nil || !res.DryRun || !res.Qualified || res.GPUUUID != testUUID || res.BytesScrubbed != 0 {
			t.Fatalf("dry run %s = %+v, %v", mode, res, err)
		}
	}
}

func TestScrub_Run_DryRunDecidesEveryMode(t *testing.T) {
	want := map[scrub.Mode]scrub.Action{
		scrub.ModeNever: scrub.ActionRefuse, scrub.ModeAlways: scrub.ActionScrub, scrub.ModeUnqualified: scrub.ActionScrub,
	}
	for mode, action := range want {
		opts := scrub.Options{
			Mode: mode, Boundary: scrub.BoundarySuspend, Allowlist: qualifiedList,
			DryRun: true, GPUName: "NVIDIA H100", DriverVersion: "580.173.02",
		}
		res, err := scrub.Run(context.Background(), &opts)
		if err != nil || res.Decision != action || res.Qualified {
			t.Fatalf("dry run %s on an unqualified GPU = %+v, %v; want %s", mode, res, err, action)
		}
	}
}

func TestScrub_Run_OverridesNeedDryRun(t *testing.T) {
	opts := scrub.Options{Mode: scrub.ModeNever, Boundary: scrub.BoundaryKill, GPUName: "NVIDIA L4"}
	res, err := scrub.Run(context.Background(), &opts)
	if err == nil || res.Error == "" {
		t.Fatalf("overrides without dry run = %+v, %v; want an error", res, err)
	}
}

func TestScrub_Run_ErrorsAreReturned(t *testing.T) {
	gpu := &fakeGPU{err: errors.New("cuMemAlloc failed")}
	opts := scrub.Options{
		Mode: scrub.ModeAlways, Boundary: scrub.BoundaryKill, Identify: gpu.identify, Exec: gpu.exec,
	}
	if res, err := scrub.Run(context.Background(), &opts); err == nil || res.Error != "cuMemAlloc failed" {
		t.Fatalf("exec error: %+v, %v", res, err)
	}
	opts.GPUUUID = "GPU-other"
	if res, err := scrub.Run(context.Background(), &opts); err == nil || res.Error == "" || res.Decision != "" {
		t.Fatalf("identify error: %+v, %v", res, err)
	}
}

func TestScrub_PickIdentity(t *testing.T) {
	other := scrub.Identity{GPUName: "NVIDIA L4", UUID: "GPU-other"}
	if got, err := scrub.PickIdentity([]scrub.Identity{l4}, ""); err != nil || got != l4 {
		t.Fatalf("single GPU: %+v, %v", got, err)
	}
	if _, err := scrub.PickIdentity([]scrub.Identity{l4, other}, ""); err == nil {
		t.Fatal("two GPUs without a UUID: want an error")
	}
	if got, err := scrub.PickIdentity([]scrub.Identity{l4, other}, "GPU-other"); err != nil || got != other {
		t.Fatalf("by UUID: %+v, %v", got, err)
	}
	if _, err := scrub.PickIdentity(nil, ""); err == nil {
		t.Fatal("no GPU: want an error")
	}
	if _, err := scrub.PickIdentity([]scrub.Identity{l4}, "GPU-missing"); err == nil {
		t.Fatal("missing UUID: want an error")
	}
}
