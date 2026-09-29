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

var l4 = scrub.Identity{GPUName: "NVIDIA L4", DriverVersion: "580.173.02", UUID: testUUID}

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

func TestScrub_Run_ScrubsTheNamedGPU(t *testing.T) {
	gpu := &fakeGPU{}
	res := runWith(t, gpu, &scrub.Options{Boundary: scrub.BoundaryKill, GPUUUID: testUUID})
	if gpu.calls != 1 || gpu.uuid != testUUID || gpu.margin != scrub.DefaultMarginMiB<<20 {
		t.Fatalf("exec calls=%d uuid=%q margin=%d", gpu.calls, gpu.uuid, gpu.margin)
	}
	if res.BytesScrubbed != 990<<20 || res.FreeBefore != 1000<<20 || res.Boundary != scrub.BoundaryKill {
		t.Fatalf("result = %+v", res)
	}
	if res.Coverage != 0.99 || res.TMemsetMs != 94 || res.GPUName != "NVIDIA L4" || res.Driver != "580.173.02" {
		t.Fatalf("result fields = %+v", res)
	}
}

func TestScrub_Run_ScrubsAtEveryBoundary(t *testing.T) {
	gpu := &fakeGPU{}
	for _, boundary := range []scrub.Boundary{scrub.BoundarySuspend, scrub.BoundaryKill} {
		res := runWith(t, gpu, &scrub.Options{Boundary: boundary})
		if res.BytesScrubbed == 0 || res.GPUUUID != testUUID {
			t.Fatalf("%s: result = %+v, want a scrub of the only GPU", boundary, res)
		}
	}
	if gpu.calls != 2 {
		t.Fatalf("exec calls = %d, want 2", gpu.calls)
	}
}

func TestScrub_Run_DryRunNeverTouchesTheGPU(t *testing.T) {
	gpu := &fakeGPU{}
	opts := scrub.Options{
		Boundary: scrub.BoundarySuspend, DryRun: true, Identify: gpu.identify,
		Exec: func(context.Context, string, uint64) (scrub.ExecResult, error) {
			return scrub.ExecResult{}, errors.New("exec called")
		},
	}
	res, err := scrub.Run(context.Background(), &opts)
	if err != nil || !res.DryRun || res.GPUUUID != testUUID || res.BytesScrubbed != 0 {
		t.Fatalf("dry run = %+v, %v", res, err)
	}
}

func TestScrub_Run_ErrorsAreReturned(t *testing.T) {
	gpu := &fakeGPU{err: errors.New("cuMemAlloc failed")}
	opts := scrub.Options{Boundary: scrub.BoundaryKill, Identify: gpu.identify, Exec: gpu.exec}
	if res, err := scrub.Run(context.Background(), &opts); err == nil || res.Error != "cuMemAlloc failed" {
		t.Fatalf("exec error: %+v, %v", res, err)
	}
	opts.GPUUUID = "GPU-other"
	if res, err := scrub.Run(context.Background(), &opts); err == nil || res.Error == "" || gpu.calls != 1 {
		t.Fatalf("identify error: %+v, %v, exec calls %d", res, err, gpu.calls)
	}
}

func TestScrub_ParseBoundary(t *testing.T) {
	for _, value := range []string{"suspend", "kill"} {
		if got, err := scrub.ParseBoundary(value); err != nil || string(got) != value {
			t.Fatalf("ParseBoundary(%q) = %s, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "resume"} {
		if _, err := scrub.ParseBoundary(value); err == nil {
			t.Fatalf("ParseBoundary(%q) succeeded, want an error", value)
		}
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
