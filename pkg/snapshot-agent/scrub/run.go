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

package scrub

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/metrics"
)

// DefaultMarginMiB is the VRAM left unscrubbed so the allocation succeeds.
const DefaultMarginMiB = 256

// ExecResult is what one scrub did on the GPU.
type ExecResult struct {
	FreeBefore           uint64
	BytesScrubbed        uint64
	ReadbackNonzeroWords int64
	CtxMs                float64
	AllocMs              float64
	MemsetMs             float64
	ReadbackMs           float64
	FreeMs               float64
	DestroyMs            float64
}

// ExecFunc scrubs the GPU with the given UUID, leaving marginBytes unscrubbed.
type ExecFunc func(ctx context.Context, uuid string, marginBytes uint64) (ExecResult, error)

// Options configures one decision and, when the decision is to scrub, the scrub.
type Options struct {
	Policy    Policy
	Mode      Mode
	Boundary  Boundary
	Allowlist Allowlist
	// GPUUUID selects the GPU; "" means the only visible GPU.
	GPUUUID   string
	MarginMiB uint64
	// DryRun decides without touching a GPU.
	DryRun bool
	// GPUName and DriverVersion replace the NVML identity. Dry-run only.
	GPUName       string
	DriverVersion string
	// Identify and Exec default to NVMLIdentify and CUDAExec.
	Identify IdentifyFunc
	Exec     ExecFunc
}

// Result is one boundary's decision and scrub, in the SCRUBJSON shape.
type Result struct {
	Policy               Policy   `json:"policy"`
	Boundary             Boundary `json:"boundary"`
	Qualified            bool     `json:"qualified"`
	Decision             Action   `json:"decision"`
	Reason               string   `json:"reason"`
	GPUName              string   `json:"gpu_name"`
	Driver               string   `json:"driver"`
	GPUUUID              string   `json:"gpu_uuid"`
	FreeBefore           uint64   `json:"free_before"`
	BytesScrubbed        uint64   `json:"bytes_scrubbed"`
	Coverage             float64  `json:"coverage"`
	ReadbackNonzeroWords int64    `json:"readback_nonzero_words"`
	TDecideMs            float64  `json:"t_decide_ms"`
	TCtxMs               float64  `json:"t_ctx_ms"`
	TAllocMs             float64  `json:"t_alloc_ms"`
	TMemsetMs            float64  `json:"t_memset_ms"`
	TReadbackMs          float64  `json:"t_readback_ms"`
	TFreeMs              float64  `json:"t_free_ms"`
	TDestroyMs           float64  `json:"t_destroy_ms"`
	TTotalMs             float64  `json:"t_total_ms"`
	Mode                 Mode     `json:"mode,omitempty"`
	DryRun               bool     `json:"dry_run"`
	Error                string   `json:"error,omitempty"`
}

// Run decides what to do at one boundary and, when the decision is to scrub
// and this is not a dry run, scrubs. This is the entry the Suspend and Kill
// pipelines call. A refusal is a normal result, not an error.
func Run(ctx context.Context, opts *Options) (Result, error) {
	start := time.Now()
	res := Result{Policy: opts.Policy, Boundary: opts.Boundary, DryRun: opts.DryRun, GPUUUID: opts.GPUUUID}
	if opts.Policy == PolicyFlag {
		res.Mode = opts.Mode
	}

	gpu, err := identify(opts)
	if err != nil {
		err = finish(ctx, start, &res, err)
		return res, err
	}
	res.GPUName, res.Driver, res.GPUUUID = gpu.GPUName, gpu.DriverVersion, gpu.UUID
	res.Qualified = opts.Allowlist.Qualified(gpu.GPUName, gpu.DriverVersion)
	decision := Decide(opts.Policy, opts.Mode, opts.Boundary, res.Qualified)
	res.Decision, res.Reason = decision.Action, decision.ReasonName()
	res.TDecideMs = msSince(start)

	if decision.Action != ActionScrub || opts.DryRun {
		err = finish(ctx, start, &res, nil)
		return res, err
	}
	exec := opts.Exec
	if exec == nil {
		exec = CUDAExec
	}
	out, err := exec(ctx, gpu.UUID, opts.MarginMiB<<20)
	res.FreeBefore, res.BytesScrubbed, res.ReadbackNonzeroWords = out.FreeBefore, out.BytesScrubbed, out.ReadbackNonzeroWords
	res.TCtxMs, res.TAllocMs, res.TMemsetMs = out.CtxMs, out.AllocMs, out.MemsetMs
	res.TReadbackMs, res.TFreeMs, res.TDestroyMs = out.ReadbackMs, out.FreeMs, out.DestroyMs
	if out.FreeBefore > 0 {
		res.Coverage = float64(out.BytesScrubbed) / float64(out.FreeBefore)
	}
	if err = finish(ctx, start, &res, err); err == nil {
		metrics.PhaseSeconds.WithLabelValues("scrub").Observe(res.TTotalMs / 1000)
	}
	return res, err
}

func identify(opts *Options) (Identity, error) {
	overridden := opts.GPUName != "" || opts.DriverVersion != ""
	if overridden && !opts.DryRun {
		return Identity{}, errors.New("GPU name and driver version overrides need a dry run")
	}
	if overridden {
		return Identity{GPUName: opts.GPUName, DriverVersion: opts.DriverVersion, UUID: opts.GPUUUID}, nil
	}
	ident := opts.Identify
	if ident == nil {
		ident = NVMLIdentify
	}
	return ident(opts.GPUUUID)
}

func finish(ctx context.Context, start time.Time, res *Result, err error) error {
	res.TTotalMs = msSince(start)
	if err != nil {
		res.Error = err.Error()
		slog.ErrorContext(ctx, "Scrub failed", "policy", res.Policy, "boundary", res.Boundary,
			"decision", res.Decision, "ms", res.TTotalMs, "error", err)
		return err
	}
	slog.InfoContext(ctx, "Scrub done", "decision", res.Decision, "boundary", res.Boundary,
		"bytes", res.BytesScrubbed, "ms", res.TTotalMs, "policy", res.Policy, "qualified", res.Qualified,
		"reason", res.Reason, "dryRun", res.DryRun)
	return nil
}

func msSince(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}
