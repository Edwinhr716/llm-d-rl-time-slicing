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

// Package scrub scrubs GPU memory at every handoff boundary: after a guest's
// checkpoint in Suspend, and after a confirmed Kill, so the next tenant never
// sees the previous tenant's VRAM, whatever the GPU or driver.
package scrub

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/metrics"
)

// DefaultMarginMiB is the VRAM left unscrubbed so the allocation succeeds.
const DefaultMarginMiB = 256

// Boundary is the handoff boundary at which the scrub runs.
type Boundary string

const (
	// BoundarySuspend is after the guest's checkpoint in Suspend.
	BoundarySuspend Boundary = "suspend"
	// BoundaryKill is after the Kill is confirmed.
	BoundaryKill Boundary = "kill"
)

// ParseBoundary parses a --boundary value.
func ParseBoundary(value string) (Boundary, error) {
	switch Boundary(value) {
	case BoundarySuspend, BoundaryKill:
		return Boundary(value), nil
	default:
		return "", fmt.Errorf("invalid boundary %q: want suspend or kill", value)
	}
}

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

// Options configures one scrub.
type Options struct {
	Boundary Boundary
	// GPUUUID selects the GPU; "" means the only visible GPU.
	GPUUUID   string
	MarginMiB uint64
	// DryRun identifies the GPU without scrubbing it.
	DryRun bool
	// Identify and Exec default to NVMLIdentify and CUDAExec.
	Identify IdentifyFunc
	Exec     ExecFunc
}

// Result is one boundary's scrub, in the SCRUBJSON shape.
type Result struct {
	Boundary             Boundary `json:"boundary"`
	GPUName              string   `json:"gpu_name"`
	Driver               string   `json:"driver"`
	GPUUUID              string   `json:"gpu_uuid"`
	FreeBefore           uint64   `json:"free_before"`
	BytesScrubbed        uint64   `json:"bytes_scrubbed"`
	Coverage             float64  `json:"coverage"`
	ReadbackNonzeroWords int64    `json:"readback_nonzero_words"`
	TIdentifyMs          float64  `json:"t_identify_ms"`
	TCtxMs               float64  `json:"t_ctx_ms"`
	TAllocMs             float64  `json:"t_alloc_ms"`
	TMemsetMs            float64  `json:"t_memset_ms"`
	TReadbackMs          float64  `json:"t_readback_ms"`
	TFreeMs              float64  `json:"t_free_ms"`
	TDestroyMs           float64  `json:"t_destroy_ms"`
	TTotalMs             float64  `json:"t_total_ms"`
	DryRun               bool     `json:"dry_run"`
	Error                string   `json:"error,omitempty"`
}

// Run scrubs the GPU at one boundary; a dry run only identifies the GPU. This
// is the entry the Suspend and Kill pipelines call.
func Run(ctx context.Context, opts *Options) (Result, error) {
	start := time.Now()
	res := Result{Boundary: opts.Boundary, DryRun: opts.DryRun, GPUUUID: opts.GPUUUID}

	ident := opts.Identify
	if ident == nil {
		ident = NVMLIdentify
	}
	gpu, err := ident(opts.GPUUUID)
	if err != nil {
		err = finish(ctx, start, &res, err)
		return res, err
	}
	res.GPUName, res.Driver, res.GPUUUID = gpu.GPUName, gpu.DriverVersion, gpu.UUID
	res.TIdentifyMs = msSince(start)

	if opts.DryRun {
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

func finish(ctx context.Context, start time.Time, res *Result, err error) error {
	res.TTotalMs = msSince(start)
	if err != nil {
		res.Error = err.Error()
		slog.ErrorContext(ctx, "Scrub failed", "boundary", res.Boundary, "gpu", res.GPUUUID,
			"ms", res.TTotalMs, "error", err)
		return err
	}
	slog.InfoContext(ctx, "Scrub done", "boundary", res.Boundary, "gpu", res.GPUUUID,
		"bytes", res.BytesScrubbed, "ms", res.TTotalMs, "dryRun", res.DryRun)
	return nil
}

func msSince(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}
