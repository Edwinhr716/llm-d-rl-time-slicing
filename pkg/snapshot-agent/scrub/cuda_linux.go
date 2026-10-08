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

//go:build linux && cgo

package scrub

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stddef.h>
#include <stdint.h>

// The CUDA driver API is loaded with dlopen, so the agent binary keeps
// starting on nodes without libcuda (the same approach go-nvml uses for NVML).
typedef int scrub_res;
typedef int scrub_dev;
typedef void *scrub_ctx;
typedef unsigned long long scrub_ptr;

static void *scrub_lib;
static const char *scrub_missing;
static scrub_res (*p_init)(unsigned int);
static scrub_res (*p_dev_count)(int *);
static scrub_res (*p_dev_get)(scrub_dev *, int);
static scrub_res (*p_dev_uuid)(unsigned char *, scrub_dev);
static scrub_res (*p_ctx_create)(scrub_ctx *, unsigned int, scrub_dev);
static scrub_res (*p_ctx_destroy)(scrub_ctx);
static scrub_res (*p_ctx_sync)(void);
static scrub_res (*p_mem_info)(size_t *, size_t *);
static scrub_res (*p_mem_alloc)(scrub_ptr *, size_t);
static scrub_res (*p_mem_free)(scrub_ptr);
static scrub_res (*p_memset_d32)(scrub_ptr, unsigned int, size_t);
static scrub_res (*p_dtoh)(void *, scrub_ptr, size_t);

static void *scrub_sym(const char *name) {
	void *sym = dlsym(scrub_lib, name);
	if (sym == NULL && scrub_missing == NULL) {
		scrub_missing = name;
	}
	return sym;
}

// scrub_load returns 0 when libcuda and every symbol resolved.
static int scrub_load(void) {
	if (scrub_lib != NULL) {
		return scrub_missing == NULL ? 0 : -2;
	}
	scrub_lib = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
	if (scrub_lib == NULL) {
		return -1;
	}
	p_init = (scrub_res (*)(unsigned int))scrub_sym("cuInit");
	p_dev_count = (scrub_res (*)(int *))scrub_sym("cuDeviceGetCount");
	p_dev_get = (scrub_res (*)(scrub_dev *, int))scrub_sym("cuDeviceGet");
	p_dev_uuid = (scrub_res (*)(unsigned char *, scrub_dev))scrub_sym("cuDeviceGetUuid");
	p_ctx_create = (scrub_res (*)(scrub_ctx *, unsigned int, scrub_dev))scrub_sym("cuCtxCreate_v2");
	p_ctx_destroy = (scrub_res (*)(scrub_ctx))scrub_sym("cuCtxDestroy_v2");
	p_ctx_sync = (scrub_res (*)(void))scrub_sym("cuCtxSynchronize");
	p_mem_info = (scrub_res (*)(size_t *, size_t *))scrub_sym("cuMemGetInfo_v2");
	p_mem_alloc = (scrub_res (*)(scrub_ptr *, size_t))scrub_sym("cuMemAlloc_v2");
	p_mem_free = (scrub_res (*)(scrub_ptr))scrub_sym("cuMemFree_v2");
	p_memset_d32 = (scrub_res (*)(scrub_ptr, unsigned int, size_t))scrub_sym("cuMemsetD32_v2");
	p_dtoh = (scrub_res (*)(void *, scrub_ptr, size_t))scrub_sym("cuMemcpyDtoH_v2");
	return scrub_missing == NULL ? 0 : -2;
}

static const char *scrub_missing_sym(void) { return scrub_missing == NULL ? "" : scrub_missing; }
static scrub_res scrub_init(void) { return p_init(0); }
static scrub_res scrub_dev_count(int *n) { return p_dev_count(n); }
static scrub_res scrub_dev_get(scrub_dev *d, int i) { return p_dev_get(d, i); }
static scrub_res scrub_dev_uuid(unsigned char *u, scrub_dev d) { return p_dev_uuid(u, d); }
// The context lives on the C side, so no Go pointer to a C pointer crosses cgo.
// One scrub runs per process (the scrub subcommand exits after it).
static scrub_ctx scrub_cur;
static scrub_res scrub_ctx_create(scrub_dev d) { return p_ctx_create(&scrub_cur, 0, d); }
static scrub_res scrub_ctx_destroy(void) {
	scrub_res rc = p_ctx_destroy(scrub_cur);
	scrub_cur = NULL;
	return rc;
}
static scrub_res scrub_ctx_sync(void) { return p_ctx_sync(); }
static scrub_res scrub_mem_info(size_t *f, size_t *t) { return p_mem_info(f, t); }
static scrub_res scrub_mem_alloc(scrub_ptr *p, size_t n) { return p_mem_alloc(p, n); }
static scrub_res scrub_mem_free(scrub_ptr p) { return p_mem_free(p); }
static scrub_res scrub_memset0(scrub_ptr p, size_t words) { return p_memset_d32(p, 0, words); }

// scrub_nonzero copies up to 4 KiB at p back to the host and counts the
// 32-bit words that are not zero. It returns -1 and sets *res on error.
static long long scrub_nonzero(scrub_ptr p, size_t n, scrub_res *res) {
	uint32_t buf[1024];
	long long nz = 0;
	size_t i;
	if (n > sizeof(buf)) {
		n = sizeof(buf);
	}
	*res = p_dtoh(buf, p, n);
	if (*res != 0) {
		return -1;
	}
	for (i = 0; i < n / sizeof(uint32_t); i++) {
		if (buf[i] != 0) {
			nz++;
		}
	}
	return nz;
}
*/
import "C"

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"strings"
	"time"
)

const (
	cudaErrorOutOfMemory = 2
	// allocGranularity keeps every chunk a whole number of 32-bit words and
	// on the allocator's 2 MiB page size.
	allocGranularity = 2 << 20
	readbackBytes    = 4096
)

// CUDAExec is the default ExecFunc. It creates its own context on the GPU
// with the given UUID, allocates free minus margin, memsets it to zero,
// synchronizes, reads back the first and last 4 KiB of every allocation,
// then frees everything and destroys the context. It keeps nothing: no VRAM
// and no context outlive the call. It only ever touches memory it allocated.
func CUDAExec(ctx context.Context, uuid string, marginBytes uint64) (ExecResult, error) {
	// A CUDA context is current on one OS thread; keep every call on it.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var res ExecResult
	start := time.Now()
	if rc := C.scrub_load(); rc != 0 {
		if rc == -1 {
			return res, fmt.Errorf("dlopen libcuda.so.1: %s", C.GoString(C.dlerror()))
		}
		return res, fmt.Errorf("libcuda.so.1 has no %s", C.GoString(C.scrub_missing_sym()))
	}
	if rc := C.scrub_init(); rc != 0 {
		return res, cudaErr("cuInit", rc)
	}
	dev, err := cudaDeviceByUUID(uuid)
	if err != nil {
		return res, err
	}
	if rc := C.scrub_ctx_create(dev); rc != 0 {
		return res, cudaErr("cuCtxCreate", rc)
	}
	destroyed := false
	defer func() {
		if !destroyed {
			C.scrub_ctx_destroy()
		}
	}()
	res.CtxMs = msSince(start)

	var free, total C.size_t
	if rc := C.scrub_mem_info(&free, &total); rc != 0 {
		return res, cudaErr("cuMemGetInfo", rc)
	}
	res.FreeBefore = uint64(free)

	phase := time.Now()
	alloc, err := allocAll(ctx, res.FreeBefore, marginBytes)
	res.AllocMs = msSince(phase)
	defer func() {
		for _, ptr := range alloc.ptrs {
			C.scrub_mem_free(ptr)
		}
	}()
	if err != nil {
		return res, err
	}
	for _, size := range alloc.sizes {
		res.BytesScrubbed += size
	}

	phase = time.Now()
	for i, ptr := range alloc.ptrs {
		if rc := C.scrub_memset0(ptr, C.size_t(alloc.sizes[i]/4)); rc != 0 {
			return res, cudaErr("cuMemsetD32", rc)
		}
	}
	if rc := C.scrub_ctx_sync(); rc != 0 {
		return res, cudaErr("cuCtxSynchronize", rc)
	}
	res.MemsetMs = msSince(phase)

	phase = time.Now()
	for i, ptr := range alloc.ptrs {
		nonzero, err := readbackEnds(ptr, alloc.sizes[i])
		if err != nil {
			return res, err
		}
		res.ReadbackNonzeroWords += nonzero
	}
	res.ReadbackMs = msSince(phase)

	phase = time.Now()
	for _, ptr := range alloc.ptrs {
		if rc := C.scrub_mem_free(ptr); rc != 0 {
			alloc.ptrs = nil
			return res, cudaErr("cuMemFree", rc)
		}
	}
	alloc.ptrs = nil
	res.FreeMs = msSince(phase)

	phase = time.Now()
	destroyed = true
	if rc := C.scrub_ctx_destroy(); rc != 0 {
		return res, cudaErr("cuCtxDestroy", rc)
	}
	res.DestroyMs = msSince(phase)
	return res, nil
}

// allocation is the device memory one scrub holds: ptrs[i] has sizes[i] bytes.
type allocation struct {
	ptrs  []C.scrub_ptr
	sizes []uint64
}

// allocAll allocates free - margin bytes, in as few chunks as the allocator
// allows: it starts with one chunk and halves the chunk size on out-of-memory.
func allocAll(ctx context.Context, free, marginBytes uint64) (allocation, error) {
	var out allocation
	if free <= marginBytes {
		return out, nil
	}
	remaining := (free - marginBytes) / allocGranularity * allocGranularity
	chunk := remaining
	for remaining > 0 && chunk >= allocGranularity {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		chunk = min(chunk, remaining)
		var ptr C.scrub_ptr
		rc := C.scrub_mem_alloc(&ptr, C.size_t(chunk))
		switch rc {
		case 0:
			out.ptrs = append(out.ptrs, ptr)
			out.sizes = append(out.sizes, chunk)
			remaining -= chunk
		case cudaErrorOutOfMemory:
			chunk = chunk / 2 / allocGranularity * allocGranularity
		default:
			return out, cudaErr("cuMemAlloc", rc)
		}
	}
	return out, nil
}

// readbackEnds counts non-zero words in the first and last 4 KiB of one allocation.
func readbackEnds(ptr C.scrub_ptr, size uint64) (int64, error) {
	var total int64
	offsets := []uint64{0}
	if size > readbackBytes {
		offsets = append(offsets, size-readbackBytes)
	}
	for _, off := range offsets {
		var rc C.scrub_res
		nonzero := C.scrub_nonzero(ptr+C.scrub_ptr(off), C.size_t(min(size, readbackBytes)), &rc)
		if rc != 0 {
			return total, cudaErr("cuMemcpyDtoH", rc)
		}
		total += int64(nonzero)
	}
	return total, nil
}

// cudaDeviceByUUID finds the CUDA device whose UUID matches an NVML UUID
// ("GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx").
func cudaDeviceByUUID(uuid string) (C.scrub_dev, error) {
	want, err := parseUUID(uuid)
	if err != nil {
		return 0, err
	}
	var count C.int
	if rc := C.scrub_dev_count(&count); rc != 0 {
		return 0, cudaErr("cuDeviceGetCount", rc)
	}
	for idx := C.int(0); idx < count; idx++ {
		var dev C.scrub_dev
		if rc := C.scrub_dev_get(&dev, idx); rc != 0 {
			return 0, cudaErr("cuDeviceGet", rc)
		}
		var raw [16]C.uchar
		if rc := C.scrub_dev_uuid(&raw[0], dev); rc != 0 {
			return 0, cudaErr("cuDeviceGetUuid", rc)
		}
		var got [16]byte
		for i := range raw {
			got[i] = byte(raw[i])
		}
		if got == want {
			return dev, nil
		}
	}
	return 0, fmt.Errorf("no CUDA device with UUID %s", uuid)
}

func parseUUID(uuid string) ([16]byte, error) {
	var out [16]byte
	hexPart := strings.ReplaceAll(strings.TrimPrefix(uuid, "GPU-"), "-", "")
	raw, err := hex.DecodeString(hexPart)
	if err != nil || len(raw) != len(out) {
		return out, fmt.Errorf("invalid GPU UUID %q", uuid)
	}
	copy(out[:], raw)
	return out, nil
}

func cudaErr(call string, rc C.scrub_res) error {
	return fmt.Errorf("%s failed: CUDA error %d", call, int(rc))
}
