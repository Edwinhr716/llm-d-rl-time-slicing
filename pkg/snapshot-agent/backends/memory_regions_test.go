package backends

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"gotest.tools/v3/assert"
)

// execCall records one fake cr_client invocation.
type execCall struct {
	name string
	args []string
}

// fakeExec captures cr_client invocations and optionally observes the
// filesystem at call time (for ordering assertions).
type fakeExec struct {
	mu      sync.Mutex
	calls   []execCall
	err     error
	observe func(name string, args ...string)
}

func (f *fakeExec) fn(ctx context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, execCall{name: name, args: args})
	if f.observe != nil {
		f.observe(name, args...)
	}
	return nil, f.err
}

func (f *fakeExec) callArgs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]string
	for _, c := range f.calls {
		out = append(out, c.args)
	}
	return out
}

// newTestBackend returns a MemoryRegions backend wired to tempdirs via env.
func newTestBackend(t *testing.T, fake *fakeExec) (g *MemoryRegions, ctlDir, snapDir string) {
	t.Helper()
	ctlDir = t.TempDir()
	snapDir = t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)
	t.Setenv("SNAPSHOT_DIR", snapDir)
	g = NewMemoryRegions()
	g.execCommand = fake.fn
	g.lookPath = func(string) (string, error) { return "", fmt.Errorf("not found") }
	return g, ctlDir, snapDir
}

// writePidMap maps pid -> id in the ctl dir.
func writePidMap(t *testing.T, ctlDir string, pid int, id string) {
	t.Helper()
	assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, fmt.Sprintf("pid_map_%d", pid)), []byte(id+"\n"), 0o644))
}

func regionsConfig(name string, regions ...*pb.MemoryRegion) *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions:      regions,
				SnapshotName: name,
			},
		},
	}
}

func region(pid int32, addr, size uint64) *pb.MemoryRegion {
	return &pb.MemoryRegion{Pid: pid, Address: addr, SizeBytes: size}
}

func TestRegionSpecs(t *testing.T) {
	type testCase struct {
		name    string
		regions []*pb.MemoryRegion
		want    map[int32][]string
		wantErr string
	}

	run := func(t *testing.T, tc testCase) {
		got, err := regionSpecs(&pb.MemoryRegionsBackendConfig{Regions: tc.regions})
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
			return
		}
		assert.NilError(t, err)
		assert.DeepEqual(t, got, tc.want)
	}

	testCases := []testCase{
		{
			name:    "single region",
			regions: []*pb.MemoryRegion{region(123, 0x7f00, 1024)},
			want:    map[int32][]string{123: {"0x7f00:1024"}},
		},
		{
			name: "two regions same pid grouped in order",
			regions: []*pb.MemoryRegion{
				region(123, 0x7f00, 1024),
				region(123, 0x8f00, 2048),
			},
			want: map[int32][]string{123: {"0x7f00:1024", "0x8f00:2048"}},
		},
		{
			name: "regions across pids",
			regions: []*pb.MemoryRegion{
				region(123, 0x7f00, 1024),
				region(456, 0x9f00, 4096),
				region(123, 0x8f00, 2048),
			},
			want: map[int32][]string{
				123: {"0x7f00:1024", "0x8f00:2048"},
				456: {"0x9f00:4096"},
			},
		},
		{
			name:    "empty regions rejected",
			regions: nil,
			wantErr: "at least one memory region is required",
		},
		{
			name:    "zero pid rejected",
			regions: []*pb.MemoryRegion{region(0, 0x7f00, 1024)},
			wantErr: "pid must be positive",
		},
		{
			name:    "negative pid rejected",
			regions: []*pb.MemoryRegion{region(-5, 0x7f00, 1024)},
			wantErr: "pid must be positive",
		},
		{
			name:    "zero size rejected",
			regions: []*pb.MemoryRegion{region(123, 0x7f00, 0)},
			wantErr: "size_bytes must be positive",
		},
		{
			name:    "address formatted as hex",
			regions: []*pb.MemoryRegion{region(1, 139637976727552, 1073741824)},
			want:    map[int32][]string{1: {"0x7f0000000000:1073741824"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestMemoryRegionsSnapshot(t *testing.T) {
	dumpContent := []byte("dump content for id 42 - definitely not a valid header")

	type testCase struct {
		name         string
		config       *pb.BackendConfig
		jobID        string
		execErr      error
		wantErr      string
		wantArgs     [][]string
		wantNoExec   bool
		wantSlotFile string // relative to SNAPSHOT_DIR; verified to equal dumpContent
	}

	run := func(t *testing.T, tc testCase) {
		fake := &fakeExec{err: tc.execErr}
		g, ctlDir, snapDir := newTestBackend(t, fake)
		writePidMap(t, ctlDir, 123, "42")
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "42"), dumpContent, 0o644))

		err := g.Snapshot(context.Background(), Request{JobID: tc.jobID, Config: tc.config})
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
		} else {
			assert.NilError(t, err)
		}
		if tc.wantNoExec {
			assert.Assert(t, len(fake.callArgs()) == 0, "cr_client must not be invoked")
		} else if tc.wantArgs != nil {
			assert.DeepEqual(t, fake.callArgs(), tc.wantArgs)
		}
		if tc.wantSlotFile != "" {
			got, rerr := os.ReadFile(filepath.Join(snapDir, tc.wantSlotFile))
			assert.NilError(t, rerr)
			assert.Assert(t, bytes.Equal(got, dumpContent), "snapshot copy differs from dump")
		}
	}

	testCases := []testCase{
		{
			name:         "single region invokes cr_client and copies dump",
			config:       regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:        "job-1",
			wantArgs:     [][]string{{"-c", "-p", "123", "-s", "0x7f00:1024"}},
			wantSlotFile: "slot-a/42",
		},
		{
			name:         "regions of one pid joined into one spec",
			config:       regionsConfig("slot-a", region(123, 0x7f00, 1024), region(123, 0x8f00, 2048)),
			jobID:        "job-1",
			wantArgs:     [][]string{{"-c", "-p", "123", "-s", "0x7f00:1024,0x8f00:2048"}},
			wantSlotFile: "slot-a/42",
		},
		{
			name:         "empty snapshot_name falls back to job id",
			config:       regionsConfig("", region(123, 0x7f00, 1024)),
			jobID:        "job-1",
			wantArgs:     [][]string{{"-c", "-p", "123", "-s", "0x7f00:1024"}},
			wantSlotFile: "job-1/42",
		},
		{
			name:    "nil config rejected",
			config:  &pb.BackendConfig{},
			jobID:   "job-1",
			wantErr: "requires BackendConfig.memory_regions",
		},
		{
			name:    "empty regions rejected",
			config:  regionsConfig("slot-a"),
			jobID:   "job-1",
			wantErr: "at least one memory region",
		},
		{
			name:       "path traversal snapshot_name rejected before exec",
			config:     regionsConfig("../../etc", region(123, 0x7f00, 1024)),
			jobID:      "job-1",
			wantErr:    "path traversal",
			wantNoExec: true,
		},
		{
			name:    "exec failure surfaces",
			config:  regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:   "job-1",
			execErr: fmt.Errorf("exec error"),
			wantErr: "cr_client checkpoint failed for PID 123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestMemoryRegionsSnapshotTimeout(t *testing.T) {
	fake := &fakeExec{}
	g, ctlDir, _ := newTestBackend(t, fake)
	t.Setenv("GPU_CR_OP_TIMEOUT_SEC", "1")
	writePidMap(t, ctlDir, 123, "42")

	// Blocking fake: returns only when the per-op context expires.
	g.execCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	err := g.Snapshot(context.Background(), Request{
		JobID:  "job-1",
		Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
	})
	assert.ErrorContains(t, err, "context deadline exceeded")
}

func TestMemoryRegionsSnapshotHostFileGating(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("copyHostFile=%v", enabled), func(t *testing.T) {
			fake := &fakeExec{}
			g, ctlDir, snapDir := newTestBackend(t, fake)
			if enabled {
				t.Setenv("GPU_CR_COPY_HOST_FILE", "1")
			}
			writePidMap(t, ctlDir, 123, "42")
			assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "42"), []byte("data"), 0o644))
			assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "42-host"), []byte("host staging"), 0o644))

			err := g.Snapshot(context.Background(), Request{
				JobID:  "job-1",
				Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			})
			assert.NilError(t, err)

			_, statErr := os.Stat(filepath.Join(snapDir, "slot-a", "42-host"))
			if enabled {
				assert.NilError(t, statErr, "-host file should be copied when GPU_CR_COPY_HOST_FILE=1")
			} else {
				assert.Assert(t, os.IsNotExist(statErr), "-host file should be skipped by default")
			}
		})
	}
}

func TestMemoryRegionsRestore(t *testing.T) {
	snapContent := []byte("saved slot-a bytes: not zeros, not a header........")

	t.Run("copy-back happens before cr_client -r", func(t *testing.T) {
		fake := &fakeExec{}
		g, ctlDir, snapDir := newTestBackend(t, fake)
		writePidMap(t, ctlDir, 123, "42")

		// Live dump buffer holds different (dirty) bytes.
		dirty := bytes.Repeat([]byte{0xAA}, len(snapContent))
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "42"), dirty, 0o644))
		// Saved snapshot slot.
		assert.NilError(t, os.MkdirAll(filepath.Join(snapDir, "slot-a"), 0o755))
		assert.NilError(t, os.WriteFile(filepath.Join(snapDir, "slot-a", "42"), snapContent, 0o644))

		// At cr_client time, the live buffer must already hold the snapshot.
		var contentAtExec []byte
		fake.observe = func(name string, args ...string) {
			data, err := os.ReadFile(filepath.Join(ctlDir, "42"))
			assert.NilError(t, err)
			contentAtExec = data
		}

		err := g.Restore(context.Background(), Request{
			JobID:  "job-1",
			Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
		})
		assert.NilError(t, err)
		assert.DeepEqual(t, fake.callArgs(), [][]string{{"-r", "-p", "123", "-s", "0x7f00:1024"}})
		assert.Assert(t, bytes.Equal(contentAtExec, snapContent),
			"live dump buffer was not restored before cr_client -r ran")
	})

	t.Run("missing snapshot slot is a clear error", func(t *testing.T) {
		fake := &fakeExec{}
		g, ctlDir, _ := newTestBackend(t, fake)
		writePidMap(t, ctlDir, 123, "42")

		err := g.Restore(context.Background(), Request{
			JobID:  "job-1",
			Config: regionsConfig("no-such-slot", region(123, 0x7f00, 1024)),
		})
		assert.ErrorContains(t, err, `snapshot slot "no-such-slot" not found`)
		assert.Assert(t, len(fake.callArgs()) == 0, "cr_client must not run without a snapshot")
	})

	t.Run("host file restored only when enabled", func(t *testing.T) {
		for _, enabled := range []bool{false, true} {
			fake := &fakeExec{}
			g, ctlDir, snapDir := newTestBackend(t, fake)
			if enabled {
				t.Setenv("GPU_CR_COPY_HOST_FILE", "1")
			} else {
				t.Setenv("GPU_CR_COPY_HOST_FILE", "")
			}
			writePidMap(t, ctlDir, 123, "42")
			assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "42"), []byte("dirty"), 0o644))
			assert.NilError(t, os.MkdirAll(filepath.Join(snapDir, "slot-a"), 0o755))
			assert.NilError(t, os.WriteFile(filepath.Join(snapDir, "slot-a", "42"), []byte("datax"), 0o644))
			assert.NilError(t, os.WriteFile(filepath.Join(snapDir, "slot-a", "42-host"), []byte("host bytes"), 0o644))

			err := g.Restore(context.Background(), Request{
				JobID:  "job-1",
				Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			})
			assert.NilError(t, err)

			_, statErr := os.Stat(filepath.Join(ctlDir, "42-host"))
			if enabled {
				assert.NilError(t, statErr)
			} else {
				assert.Assert(t, os.IsNotExist(statErr))
			}
		}
	})
}

func TestDumpDataLimit(t *testing.T) {
	type testCase struct {
		name  string
		setup func(t *testing.T) string // returns path
		want  int64
	}

	makeDump := func(t *testing.T, size int, offset uint64) string {
		t.Helper()
		buf := make([]byte, size)
		binary.LittleEndian.PutUint64(buf[8:16], offset)
		path := filepath.Join(t.TempDir(), "dump")
		assert.NilError(t, os.WriteFile(path, buf, 0o644))
		return path
	}

	run := func(t *testing.T, tc testCase) {
		assert.Equal(t, dumpDataLimit(tc.setup(t)), tc.want)
	}

	testCases := []testCase{
		{
			name:  "valid header returns current_offset",
			setup: func(t *testing.T) string { return makeDump(t, 64, 48) },
			want:  48,
		},
		{
			name:  "offset beyond file size ignored",
			setup: func(t *testing.T) string { return makeDump(t, 64, 4096) },
			want:  0,
		},
		{
			name:  "offset inside header ignored",
			setup: func(t *testing.T) string { return makeDump(t, 64, 8) },
			want:  0,
		},
		{
			name:  "unreadable file returns zero",
			setup: func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing") },
			want:  0,
		},
		{
			name: "file shorter than header returns zero",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "short")
				assert.NilError(t, os.WriteFile(path, []byte{1, 2, 3}, 0o644))
				return path
			},
			want: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestCopyFile(t *testing.T) {
	t.Run("zero blocks skipped but size preserved", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		dst := filepath.Join(dir, "dst")
		assert.NilError(t, os.WriteFile(src, make([]byte, 1024), 0o644))

		assert.NilError(t, copyFile(src, dst, 0))

		got, err := os.ReadFile(dst)
		assert.NilError(t, err)
		assert.Equal(t, len(got), 1024)
		assert.Assert(t, isAllZeros(got))
	})

	t.Run("non-zero content copied", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		dst := filepath.Join(dir, "dst")
		content := []byte(strings.Repeat("payload!", 128))
		assert.NilError(t, os.WriteFile(src, content, 0o644))

		assert.NilError(t, copyFile(src, dst, 0))

		got, err := os.ReadFile(dst)
		assert.NilError(t, err)
		assert.Assert(t, bytes.Equal(got, content))
	})

	t.Run("limit honored and dst sized to src", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		dst := filepath.Join(dir, "dst")
		content := bytes.Repeat([]byte{0xFF}, 100)
		assert.NilError(t, os.WriteFile(src, content, 0o644))

		assert.NilError(t, copyFile(src, dst, 10))

		got, err := os.ReadFile(dst)
		assert.NilError(t, err)
		assert.Equal(t, len(got), 100)
		assert.Assert(t, bytes.Equal(got[:10], content[:10]))
		assert.Assert(t, isAllZeros(got[10:]), "bytes past limit must not be copied")
	})

	t.Run("dst truncated to src size", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		dst := filepath.Join(dir, "dst")
		assert.NilError(t, os.WriteFile(src, []byte("small"), 0o644))
		assert.NilError(t, os.WriteFile(dst, bytes.Repeat([]byte{0xBB}, 200), 0o644))

		assert.NilError(t, copyFile(src, dst, 0))

		st, err := os.Stat(dst)
		assert.NilError(t, err)
		assert.Equal(t, st.Size(), int64(5))
	})

	t.Run("dst directory created", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		dst := filepath.Join(dir, "nested", "deeper", "dst")
		assert.NilError(t, os.WriteFile(src, []byte("x"), 0o644))
		assert.NilError(t, copyFile(src, dst, 0))
		_, err := os.Stat(dst)
		assert.NilError(t, err)
	})
}

// TestCopyBackHoleFaithful is the regression test for the 2026-08-04
// co-resident-checkpoint corruption: zeros inside the dump extent are
// authoritative payload and MUST overwrite whatever is in the destination
// buffer (no sparse skipping on restore).
func TestCopyBackHoleFaithful(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	const size = 64
	const extent = 48

	// Source: valid header with current_offset=extent; data at [16,32),
	// authoritative ZEROS at [32,48), and trailing garbage at [48,64) that
	// lies beyond the extent and must NOT be copied.
	srcBuf := make([]byte, size)
	binary.LittleEndian.PutUint64(srcBuf[8:16], extent)
	copy(srcBuf[16:32], bytes.Repeat([]byte{0x11}, 16))
	// srcBuf[32:48] stays zero.
	copy(srcBuf[48:64], bytes.Repeat([]byte{0x22}, 16))
	assert.NilError(t, os.WriteFile(src, srcBuf, 0o644))

	// Destination buffer pre-dirtied with a co-resident checkpoint's bytes.
	assert.NilError(t, os.WriteFile(dst, bytes.Repeat([]byte{0xAA}, size), 0o644))

	assert.NilError(t, copyIntoHugetlbfs(src, dst))

	got, err := os.ReadFile(dst)
	assert.NilError(t, err)
	assert.Assert(t, bytes.Equal(got[:extent], srcBuf[:extent]),
		"full extent must be copied byte-for-byte, including zeros")
	assert.Assert(t, isAllZeros(got[32:48]),
		"authoritative zeros must overwrite pre-existing dirt")
	assert.Assert(t, bytes.Equal(got[48:], bytes.Repeat([]byte{0xAA}, size-extent)),
		"bytes beyond the dump extent must be left untouched")
}

func TestResolvePidToId(t *testing.T) {
	t.Run("NUL-padded pid_map parses", func(t *testing.T) {
		g, ctlDir, _ := newTestBackend(t, &fakeExec{})
		content := append([]byte("77"), make([]byte, 30)...) // zero-padded tail
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_555"), content, 0o644))

		id, err := g.resolvePidToId("555")
		assert.NilError(t, err)
		assert.Equal(t, id, "77")
	})

	t.Run("empty pid_map falls back to proc maps", func(t *testing.T) {
		g, ctlDir, _ := newTestBackend(t, &fakeExec{})
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_556"), nil, 0o644))

		procRoot := t.TempDir()
		g.procRoot = procRoot
		assert.NilError(t, os.MkdirAll(filepath.Join(procRoot, "556"), 0o755))
		maps := "7f0000000000-7f0040000000 rw-s 00000000 00:0f 12345 /var/tmp/huge-ckpt/88\n"
		assert.NilError(t, os.WriteFile(filepath.Join(procRoot, "556", "maps"), []byte(maps), 0o644))

		id, err := g.resolvePidToId("556")
		assert.NilError(t, err)
		assert.Equal(t, id, "88")
	})

	t.Run("neither source yields id names both causes", func(t *testing.T) {
		g, _, _ := newTestBackend(t, &fakeExec{})
		g.procRoot = t.TempDir()

		_, err := g.resolvePidToId("557")
		assert.ErrorContains(t, err, "unusable")
		assert.ErrorContains(t, err, "fallback failed")
	})
}

func TestMemoryRegionsHealthCheck(t *testing.T) {
	t.Run("cr_client found", func(t *testing.T) {
		g := NewMemoryRegions()
		g.lookPath = func(name string) (string, error) { return "/usr/local/bin/cr_client", nil }
		assert.NilError(t, g.HealthCheck(context.Background()))
	})

	t.Run("cr_client missing", func(t *testing.T) {
		g := NewMemoryRegions()
		g.lookPath = func(name string) (string, error) { return "", fmt.Errorf("not in PATH") }
		err := g.HealthCheck(context.Background())
		assert.ErrorContains(t, err, "cr_client executable not found")
	})
}

func TestSlotDir(t *testing.T) {
	type testCase struct {
		name    string
		slot    string
		wantErr bool
	}

	store := "/snapshots"
	run := func(t *testing.T, tc testCase) {
		_, err := slotDir(store, tc.slot)
		if tc.wantErr {
			assert.ErrorContains(t, err, "path traversal")
		} else {
			assert.NilError(t, err)
		}
	}

	testCases := []testCase{
		{name: "plain slot", slot: "slot-a"},
		{name: "nested slot", slot: "job/slot-a"},
		{name: "dot dot escape", slot: "../../etc", wantErr: true},
		{name: "dot dot exact", slot: "..", wantErr: true},
		{name: "dot only", slot: ".", wantErr: true},
		{name: "sneaky traversal", slot: "a/../../etc", wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}
