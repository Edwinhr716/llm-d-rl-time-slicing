package backends

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	out := make([][]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.args)
	}
	return out
}

// newTestBackend returns a MemoryRegions backend wired to tempdirs via env.
// The returned store dir is where destination-slot dumps land (-o paths).
//
//nolint:gocritic // The project configuration bans named returns, conflicting with unnamedResult
func newTestBackend(t *testing.T, fake *fakeExec) (*MemoryRegions, string, string) {
	t.Helper()
	ctlDir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)
	t.Setenv("GPU_CR_CTL_PATH", "")
	t.Setenv("GPU_CR_GROUP_STORE", "")
	g := NewMemoryRegions()
	g.execCommand = fake.fn
	g.lookPath = func(string) (string, error) { return "", fmt.Errorf("not found") }
	return g, ctlDir, filepath.Join(ctlDir, "groups")
}

// writePidMap maps the test PID 123 to dump-buffer id 42 in the ctl dir.
func writePidMap(t *testing.T, ctlDir string) {
	t.Helper()
	assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_123"), []byte("42\n"), 0o600))
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
		t.Helper()
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
	type testCase struct {
		name       string
		config     *pb.BackendConfig
		jobID      string
		execErr    error
		wantErr    string
		wantSpec   string // -s argument
		wantSlot   string // -o = <store>/<wantSlot>/42; empty = no args check
		wantNoExec bool
	}

	run := func(t *testing.T, tc testCase) {
		t.Helper()
		fake := &fakeExec{err: tc.execErr}
		g, ctlDir, storeDir := newTestBackend(t, fake)
		writePidMap(t, ctlDir)

		err := g.Snapshot(context.Background(), Request{JobID: tc.jobID, Config: tc.config})
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr)
		} else {
			assert.NilError(t, err)
		}
		if tc.wantNoExec {
			assert.Assert(t, len(fake.callArgs()) == 0, "cr_client must not be invoked")
		} else if tc.wantSlot != "" {
			dest := filepath.Join(storeDir, tc.wantSlot, "42")
			assert.DeepEqual(t, fake.callArgs(), [][]string{{"-c", "-p", "123", "-s", tc.wantSpec, "-o", dest}})
			if tc.wantErr == "" {
				// The slot dir must exist for the preloader to dump into.
				_, statErr := os.Stat(filepath.Join(storeDir, tc.wantSlot))
				assert.NilError(t, statErr, "slot dir not created")
			}
		}
	}

	testCases := []testCase{
		{
			name:     "single region invokes cr_client with destination path",
			config:   regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:    "job-1",
			wantSpec: "0x7f00:1024",
			wantSlot: "slot-a",
		},
		{
			name:     "regions of one pid joined into one spec",
			config:   regionsConfig("slot-a", region(123, 0x7f00, 1024), region(123, 0x8f00, 2048)),
			jobID:    "job-1",
			wantSpec: "0x7f00:1024,0x8f00:2048",
			wantSlot: "slot-a",
		},
		{
			name:     "empty snapshot_name falls back to job id",
			config:   regionsConfig("", region(123, 0x7f00, 1024)),
			jobID:    "job-1",
			wantSpec: "0x7f00:1024",
			wantSlot: "job-1",
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
			name:       "nested snapshot_name rejected before exec",
			config:     regionsConfig("job/slot-a", region(123, 0x7f00, 1024)),
			jobID:      "job-1",
			wantErr:    "path traversal or nested",
			wantNoExec: true,
		},
		{
			name:     "exec failure surfaces",
			config:   regionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:    "job-1",
			execErr:  fmt.Errorf("exec error"),
			wantErr:  "cr_client checkpoint failed for PID 123",
			wantSpec: "0x7f00:1024",
			wantSlot: "slot-a",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

// TestMemoryRegionsSnapshotOwnersMeta verifies the .owners liveness metadata
// (pid + starttime) that GC's owner-liveness sweep relies on. Needs a real
// procfs, so it uses the test process itself as the owner.
func TestMemoryRegionsSnapshotOwnersMeta(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("no procfs on this host")
	}
	fake := &fakeExec{}
	g, ctlDir, storeDir := newTestBackend(t, fake)
	pid := strconv.Itoa(os.Getpid())
	assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_"+pid), []byte("42\n"), 0o600))

	pidNum, err := strconv.ParseInt(pid, 10, 32)
	assert.NilError(t, err)
	err = g.Snapshot(context.Background(), Request{
		JobID:  "job-1",
		Config: regionsConfig("slot-a", region(int32(pidNum), 0x7f00, 1024)),
	})
	assert.NilError(t, err)

	owners, err := readGroupMeta(filepath.Join(storeDir, "slot-a"))
	assert.NilError(t, err)
	st, ok := owners[pid]
	assert.Assert(t, ok, "own pid missing from .owners: %v", owners)
	want, err := procStarttime(pid)
	assert.NilError(t, err)
	assert.Equal(t, st, want)
}

func TestMemoryRegionsSnapshotTimeout(t *testing.T) {
	fake := &fakeExec{}
	g, ctlDir, _ := newTestBackend(t, fake)
	t.Setenv("GPU_CR_OP_TIMEOUT_SEC", "1")
	writePidMap(t, ctlDir)

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

func TestMemoryRegionsRestore(t *testing.T) {
	t.Run("restore reads straight from the destination path", func(t *testing.T) {
		fake := &fakeExec{}
		g, ctlDir, storeDir := newTestBackend(t, fake)
		writePidMap(t, ctlDir)
		assert.NilError(t, os.MkdirAll(filepath.Join(storeDir, "slot-a"), 0o755))

		err := g.Restore(context.Background(), Request{
			JobID:  "job-1",
			Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
		})
		assert.NilError(t, err)
		dest := filepath.Join(storeDir, "slot-a", "42")
		assert.DeepEqual(t, fake.callArgs(), [][]string{{"-r", "-p", "123", "-s", "0x7f00:1024", "-o", dest}})
	})

	t.Run("missing snapshot slot is a clear error", func(t *testing.T) {
		fake := &fakeExec{}
		g, ctlDir, _ := newTestBackend(t, fake)
		writePidMap(t, ctlDir)

		err := g.Restore(context.Background(), Request{
			JobID:  "job-1",
			Config: regionsConfig("no-such-slot", region(123, 0x7f00, 1024)),
		})
		assert.ErrorContains(t, err, `snapshot slot "no-such-slot" not found`)
		assert.Assert(t, len(fake.callArgs()) == 0, "cr_client must not run without a snapshot")
	})

	t.Run("exec failure surfaces", func(t *testing.T) {
		fake := &fakeExec{err: fmt.Errorf("exec error")}
		g, ctlDir, storeDir := newTestBackend(t, fake)
		writePidMap(t, ctlDir)
		assert.NilError(t, os.MkdirAll(filepath.Join(storeDir, "slot-a"), 0o755))

		err := g.Restore(context.Background(), Request{
			JobID:  "job-1",
			Config: regionsConfig("slot-a", region(123, 0x7f00, 1024)),
		})
		assert.ErrorContains(t, err, "cr_client restore failed for PID 123")
	})
}

func TestResolvePidToId(t *testing.T) {
	t.Run("NUL-padded pid_map parses", func(t *testing.T) {
		g, ctlDir, _ := newTestBackend(t, &fakeExec{})
		content := append([]byte("77"), make([]byte, 30)...) // zero-padded tail
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_555"), content, 0o600))

		id, err := g.resolvePidToID("555")
		assert.NilError(t, err)
		assert.Equal(t, id, "77")
	})

	t.Run("ctl dir consulted before data dir", func(t *testing.T) {
		g, ctlDir, _ := newTestBackend(t, &fakeExec{})
		tmpfsDir := t.TempDir()
		t.Setenv("GPU_CR_CTL_PATH", tmpfsDir)
		// Stale/empty map in the data dir, good map on the ctl tmpfs.
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_555"), nil, 0o600))
		assert.NilError(t, os.WriteFile(filepath.Join(tmpfsDir, "pid_map_555"), []byte("91\n"), 0o600))

		id, err := g.resolvePidToID("555")
		assert.NilError(t, err)
		assert.Equal(t, id, "91")
	})

	t.Run("empty pid_map falls back to proc maps", func(t *testing.T) {
		g, ctlDir, _ := newTestBackend(t, &fakeExec{})
		assert.NilError(t, os.WriteFile(filepath.Join(ctlDir, "pid_map_556"), nil, 0o600))

		procRoot := t.TempDir()
		g.procRoot = procRoot
		assert.NilError(t, os.MkdirAll(filepath.Join(procRoot, "556"), 0o755))
		maps := "7f0000000000-7f0040000000 rw-s 00000000 00:0f 12345 /var/tmp/huge-ckpt/88\n"
		assert.NilError(t, os.WriteFile(filepath.Join(procRoot, "556", "maps"), []byte(maps), 0o600))

		id, err := g.resolvePidToID("556")
		assert.NilError(t, err)
		assert.Equal(t, id, "88")
	})

	t.Run("neither source yields id names both causes", func(t *testing.T) {
		g, _, _ := newTestBackend(t, &fakeExec{})
		g.procRoot = t.TempDir()

		_, err := g.resolvePidToID("557")
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

func TestGroupDir(t *testing.T) {
	type testCase struct {
		name    string
		slot    string
		wantErr bool
	}

	run := func(t *testing.T, tc testCase) {
		t.Helper()
		t.Setenv("GPU_CR_GROUP_STORE", "/store/groups")
		_, err := groupDir(tc.slot)
		if tc.wantErr {
			assert.ErrorContains(t, err, "path traversal")
		} else {
			assert.NilError(t, err)
		}
	}

	testCases := []testCase{
		{name: "plain slot", slot: "slot-a"},
		{name: "nested slot rejected (GC reaps top level only)", slot: "job/slot-a", wantErr: true},
		{name: "dot dot escape", slot: "../../etc", wantErr: true},
		{name: "dot dot exact", slot: "..", wantErr: true},
		{name: "dot only", slot: ".", wantErr: true},
		{name: "sneaky traversal", slot: "a/../../etc", wantErr: true},
		{name: "empty", slot: "", wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}
