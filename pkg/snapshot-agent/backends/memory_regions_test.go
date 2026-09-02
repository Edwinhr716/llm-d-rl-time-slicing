package backends_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
)

const testCrClient = "/usr/local/bin/cr_client"

func region(pid int32, addr, size uint64) *pb.MemoryRegion {
	return &pb.MemoryRegion{Pid: pid, Address: addr, SizeBytes: size}
}

func memoryRegionsConfig(slot string, regions ...*pb.MemoryRegion) *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_MemoryRegions{
			MemoryRegions: &pb.MemoryRegionsBackendConfig{
				Regions:      regions,
				SnapshotName: slot,
			},
		},
	}
}

// newMemoryRegions returns a backend pointed at a tempdir layout via env.
// Destination-slot dumps land under the returned store dir (the -o paths).
//
//nolint:gocritic // The project configuration bans named returns, conflicting with unnamedResult
func newMemoryRegions(t *testing.T) (*backends.MemoryRegions, string, string) {
	t.Helper()
	ctlDir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", ctlDir)
	t.Setenv("GPU_CR_CTL_PATH", "")
	t.Setenv("GPU_CR_GROUP_STORE", "")
	mr := backends.NewMemoryRegions()
	mr.SetLookPath(func(string) (string, error) { return testCrClient, nil })
	return mr, ctlDir, filepath.Join(ctlDir, "groups")
}

// writePidMap maps a PID to its dump-buffer id in the ctl dir, as the
// workload's preloader does at startup.
func writePidMap(t *testing.T, dir, pid, id string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "pid_map_"+pid), []byte(id+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// expandStore substitutes the per-test group-store dir into expected args
// ("<store>" placeholder), so tables can spell out full -o paths.
func expandStore(args [][]string, store string) [][]string {
	out := make([][]string, len(args))
	for i, call := range args {
		out[i] = make([]string, len(call))
		for j, a := range call {
			out[i][j] = strings.ReplaceAll(a, "<store>", store)
		}
	}
	return out
}

func TestNewMemoryRegions(t *testing.T) {
	mr := backends.NewMemoryRegions()
	if mr == nil {
		t.Fatal("NewMemoryRegions returned nil")
	}
}

func TestMemoryRegionsSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		config      *pb.BackendConfig
		jobID       string
		execErr     error
		expectedErr bool
		expectNoRun bool       // cr_client must not be invoked at all
		expectArgs  [][]string // checked on success; "<store>" expands to the group-store dir
	}{
		{
			name:   "SingleRegion",
			config: memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:  "test-job",
			expectArgs: [][]string{
				{"-c", "-p", "123", "-s", "0x7f00:1024", "-o", "<store>/slot-a/42"},
			},
		},
		{
			name:   "RegionsOfOnePIDJoined",
			config: memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024), region(123, 0x8f00, 2048)),
			jobID:  "test-job",
			expectArgs: [][]string{
				{"-c", "-p", "123", "-s", "0x7f00:1024,0x8f00:2048", "-o", "<store>/slot-a/42"},
			},
		},
		{
			name:   "AddressFormattedAsHex",
			config: memoryRegionsConfig("slot-a", region(123, 139637976727552, 1073741824)),
			jobID:  "test-job",
			expectArgs: [][]string{
				{"-c", "-p", "123", "-s", "0x7f0000000000:1073741824", "-o", "<store>/slot-a/42"},
			},
		},
		{
			name:   "EmptySnapshotNameFallsBackToJobID",
			config: memoryRegionsConfig("", region(123, 0x7f00, 1024)),
			jobID:  "job-1",
			expectArgs: [][]string{
				{"-c", "-p", "123", "-s", "0x7f00:1024", "-o", "<store>/job-1/42"},
			},
		},
		{
			name:        "NilConfig",
			config:      nil,
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "NoRegions",
			config:      memoryRegionsConfig("slot-a"),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "ZeroPID",
			config:      memoryRegionsConfig("slot-a", region(0, 0x7f00, 1024)),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "NegativePID",
			config:      memoryRegionsConfig("slot-a", region(-5, 0x7f00, 1024)),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "ZeroSize",
			config:      memoryRegionsConfig("slot-a", region(123, 0x7f00, 0)),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "PathTraversalSnapshotName",
			config:      memoryRegionsConfig("../../etc", region(123, 0x7f00, 1024)),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "NestedSnapshotName",
			config:      memoryRegionsConfig("job/slot-a", region(123, 0x7f00, 1024)),
			jobID:       "test-job",
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "ExecFailure",
			config:      memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			jobID:       "test-job",
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, ctlDir, storeDir := newMemoryRegions(t)
			writePidMap(t, ctlDir, "123", "42")
			var calledArgs [][]string
			mr.SetExecCommand(func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != testCrClient {
					t.Errorf("exec binary = %q, want %q", name, testCrClient)
				}
				calledArgs = append(calledArgs, args)
				return nil, tt.execErr
			})

			err := mr.Snapshot(context.Background(), backends.Request{JobID: tt.jobID, Config: tt.config})
			if (err != nil) != tt.expectedErr {
				t.Errorf("Snapshot() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if tt.expectNoRun && len(calledArgs) != 0 {
				t.Errorf("Snapshot() invoked cr_client with %v despite invalid request", calledArgs)
			}
			if !tt.expectedErr {
				want := expandStore(tt.expectArgs, storeDir)
				if !reflect.DeepEqual(calledArgs, want) {
					t.Errorf("Snapshot() calledArgs = %v, expected %v", calledArgs, want)
				}
				// The slot dir must exist for the preloader to dump into.
				if _, err := os.Stat(filepath.Dir(want[0][len(want[0])-1])); err != nil {
					t.Errorf("slot dir not created: %v", err)
				}
			}
		})
	}
}

func TestMemoryRegionsRestore(t *testing.T) {
	tests := []struct {
		name        string
		config      *pb.BackendConfig
		makeSlot    bool
		execErr     error
		expectedErr bool
		expectNoRun bool
		expectArgs  [][]string
	}{
		{
			name:     "Success",
			config:   memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			makeSlot: true,
			expectArgs: [][]string{
				{"-r", "-p", "123", "-s", "0x7f00:1024", "-o", "<store>/slot-a/42"},
			},
		},
		{
			name:        "MissingSlot",
			config:      memoryRegionsConfig("no-such-slot", region(123, 0x7f00, 1024)),
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "NilConfig",
			config:      nil,
			expectedErr: true,
			expectNoRun: true,
		},
		{
			name:        "ExecFailure",
			config:      memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			makeSlot:    true,
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, ctlDir, storeDir := newMemoryRegions(t)
			writePidMap(t, ctlDir, "123", "42")
			if tt.makeSlot {
				if err := os.MkdirAll(filepath.Join(storeDir, "slot-a"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			var calledArgs [][]string
			mr.SetExecCommand(func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != testCrClient {
					t.Errorf("exec binary = %q, want %q", name, testCrClient)
				}
				calledArgs = append(calledArgs, args)
				return nil, tt.execErr
			})

			err := mr.Restore(context.Background(), backends.Request{JobID: "test-job", Config: tt.config})
			if (err != nil) != tt.expectedErr {
				t.Errorf("Restore() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if tt.expectNoRun && len(calledArgs) != 0 {
				t.Errorf("Restore() invoked cr_client with %v despite invalid request", calledArgs)
			}
			if !tt.expectedErr {
				want := expandStore(tt.expectArgs, storeDir)
				if !reflect.DeepEqual(calledArgs, want) {
					t.Errorf("Restore() calledArgs = %v, expected %v", calledArgs, want)
				}
			}
		})
	}
}

// TestMemoryRegionsLazyInit covers the pid->id fallback: destination-path
// ops need the dump-buffer id before the first cr_client signal, but the
// preloader only writes pid_map inside init_CR, so an unresolvable PID is
// driven through cr_client -i (idempotent) and re-resolved.
func TestMemoryRegionsLazyInit(t *testing.T) {
	tests := []struct {
		name           string
		execErr        error
		writeMapOnInit bool
		wantErr        string // substring the error must contain; unset means success
		expectArgs     [][]string
	}{
		{
			name:           "InitThenResolve",
			writeMapOnInit: true,
			expectArgs: [][]string{
				{"-i", "-p", "123"},
				{"-c", "-p", "123", "-s", "0x7f00:1024", "-o", "<store>/slot-a/42"},
			},
		},
		{
			name:       "InitExecFailureSurfacesBothErrors",
			execErr:    fmt.Errorf("no such process"),
			wantErr:    "preloader init failed",
			expectArgs: [][]string{{"-i", "-p", "123"}},
		},
		{
			name:       "StillUnresolvableAfterInit",
			wantErr:    "failed to resolve PID 123",
			expectArgs: [][]string{{"-i", "-p", "123"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, ctlDir, storeDir := newMemoryRegions(t)
			mr.SetProcRoot(t.TempDir()) // no /proc/<pid>/maps fallback either
			var calledArgs [][]string
			mr.SetExecCommand(func(_ context.Context, _ string, args ...string) ([]byte, error) {
				calledArgs = append(calledArgs, args)
				if tt.writeMapOnInit && args[0] == "-i" {
					writePidMap(t, ctlDir, "123", "42")
				}
				return nil, tt.execErr
			})

			err := mr.Snapshot(context.Background(), backends.Request{
				JobID:  "test-job",
				Config: memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Snapshot() unexpected error: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Snapshot() error = %v, want substring %q", err, tt.wantErr)
			}
			want := expandStore(tt.expectArgs, storeDir)
			if !reflect.DeepEqual(calledArgs, want) {
				t.Errorf("Snapshot() calledArgs = %v, expected %v", calledArgs, want)
			}
		})
	}
}

// TestMemoryRegionsPidResolution covers the pid_map read paths, observable
// through the id in the -o destination the backend hands cr_client.
func TestMemoryRegionsPidResolution(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T, mr *backends.MemoryRegions, ctlDir string)
		wantID string
	}{
		{
			name: "NULPaddedPidMap", // an mmap-written map file has a zero-padded tail
			setup: func(t *testing.T, _ *backends.MemoryRegions, ctlDir string) {
				t.Helper()
				content := append([]byte("77"), make([]byte, 30)...)
				if err := os.WriteFile(filepath.Join(ctlDir, "pid_map_123"), content, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantID: "77",
		},
		{
			name: "CtlDirConsultedBeforeDataDir",
			setup: func(t *testing.T, _ *backends.MemoryRegions, ctlDir string) {
				t.Helper()
				tmpfsDir := t.TempDir()
				t.Setenv("GPU_CR_CTL_PATH", tmpfsDir)
				// Stale/empty map in the data dir, good map on the ctl tmpfs.
				if err := os.WriteFile(filepath.Join(ctlDir, "pid_map_123"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				writePidMap(t, tmpfsDir, "123", "91")
			},
			wantID: "91",
		},
		{
			name: "EmptyPidMapFallsBackToProcMaps", // older preloaders leave an empty file
			setup: func(t *testing.T, mr *backends.MemoryRegions, ctlDir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(ctlDir, "pid_map_123"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				procRoot := t.TempDir()
				mr.SetProcRoot(procRoot)
				if err := os.MkdirAll(filepath.Join(procRoot, "123"), 0o755); err != nil {
					t.Fatal(err)
				}
				maps := "7f0000000000-7f0040000000 rw-s 00000000 00:0f 12345 /var/tmp/huge-ckpt/88\n"
				if err := os.WriteFile(filepath.Join(procRoot, "123", "maps"), []byte(maps), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantID: "88",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, ctlDir, storeDir := newMemoryRegions(t)
			tt.setup(t, mr, ctlDir)
			var calledArgs [][]string
			mr.SetExecCommand(func(_ context.Context, _ string, args ...string) ([]byte, error) {
				calledArgs = append(calledArgs, args)
				return nil, nil
			})

			err := mr.Snapshot(context.Background(), backends.Request{
				JobID:  "test-job",
				Config: memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024)),
			})
			if err != nil {
				t.Fatalf("Snapshot() unexpected error: %v", err)
			}
			want := [][]string{{"-c", "-p", "123", "-s", "0x7f00:1024", "-o", filepath.Join(storeDir, "slot-a", tt.wantID)}}
			if !reflect.DeepEqual(calledArgs, want) {
				t.Errorf("Snapshot() calledArgs = %v, expected %v", calledArgs, want)
			}
		})
	}
}

// TestMemoryRegionsSnapshotOwnersMeta verifies the .owners liveness metadata
// (pid + starttime) that GC's owner-liveness sweep relies on. Needs a real
// procfs, so it uses the test process itself as the owner.
func TestMemoryRegionsSnapshotOwnersMeta(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("no procfs on this host")
	}
	mr, ctlDir, storeDir := newMemoryRegions(t)
	mr.SetExecCommand(func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	pid := strconv.Itoa(os.Getpid())
	writePidMap(t, ctlDir, pid, "42")

	pidNum, err := strconv.ParseInt(pid, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	err = mr.Snapshot(context.Background(), backends.Request{
		JobID:  "test-job",
		Config: memoryRegionsConfig("slot-a", region(int32(pidNum), 0x7f00, 1024)),
	})
	if err != nil {
		t.Fatalf("Snapshot() unexpected error: %v", err)
	}

	owners, err := utils.ReadGroupMeta(filepath.Join(storeDir, "slot-a"))
	if err != nil {
		t.Fatalf("ReadGroupMeta() unexpected error: %v", err)
	}
	st, ok := owners[pid]
	if !ok {
		t.Fatalf("own pid missing from %s: %v", utils.GroupMetaName, owners)
	}
	want, err := utils.ProcStarttime(pid)
	if err != nil {
		t.Fatal(err)
	}
	if st != want {
		t.Errorf("recorded starttime = %d, want %d", st, want)
	}
}

func TestMemoryRegionsHealthCheck(t *testing.T) {
	tests := []struct {
		name        string
		lookErr     error
		expectedErr bool
	}{
		{
			name:        "Installed",
			lookErr:     nil,
			expectedErr: false,
		},
		{
			name:        "Missing",
			lookErr:     fmt.Errorf("not in PATH"),
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := backends.NewMemoryRegions()
			mr.SetLookPath(func(string) (string, error) {
				if tt.lookErr != nil {
					return "", tt.lookErr
				}
				return testCrClient, nil
			})

			err := mr.HealthCheck(context.Background())
			if (err != nil) != tt.expectedErr {
				t.Errorf("HealthCheck() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestMemoryRegionsOpTimeout(t *testing.T) {
	t.Setenv("GPU_CR_OP_TIMEOUT_SEC", "1")

	mr, ctlDir, storeDir := newMemoryRegions(t)
	writePidMap(t, ctlDir, "123", "42")
	if err := os.MkdirAll(filepath.Join(storeDir, "slot-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	mr.SetExecCommand(func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
		// Simulate cr_client hanging on a dead workload's control channel:
		// block until the per-operation deadline cancels the context.
		<-ctx.Done()
		return nil, ctx.Err()
	})

	req := backends.Request{JobID: "test-job", Config: memoryRegionsConfig("slot-a", region(123, 0x7f00, 1024))}
	start := time.Now()
	if err := mr.Snapshot(context.Background(), req); err == nil {
		t.Fatal("Snapshot() expected timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Snapshot() took %v; the 1s GPU_CR_OP_TIMEOUT_SEC deadline was not applied", elapsed)
	}

	if err := mr.Restore(context.Background(), req); err == nil {
		t.Fatal("Restore() expected timeout error, got nil")
	}
}

// TestMemoryRegionsMetaFile covers the slot-metadata write path. A hugetlbfs
// group store rejects write(2) with EINVAL (leaving a 0-byte stub), so the
// metadata must land on the ctl tmpfs instead, where ReadGroupMeta picks it
// up; without a ctl tmpfs the original error must surface.
func TestMemoryRegionsMetaFile(t *testing.T) {
	t.Run("PlainWrite", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("GPU_CR_CTL_PATH", t.TempDir())
		if err := backends.WriteMetaFile(dir, []byte("123 456\n")); err != nil {
			t.Fatalf("WriteMetaFile() unexpected error: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, utils.GroupMetaName))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "123 456\n" {
			t.Errorf("meta content = %q, want %q", data, "123 456\n")
		}
		if _, err := os.Stat(utils.GroupMetaFallbackPath(dir)); !os.IsNotExist(err) {
			t.Errorf("fallback file should not exist (stat err: %v)", err)
		}
	})

	t.Run("EINVALFallsBackToCtlTmpfs", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("GPU_CR_CTL_PATH", t.TempDir())
		restore := backends.SetWriteFile(func(name string, data []byte, perm os.FileMode) error {
			if filepath.Dir(name) == dir {
				// Mimic hugetlbfs: open+create succeed, write(2) EINVALs.
				f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
				if err != nil {
					return err
				}
				f.Close()
				return &os.PathError{Op: "write", Path: name, Err: syscall.EINVAL}
			}
			return os.WriteFile(name, data, perm)
		})
		defer restore()

		if err := backends.WriteMetaFile(dir, []byte("123 456\n789 42\n")); err != nil {
			t.Fatalf("WriteMetaFile() unexpected error: %v", err)
		}
		data, err := os.ReadFile(utils.GroupMetaFallbackPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "123 456\n789 42\n" {
			t.Errorf("fallback content = %q, want %q", data, "123 456\n789 42\n")
		}
		owners, err := utils.ReadGroupMeta(dir)
		if err != nil {
			t.Fatalf("ReadGroupMeta() unexpected error: %v", err)
		}
		if len(owners) != 2 || owners["123"] != int64(456) {
			t.Errorf("ReadGroupMeta() = %v, want 2 owners with 123->456", owners)
		}
	})

	t.Run("EINVALWithoutCtlDirSurfacesError", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("GPU_CR_CTL_PATH", "")
		restore := backends.SetWriteFile(func(name string, _ []byte, _ os.FileMode) error {
			return &os.PathError{Op: "write", Path: name, Err: syscall.EINVAL}
		})
		defer restore()

		if err := backends.WriteMetaFile(dir, []byte("123 456\n")); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("WriteMetaFile() error = %v, want EINVAL", err)
		}
	})
}
