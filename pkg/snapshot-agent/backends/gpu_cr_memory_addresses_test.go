package backends_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
)

func TestNewGpuCrMemoryAddresses(t *testing.T) {
	g := backends.NewGpuCrMemoryAddresses()
	if g == nil {
		t.Fatal("NewGpuCrMemoryAddresses returned nil")
	}
}

// setupStore points EXPORT_FILE_PATH at a temp dir seeded with a pid_map so
// resolvePidToId succeeds without /proc, and returns the expected -o path
// for the given group and id.
func setupStore(t *testing.T, pid, id, groupID string) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("EXPORT_FILE_PATH", dir)
	t.Setenv("GPU_CR_CTL_PATH", "")
	if err := os.WriteFile(filepath.Join(dir, "pid_map_"+pid), []byte(id+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "groups", groupID, id)
}

func TestGpuCrMemoryAddressesSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		targets     []string
		execErr     error
		expectedErr bool
		wantSpec    string // -s argument; empty = exec not expected
	}{
		{
			name:        "SuccessSingle",
			targets:     []string{"123:0x7f00:1024"},
			expectedErr: false,
			wantSpec:    "0x7f00:1024",
		},
		{
			name:        "SuccessMultipleSamePID",
			targets:     []string{"123:0x7f00:1024", "123:0x8f00:2048"},
			expectedErr: false,
			wantSpec:    "0x7f00:1024,0x8f00:2048",
		},
		{
			name:        "InvalidTarget",
			targets:     []string{"invalid-format"},
			expectedErr: true,
		},
		{
			name:        "ExecFailure",
			targets:     []string{"123:0x7f00:1024"},
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
			wantSpec:    "0x7f00:1024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := setupStore(t, "123", "7", "test-group")
			expectedArgs := []string{"-c", "-p", "123", "-s", tt.wantSpec, "-o", dest}

			g := backends.NewGpuCrMemoryAddresses()
			g.SetExecCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "/usr/local/bin/cr_client" {
					t.Errorf("expected command /usr/local/bin/cr_client, got %s", name)
				}
				if tt.wantSpec != "" {
					assertArgs(t, expectedArgs, args)
				}
				return nil, tt.execErr
			})

			err := g.Snapshot(context.Background(), "test-group", tt.targets)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Snapshot() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if !tt.expectedErr {
				// The group dir must exist for the preloader to dump into.
				if _, err := os.Stat(filepath.Dir(dest)); err != nil {
					t.Errorf("group dir not created: %v", err)
				}
			}
		})
	}
}

func TestGpuCrMemoryAddressesRestore(t *testing.T) {
	tests := []struct {
		name        string
		targets     []string
		execErr     error
		expectedErr bool
		wantSpec    string
	}{
		{
			name:        "SuccessSingle",
			targets:     []string{"123:0x7f00:1024"},
			expectedErr: false,
			wantSpec:    "0x7f00:1024",
		},
		{
			name:        "SuccessMultipleSamePID",
			targets:     []string{"123:0x7f00:1024", "123:0x8f00:2048"},
			expectedErr: false,
			wantSpec:    "0x7f00:1024,0x8f00:2048",
		},
		{
			name:        "InvalidTarget",
			targets:     []string{"invalid-format"},
			expectedErr: true,
		},
		{
			name:        "ExecFailure",
			targets:     []string{"123:0x7f00:1024"},
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
			wantSpec:    "0x7f00:1024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := setupStore(t, "123", "7", "test-group")
			expectedArgs := []string{"-r", "-p", "123", "-s", tt.wantSpec, "-o", dest}

			g := backends.NewGpuCrMemoryAddresses()
			g.SetExecCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "/usr/local/bin/cr_client" {
					t.Errorf("expected command /usr/local/bin/cr_client, got %s", name)
				}
				if tt.wantSpec != "" {
					assertArgs(t, expectedArgs, args)
				}
				return nil, tt.execErr
			})

			err := g.Restore(context.Background(), "test-group", tt.targets)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Restore() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestGroupIDValidation(t *testing.T) {
	setupStore(t, "123", "7", "x")
	g := backends.NewGpuCrMemoryAddresses()
	g.SetExecCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return nil, nil
	})
	for _, bad := range []string{"../escape", "a/b", ".", ""} {
		if err := g.Snapshot(context.Background(), bad, []string{"123:0x7f00:1024"}); err == nil {
			t.Errorf("Snapshot accepted invalid group ID %q", bad)
		}
	}
}

func assertArgs(t *testing.T, expected, got []string) {
	t.Helper()
	if len(got) != len(expected) {
		t.Errorf("expected %d args, got %d. args: %v", len(expected), len(got), got)
		return
	}
	for i, arg := range got {
		if arg != expected[i] {
			t.Errorf("expected arg %d to be %s, got %s", i, expected[i], arg)
		}
	}
}
