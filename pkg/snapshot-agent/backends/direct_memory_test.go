package backends_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
)

func directMemoryConfig(pids ...int32) *pb.BackendConfig {
	return &pb.BackendConfig{
		Backend: &pb.BackendConfig_DirectMemory{
			DirectMemory: &pb.DirectMemoryBackendConfig{
				ExplicitTarget: &pb.ProcessTarget{Pids: pids},
			},
		},
	}
}

func TestNewDirectMemory(t *testing.T) {
	dm := backends.NewDirectMemory()
	if dm == nil {
		t.Fatal("NewDirectMemory returned nil")
	}
}

func TestDirectMemorySnapshot(t *testing.T) {
	tests := []struct {
		name        string
		config      *pb.BackendConfig
		execErr     error
		expectedErr bool
		expectArgs  [][]string
	}{
		{
			name:   "SuccessMultiplePIDs",
			config: directMemoryConfig(123, 456),
			expectArgs: [][]string{
				{"-c", "-p", "123"},
				{"-c", "-p", "456"},
			},
		},
		{
			name:        "ExecFailure",
			config:      directMemoryConfig(123),
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
			expectArgs: [][]string{
				{"-c", "-p", "123"},
			},
		},
		{
			name:        "NoPIDs",
			config:      directMemoryConfig(),
			expectedErr: true,
		},
		{
			name:        "NilConfig",
			config:      nil,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := backends.NewDirectMemory()
			dm.SetStatFunc(func(string) (os.FileInfo, error) { return os.Stat(".") })
			var calledArgs [][]string
			dm.SetExecCommand(func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != backends.CrClientPath {
					t.Errorf("exec binary = %q, want %q", name, backends.CrClientPath)
				}
				calledArgs = append(calledArgs, args)
				return nil, tt.execErr
			})

			err := dm.Snapshot(context.Background(), backends.Request{JobID: "test-job", Config: tt.config})
			if (err != nil) != tt.expectedErr {
				t.Errorf("Snapshot() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if !tt.expectedErr && !reflect.DeepEqual(calledArgs, tt.expectArgs) {
				t.Errorf("Snapshot() calledArgs = %v, expected %v", calledArgs, tt.expectArgs)
			}
		})
	}
}

func TestDirectMemoryRestore(t *testing.T) {
	tests := []struct {
		name        string
		config      *pb.BackendConfig
		execErr     error
		expectedErr bool
		expectArgs  [][]string
	}{
		{
			name:   "SuccessMultiplePIDs",
			config: directMemoryConfig(123, 456),
			expectArgs: [][]string{
				{"-r", "-p", "123"},
				{"-r", "-p", "456"},
			},
		},
		{
			name:        "NoPIDs",
			config:      directMemoryConfig(),
			expectedErr: true,
		},
		{
			name:        "NilConfig",
			config:      nil,
			expectedErr: true,
		},
		{
			name:        "ExecFailure",
			config:      directMemoryConfig(123),
			execErr:     fmt.Errorf("exec error"),
			expectedErr: true,
			expectArgs: [][]string{
				{"-r", "-p", "123"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := backends.NewDirectMemory()
			dm.SetStatFunc(func(string) (os.FileInfo, error) { return os.Stat(".") })
			var calledArgs [][]string
			dm.SetExecCommand(func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != backends.CrClientPath {
					t.Errorf("exec binary = %q, want %q", name, backends.CrClientPath)
				}
				calledArgs = append(calledArgs, args)
				return nil, tt.execErr
			})

			err := dm.Restore(context.Background(), backends.Request{JobID: "test-job", Config: tt.config})
			if (err != nil) != tt.expectedErr {
				t.Errorf("Restore() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if !tt.expectedErr && !reflect.DeepEqual(calledArgs, tt.expectArgs) {
				t.Errorf("Restore() calledArgs = %v, expected %v", calledArgs, tt.expectArgs)
			}
		})
	}
}

func TestDirectMemoryHealthCheck(t *testing.T) {
	tests := []struct {
		name        string
		statErr     error
		expectedErr bool
	}{
		{
			name:        "Installed",
			statErr:     nil,
			expectedErr: false,
		},
		{
			name:        "Missing",
			statErr:     fmt.Errorf("no stat"),
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := backends.NewDirectMemory()
			dm.SetStatFunc(func(path string) (os.FileInfo, error) {
				if path != backends.CrClientPath {
					t.Errorf("stat path = %q, want %q", path, backends.CrClientPath)
				}
				if tt.statErr != nil {
					return nil, tt.statErr
				}
				return os.Stat(".")
			})

			err := dm.HealthCheck(context.Background())
			if (err != nil) != tt.expectedErr {
				t.Errorf("HealthCheck() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestDirectMemoryCrClientMissing(t *testing.T) {
	// cr_client lives at exactly one path in the image; a deployment without
	// it must fail the operation outright, not exec a dangling fallback.
	dm := backends.NewDirectMemory()
	dm.SetStatFunc(func(string) (os.FileInfo, error) { return nil, os.ErrNotExist })
	dm.SetExecCommand(func(_ context.Context, name string, _ ...string) ([]byte, error) {
		t.Errorf("exec called with %q despite missing cr_client", name)
		return nil, nil
	})

	req := backends.Request{JobID: "test-job", Config: directMemoryConfig(123)}
	if err := dm.Snapshot(context.Background(), req); err == nil {
		t.Error("Snapshot() expected error for missing cr_client, got nil")
	}
	if err := dm.Restore(context.Background(), req); err == nil {
		t.Error("Restore() expected error for missing cr_client, got nil")
	}
}

func TestDirectMemoryConfigHelpers(t *testing.T) {
	pids := []string{"100", "200"}
	cfg, err := backends.BuildDirectMemoryConfig(pids)
	if err != nil {
		t.Fatalf("BuildDirectMemoryConfig() unexpected error: %v", err)
	}
	extracted := backends.ExtractDirectMemoryPIDStrings(cfg)
	if !reflect.DeepEqual(extracted, pids) {
		t.Errorf("ExtractDirectMemoryPIDStrings() = %v, want %v", extracted, pids)
	}

	if len(backends.ExtractDirectMemoryPIDStrings(nil)) != 0 {
		t.Errorf("Expected nil when extracting from nil config")
	}

	_, err = backends.BuildDirectMemoryConfig([]string{"100", "not-a-pid"})
	if err == nil {
		t.Errorf("BuildDirectMemoryConfig() expected error for invalid PID string, got nil")
	}
}

func TestDirectMemoryOpTimeout(t *testing.T) {
	t.Setenv("DIRECT_MEMORY_OP_TIMEOUT_SEC", "1")

	dm := backends.NewDirectMemory()
	dm.SetStatFunc(func(string) (os.FileInfo, error) { return os.Stat(".") })
	dm.SetExecCommand(func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
		// Simulate cr_client hanging on a dead workload's control channel:
		// block until the per-operation deadline cancels the context.
		<-ctx.Done()
		return nil, ctx.Err()
	})

	start := time.Now()
	err := dm.Snapshot(context.Background(), backends.Request{JobID: "test-job", Config: directMemoryConfig(123)})
	if err == nil {
		t.Fatal("Snapshot() expected timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Snapshot() took %v; the 1s DIRECT_MEMORY_OP_TIMEOUT_SEC deadline was not applied", elapsed)
	}

	err = dm.Restore(context.Background(), backends.Request{JobID: "test-job", Config: directMemoryConfig(123)})
	if err == nil {
		t.Fatal("Restore() expected timeout error, got nil")
	}
}
