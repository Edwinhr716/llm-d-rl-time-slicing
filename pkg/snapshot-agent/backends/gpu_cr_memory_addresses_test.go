package backends_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/backends"
)

func TestNewGpuCrMemoryAddresses(t *testing.T) {
	g := backends.NewGpuCrMemoryAddresses()
	if g == nil {
		t.Fatal("NewGpuCrMemoryAddresses returned nil")
	}
}

func TestGpuCrMemoryAddressesSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		targets      []string
		execErr      error
		expectedErr  bool
		expectedArgs []string
	}{
		{
			name:         "SuccessSingle",
			targets:      []string{"123:0x7f00:1024"},
			execErr:      nil,
			expectedErr:  false,
			expectedArgs: []string{"-c", "-p", "123", "-s", "0x7f00:1024"},
		},
		{
			name:         "SuccessMultipleSamePID",
			targets:      []string{"123:0x7f00:1024", "123:0x8f00:2048"},
			execErr:      nil,
			expectedErr:  false,
			expectedArgs: []string{"-c", "-p", "123", "-s", "0x7f00:1024,0x8f00:2048"},
		},
		{
			name:        "InvalidTarget",
			targets:     []string{"invalid-format"},
			expectedErr: true,
		},
		{
			name:         "ExecFailure",
			targets:      []string{"123:0x7f00:1024"},
			execErr:      fmt.Errorf("exec error"),
			expectedErr:  true,
			expectedArgs: []string{"-c", "-p", "123", "-s", "0x7f00:1024"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := backends.NewGpuCrMemoryAddresses()
			g.SetExecCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "/usr/local/bin/cr_client" {
					t.Errorf("expected command /usr/local/bin/cr_client, got %s", name)
				}
				if tt.expectedArgs != nil {
					if len(args) != len(tt.expectedArgs) {
						t.Errorf("expected %d args, got %d. args: %v", len(tt.expectedArgs), len(args), args)
					} else {
						for i, arg := range args {
							if arg != tt.expectedArgs[i] {
								t.Errorf("expected arg %d to be %s, got %s", i, tt.expectedArgs[i], arg)
							}
						}
					}
				}
				return nil, tt.execErr
			})

			err := g.Snapshot(context.Background(), tt.targets)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Snapshot() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestGpuCrMemoryAddressesRestore(t *testing.T) {
	tests := []struct {
		name         string
		targets      []string
		execErr      error
		expectedErr  bool
		expectedArgs []string
	}{
		{
			name:         "SuccessSingle",
			targets:      []string{"123:0x7f00:1024"},
			execErr:      nil,
			expectedErr:  false,
			expectedArgs: []string{"-r", "-p", "123", "-s", "0x7f00:1024"},
		},
		{
			name:         "SuccessMultipleSamePID",
			targets:      []string{"123:0x7f00:1024", "123:0x8f00:2048"},
			execErr:      nil,
			expectedErr:  false,
			expectedArgs: []string{"-r", "-p", "123", "-s", "0x7f00:1024,0x8f00:2048"},
		},
		{
			name:        "InvalidTarget",
			targets:     []string{"invalid-format"},
			expectedErr: true,
		},
		{
			name:         "ExecFailure",
			targets:      []string{"123:0x7f00:1024"},
			execErr:      fmt.Errorf("exec error"),
			expectedErr:  true,
			expectedArgs: []string{"-r", "-p", "123", "-s", "0x7f00:1024"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := backends.NewGpuCrMemoryAddresses()
			g.SetExecCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "/usr/local/bin/cr_client" {
					t.Errorf("expected command /usr/local/bin/cr_client, got %s", name)
				}
				if tt.expectedArgs != nil {
					if len(args) != len(tt.expectedArgs) {
						t.Errorf("expected %d args, got %d. args: %v", len(tt.expectedArgs), len(args), args)
					} else {
						for i, arg := range args {
							if arg != tt.expectedArgs[i] {
								t.Errorf("expected arg %d to be %s, got %s", i, tt.expectedArgs[i], arg)
							}
						}
					}
				}
				return nil, tt.execErr
			})

			err := g.Restore(context.Background(), tt.targets)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Restore() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}
