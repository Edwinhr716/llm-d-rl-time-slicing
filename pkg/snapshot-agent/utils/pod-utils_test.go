package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIsPIDInPodCgroup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "proc-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	podUID := "a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6"
	pid := 1234

	// Create a mock /proc/<pid>/cgroup file
	procDir := filepath.Join(tmpDir, fmt.Sprintf("%d", pid))
	if err := os.MkdirAll(procDir, 0755); err != nil {
		t.Fatalf("failed to create mock proc dir: %v", err)
	}
	cgroupFile := filepath.Join(procDir, "cgroup")

	tests := []struct {
		name     string
		content  string
		expected bool
	}{
		{
			name:     "matching pod uid with dashes",
			content:  "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" + podUID + ".slice/cri-containerd-abc.scope\n",
			expected: true,
		},
		{
			name:     "matching pod uid with underscores",
			content:  "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-poda1b2c3d4_e5f6_7g8h_9i0j_k1l2m3n4o5p6.slice/cri-containerd-abc.scope\n",
			expected: true,
		},
		{
			name:     "non-matching pod uid",
			content:  "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-poddifferent-uid.slice/cri-containerd-abc.scope\n",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(cgroupFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("failed to write mock cgroup file: %v", err)
			}

			result, err := isPIDInPodCgroupInternal(cgroupFile, podUID)
			if err != nil {
				t.Fatalf("isPIDInPodCgroupInternal failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
