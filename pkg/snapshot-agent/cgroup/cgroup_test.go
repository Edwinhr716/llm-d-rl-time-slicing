package cgroup_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/cgroup"
)

const (
	podUID      = "0c1d2e3f-aaaa-bbbb-cccc-123456789abc"
	podUIDSd    = "0c1d2e3f_aaaa_bbbb_cccc_123456789abc"
	containerID = "c0ffee"
	pauseID     = "5a5a5a"
	systemdPod  = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod" + podUIDSd + ".slice"
)

// writeFile creates root/rel with content, and its parent directories.
func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// systemdTree builds a cgroup v2 root with one burstable pod in the systemd
// layout: a guest container with PIDs 101 and 102, and the pause sandbox
// with PID 7.
func systemdTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, "cgroup.controllers", "cpu memory pids")
	writeFile(t, root, "system.slice/pod"+podUID+"/cgroup.procs", "1\n") // Not under kubepods: ignored.
	writeFile(t, root, systemdPod+"/cgroup.kill", "")
	writeFile(t, root, systemdPod+"/cgroup.procs", "")
	writeFile(t, root, systemdPod+"/cri-containerd-"+containerID+".scope/cgroup.procs", "101\n102\n")
	writeFile(t, root, systemdPod+"/cri-containerd-"+pauseID+".scope/cgroup.procs", "7\n")
	return root
}

func TestPodCgroupPath(t *testing.T) {
	t.Run("systemd layout with underscores", func(t *testing.T) {
		got, err := cgroup.New(systemdTree(t)).PodCgroupPath(podUID)
		if err != nil || got != systemdPod {
			t.Fatalf("got %q, %v; want %q", got, err, systemdPod)
		}
	})
	t.Run("guaranteed pod directly under kubepods", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "cgroup.controllers", "")
		want := "kubepods.slice/kubepods-pod" + podUIDSd + ".slice"
		writeFile(t, root, want+"/cgroup.procs", "")
		got, err := cgroup.New(root).PodCgroupPath(podUID)
		if err != nil || got != want {
			t.Fatalf("got %q, %v; want %q", got, err, want)
		}
	})
	t.Run("cgroupfs layout with dashes", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "cgroup.controllers", "")
		want := "kubepods/besteffort/pod" + podUID
		writeFile(t, root, want+"/cgroup.procs", "")
		got, err := cgroup.New(root).PodCgroupPath(podUID)
		if err != nil || got != want {
			t.Fatalf("got %q, %v; want %q", got, err, want)
		}
	})
	t.Run("unknown pod", func(t *testing.T) {
		_, err := cgroup.New(systemdTree(t)).PodCgroupPath("ffffffff-0000-0000-0000-000000000000")
		if !errors.Is(err, cgroup.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})
	t.Run("not a cgroup v2 root", func(t *testing.T) {
		_, err := cgroup.New(t.TempDir()).PodCgroupPath(podUID)
		if err == nil || errors.Is(err, cgroup.ErrNotFound) {
			t.Fatalf("want a mount error, got %v", err)
		}
	})
	t.Run("empty UID", func(t *testing.T) {
		if _, err := cgroup.New(systemdTree(t)).PodCgroupPath(""); err == nil {
			t.Fatal("want an error")
		}
	})
}

func TestProcs(t *testing.T) {
	root := systemdTree(t)
	fsys := cgroup.New(root)

	t.Run("containers only, pause excluded", func(t *testing.T) {
		got, err := fsys.Procs(systemdPod, []string{"containerd://" + containerID})
		if err != nil || !slices.Equal(got, []int{101, 102}) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("whole pod without container IDs", func(t *testing.T) {
		got, err := fsys.Procs(systemdPod, nil)
		slices.Sort(got)
		if err != nil || !slices.Equal(got, []int{7, 101, 102}) {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("exited container cgroup counts as empty", func(t *testing.T) {
		got, err := fsys.Procs(systemdPod, []string{"containerd://gone"})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("removed pod cgroup counts as empty", func(t *testing.T) {
		got, err := fsys.Procs("kubepods.slice/missing.slice", nil)
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("malformed cgroup.procs is an error", func(t *testing.T) {
		bad := t.TempDir()
		writeFile(t, bad, "kubepods/pod1/cgroup.procs", "12 x\n")
		if _, err := cgroup.New(bad).Procs("kubepods/pod1", nil); err == nil {
			t.Fatal("want an error")
		}
	})
}

func TestKill(t *testing.T) {
	t.Run("writes cgroup.kill at the pod level", func(t *testing.T) {
		root := systemdTree(t)
		fsys := cgroup.New(root)
		fsys.SetSignal(func(int) error {
			t.Error("no signal expected when cgroup.kill exists")
			return nil
		})
		if err := fsys.Kill(systemdPod); err != nil {
			t.Fatal(err)
		}
		if got := readFile(t, root, systemdPod+"/cgroup.kill"); got != "1" {
			t.Fatalf("cgroup.kill = %q, want 1", got)
		}
	})
	t.Run("falls back to SIGKILL for every PID", func(t *testing.T) {
		root := systemdTree(t)
		if err := os.Remove(filepath.Join(root, systemdPod, "cgroup.kill")); err != nil {
			t.Fatal(err)
		}
		fsys := cgroup.New(root)
		var signalled []int
		fsys.SetSignal(func(pid int) error {
			signalled = append(signalled, pid)
			if pid == 7 {
				return syscall.ESRCH // Already gone: not an error.
			}
			return nil
		})
		if err := fsys.Kill(systemdPod); err != nil {
			t.Fatal(err)
		}
		slices.Sort(signalled)
		if !slices.Equal(signalled, []int{7, 101, 102}) {
			t.Fatalf("signalled %v", signalled)
		}
	})
	t.Run("SIGKILL failure is reported", func(t *testing.T) {
		root := systemdTree(t)
		if err := os.Remove(filepath.Join(root, systemdPod, "cgroup.kill")); err != nil {
			t.Fatal(err)
		}
		fsys := cgroup.New(root)
		fsys.SetSignal(func(int) error { return syscall.EPERM })
		if err := fsys.Kill(systemdPod); !errors.Is(err, syscall.EPERM) {
			t.Fatalf("want EPERM, got %v", err)
		}
	})
	t.Run("a removed pod cgroup has nothing to kill", func(t *testing.T) {
		if err := cgroup.New(systemdTree(t)).Kill("kubepods.slice/missing.slice"); err != nil {
			t.Fatal(err)
		}
	})
}
