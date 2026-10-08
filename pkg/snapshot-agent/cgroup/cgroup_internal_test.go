package cgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

const uid = "1a2b3c4d-0000-1111-2222-333344445555"

func TestPodCgroupPath(t *testing.T) {
	uidU := "1a2b3c4d_0000_1111_2222_333344445555"
	cases := map[string]string{
		"systemd burstable":   "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod" + uidU + ".slice",
		"systemd besteffort":  "kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" + uidU + ".slice",
		"systemd guaranteed":  "kubepods.slice/kubepods-pod" + uidU + ".slice",
		"cgroupfs":            "kubepods/burstable/pod" + uid,
		"cgroupfs guaranteed": "kubepods/pod" + uid,
	}
	for name, rel := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			mkdir(t, filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", "kubepods-burstable-podother.slice"))
			mkdir(t, filepath.Join(root, rel))
			got, err := New(root).PodCgroupPath(uid)
			if err != nil {
				t.Fatal(err)
			}
			if want := filepath.Join(root, rel); got != want {
				t.Errorf("got %s, want %s", got, want)
			}
		})
	}

	t.Run("missing", func(t *testing.T) {
		root := t.TempDir()
		mkdir(t, filepath.Join(root, "kubepods.slice"))
		if _, err := New(root).PodCgroupPath(uid); !errors.Is(err, ErrNotFound) {
			t.Errorf("want ErrNotFound, got %v", err)
		}
	})
	t.Run("no prefix match", func(t *testing.T) {
		root := t.TempDir()
		// A UID that only shares a prefix must not match.
		mkdir(t, filepath.Join(root, "kubepods.slice", "kubepods-pod"+uid+"0.slice"))
		if _, err := New(root).PodCgroupPath(uid); !errors.Is(err, ErrNotFound) {
			t.Errorf("want ErrNotFound, got %v", err)
		}
	})
}

func TestContainerCgroupPathAndProcs(t *testing.T) {
	root := t.TempDir()
	pod := filepath.Join(root, "kubepods.slice", "kubepods-pod.slice")
	write(t, filepath.Join(pod, "cgroup.procs"), "")
	write(t, filepath.Join(pod, "cri-containerd-sandbox.scope", "cgroup.procs"), "10\n")
	write(t, filepath.Join(pod, "cri-containerd-abc.scope", "cgroup.procs"), "300\n200\n")
	write(t, filepath.Join(pod, "cri-containerd-abc.scope", "child", "cgroup.procs"), "400\n200\n")
	write(t, filepath.Join(pod, "def", "cgroup.procs"), "500\n")
	m := New(root)

	abc, err := m.ContainerCgroupPath(pod, "containerd://abc")
	if err != nil {
		t.Fatal(err)
	}
	def, err := m.ContainerCgroupPath(pod, "def")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ContainerCgroupPath(pod, "containerd://zzz"); !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}

	pids, err := m.Procs(abc, def, filepath.Join(pod, "gone"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{200, 300, 400, 500}; !reflect.DeepEqual(pids, want) {
		t.Errorf("got %v, want %v (sandbox excluded, deduplicated, sorted)", pids, want)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestFreezeThaw(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "cgroup.events"), "populated 1\nfrozen 0\n")
	write(t, filepath.Join(dir, "cgroup.freeze"), "0")
	mgr := New(t.TempDir())

	// The kernel updates cgroup.events after cgroup.freeze is written.
	go func() {
		for range 1000 {
			data, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze"))
			if err == nil && string(data) == "1" {
				if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated 1\nfrozen 1\n"), 0o600); err != nil {
					t.Error(err)
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mgr.Freeze(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if frozen, err := mgr.Frozen(dir); err != nil || !frozen {
		t.Fatalf("not frozen (err: %v)", err)
	}
	// Already frozen: no write, no wait.
	if err := mgr.Freeze(ctx, dir); err != nil {
		t.Fatal(err)
	}

	// Thaw that never completes stops at the context deadline.
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if err := mgr.Thaw(short, dir); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("want DeadlineExceeded, got %v", err)
	}
	if data := read(t, filepath.Join(dir, "cgroup.freeze")); data != "0" {
		t.Errorf("thaw wrote %q", data)
	}

	write(t, filepath.Join(dir, "cgroup.events"), "populated 1\nfrozen 0\n")
	if err := mgr.Thaw(ctx, dir); err != nil {
		t.Fatal(err)
	}

	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := mgr.Freeze(cancelled, dir); !errors.Is(err, context.Canceled) {
		t.Errorf("want Canceled, got %v", err)
	}
}

func TestKill(t *testing.T) {
	t.Run("cgroup.kill", func(t *testing.T) {
		dir := t.TempDir()
		write(t, filepath.Join(dir, "cgroup.kill"), "")
		m := New(t.TempDir())
		m.kill = func(int, syscall.Signal) error { t.Fatal("must not signal"); return nil }
		if err := m.Kill(dir); err != nil {
			t.Fatal(err)
		}
		if data := read(t, filepath.Join(dir, "cgroup.kill")); data != "1" {
			t.Errorf("cgroup.kill = %q", data)
		}
	})
	t.Run("fallback SIGKILL", func(t *testing.T) {
		dir := t.TempDir()
		write(t, filepath.Join(dir, "cgroup.procs"), "7\n8\n")
		write(t, filepath.Join(dir, "c", "cgroup.procs"), "9\n")
		m := New(t.TempDir())
		var got []int
		m.kill = func(pid int, sig syscall.Signal) error {
			if sig != syscall.SIGKILL {
				t.Errorf("signal %v", sig)
			}
			got = append(got, pid)
			if pid == 8 {
				return syscall.ESRCH // already gone is fine
			}
			return nil
		}
		if err := m.Kill(dir); err != nil {
			t.Fatal(err)
		}
		if want := []int{7, 8, 9}; !reflect.DeepEqual(got, want) {
			t.Errorf("signalled %v, want %v", got, want)
		}
	})
	t.Run("missing cgroup", func(t *testing.T) {
		if err := New(t.TempDir()).Kill(filepath.Join(t.TempDir(), "gone")); err == nil {
			t.Error("want an error")
		}
	})
}

func TestMemory(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "memory.current"), "1234\n")
	write(t, filepath.Join(dir, "memory.max"), "5000\n")
	write(t, filepath.Join(dir, "memory.stat"), "anon 100\nfile 900\nkernel 20\nshmem 3\nbogus x\n")
	m := New(t.TempDir())

	if v, err := m.MemoryCurrent(dir); err != nil || v != 1234 {
		t.Errorf("MemoryCurrent = %d, %v", v, err)
	}
	if v, unlimited, err := m.MemoryMax(dir); err != nil || unlimited || v != 5000 {
		t.Errorf("MemoryMax = %d, %v, %v", v, unlimited, err)
	}
	write(t, filepath.Join(dir, "memory.max"), "max\n")
	if _, unlimited, err := m.MemoryMax(dir); err != nil || !unlimited {
		t.Errorf("MemoryMax max: unlimited=%v err=%v", unlimited, err)
	}
	stat, err := m.MemoryStat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := HostBytes(stat); got != 123 {
		t.Errorf("HostBytes = %d, want 123", got)
	}
	if got := HostBytes(map[string]int64{"anon": 1, "shmem": 2, "slab": 4, "kernel_stack": 8}); got != 15 {
		t.Errorf("HostBytes without kernel = %d, want 15", got)
	}
}

func TestIsV2(t *testing.T) {
	root := t.TempDir()
	if New(root).IsV2() {
		t.Error("empty dir is not v2")
	}
	write(t, filepath.Join(root, "cgroup.controllers"), "cpu memory\n")
	if !New(root).IsV2() {
		t.Error("want v2")
	}
}
