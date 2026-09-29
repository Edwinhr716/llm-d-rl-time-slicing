package freeze_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

const testUID = types.UID("0f1e2d3c-aaaa-bbbb-cccc-123456789abc")

func testPod(qos corev1.PodQOSClass) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "g-m", UID: testUID},
		Status:     corev1.PodStatus{QOSClass: qos},
	}
}

// fakeCgroup makes a pod cgroup directory and plays the kernel: whatever is written to
// cgroup.freeze shows up as "frozen N" in cgroup.events after delay.
func fakeCgroup(t *testing.T, root, rel string, delay time.Duration) string {
	t.Helper()
	dir := filepath.Join(root, rel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Write through a rename: the kernel's files never show a half-written state, and a
	// truncate-then-write would let the poller read an empty cgroup.events.
	write := func(name, s string) error {
		tmp := filepath.Join(dir, "."+name+".tmp")
		if err := os.WriteFile(tmp, []byte(s), 0o600); err != nil {
			return err
		}
		return os.Rename(tmp, filepath.Join(dir, name))
	}
	if err := write("cgroup.freeze", "0\n"); err != nil {
		t.Fatal(err)
	}
	if err := write("cgroup.events", "populated 1\nfrozen 0\n"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done }) // runs before TempDir's cleanup
	go func() {
		defer close(done)
		last := "0"
		for {
			b, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze"))
			if err == nil && len(b) > 0 && string(b[:1]) != last {
				last = string(b[:1])
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
				if err := write("cgroup.events", "populated 1\nfrozen "+last+"\n"); err != nil {
					t.Error(err)
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return dir
}

func TestCandidates_SystemdAndCgroupfs(t *testing.T) {
	got := freeze.Candidates(testUID, corev1.PodQOSBurstable)
	want0 := "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod0f1e2d3c_aaaa_bbbb_cccc_123456789abc.slice"
	if got[0] != want0 || got[1] != "kubepods/burstable/pod"+string(testUID) {
		t.Fatalf("first candidates = %v", got[:2])
	}
	if len(got) != 6 {
		t.Fatalf("want 6 candidates (3 QoS x 2 drivers), got %d: %v", len(got), got)
	}
	g := freeze.Candidates(testUID, corev1.PodQOSGuaranteed)
	if g[0] != "kubepods.slice/kubepods-pod0f1e2d3c_aaaa_bbbb_cccc_123456789abc.slice" {
		t.Fatalf("guaranteed first = %s", g[0])
	}
}

func TestCgroup_FreezeThaw(t *testing.T) {
	root := t.TempDir()
	dir := fakeCgroup(t, root, freeze.Candidates(testUID, corev1.PodQOSBurstable)[0], 20*time.Millisecond)
	c := &freeze.Cgroup{Root: root, Poll: time.Millisecond}
	pod := testPod(corev1.PodQOSBurstable)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Suspend(ctx, pod, 1); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze")); err != nil || string(b) != "1" {
		t.Fatalf("cgroup.freeze = %q, %v", b, err)
	}
	if f, err := c.Frozen(pod); err != nil || !f {
		t.Fatalf("Frozen = %v, %v", f, err)
	}
	if err := c.Resume(ctx, pod, 2); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Frozen(pod); err != nil || f {
		t.Fatalf("Frozen after resume = %v, %v", f, err)
	}
}

// The QoS class in status may be missing or wrong; every candidate is still tried.
func TestCgroup_FindsDirWhateverTheQoS(t *testing.T) {
	root := t.TempDir()
	fakeCgroup(t, root, "kubepods/besteffort/pod"+string(testUID), 0)
	c := &freeze.Cgroup{Root: root}
	d, err := c.PodDir(testPod(""))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(d) != "pod"+string(testUID) {
		t.Fatalf("dir = %s", d)
	}
}

func TestCgroup_NoCgroup(t *testing.T) {
	c := &freeze.Cgroup{Root: t.TempDir()}
	err := c.Suspend(context.Background(), testPod(corev1.PodQOSBurstable), 1)
	if !errors.Is(err, freeze.ErrNoCgroup) {
		t.Fatalf("err = %v, want ErrNoCgroup", err)
	}
}

// A freeze that never completes (a task stuck in the kernel) fails at the deadline.
func TestCgroup_FreezeTimesOut(t *testing.T) {
	root := t.TempDir()
	fakeCgroup(t, root, freeze.Candidates(testUID, corev1.PodQOSBurstable)[0], time.Hour)
	c := &freeze.Cgroup{Root: root, Poll: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Suspend(ctx, testPod(corev1.PodQOSBurstable), 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}
