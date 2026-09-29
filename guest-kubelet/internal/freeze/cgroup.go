package freeze

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Cgroup freezes a pod through the cgroup v2 freezer: it writes cgroup.freeze on the pod-level
// cgroup and waits until cgroup.events reports the new "frozen" value. The whole pod is frozen,
// sandbox included, as in the Q2 and Q8 measurements. The guest kubelet needs the host's
// cgroup tree mounted read-write (a hostPath) and a privileged container for this.
type Cgroup struct {
	// Root is where the host's cgroup v2 hierarchy is mounted in this container, for example
	// /host/cgroup. (/sys/fs/cgroup inside a container is usually its own cgroup namespace.)
	Root string
	// Poll is how often cgroup.events is read while waiting. Default 5 ms.
	Poll time.Duration
}

var _ Backend = (*Cgroup)(nil)

// ErrNoCgroup means no pod-level cgroup was found for the pod on this host.
var ErrNoCgroup = errors.New("pod cgroup not found")

// Suspend freezes the pod cgroup and waits for "frozen 1".
func (c *Cgroup) Suspend(ctx context.Context, pod *corev1.Pod, _ int64) error {
	return c.set(ctx, pod, true)
}

// Resume thaws the pod cgroup and waits for "frozen 0".
func (c *Cgroup) Resume(ctx context.Context, pod *corev1.Pod, _ int64) error {
	return c.set(ctx, pod, false)
}

// Frozen reads "frozen" from the pod cgroup's cgroup.events.
func (c *Cgroup) Frozen(pod *corev1.Pod) (bool, error) {
	dir, err := c.PodDir(pod)
	if err != nil {
		return false, err
	}
	return readFrozen(dir)
}

// Procs counts the processes in the pod cgroup and every cgroup below it (cgroup.procs). The
// ids themselves are not returned: read from another PID namespace they show as 0.
func (c *Cgroup) Procs(pod *corev1.Pod) (int, error) {
	dir, err := c.PodDir(pod)
	if err != nil {
		return 0, err
	}
	n := 0
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || d.Name() != "cgroup.procs" {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		n += len(strings.Fields(string(b)))
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("count processes under %s: %w", dir, err)
	}
	return n, nil
}

// PodDir returns the pod-level cgroup directory of pod, trying the pod's QoS class first.
// Both kubelet cgroup drivers are covered: systemd (GKE COS with cgroup v2:
// kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid_with_underscores>.slice)
// and cgroupfs (kubepods/burstable/pod<uid>).
func (c *Cgroup) PodDir(pod *corev1.Pod) (string, error) {
	for _, rel := range Candidates(pod.UID, pod.Status.QOSClass) {
		d := filepath.Join(c.Root, rel)
		if st, err := os.Stat(filepath.Join(d, "cgroup.freeze")); err == nil && !st.IsDir() {
			return d, nil
		}
	}
	return "", fmt.Errorf("%w: pod %s/%s uid %s under %s", ErrNoCgroup, pod.Namespace, pod.Name, pod.UID, c.Root)
}

// Candidates lists the pod-level cgroup paths (relative to the cgroup root) a kubelet may use
// for a pod, the given QoS class first.
func Candidates(uid types.UID, qos corev1.PodQOSClass) []string {
	order := []corev1.PodQOSClass{corev1.PodQOSBurstable, corev1.PodQOSBestEffort, corev1.PodQOSGuaranteed}
	if qos != "" {
		order = append([]corev1.PodQOSClass{qos}, order...)
	}
	u := string(uid)
	u2 := strings.ReplaceAll(u, "-", "_")
	var out []string
	seen := map[corev1.PodQOSClass]bool{}
	for _, q := range order {
		if seen[q] {
			continue
		}
		seen[q] = true
		if q == corev1.PodQOSGuaranteed {
			out = append(out,
				"kubepods.slice/kubepods-pod"+u2+".slice",
				"kubepods/pod"+u)
			continue
		}
		l := strings.ToLower(string(q))
		out = append(out,
			"kubepods.slice/kubepods-"+l+".slice/kubepods-"+l+"-pod"+u2+".slice",
			"kubepods/"+l+"/pod"+u)
	}
	return out
}

func (c *Cgroup) set(ctx context.Context, pod *corev1.Pod, frozen bool) error {
	dir, err := c.PodDir(pod)
	if err != nil {
		return err
	}
	val := "0"
	if frozen {
		val = "1"
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte(val), 0); err != nil {
		return fmt.Errorf("write %s/cgroup.freeze: %w", dir, err)
	}
	return c.wait(ctx, dir, frozen)
}

// wait polls cgroup.events until "frozen" equals want. A freeze completes only when every
// task in the subtree has stopped, which is why the write alone is not enough.
func (c *Cgroup) wait(ctx context.Context, dir string, want bool) error {
	poll := c.Poll
	if poll <= 0 {
		poll = 5 * time.Millisecond
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		got, err := readFrozen(dir)
		if err != nil {
			return err
		}
		if got == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for frozen=%t in %s: %w", want, dir, ctx.Err())
		case <-t.C:
		}
	}
}

func readFrozen(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if err != nil {
		return false, err
	}
	s := bufio.NewScanner(bytes.NewReader(b))
	for s.Scan() {
		f := strings.Fields(s.Text())
		if len(f) == 2 && f[0] == "frozen" {
			return f[1] == "1", nil
		}
	}
	return false, fmt.Errorf("no frozen key in %s/cgroup.events", dir)
}
