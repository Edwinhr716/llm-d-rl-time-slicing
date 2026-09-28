package group_test

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/group"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := strings.Split(strings.TrimSpace(s.b.String()), "\n")
	if len(out) == 1 && out[0] == "" {
		return nil
	}
	return out
}

var lineRE = regexp.MustCompile(`^time=\S+ level=INFO msg="group resolved" host=(\S+) groups=(\S*) reason=(\S+)$`)

func TestResolverLogsAndNotifies(t *testing.T) {
	var out syncBuf
	r := group.NewResolver("host-1", &out)
	var got []group.Result
	r.OnChange = func(res group.Result) { got = append(got, res) }

	if _, ok := r.Current().Group(); ok {
		t.Fatal("no group before the host node is seen")
	}
	r.Observe(map[string]string{group.LabelDonor: "true", group.LabelGroup: full})
	// No change: silent.
	r.Observe(map[string]string{group.LabelDonor: "true", group.LabelGroup: full})
	// A label change with the same result: logged, no OnChange.
	r.Observe(map[string]string{group.LabelDonor: "true", group.LabelGroup: full, "unrelated": "1"})
	// Donor removed.
	r.Observe(map[string]string{group.LabelGroup: full})
	r.Gone()

	lines := out.lines()
	want := [][2]string{
		{full, group.ReasonOK},
		{full, group.ReasonOK},
		{"", group.ReasonGroupWithoutDonor},
		{"", group.ReasonHostNodeGone},
	}
	if len(lines) != len(want) {
		t.Fatalf("want %d lines, got %d:\n%s", len(want), len(lines), strings.Join(lines, "\n"))
	}
	for i, l := range lines {
		m := lineRE.FindStringSubmatch(l)
		if m == nil {
			t.Fatalf("line %d does not match: %s", i, l)
		}
		if m[1] != "host-1" || m[2] != want[i][0] || m[3] != want[i][1] {
			t.Errorf("line %d: %s", i, l)
		}
	}
	// An empty set prints as "groups= " so `groups=\([^ ]*\)` extracts an empty string.
	if !strings.Contains(lines[2], " groups= reason=") {
		t.Errorf("empty groups format: %s", lines[2])
	}
	if len(got) != 3 {
		t.Fatalf("OnChange calls: %+v", got)
	}
	if g, ok := got[0].Group(); !ok || g != full {
		t.Errorf("first: %+v", got[0])
	}
	if _, ok := got[1].Group(); ok || got[1].Reason != group.ReasonGroupWithoutDonor {
		t.Errorf("second: %+v", got[1])
	}
}

func TestResolverWatchesHostNode(t *testing.T) {
	host := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "host-1",
		Labels: map[string]string{group.LabelDonor: "true", group.LabelGroup: full},
	}}
	node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "host-2",
		Labels: map[string]string{group.LabelDonor: "true", group.LabelGroup: "someone.else.trainers"},
	}}
	client := fake.NewClientset(host, node2)
	var out syncBuf
	resolver := group.NewResolver("host-1", &out)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := resolver.Start(ctx, client); err != nil {
		t.Fatal(err)
	}
	if g, ok := resolver.Current().Group(); !ok || g != full {
		t.Fatalf("after start: %+v", resolver.Current())
	}

	relabeled := node2.Labels[group.LabelGroup] + "-2"
	// Relabel live (case C11), then remove the labels (case C12).
	for _, step := range []struct {
		labels map[string]string
		want   string
	}{
		{map[string]string{group.LabelDonor: "true", group.LabelGroup: relabeled}, relabeled},
		{map[string]string{}, ""},
	} {
		n := host.DeepCopy()
		n.Labels = step.labels
		if _, err := client.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			g, _ := resolver.Current().Group()
			if g == step.want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("want %q, still %+v", step.want, resolver.Current())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, l := range out.lines() {
		if !strings.Contains(l, "host=host-1 ") {
			t.Errorf("line about another node: %s", l)
		}
	}
}

func TestResolverMissingHostNode(t *testing.T) {
	var out syncBuf
	r := group.NewResolver("absent", &out)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx, fake.NewClientset()); err != nil {
		t.Fatal(err)
	}
	if res := r.Current(); res.Reason != group.ReasonHostNodeGone {
		t.Errorf("%+v", res)
	}
	if l := out.lines(); len(l) != 1 || !strings.Contains(l[0], "reason="+group.ReasonHostNodeGone) {
		t.Errorf("lines: %v", l)
	}
}
