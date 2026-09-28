package group_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/group"
)

// lbl builds a label map from key, value pairs.
func lbl(kv ...string) map[string]string {
	out := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

func TestFromNodeLabels(t *testing.T) {
	const (
		pfx   = group.NodeLabelPrefix
		donor = "timeslice.io/donor"
		nsKey = "timeslice.io/group"
	)
	long := strings.Repeat("a", 63)
	cases := []struct {
		name   string
		labels map[string]string
		want   string
		reason group.Reason
	}{
		{"one group", lbl(pfx+"g", "true", "other", "x"), "g", group.ReasonOK},
		{"north-star keys are not this option's form", lbl(donor, "true", nsKey, "g"), "", group.ReasonNoLabel},
		{"both forms: only the prefix counts", lbl(pfx+"g", "true", donor, "true", nsKey, "g"), "g", group.ReasonOK},
		{"conflicting forms: prefix wins", lbl(pfx+"g1", "true", donor, "true", nsKey, "g2"), "g1", group.ReasonOK},
		{"value false is not a member", lbl(pfx+"g", "false"), "", group.ReasonPartial},
		{"empty value is not a member", lbl(pfx+"g", ""), "", group.ReasonPartial},
		{"value True is not a member", lbl(pfx+"g", "True"), "", group.ReasonPartial},
		{"no name is not a member", lbl(pfx, "true"), "", group.ReasonPartial},
		{"partial next to a full label", lbl(pfx+"g1", "true", pfx+"g2", "false"), "g1", group.ReasonOK},
		{"two groups refused", lbl(pfx+"g1", "true", pfx+"g2", "true"), "", group.ReasonMultipleGroups},
		{"no labels", nil, "", group.ReasonNoLabel},
		{"63-char group name", lbl(pfx+long, "true"), long, group.ReasonOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			groups, reason := group.FromNodeLabels("", c.labels)
			if group.One(groups) != c.want || reason != c.reason {
				t.Errorf("got %v %q, want %q %q", groups, reason, c.want, c.reason)
			}
			if len(groups) > 1 {
				t.Errorf("more than one group returned: %v", groups)
			}
			// source is ignored on this branch.
			if g2, r2 := group.FromNodeLabels("ns", c.labels); group.One(g2) != c.want || r2 != c.reason {
				t.Errorf("source changed the result: %v %q", g2, r2)
			}
		})
	}
}

func TestLine(t *testing.T) {
	ts := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	got := group.Line(ts, "h", []string{"g"}, group.ReasonOK)
	want := `time=2026-09-28T00:00:00Z level=INFO msg="group resolved" host=h groups=g source=prefix reason=ok`
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	if got := group.Line(ts, "h", nil, group.ReasonNoLabel); !strings.Contains(got, " groups= source=prefix reason=no-group-label") {
		t.Errorf("empty groups: %s", got)
	}
}

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
	return strings.Split(strings.TrimSpace(s.b.String()), "\n")
}

func host(labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "real", Labels: labels}}
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func drain(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func startResolver(t *testing.T, objs ...runtime.Object) (*group.Resolver, *fake.Clientset, *syncBuf, *record.FakeRecorder) {
	t.Helper()
	client := fake.NewClientset(objs...)
	out := &syncBuf{}
	rec := record.NewFakeRecorder(100)
	res := &group.Resolver{Host: "real", VirtualNode: "vk-x", Recorder: rec, Out: out}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := res.Start(ctx, client); err != nil {
		t.Fatal(err)
	}
	return res, client, out, rec
}

func relabel(t *testing.T, client *fake.Clientset, labels map[string]string) {
	t.Helper()
	n, err := client.CoreV1().Nodes().Get(context.Background(), "real", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n.Labels = labels
	if _, err := client.CoreV1().Nodes().Update(context.Background(), n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestResolverFollowsHostLabels(t *testing.T) {
	res, client, out, rec := startResolver(t,
		host(map[string]string{"group.timeslice.io/g1": "true"}),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other", Labels: map[string]string{"group.timeslice.io/zz": "true"}}})

	if g, reason := res.Group(); g != "g1" || reason != group.ReasonOK {
		t.Fatalf("start: %q %q", g, reason)
	}
	if l := out.lines(); len(l) != 1 || !strings.Contains(l[0], `msg="group resolved" host=real groups=g1 source=prefix reason=ok`) {
		t.Fatalf("start line: %v", l)
	}
	if ev := drain(rec); len(ev) != 0 {
		t.Errorf("no event expected while resolved: %v", ev)
	}

	// Era change: relabelled live to another group.
	relabel(t, client, map[string]string{"group.timeslice.io/g2": "true"})
	eventually(t, "g2", func() bool { g, _ := res.Group(); return g == "g2" })

	// A node in two groups is refused and reported.
	relabel(t, client, map[string]string{"group.timeslice.io/g2": "true", "group.timeslice.io/g3": "true"})
	eventually(t, "refusal", func() bool { _, reason := res.Group(); return reason == group.ReasonMultipleGroups })
	if g, _ := res.Group(); g != "" {
		t.Errorf("two groups resolved to %q", g)
	}
	eventually(t, "GroupUnresolved", func() bool {
		for _, e := range drain(rec) {
			if strings.Contains(e, group.EventGroupUnresolved) && strings.Contains(e, string(group.ReasonMultipleGroups)) {
				return true
			}
		}
		return false
	})

	// Era end: labels removed.
	relabel(t, client, nil)
	eventually(t, "unlabelled", func() bool { _, reason := res.Group(); return reason == group.ReasonNoLabel })

	l := out.lines()
	if len(l) != 4 || !strings.HasSuffix(l[3], " groups= source=prefix reason=no-group-label") {
		t.Errorf("want one line per change, got %d: %v", len(l), l)
	}
	for _, s := range l {
		if strings.Contains(s, "zz") {
			t.Errorf("resolver read another node: %s", s)
		}
	}
}

func TestResolverStatusOnlyUpdateIsQuiet(t *testing.T) {
	res, client, out, _ := startResolver(t, host(map[string]string{"group.timeslice.io/g": "true"}))
	n, err := client.CoreV1().Nodes().Get(context.Background(), "real", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n.Status.Phase = corev1.NodeRunning
	if _, err := client.CoreV1().Nodes().UpdateStatus(context.Background(), n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	relabel(t, client, map[string]string{"group.timeslice.io/g": "true", "x": "y"})
	// Events arrive in order, so once the relabel's line is in, the status update was handled.
	eventually(t, "second line", func() bool { return len(out.lines()) >= 2 })
	if l := out.lines(); len(l) != 2 {
		t.Errorf("status-only update logged a line: %v", l)
	}
	if g, _ := res.Group(); g != "g" {
		t.Errorf("group %q", g)
	}
}

func TestResolverHostMissingFailsClosed(t *testing.T) {
	res, _, out, rec := startResolver(t)
	if g, reason := res.Group(); g != "" || reason != group.ReasonHostNotFound {
		t.Errorf("got %q %q", g, reason)
	}
	if l := out.lines(); len(l) != 1 || !strings.Contains(l[0], "groups= source=prefix reason=host-node-not-found") {
		t.Errorf("line: %v", l)
	}
	if ev := drain(rec); len(ev) != 1 || !strings.Contains(ev[0], "Warning "+group.EventGroupUnresolved) {
		t.Errorf("events: %v", ev)
	}
}

func TestGroupBeforeStartIsUnresolved(t *testing.T) {
	var res group.Resolver
	if g, _ := res.Group(); g != "" {
		t.Errorf("got %q before start", g)
	}
}
