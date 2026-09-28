package orchestrator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/orchestrator"
)

func TestGroupFromLabels(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   string
		err    bool
	}{
		{name: "one group", labels: map[string]string{"group.timeslice.io/rl": "true", "other": "x"}, want: "rl"},
		{name: "none", labels: map[string]string{"timeslice.io/virtual-node": "true"}, err: true},
		{name: "not true", labels: map[string]string{"group.timeslice.io/rl": "false"}, err: true},
		{name: "two groups", labels: map[string]string{"group.timeslice.io/a": "true", "group.timeslice.io/b": "true"}, err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := orchestrator.GroupFromLabels(tc.labels)
			if tc.err {
				if !errors.Is(err, orchestrator.ErrNoGroup) {
					t.Fatalf("want ErrNoGroup, got %q, %v", got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestNodeLabelGroup(t *testing.T) {
	client := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "real", Labels: map[string]string{"group.timeslice.io/rl": "true"},
	}})
	got, err := orchestrator.NodeLabelGroup(client, "real")(context.Background())
	if err != nil || got != "rl" {
		t.Fatalf("got %q, %v; want rl", got, err)
	}
	if _, err := orchestrator.NodeLabelGroup(client, "missing")(context.Background()); err == nil {
		t.Fatal("want an error for a missing node")
	}
}

func TestFakeFreezerHonoursDeadlineAndAnnotates(t *testing.T) {
	var got []string
	f := &orchestrator.FakeFreezer{
		SuspendDelay: time.Hour,
		ResumeDelay:  10 * time.Millisecond,
		Annotate: func(_ context.Context, _ *corev1.Pod, key, value string) error {
			got = append(got, key+"="+value)
			return nil
		},
	}
	m := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "g-m"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.Suspend(ctx, m, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded from a suspend that cannot finish, got %v", err)
	}
	if err := f.Resume(context.Background(), m, 2); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != orchestrator.AnnotationFakeFreezer+"="+orchestrator.FakeRunning {
		t.Fatalf("annotations written: %v", got)
	}
	if calls := f.Calls(); len(calls) != 2 || calls[0].Err == nil || calls[1].Epoch != 2 {
		t.Fatalf("calls: %+v", calls)
	}
}
