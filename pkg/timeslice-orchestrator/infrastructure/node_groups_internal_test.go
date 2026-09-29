// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infrastructure

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	tgG   = "ns1.rc-1.trainers" // <namespace>.<job-id>.<worker group>
	tgG1  = "ns1.rc-1.g1"
	tgG2  = "ns1.rc-1.g2"
	tgG63 = "a-namespace-of-some-length.a-raycluster-name-of-length.trainers"
	tgG64 = tgG63 + "x"
)

// newTestOrchestrator returns an orchestrator over cs whose node and pod
// informers have synced. The informers stop when ctx is done.
func newTestOrchestrator(ctx context.Context, cs kubernetes.Interface) (*KubernetesOrchestrator, error) {
	factory := informers.NewSharedInformerFactory(cs, 0)
	k := NewKubernetesOrchestrator(
		factory.Core().V1().Nodes(),
		factory.Core().V1().Pods(),
		store.NewGroupStore(store.NewMemLockStore()),
		store.NewJobStore(),
		store.NewGRPCSnapshotAgentStore(0, 0),
	)
	factory.Start(ctx.Done())
	if err := k.Init(ctx); err != nil {
		return nil, err
	}
	return k, nil
}

// TestNodeGroups_Cases checks, for every set of node labels, that
// GroupsFromNodeLabels finds the expected group (or none), NodeInGroup agrees,
// and both lookup paths of a running orchestrator (getGroupsFromNode, used by
// the node watch, and getNodesForGroup, used by the reconcile) agree too.
func TestNodeGroups_Cases(t *testing.T) {
	type labelSet = map[string]string
	cases := []struct {
		name   string
		labels labelSet
		want   string // "" = in no group
	}{
		{"C1 prefix only", labelSet{NodeLabelPrefix + tgG: "true"}, tgG},
		{"C2 donor pair", labelSet{DonorLabelKey: "true", NodeGroupLabelKey: tgG}, tgG},
		{"C3 both forms, same group", labelSet{NodeLabelPrefix + tgG: "true", DonorLabelKey: "true", NodeGroupLabelKey: tgG}, tgG},
		{
			"C4 both forms, different groups",
			labelSet{NodeLabelPrefix + tgG1: "true", DonorLabelKey: "true", NodeGroupLabelKey: tgG2},
			"",
		},
		{"C5 group without donor", labelSet{NodeGroupLabelKey: tgG}, ""},
		{"C6 donor without group", labelSet{DonorLabelKey: "true"}, ""},
		// Before, the node watch enqueued G for this node while the group lookup did not list it.
		{"C7 prefix label false", labelSet{NodeLabelPrefix + tgG: "false"}, ""},
		{"C8 two prefix groups", labelSet{NodeLabelPrefix + tgG1: "true", NodeLabelPrefix + tgG2: "true"}, ""},
		{"C9 no labels", labelSet{}, ""},
		{"C10 63-character value", labelSet{DonorLabelKey: "true", NodeGroupLabelKey: tgG63}, tgG63},
		{"64-character value refused", labelSet{DonorLabelKey: "true", NodeGroupLabelKey: tgG64}, ""},
		{"donor not true", labelSet{DonorLabelKey: "false", NodeGroupLabelKey: tgG}, ""},
		{"empty group value", labelSet{DonorLabelKey: "true", NodeGroupLabelKey: ""}, ""},
		{"empty prefix suffix", labelSet{NodeLabelPrefix: "true"}, ""},
		{"group without donor next to prefix", labelSet{NodeLabelPrefix + tgG: "true", NodeGroupLabelKey: tgG}, ""},
		{"donor without group next to prefix", labelSet{NodeLabelPrefix + tgG: "true", DonorLabelKey: "true"}, ""},
		{
			"donor not true next to prefix",
			labelSet{NodeLabelPrefix + tgG: "true", DonorLabelKey: "false", NodeGroupLabelKey: tgG},
			"",
		},
		{
			"prefix false next to donor pair",
			labelSet{NodeLabelPrefix + tgG1: "false", DonorLabelKey: "true", NodeGroupLabelKey: tgG},
			tgG,
		},
		{
			"donor pair plus two prefix groups",
			labelSet{NodeLabelPrefix + tgG1: "true", NodeLabelPrefix + tgG2: "true", DonorLabelKey: "true", NodeGroupLabelKey: tgG1},
			"",
		},
		{"unrelated labels", labelSet{"kubernetes.io/hostname": "host"}, ""},
	}
	candidates := []string{tgG, tgG1, tgG2, tgG63, tgG64}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := strings.Join(GroupsFromNodeLabels(tc.labels), ","); got != tc.want {
				t.Fatalf("GroupsFromNodeLabels = %q, want %q", got, tc.want)
			}

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host", Labels: tc.labels}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			orch, err := newTestOrchestrator(ctx, fake.NewClientset(node))
			if err != nil {
				t.Fatal(err)
			}
			if fromNode := strings.Join(orch.getGroupsFromNode(node), ","); fromNode != tc.want {
				t.Errorf("getGroupsFromNode = %q, want %q", fromNode, tc.want)
			}
			for _, group := range candidates {
				in := group == tc.want
				if NodeInGroup(tc.labels, group) != in {
					t.Errorf("NodeInGroup(%s) = %v, want %v", group, !in, in)
				}
				nodes, err := orch.getNodesForGroup(group)
				if err != nil {
					t.Fatal(err)
				}
				if (len(nodes) == 1) != in {
					t.Errorf("getNodesForGroup(%s) = %v, want host listed: %v", group, nodes, in)
				}
			}
		})
	}
}

// With the donor pair the node key equals the pod key, so one group id finds
// the node and the pods.
func TestNodeGroups_PodAndNodeShareKey(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "host", Labels: map[string]string{DonorLabelKey: "true", NodeGroupLabelKey: tgG},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "trainer-0", Namespace: "ns1", UID: "uid-1",
			Labels: map[string]string{PodLabelKey: tgG, JobLabelKey: "rc-1"},
		},
		Spec: corev1.PodSpec{NodeName: "host"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orch, err := newTestOrchestrator(ctx, fake.NewClientset(node, pod))
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.ObserveGroupState(ctx, tgG); err != nil {
		t.Fatal(err)
	}
	g, err := orch.groupStore.Get(ctx, tgG)
	if err != nil {
		t.Fatal(err)
	}
	if nodes := g.Status().Nodes(); len(nodes) != 1 || nodes[0] != "host" {
		t.Errorf("group nodes = %v, want [host]", nodes)
	}
	if _, err := orch.jobStore.Get(ctx, tgG, "rc-1"); err != nil {
		t.Errorf("job rc-1: %v", err)
	}
}

// A live relabel that makes the two forms disagree takes the node out of its
// group (fail closed), and the "group nodes" line is logged when the node set
// changes, and only then.
func TestNodeGroups_ConflictingRelabelLeavesGroup(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "host", Labels: map[string]string{DonorLabelKey: "true", NodeGroupLabelKey: tgG},
	}}
	cs := fake.NewClientset(node)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orch, err := newTestOrchestrator(ctx, cs)
	if err != nil {
		t.Fatal(err)
	}
	const added = `msg="group nodes" group=` + tgG + ` nodes=host`
	const removed = `msg="group nodes" group=` + tgG + ` nodes=""`

	for range 2 {
		if err := orch.ObserveGroupState(ctx, tgG); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), added); n != 1 {
		t.Fatalf("want 1 %q line after two observations, got %d in:\n%s", added, n, buf.String())
	}

	node.Labels = map[string]string{DonorLabelKey: "true", NodeGroupLabelKey: tgG, NodeLabelPrefix + tgG1: "true"}
	if _, err := cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		n, err := orch.nodeLister.Get("host")
		return err == nil && len(n.Labels) == 3, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.ObserveGroupState(ctx, tgG); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), removed) {
		t.Fatalf("want %q after the conflicting relabel, got:\n%s", removed, buf.String())
	}
	if nodes, err := orch.getNodesForGroup(tgG1); err != nil || len(nodes) != 0 {
		t.Fatalf("getNodesForGroup(%s) = %v, %v; want no node", tgG1, nodes, err)
	}
}
