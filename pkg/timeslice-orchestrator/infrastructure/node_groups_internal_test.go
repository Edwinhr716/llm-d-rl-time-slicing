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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	tgG   = "ns1.rc-1.trainers"
	tgG1  = "ns1.rc-1.g1"
	tgG2  = "ns1.rc-1.g2"
	tgG63 = "a-namespace-of-some-length.a-raycluster-name-of-length.trainers"
)

// nodeGroupCase is one set of node labels and the group each mode must find.
type nodeGroupCase struct {
	name                 string
	labels               map[string]string
	prefix, ns, eitherOf string
}

var nodeGroupCases = []nodeGroupCase{
	{"C1 prefix label", map[string]string{NodeLabelPrefix + tgG: "true"}, tgG, "", tgG},
	{"C2 ns labels", map[string]string{DonorLabelKey: "true", NodeGroupLabelKey: tgG}, "", tgG, tgG},
	{
		"C3 both forms, same group",
		map[string]string{NodeLabelPrefix + tgG: "true", DonorLabelKey: "true", NodeGroupLabelKey: tgG},
		tgG, tgG, tgG,
	},
	{
		"C4 both forms, different groups",
		map[string]string{NodeLabelPrefix + tgG1: "true", DonorLabelKey: "true", NodeGroupLabelKey: tgG2},
		tgG1, tgG2, "",
	},
	{"C5 group without donor", map[string]string{NodeGroupLabelKey: tgG}, "", "", ""},
	{"C6 donor without group", map[string]string{DonorLabelKey: "true"}, "", "", ""},
	{"C7 prefix label false", map[string]string{NodeLabelPrefix + tgG: "false"}, "", "", ""},
	{
		"C8 two prefix groups",
		map[string]string{NodeLabelPrefix + tgG1: "true", NodeLabelPrefix + tgG2: "true"},
		"", "", "",
	},
	{"C9 no labels", map[string]string{}, "", "", ""},
	{"C10 63-char value", map[string]string{DonorLabelKey: "true", NodeGroupLabelKey: tgG63}, "", tgG63, tgG63},
	{"donor false", map[string]string{DonorLabelKey: "false", NodeGroupLabelKey: tgG}, "", "", ""},
	{"empty prefix suffix", map[string]string{NodeLabelPrefix: "true"}, "", "", ""},
	{
		"either: ns group plus two prefix groups",
		map[string]string{
			NodeLabelPrefix + tgG1: "true", NodeLabelPrefix + tgG2: "true",
			DonorLabelKey: "true", NodeGroupLabelKey: tgG1,
		},
		"", tgG1, "",
	},
}

func (c nodeGroupCase) want(mode string) string {
	switch mode {
	case NodeGroupLabelsNS:
		return c.ns
	case NodeGroupLabelsEither:
		return c.eitherOf
	}
	return c.prefix
}

// checkNodeGroupCases checks, for every case, that GroupsFromNodeLabels finds
// the expected group, NodeInGroup agrees with it, and both lookup paths of a
// running orchestrator (getNodesForGroup and getGroupsFromNode) agree too.
func checkNodeGroupCases(t *testing.T, mode string) {
	t.Helper()
	candidates := []string{tgG, tgG1, tgG2, tgG63}
	for _, tc := range nodeGroupCases {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.want(mode)
			got := GroupsFromNodeLabels(mode, tc.labels)
			if strings.Join(got, ",") != want {
				t.Fatalf("GroupsFromNodeLabels(%s) = %v, want %q", mode, got, want)
			}

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host", Labels: tc.labels}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			orch, err := newTestOrchestrator(ctx, fake.NewClientset(node), mode)
			if err != nil {
				t.Fatal(err)
			}
			if fromNode := strings.Join(orch.getGroupsFromNode(node), ","); fromNode != want {
				t.Errorf("getGroupsFromNode = %q, want %q", fromNode, want)
			}
			for _, g := range candidates {
				in := g == want
				if NodeInGroup(mode, tc.labels, g) != in {
					t.Errorf("NodeInGroup(%s) = %v, want %v", g, !in, in)
				}
				nodes, err := orch.getNodesForGroup(g)
				if err != nil {
					t.Fatal(err)
				}
				if (len(nodes) == 1) != in {
					t.Errorf("getNodesForGroup(%s) = %v, want host listed: %v", g, nodes, in)
				}
			}
		})
	}
}

func TestNodeGroupLabels_Prefix_Cases(t *testing.T) { checkNodeGroupCases(t, NodeGroupLabelsPrefix) }

func TestNodeGroupLabels_NS_Cases(t *testing.T) { checkNodeGroupCases(t, NodeGroupLabelsNS) }

func TestNodeGroupLabels_Either_Cases(t *testing.T) { checkNodeGroupCases(t, NodeGroupLabelsEither) }

// Case C7 on the base: getGroupsFromNode enqueued G for group.timeslice.io/G=false
// while getNodesForGroup did not list the node. Both now say "not a member".
func TestNodeGroupLabels_Prefix_C7FalseValueIsNotMember(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "host", Labels: map[string]string{NodeLabelPrefix + tgG: "false"},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orch, err := newTestOrchestrator(ctx, fake.NewClientset(node), "")
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := orch.getNodesForGroup(tgG)
	if err != nil {
		t.Fatal(err)
	}
	if groups := orch.getGroupsFromNode(node); len(groups) != 0 || len(nodes) != 0 {
		t.Fatalf("getGroupsFromNode = %v, getNodesForGroup = %v; want both empty", groups, nodes)
	}
}

// Under ns the node key equals the pod key, so one group id finds the node and the pods.
func TestNodeGroupLabels_NS_PodAndNodeShareKey(t *testing.T) {
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
	orch, err := newTestOrchestrator(ctx, fake.NewClientset(node, pod), NodeGroupLabelsNS)
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

// The "group nodes" line is logged when the node set changes, and only then.
func TestNodeGroupLabels_Either_GroupNodesLoggedOnChange(t *testing.T) {
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
	orch, err := newTestOrchestrator(ctx, cs, NodeGroupLabelsEither)
	if err != nil {
		t.Fatal(err)
	}
	const added = `msg="group nodes" group=` + tgG + ` nodes=host mode=either`
	const removed = `msg="group nodes" group=` + tgG + ` nodes="" mode=either`

	for range 2 {
		if err := orch.ObserveGroupState(ctx, tgG); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), added); n != 1 {
		t.Fatalf("want 1 %q line after two observations, got %d in:\n%s", added, n, buf.String())
	}

	node.Labels = map[string]string{}
	if _, err := cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		n, err := orch.nodeLister.Get("host")
		return err == nil && len(n.Labels) == 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.ObserveGroupState(ctx, tgG); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), removed) {
		t.Fatalf("want %q after relabel, got:\n%s", removed, buf.String())
	}
}

func TestNodeGroupLabels_ValidateAndNodeSelector(t *testing.T) {
	for mode, sel := range map[string]string{
		NodeGroupLabelsPrefix: "",
		NodeGroupLabelsNS:     "timeslice.io/donor=true",
		NodeGroupLabelsEither: "",
	} {
		if err := ValidateNodeGroupLabels(mode); err != nil {
			t.Errorf("ValidateNodeGroupLabels(%s): %v", mode, err)
		}
		if got := RecommendedNodeSelector(mode); got != sel {
			t.Errorf("RecommendedNodeSelector(%s) = %q, want %q", mode, got, sel)
		}
		if _, err := labels.Parse(sel); err != nil {
			t.Errorf("selector %q does not parse: %v", sel, err)
		}
	}
	if ValidateNodeGroupLabels("") == nil || ValidateNodeGroupLabels("both") == nil {
		t.Error("want an error for an empty or unknown mode")
	}
	if GroupsFromNodeLabels("both", map[string]string{NodeLabelPrefix + tgG: "true"}) != nil {
		t.Error("an unknown mode must yield no group")
	}
}
