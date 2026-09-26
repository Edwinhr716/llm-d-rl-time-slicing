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

package infrastructure_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestParseScope(t *testing.T) {
	tests := []struct {
		name         string
		namespaces   string
		nodeSelector string
		want         infrastructure.Scope
		wantAllNS    bool
		wantAllNodes bool
		wantErr      bool
	}{
		{name: "defaults watch everything", wantAllNS: true, wantAllNodes: true},
		{name: "blank entries ignored", namespaces: " , ,", nodeSelector: "  ", wantAllNS: true, wantAllNodes: true},
		{
			name:         "namespaces trimmed, deduplicated and sorted",
			namespaces:   " demo-b ,demo-a,demo-b",
			want:         infrastructure.Scope{Namespaces: []string{"demo-a", "demo-b"}},
			wantAllNodes: true,
		},
		{
			name:         "node selector canonical form",
			nodeSelector: " pool=demo ",
			want:         infrastructure.Scope{NodeSelector: "pool=demo"},
			wantAllNS:    true,
		},
		{
			name:         "set-based node selector",
			namespaces:   "demo",
			nodeSelector: "pool in (a,b),!excluded",
			want:         infrastructure.Scope{Namespaces: []string{"demo"}, NodeSelector: "!excluded,pool in (a,b)"},
		},
		{name: "invalid namespace", namespaces: "Demo_NS", wantErr: true},
		{name: "invalid node selector", nodeSelector: "pool==(", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := infrastructure.ParseScope(tc.namespaces, tc.nodeSelector)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseScope() = %+v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScope() error = %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParseScope() = %+v, want %+v", got, tc.want)
			}
			if got.AllNamespaces() != tc.wantAllNS {
				t.Errorf("AllNamespaces() = %v, want %v", got.AllNamespaces(), tc.wantAllNS)
			}
			if got.AllNodes() != tc.wantAllNodes {
				t.Errorf("AllNodes() = %v, want %v", got.AllNodes(), tc.wantAllNodes)
			}
		})
	}
}

// recordingQueue records the group IDs enqueued by the informer handlers.
type recordingQueue struct {
	mu    sync.Mutex
	added map[string]bool
}

func newRecordingQueue() *recordingQueue { return &recordingQueue{added: map[string]bool{}} }

func (q *recordingQueue) Add(g string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.added[g] = true
}
func (q *recordingQueue) AddRateLimited(g string) { q.Add(g) }

func (q *recordingQueue) Forget(string) {}

func (q *recordingQueue) Done(string) {}

func (q *recordingQueue) Get() (string, bool) { return "", true }

func (q *recordingQueue) ShutDown() {}

func (q *recordingQueue) has(g string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.added[g]
}

func testNode(name string, nodeLabels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: nodeLabels}}
}

func testPod(ns, name, group, job, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			UID:       types.UID("uid-" + ns + "-" + name),
			Labels:    map[string]string{infrastructure.PodLabelKey: group, infrastructure.JobLabelKey: job},
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

// startScoped builds the orchestrator the way main.go does for the given
// flag values, starts its informers and waits for them to sync. All objects
// must be passed up front: the fake clientset filters List by label selector
// but not Watch.
func startScoped(
	t *testing.T, ctx context.Context, watchNamespaces, nodeSelector string, objs ...runtime.Object,
) (*infrastructure.KubernetesOrchestrator, *store.JobStore, *store.GroupStore, *recordingQueue, *fake.Clientset) {
	t.Helper()
	scope, err := infrastructure.ParseScope(watchNamespaces, nodeSelector)
	if err != nil {
		t.Fatalf("ParseScope() error = %v", err)
	}
	clientset := fake.NewClientset(objs...)
	factories := scope.NewInformerFactories(clientset, 0)

	opts := make([]infrastructure.Option, 0, len(factories.Pods))
	for _, f := range factories.Pods[1:] {
		opts = append(opts, infrastructure.WithPodInformers(f.Core().V1().Pods()))
	}
	if !scope.AllNodes() {
		opts = append(opts, infrastructure.WithNodeScopedPods())
	}
	groupStore := store.NewGroupStore(store.NewMemLockStore())
	jobStore := store.NewJobStore()
	infraOrch := infrastructure.NewKubernetesOrchestrator(
		factories.Nodes.Core().V1().Nodes(), factories.Pods[0].Core().V1().Pods(),
		groupStore, jobStore, &fakeSnapshotAgentStore{}, opts...)

	queue := newRecordingQueue()
	if err := infraOrch.Start(ctx, queue); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	factories.Nodes.Start(ctx.Done())
	for _, f := range factories.Pods {
		f.Start(ctx.Done())
	}
	if err := infraOrch.Init(ctx); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return infraOrch, jobStore, groupStore, queue, clientset
}

func jobIDs(t *testing.T, ctx context.Context, js *store.JobStore, group string) []string {
	t.Helper()
	jobs, err := js.ListByGroup(ctx, group)
	if err != nil {
		t.Fatalf("ListByGroup(%s) error = %v", group, err)
	}
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.JobID())
	}
	sort.Strings(ids)
	return ids
}

func waitEnqueued(t *testing.T, ctx context.Context, q *recordingQueue, group string) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 3*time.Second, true,
		func(context.Context) (bool, error) { return q.has(group), nil })
	if err != nil {
		t.Fatalf("group %s was never enqueued", group)
	}
}

func TestScope_DefaultWatchesAllNamespacesAndNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, _, _, _ := startScoped(t, ctx, "", "",
		testNode("node-a", map[string]string{"group.timeslice.io/g1": "true"}),
		testNode("node-b", map[string]string{"group.timeslice.io/g1": "true"}),
		testPod("ns-1", "p1", "g1", "job-1", "node-a"),
		testPod("ns-2", "p2", "g1", "job-2", "node-b"),
		testPod("ns-2", "p3", "g1", "job-3", "node-unknown"),
	)
	if err := infraOrch.ObserveGroupState(ctx, "g1"); err != nil {
		t.Fatalf("ObserveGroupState() error = %v", err)
	}
	want := []string{"job-1", "job-2", "job-3"}
	if got := jobIDs(t, ctx, jobStore, "g1"); !reflect.DeepEqual(got, want) {
		t.Errorf("jobs = %v, want %v (unscoped behaviour unchanged)", got, want)
	}
}

func TestScope_WatchNamespaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, _, queue, clientset := startScoped(t, ctx, "demo-a,demo-b", "",
		testNode("node-a", map[string]string{"group.timeslice.io/g1": "true"}),
		testPod("demo-a", "p1", "g1", "job-a", "node-a"),
		testPod("demo-b", "p2", "g1", "job-b", "node-a"),
		testPod("other", "p3", "g1", "job-other", "node-a"),
		testPod("other", "p4", "g-other", "job-x", "node-a"),
	)
	if err := infraOrch.ObserveGroupState(ctx, "g1"); err != nil {
		t.Fatalf("ObserveGroupState() error = %v", err)
	}
	want := []string{"job-a", "job-b"}
	if got := jobIDs(t, ctx, jobStore, "g1"); !reflect.DeepEqual(got, want) {
		t.Errorf("jobs = %v, want %v (pods outside --watch-namespaces must be ignored)", got, want)
	}

	// Pods were listed and watched per namespace, never cluster-wide.
	for _, a := range clientset.Actions() {
		if a.GetResource().Resource != "pods" {
			continue
		}
		if a.GetVerb() == "list" || a.GetVerb() == "watch" {
			if ns := a.GetNamespace(); ns != "demo-a" && ns != "demo-b" {
				t.Errorf("pod %s in namespace %q, want only demo-a or demo-b", a.GetVerb(), ns)
			}
		}
	}

	waitEnqueued(t, ctx, queue, "g1")
	if queue.has("g-other") {
		t.Errorf("group g-other from an unwatched namespace was enqueued")
	}
}

func TestScope_NodeSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, groupStore, queue, clientset := startScoped(t, ctx, "", "pool=demo",
		testNode("node-demo", map[string]string{"pool": "demo", "group.timeslice.io/g1": "true"}),
		testNode("node-other", map[string]string{
			"pool": "other", "group.timeslice.io/g1": "true", "group.timeslice.io/g2": "true",
		}),
		testPod("demo", "p1", "g1", "job-demo", "node-demo"),
		testPod("demo", "p2", "g1", "job-pending", ""),
		testPod("demo", "p3", "g1", "job-on-other-node", "node-other"),
		testPod("demo", "p4", "g2", "job-g2", "node-other"),
	)

	// Nodes were listed with the selector.
	sawNodeList := false
	for _, a := range clientset.Actions() {
		if a.GetResource().Resource == "nodes" && a.GetVerb() == "list" {
			sawNodeList = true
			listAction, ok := a.(k8stesting.ListAction)
			if !ok {
				t.Fatalf("node list action has type %T", a)
			}
			if sel := listAction.GetListRestrictions().Labels.String(); sel != "pool=demo" {
				t.Errorf("node list selector = %q, want pool=demo", sel)
			}
		}
	}
	if !sawNodeList {
		t.Fatalf("no node list action recorded")
	}

	if err := infraOrch.ObserveGroupState(ctx, "g1"); err != nil {
		t.Fatalf("ObserveGroupState(g1) error = %v", err)
	}
	g1, err := groupStore.Get(ctx, "g1")
	if err != nil {
		t.Fatalf("group g1 not in store: %v", err)
	}
	if got := g1.Status().Nodes(); !reflect.DeepEqual(got, []string{"node-demo"}) {
		t.Errorf("g1 nodes = %v, want [node-demo]", got)
	}
	want := []string{"job-demo", "job-pending"}
	if got := jobIDs(t, ctx, jobStore, "g1"); !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (pod on a node outside --node-selector must be ignored)", got, want)
	}

	// g2 lives only on the unselected node: it must never become a group.
	if err := infraOrch.ObserveGroupState(ctx, "g2"); err != nil {
		t.Fatalf("ObserveGroupState(g2) error = %v", err)
	}
	if _, err := groupStore.Get(ctx, "g2"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("groupStore.Get(g2) error = %v, want ErrNotFound", err)
	}
	if got := jobIDs(t, ctx, jobStore, "g2"); len(got) != 0 {
		t.Errorf("g2 jobs = %v, want none", got)
	}

	waitEnqueued(t, ctx, queue, "g1")
	if queue.has("g2") {
		t.Errorf("group g2 from an unselected node was enqueued")
	}
}
