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
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const keepLog = "Keeping background pod bound to a node outside --node-selector"

// syncBuffer is a bytes.Buffer safe for the informer goroutines that log.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs sends the default slog logger to a buffer for the rest of the
// test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// logLines returns the captured lines that contain every one of parts.
func logLines(buf *syncBuffer, parts ...string) []string {
	var out []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		matched := line != ""
		for _, p := range parts {
			matched = matched && strings.Contains(line, p)
		}
		if matched {
			out = append(out, line)
		}
	}
	return out
}

func withRole(pod *corev1.Pod, role string) *corev1.Pod {
	pod.Labels["timeslice.io/role"] = role
	return pod
}

// startWithPodWatch builds the orchestrator the way main.go does for
// --node-selector=nodeSelector, starts its informers and waits for them to
// sync and for the pod watch to start, so pods created afterwards reach the
// handlers.
func startWithPodWatch(
	t *testing.T, ctx context.Context, nodeSelector string, objs ...runtime.Object,
) (*infrastructure.KubernetesOrchestrator, *store.JobStore, *store.GroupStore, *recordingQueue, *fake.Clientset) {
	t.Helper()
	scope, err := infrastructure.ParseScope("", nodeSelector)
	if err != nil {
		t.Fatalf("ParseScope() error = %v", err)
	}
	clientset := fake.NewClientset(objs...)
	podWatchStarted := make(chan struct{})
	var once sync.Once
	clientset.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		w, err := clientset.Tracker().Watch(action.GetResource(), action.GetNamespace())
		if err != nil {
			return false, nil, err
		}
		once.Do(func() { close(podWatchStarted) })
		return true, w, nil
	})
	factories := scope.NewInformerFactories(clientset, 0)

	var opts []infrastructure.Option
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
	factories.Pods[0].Start(ctx.Done())
	if err := infraOrch.Init(ctx); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	select {
	case <-podWatchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("pod watch did not start")
	}
	return infraOrch, jobStore, groupStore, queue, clientset
}

// backgroundFixture: node-demo is selected by pool=demo, node-other is not,
// and both carry the g1 group label. A trainer runs on node-demo, a
// background (mirror) pod on node-other.
func backgroundFixture() []runtime.Object {
	return []runtime.Object{
		testNode("node-demo", map[string]string{"pool": "demo", "group.timeslice.io/g1": "true"}),
		testNode("node-other", map[string]string{"pool": "other", "group.timeslice.io/g1": "true"}),
		testPod("demo", "trainer", "g1", "job-trainer", "node-demo"),
		withRole(testPod("demo", "mirror", "g1", "vk/node-other", "node-other"), "background"),
	}
}

func observe(t *testing.T, ctx context.Context, k *infrastructure.KubernetesOrchestrator, group string) {
	t.Helper()
	if err := k.ObserveGroupState(ctx, group); err != nil {
		t.Fatalf("ObserveGroupState(%s) error = %v", group, err)
	}
}

func groupNodes(t *testing.T, ctx context.Context, gs *store.GroupStore, group string) []string {
	t.Helper()
	g, err := gs.Get(ctx, group)
	if err != nil {
		t.Fatalf("group %s not in store: %v", group, err)
	}
	return g.Status().Nodes()
}

// createAndSentinel creates pod, then a sentinel pod for group sentinel on
// node-demo, and waits for the sentinel's group to be enqueued. The informer
// delivers events in order, so pod's event has been handled by then.
func createAndSentinel(
	t *testing.T, ctx context.Context, cs *fake.Clientset, q *recordingQueue, pod *corev1.Pod, sentinel string,
) {
	t.Helper()
	if _, err := cs.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod %s: %v", pod.Name, err)
	}
	s := testPod("demo", "sentinel-"+sentinel, sentinel, "job-"+sentinel, "node-demo")
	if _, err := cs.CoreV1().Pods(s.Namespace).Create(ctx, s, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create sentinel pod: %v", err)
	}
	waitEnqueued(t, ctx, q, sentinel)
}

func TestNodeSelectorBackground_Kept(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, groupStore, queue, cs := startWithPodWatch(t, ctx, "pool=demo", backgroundFixture()...)
	observe(t, ctx, infraOrch, "g1")

	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-trainer", "vk/node-other"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (background pod must be kept)", got, want)
	}
	// Only pods are kept: the unselected node still is not a group node.
	if got, want := groupNodes(t, ctx, groupStore, "g1"), []string{"node-demo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 nodes = %v, want %v (node list must not change)", got, want)
	}
	if lines := logLines(logs, keepLog, "pod=demo/mirror", "node=node-other", "group=g1"); len(lines) == 0 {
		t.Errorf("no %q line for demo/mirror; logs:\n%s", keepLog, logs)
	}

	// A group that lives only on the unselected node joins with no nodes.
	createAndSentinel(t, ctx, cs, queue,
		withRole(testPod("demo", "late-mirror", "g-late", "vk/late", "node-other"), "background"), "g-sentinel")
	if !queue.has("g-late") {
		t.Errorf("group g-late of a background pod on an unselected node was not enqueued")
	}
	observe(t, ctx, infraOrch, "g-late")
	if got, want := jobIDs(t, ctx, jobStore, "g-late"), []string{"vk/late"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g-late jobs = %v, want %v", got, want)
	}
	if got := groupNodes(t, ctx, groupStore, "g-late"); len(got) != 0 {
		t.Errorf("g-late nodes = %v, want none", got)
	}
}

func TestNodeSelectorBackground_ForegroundStillDropped(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objs := append(backgroundFixture(),
		testPod("demo", "fg-unlabelled", "g1", "job-fg-unlabelled", "node-other"),
		withRole(testPod("demo", "fg-labelled", "g1", "job-fg-labelled", "node-other"), "foreground"),
		withRole(testPod("demo", "bg-wrong-case", "g1", "job-bg-wrong-case", "node-other"), "Background"),
		withRole(testPod("demo", "bg-empty", "g1", "job-bg-empty", "node-other"), ""),
	)
	infraOrch, jobStore, _, queue, cs := startWithPodWatch(t, ctx, "pool=demo", objs...)
	observe(t, ctx, infraOrch, "g1")

	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-trainer", "vk/node-other"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (only role=background is kept)", got, want)
	}
	for _, name := range []string{"fg-unlabelled", "fg-labelled", "bg-wrong-case", "bg-empty"} {
		if lines := logLines(logs, keepLog, "pod=demo/"+name+" "); len(lines) != 0 {
			t.Errorf("logged %q for non-background pod %s: %v", keepLog, name, lines)
		}
	}

	createAndSentinel(t, ctx, cs, queue,
		testPod("demo", "late-fg", "g-late", "job-late", "node-other"), "g-sentinel")
	if queue.has("g-late") {
		t.Errorf("group g-late of a foreground pod on an unselected node was enqueued")
	}
	if lines := logLines(logs, "Ignoring pod bound to a node outside --node-selector", "demo/late-fg"); len(lines) == 0 {
		t.Errorf("no Ignoring log for demo/late-fg; logs:\n%s", logs)
	}
}

func TestNodeSelectorBackground_UnboundUnchanged(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, _, _, _ := startWithPodWatch(t, ctx, "pool=demo",
		testNode("node-demo", map[string]string{"pool": "demo", "group.timeslice.io/g1": "true"}),
		testPod("demo", "pending-fg", "g1", "job-pending-fg", ""),
		withRole(testPod("demo", "pending-bg", "g1", "vk/pending", ""), "background"),
	)
	observe(t, ctx, infraOrch, "g1")

	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-pending-fg", "vk/pending"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (unbound pods join)", got, want)
	}
	if lines := logLines(logs, keepLog); len(lines) != 0 {
		t.Errorf("logged %q for unbound pods: %v", keepLog, lines)
	}
}

// With no --node-selector every pod joins, and nothing is logged as kept.
func TestNodeSelectorBackground_NoSelector(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infraOrch, jobStore, _, _, _ := startWithPodWatch(t, ctx, "",
		testNode("node-a", map[string]string{"group.timeslice.io/g1": "true"}),
		testPod("demo", "p1", "g1", "job-1", "node-a"),
		withRole(testPod("demo", "mirror", "g1", "vk/x", "node-unknown"), "background"),
		testPod("demo", "p3", "g1", "job-3", "node-unknown"),
	)
	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-1", "job-3", "vk/x"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (unscoped behaviour unchanged)", got, want)
	}
	if lines := logLines(logs, keepLog); len(lines) != 0 {
		t.Errorf("logged %q without a node selector: %v", keepLog, lines)
	}
}
