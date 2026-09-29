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
	"errors"
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
	"k8s.io/client-go/kubernetes/fake"
)

const skipUnboundLogMsg = "Skipping unbound pod until it is bound"

// skipLogBuffer is a bytes.Buffer safe for concurrent writers.
type skipLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *skipLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// skipLogs returns the number of "Skipping unbound pod" records for the pod.
func (b *skipLogBuffer) skipLogs(pod string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(line, skipUnboundLogMsg) && strings.Contains(line, "pod="+pod+" ") {
			n++
		}
	}
	return n
}

func (b *skipLogBuffer) anySkipLog() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Contains(b.buf.String(), skipUnboundLogMsg)
}

// captureSkipLogs sends the default slog logger to a buffer for the test.
func captureSkipLogs(t *testing.T) *skipLogBuffer {
	t.Helper()
	logs := &skipLogBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// startSkipUnbound builds the orchestrator as main.go does for
// --node-selector=nodeSelector, except that WithSkipUnboundPods is passed
// whenever skip is true, also without a selector, so the tests check that the
// option itself is a no-op there (main.go does not even pass it).
func startSkipUnbound(
	t *testing.T, ctx context.Context, nodeSelector string, skip bool, objs ...runtime.Object,
) (*infrastructure.KubernetesOrchestrator, *store.JobStore, *store.GroupStore, *recordingQueue, *fake.Clientset) {
	t.Helper()
	scope, err := infrastructure.ParseScope("", nodeSelector)
	if err != nil {
		t.Fatalf("ParseScope() error = %v", err)
	}
	clientset := fake.NewClientset(objs...)
	factories := scope.NewInformerFactories(clientset, 0)

	var opts []infrastructure.Option
	if !scope.AllNodes() {
		opts = append(opts, infrastructure.WithNodeScopedPods())
	}
	if skip {
		opts = append(opts, infrastructure.WithSkipUnboundPods())
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

func observe(t *testing.T, ctx context.Context, k *infrastructure.KubernetesOrchestrator, group string) {
	t.Helper()
	if err := k.ObserveGroupState(ctx, group); err != nil {
		t.Fatalf("ObserveGroupState(%s) error = %v", group, err)
	}
}

// bindPod plays the scheduler: it sets spec.nodeName of pod demo/<name>.
func bindPod(t *testing.T, ctx context.Context, cs *fake.Clientset, name, node string) {
	t.Helper()
	const ns = "demo"
	pod, err := cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s/%s: %v", ns, name, err)
	}
	pod.Spec.NodeName = node
	if _, err := cs.CoreV1().Pods(ns).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("bind pod %s/%s to %s: %v", ns, name, node, err)
	}
}

func resetQueue(q *recordingQueue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.added = map[string]bool{}
}

// skipUnboundObjects is the shared layout: node-demo is selected, node-other
// is not, both carry g1. g-pending has no node and only an unbound pod.
func skipUnboundObjects() []runtime.Object {
	return []runtime.Object{
		testNode("node-demo", map[string]string{"pool": "demo", "group.timeslice.io/g1": "true"}),
		testNode("node-other", map[string]string{"pool": "other", "group.timeslice.io/g1": "true"}),
		testPod("demo", "p-bound", "g1", "job-demo", "node-demo"),
		testPod("demo", "p-pending", "g1", "job-pending", ""),
		testPod("demo", "p-lone", "g-pending", "job-lone", ""),
		// An unbound pod without timeslice labels: never logged.
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "p-plain", UID: "uid-plain"}},
	}
}

// Off (join, the default): with a selector, an unbound pod counts toward its
// group at once, and a group whose only pod is unbound is created.
func TestSkipUnbound_Off(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := captureSkipLogs(t)

	infraOrch, jobStore, groupStore, queue, _ := startSkipUnbound(t, ctx, "pool=demo", false, skipUnboundObjects()...)

	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-demo", "job-pending"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (unbound pod joins with the flag off)", got, want)
	}
	observe(t, ctx, infraOrch, "g-pending")
	if _, err := groupStore.Get(ctx, "g-pending"); err != nil {
		t.Errorf("group g-pending not in store: %v (a group with only an unbound pod exists with the flag off)", err)
	}
	if got, want := jobIDs(t, ctx, jobStore, "g-pending"), []string{"job-lone"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g-pending jobs = %v, want %v", got, want)
	}
	waitEnqueued(t, ctx, queue, "g-pending")
	if logs.anySkipLog() {
		t.Errorf("log has %q with the flag off", skipUnboundLogMsg)
	}
}

// On (skip): with a selector, an unbound pod counts toward no group, its
// group is not enqueued for it, and it is logged once however often it is
// seen. Binding it to an unselected node keeps it out.
func TestSkipUnbound_On(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := captureSkipLogs(t)

	infraOrch, jobStore, groupStore, queue, clientset := startSkipUnbound(t, ctx, "pool=demo", true,
		skipUnboundObjects()...)

	for range 3 {
		observe(t, ctx, infraOrch, "g1")
		observe(t, ctx, infraOrch, "g-pending")
	}
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-demo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (unbound pod skipped)", got, want)
	}
	if _, err := groupStore.Get(ctx, "g-pending"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("groupStore.Get(g-pending) error = %v, want ErrNotFound", err)
	}
	if got := jobIDs(t, ctx, jobStore, "g-pending"); len(got) != 0 {
		t.Errorf("g-pending jobs = %v, want none", got)
	}

	// g1 is enqueued by its nodes and bound pod; g-pending only by its unbound
	// pod, so it must not be.
	waitEnqueued(t, ctx, queue, "g1")
	time.Sleep(300 * time.Millisecond)
	if queue.has("g-pending") {
		t.Errorf("group g-pending was enqueued for an unbound pod")
	}

	for pod, want := range map[string]int{"demo/p-pending": 1, "demo/p-lone": 1, "demo/p-bound": 0} {
		if got := logs.skipLogs(pod); got != want {
			t.Errorf("%q logged %d times for %s, want %d", skipUnboundLogMsg, got, pod, want)
		}
	}
	if logs.skipLogs("demo/p-plain") != 0 {
		t.Errorf("unbound pod without a group label was logged")
	}

	// J1 shape: the pod binds to the unselected node. It stays out, and the
	// update (old object unbound) does not log it again.
	bindPod(t, ctx, clientset, "p-pending", "node-other")
	time.Sleep(300 * time.Millisecond)
	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-demo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs after bind to unselected node = %v, want %v", got, want)
	}
	if got := logs.skipLogs("demo/p-pending"); got != 1 {
		t.Errorf("%q logged %d times for demo/p-pending after its bind, want 1", skipUnboundLogMsg, got)
	}
}

// BindToSelectedJoins (skip): the bind Update enqueues the group and the pod
// joins; a pod created already bound (like a mirror pod) joins at once.
func TestSkipUnbound_BindToSelectedJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := captureSkipLogs(t)

	infraOrch, jobStore, _, queue, clientset := startSkipUnbound(t, ctx, "pool=demo", true,
		testNode("node-demo", map[string]string{"pool": "demo", "group.timeslice.io/g1": "true"}),
		testPod("demo", "p-mirror", "g1", "job-mirror", "node-demo"),
		testPod("demo", "p-trainer", "g1", "job-trainer", ""),
	)

	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-mirror"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("g1 jobs before bind = %v, want %v (bound pod joins at once, unbound skipped)", got, want)
	}

	// Let the initial Add events settle, then check the bind alone enqueues g1.
	waitEnqueued(t, ctx, queue, "g1")
	time.Sleep(200 * time.Millisecond)
	resetQueue(queue)

	bindPod(t, ctx, clientset, "p-trainer", "node-demo")
	waitEnqueued(t, ctx, queue, "g1")
	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-mirror", "job-trainer"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs after bind to selected node = %v, want %v", got, want)
	}
	if got := logs.skipLogs("demo/p-trainer"); got != 1 {
		t.Errorf("%q logged %d times for demo/p-trainer, want 1", skipUnboundLogMsg, got)
	}
	if got := logs.skipLogs("demo/p-mirror"); got != 0 {
		t.Errorf("bound pod demo/p-mirror was logged as skipped")
	}
}

// NoSelectorNoop (skip without --node-selector): nothing is dropped, unbound
// pods join and enqueue their group as with the flag off.
func TestSkipUnbound_NoSelectorNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := captureSkipLogs(t)

	infraOrch, jobStore, groupStore, queue, _ := startSkipUnbound(t, ctx, "", true,
		testNode("node-a", map[string]string{"group.timeslice.io/g1": "true"}),
		testPod("demo", "p1", "g1", "job-1", "node-a"),
		testPod("demo", "p2", "g1", "job-pending", ""),
		testPod("demo", "p3", "g1", "job-3", "node-unknown"),
		testPod("demo", "p-lone", "g-pending", "job-lone", ""),
	)

	observe(t, ctx, infraOrch, "g1")
	if got, want := jobIDs(t, ctx, jobStore, "g1"), []string{"job-1", "job-3", "job-pending"}; !reflect.DeepEqual(got, want) {
		t.Errorf("g1 jobs = %v, want %v (no selector: unbound pod kept)", got, want)
	}
	observe(t, ctx, infraOrch, "g-pending")
	if _, err := groupStore.Get(ctx, "g-pending"); err != nil {
		t.Errorf("group g-pending not in store: %v", err)
	}
	waitEnqueued(t, ctx, queue, "g-pending")
	if logs.anySkipLog() {
		t.Errorf("log has %q without a selector", skipUnboundLogMsg)
	}
}
