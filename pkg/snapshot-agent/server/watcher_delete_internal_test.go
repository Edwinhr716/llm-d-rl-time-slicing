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

package server

import (
	"testing"

	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func jobPod(name, uid, jobID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(uid),
			Labels: map[string]string{podutils.JobIDLabel: jobID},
		},
		Spec: corev1.PodSpec{NodeName: "test-node"},
	}
}

func newDeleteWatcher(t *testing.T) (*Watcher, *sm.StateManager) {
	t.Helper()
	t.Setenv("NODE_NAME", "test-node")
	state := sm.NewStateManager()
	watcher, err := NewWatcher(fakek8s.NewSimpleClientset(), state)
	if err != nil {
		t.Fatal(err)
	}
	return watcher, state
}

func jobOneKnown(state *sm.StateManager) bool {
	for _, st := range state.GetJobStatus() {
		if st.GetJobId() == "job-1" {
			return true
		}
	}
	return false
}

func TestWatcher_HandlePodDelete(t *testing.T) {
	t.Run("pod", func(t *testing.T) {
		watcher, state := newDeleteWatcher(t)
		state.RegisterJob("job-1", "")
		watcher.handlePodDelete(jobPod("a", "uid-a", "job-1"))
		if jobOneKnown(state) {
			t.Fatal("job still known after its only pod was deleted")
		}
	})

	t.Run("tombstone", func(t *testing.T) {
		watcher, state := newDeleteWatcher(t)
		state.RegisterJob("job-1", "")
		watcher.handlePodDelete(cache.DeletedFinalStateUnknown{Key: "default/a", Obj: jobPod("a", "uid-a", "job-1")})
		if jobOneKnown(state) {
			t.Fatal("job still known after a tombstone for its only pod")
		}
	})

	t.Run("tombstone without a pod, pod without a job, other objects", func(t *testing.T) {
		watcher, state := newDeleteWatcher(t)
		state.RegisterJob("job-1", "")
		watcher.handlePodDelete(cache.DeletedFinalStateUnknown{Key: "default/a", Obj: "not a pod"})
		watcher.handlePodDelete(jobPod("a", "uid-a", ""))
		unlabelled := jobPod("b", "uid-b", "job-1")
		unlabelled.Labels = nil
		watcher.handlePodDelete(unlabelled)
		watcher.handlePodDelete(42)
		if !jobOneKnown(state) {
			t.Fatal("an unrelated delete removed the job")
		}
	})

	t.Run("another local pod keeps the job", func(t *testing.T) {
		watcher, state := newDeleteWatcher(t)
		state.RegisterJob("job-1", "")
		first, second := jobPod("a", "uid-a", "job-1"), jobPod("b", "uid-b", "job-1")
		if err := watcher.informer.GetStore().Add(second); err != nil {
			t.Fatal(err)
		}
		watcher.handlePodDelete(first)
		if !jobOneKnown(state) {
			t.Fatal("job removed while another local pod still carries it")
		}

		if err := watcher.informer.GetStore().Delete(second); err != nil {
			t.Fatal(err)
		}
		watcher.handlePodDelete(second)
		if jobOneKnown(state) {
			t.Fatal("job still known after its last pod was deleted")
		}
	})

	t.Run("the deleted pod still in the store", func(t *testing.T) {
		watcher, state := newDeleteWatcher(t)
		state.RegisterJob("job-1", "")
		pod := jobPod("a", "uid-a", "job-1")
		if err := watcher.informer.GetStore().Add(pod); err != nil {
			t.Fatal(err)
		}
		watcher.handlePodDelete(pod)
		if jobOneKnown(state) {
			t.Fatal("the deleted pod itself kept the job")
		}
	})
}

func TestWatcher_PodsForJob(t *testing.T) {
	watcher, _ := newDeleteWatcher(t)
	for _, pod := range []*corev1.Pod{
		jobPod("a", "uid-a", "job-1"), jobPod("b", "uid-b", "job-2"), jobPod("c", "uid-c", "job-1"),
	} {
		if err := watcher.informer.GetStore().Add(pod); err != nil {
			t.Fatal(err)
		}
	}
	if got := watcher.PodsForJob("job-1"); len(got) != 2 {
		t.Fatalf("want 2 pods for job-1, got %d", len(got))
	}
	if got := watcher.PodsForJob("job-3"); len(got) != 0 {
		t.Fatalf("want no pods for job-3, got %d", len(got))
	}
}
