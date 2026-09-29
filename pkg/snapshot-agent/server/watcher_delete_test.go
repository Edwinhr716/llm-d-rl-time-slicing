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

package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/server"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakek8s "k8s.io/client-go/kubernetes/fake"
)

// TestWatcherDeleteClearsFaultedJob: a job left FAULTED (for example by
// KILL_UNCONFIRMED) is forgotten when its mirror pod is deleted, and a new
// pod with the same job ID starts clean.
func TestWatcherDeleteClearsFaultedJob(t *testing.T) {
	origGetPodPIDs := podutils.GetPodPIDs
	defer func() { podutils.GetPodPIDs = origGetPodPIDs }()
	podutils.GetPodPIDs = func(context.Context, string, string) ([]int, error) { return nil, nil }
	t.Setenv("NODE_NAME", "test-node")

	fakeClient := fakek8s.NewSimpleClientset()
	state := sm.NewStateManager()
	watcher, err := server.NewWatcher(fakeClient, state)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher.Start(ctx)

	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror-1", Namespace: "default", UID: "uid-1",
			Labels: map[string]string{podutils.JobIDLabel: "guest-1"},
		},
		Spec: corev1.PodSpec{NodeName: "test-node"},
	}
	if _, err := fakeClient.CoreV1().Pods("default").Create(ctx, mirror, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitJob(t, state, func(st *pb.JobStatus) bool { return st != nil })

	if err := state.TransitionToRunning("guest-1", []int{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.StartGuestOp("guest-1", sm.OpTypeSuspend, 1, time.Now().Add(time.Minute),
		func(context.Context) (sm.GuestResult, error) {
			return sm.GuestResult{}, errors.New("checkpoint failed")
		},
	); err != nil {
		t.Fatal(err)
	}
	waitJob(t, state, func(st *pb.JobStatus) bool {
		return st != nil && st.GetState() == pb.JobState_JOB_STATE_FAULTED
	})

	if err := fakeClient.CoreV1().Pods("default").Delete(ctx, "mirror-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitJob(t, state, func(st *pb.JobStatus) bool { return st == nil })

	mirror.UID = "uid-2"
	if _, err := fakeClient.CoreV1().Pods("default").Create(ctx, mirror, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitJob(t, state, func(st *pb.JobStatus) bool {
		return st != nil && st.GetState() == pb.JobState_JOB_STATE_IDLE && st.GetEpoch() == 0
	})
}

// waitJob waits until ok holds for guest-1's status (nil when unknown).
func waitJob(t *testing.T, state *sm.StateManager, ok func(*pb.JobStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var found *pb.JobStatus
		for _, st := range state.GetJobStatus() {
			if st.GetJobId() == "guest-1" {
				found = st
			}
		}
		if ok(found) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("guest-1 did not reach the expected status; last %v", found)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
