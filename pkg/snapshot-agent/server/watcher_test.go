// Copyright 2025 The llm-d Authors.
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
	"os"
	"reflect"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/server"
	sm "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/state-machine"
	podutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakek8s "k8s.io/client-go/kubernetes/fake"
)

func TestWatcher(t *testing.T) {
	// Save original functions and restore after tests
	origGetK8sClient := podutils.GetK8sClient
	origGetPodPIDs := podutils.GetPodPIDs
	defer func() {
		podutils.GetK8sClient = origGetK8sClient
		podutils.GetPodPIDs = origGetPodPIDs
	}()

	os.Setenv("NODE_NAME", "test-node")
	defer os.Unsetenv("NODE_NAME")

	tests := []struct {
		name          string
		pods          []*corev1.Pod
		mockPIDs      func(ctx context.Context, podName, namespace string) ([]int, error)
		expectedState map[string]pb.JobState // jobID -> expected state
		expectedPIDs  map[string][]int       // jobID -> expected PIDs
		timeout       time.Duration
	}{
		{
			name: "Register Job on Pod Add (IDLE)",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-1",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-1",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			mockPIDs: func(ctx context.Context, podName, namespace string) ([]int, error) {
				return nil, nil // No PIDs yet
			},
			expectedState: map[string]pb.JobState{
				"job-1": pb.JobState_JOB_STATE_IDLE,
			},
			timeout: 2 * time.Second,
		},
		{
			name: "Transition to RUNNING on GPU Activity",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-2",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-2",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			mockPIDs: func(ctx context.Context, podName, namespace string) ([]int, error) {
				return []int{123, 456}, nil
			},
			expectedState: map[string]pb.JobState{
				"job-2": pb.JobState_JOB_STATE_RUNNING,
			},
			expectedPIDs: map[string][]int{
				"job-2": {123, 456},
			},
			timeout: 3 * time.Second,
		},
		{
			name: "Ignore Pod on Different Node",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-3",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-3",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "other-node",
					},
				},
			},
			expectedState: map[string]pb.JobState{}, // No jobs expected
			timeout:       1 * time.Second,
		},
		{
			name: "Ignore Pod without Job Label",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-4",
						Namespace: "default",
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			expectedState: map[string]pb.JobState{}, // No jobs expected
			timeout:       1 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fakek8s.NewSimpleClientset()
			podutils.GetK8sClient = func() (kubernetes.Interface, error) {
				return fakeClient, nil
			}

			if tc.mockPIDs != nil {
				podutils.GetPodPIDs = tc.mockPIDs
			} else {
				podutils.GetPodPIDs = func(ctx context.Context, podName, namespace string) ([]int, error) {
					return nil, nil
				}
			}

			state := sm.NewStateManager()
			watcher, err := server.NewWatcher(fakeClient, state)
			if err != nil {
				t.Fatalf("Failed to create watcher: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			watcher.Start(ctx)

			// Create pods
			for _, pod := range tc.pods {
				_, err = fakeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
				}
			}

			// Wait and verify states
			deadline := time.Now().Add(tc.timeout)
			success := false
			for time.Now().Before(deadline) {
				success = true
				statuses := state.GetJobStatus()

				// Check if all expected jobs are in the expected state
				for expectedJobID, expectedState := range tc.expectedState {
					found := false
					for _, s := range statuses {
						if s.JobId == expectedJobID {
							found = true
							if s.State != expectedState {
								success = false
							}
							break
						}
					}
					if !found {
						success = false
					}
				}

				// If we expected no jobs, check that len is 0
				if len(tc.expectedState) == 0 && len(statuses) != 0 {
					success = false
				}

				if success {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}

			if !success {
				t.Errorf("Timeout waiting for expected states. Current: %+v", state.GetJobStatus())
			}

			// Verify PIDs if expected
			for jobID, expectedPIDs := range tc.expectedPIDs {
				pids, err := state.GetJobPIDs(jobID)
				if err != nil {
					t.Errorf("Failed to get PIDs for job %s: %v", jobID, err)
					continue
				}
				if !reflect.DeepEqual(pids, expectedPIDs) {
					t.Errorf("Job %s PIDs: expected %v, got %v", jobID, expectedPIDs, pids)
				}
			}
		})
	}
}

func TestWatcherSeedsGuestEpoch(t *testing.T) {
	origGetK8sClient := podutils.GetK8sClient
	origGetPodPIDs := podutils.GetPodPIDs
	defer func() {
		podutils.GetK8sClient = origGetK8sClient
		podutils.GetPodPIDs = origGetPodPIDs
	}()
	t.Setenv("NODE_NAME", "test-node")

	fakeClient := fakek8s.NewSimpleClientset()
	podutils.GetK8sClient = func() (kubernetes.Interface, error) {
		return fakeClient, nil
	}
	podutils.GetPodPIDs = func(context.Context, string, string) ([]int, error) {
		return nil, nil
	}

	state := sm.NewStateManager()
	watcher, err := server.NewWatcher(fakeClient, state)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher.Start(ctx)

	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mirror-1",
			Namespace:   "default",
			Labels:      map[string]string{podutils.JobIDLabel: "guest-1", podutils.GroupLabel: "group-1"},
			Annotations: map[string]string{podutils.GuestEpochAnnotation: "7"},
		},
		Spec: corev1.PodSpec{NodeName: "test-node"},
	}
	if _, err := fakeClient.CoreV1().Pods("default").Create(ctx, mirror, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}
	waitForEpoch(t, state, 7)

	setEpoch := func(value string) {
		t.Helper()
		updated := mirror.DeepCopy()
		updated.Annotations[podutils.GuestEpochAnnotation] = value
		if _, err := fakeClient.CoreV1().Pods("default").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("Failed to update pod: %v", err)
		}
	}

	setEpoch("9")
	waitForEpoch(t, state, 9)

	// Lower and invalid values are ignored (the state-machine tests cover
	// that the fence never goes down); the watcher keeps going.
	setEpoch("3")
	setEpoch("not-a-number")
	setEpoch("10")
	waitForEpoch(t, state, 10)

	// A late call below the annotation is refused.
	_, err = state.StartGuestOp("guest-1", sm.OpTypeResume, 8, time.Now().Add(time.Minute),
		func(context.Context) (sm.GuestResult, error) {
			t.Error("worker must not run")
			return sm.GuestResult{}, nil
		})
	if got := sm.ErrorReasonOf(err); got != pb.ErrorReason_STALE_EPOCH {
		t.Errorf("expected STALE_EPOCH, got %v (%v)", got, err)
	}
}

func waitForEpoch(t *testing.T, state *sm.StateManager, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		for _, st := range state.GetJobStatus() {
			if st.GetJobId() == "guest-1" {
				got = st.GetEpoch()
			}
		}
		if got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected epoch %d, got %d", want, got)
}
