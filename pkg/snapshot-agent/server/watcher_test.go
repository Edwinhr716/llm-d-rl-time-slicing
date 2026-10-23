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
	"errors"
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
	origGetPodGPUInfo := podutils.GetPodGPUInfo
	defer func() {
		podutils.GetK8sClient = origGetK8sClient
		podutils.GetPodPIDs = origGetPodPIDs
		podutils.GetPodGPUInfo = origGetPodGPUInfo
	}()

	os.Setenv("NODE_NAME", "test-node")
	defer os.Unsetenv("NODE_NAME")

	tests := []struct {
		name          string
		accelType     string
		residueMB     int
		pods          []*corev1.Pod
		mockPIDs      func(ctx context.Context, podName, namespace string) ([]int, error)
		mockGPUInfo   func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error)
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
		{
			name:      "Transition to SAVED when VRAM is below residue threshold",
			residueMB: 4096,
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-offloaded",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-offloaded",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			mockGPUInfo: func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error) {
				return &podutils.PodGPUInfo{PIDs: []int{789}, TotalVRAMBytes: 2000 * 1024 * 1024}, nil
			},
			expectedState: map[string]pb.JobState{
				"job-offloaded": pb.JobState_JOB_STATE_SAVED,
			},
			expectedPIDs: map[string][]int{
				"job-offloaded": {789},
			},
			timeout: 3 * time.Second,
		},
		{
			name:      "TPU discovery transitions to RUNNING on detected PIDs",
			accelType: "tpu",
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-tpu",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-tpu",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			mockGPUInfo: func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error) {
				t.Fatal("GetPodGPUInfo should not be called on TPU nodes")
				return nil, errors.New("should not be called")
			},
			mockPIDs: func(ctx context.Context, podName, namespace string) ([]int, error) {
				return []int{555}, nil
			},
			expectedState: map[string]pb.JobState{
				"job-tpu": pb.JobState_JOB_STATE_RUNNING,
			},
			expectedPIDs: map[string][]int{
				"job-tpu": {555},
			},
			timeout: 3 * time.Second,
		},
		{
			name:      "Multi-pod job transitions to RUNNING if any pod exceeds threshold",
			residueMB: 1024,
			pods: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-multi-1",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-multi",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod-multi-2",
						Namespace: "default",
						Labels: map[string]string{
							podutils.JobIDLabel: "job-multi",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			mockGPUInfo: func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error) {
				if podName == "pod-multi-1" {
					return &podutils.PodGPUInfo{PIDs: []int{101}, TotalVRAMBytes: 200 * 1024 * 1024}, nil
				}
				return &podutils.PodGPUInfo{PIDs: []int{102}, TotalVRAMBytes: 2000 * 1024 * 1024}, nil
			},
			expectedState: map[string]pb.JobState{
				"job-multi": pb.JobState_JOB_STATE_RUNNING,
			},
			expectedPIDs: map[string][]int{
				"job-multi": {101, 102},
			},
			timeout: 3 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ACCELERATOR_TYPE", tc.accelType)
			fakeClient := fakek8s.NewSimpleClientset()
			podutils.GetK8sClient = func() (kubernetes.Interface, error) {
				return fakeClient, nil
			}

			switch {
			case tc.mockGPUInfo != nil && tc.mockPIDs != nil:
				podutils.GetPodGPUInfo = tc.mockGPUInfo
				podutils.GetPodPIDs = tc.mockPIDs
			case tc.mockGPUInfo != nil:
				podutils.GetPodGPUInfo = tc.mockGPUInfo
			case tc.mockPIDs != nil:
				podutils.GetPodPIDs = tc.mockPIDs
				podutils.GetPodGPUInfo = func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error) {
					pids, err := tc.mockPIDs(ctx, podName, namespace)
					if err != nil {
						return nil, err
					}
					return &podutils.PodGPUInfo{PIDs: pids, TotalVRAMBytes: 0}, nil
				}
			default:
				podutils.GetPodPIDs = func(ctx context.Context, podName, namespace string) ([]int, error) {
					return nil, nil
				}
				podutils.GetPodGPUInfo = func(ctx context.Context, podName, namespace string) (*podutils.PodGPUInfo, error) {
					return &podutils.PodGPUInfo{PIDs: nil, TotalVRAMBytes: 0}, nil
				}
			}

			state := sm.NewStateManager()
			watcher, err := server.NewWatcher(fakeClient, state, tc.residueMB)
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
