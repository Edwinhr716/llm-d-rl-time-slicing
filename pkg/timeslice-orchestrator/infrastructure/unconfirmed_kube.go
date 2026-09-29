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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// EventComponent is the source component of the events the orchestrator
// records.
const EventComponent = "timeslice-orchestrator"

// KubeActions implements controller.UnconfirmedKube: Warning events on the
// mirror pods of a guest and on the pods of a foreground job. Pods are found
// through the orchestrator's pod listers.
//
// RBAC: events create.
type KubeActions struct {
	cs   kubernetes.Interface
	orch *KubernetesOrchestrator
}

var _ controller.UnconfirmedKube = (*KubeActions)(nil)

// NewKubeActions returns the KubeActions of an orchestrator.
func NewKubeActions(cs kubernetes.Interface, orch *KubernetesOrchestrator) *KubeActions {
	return &KubeActions{cs: cs, orch: orch}
}

// jobPods returns the watched pods of the group's job, background (mirror)
// pods or not, on node when node is set.
func (a *KubeActions) jobPods(group, job, node string, background bool) ([]*corev1.Pod, error) {
	selector := labels.SelectorFromSet(labels.Set{PodLabelKey: group, JobLabelKey: job})
	var out []*corev1.Pod
	for _, lister := range a.orch.podListers {
		pods, err := lister.List(selector)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if (pod.Labels[RoleLabelKey] == RoleBackground) != background {
				continue
			}
			if node != "" && pod.Spec.NodeName != node {
				continue
			}
			out = append(out, pod)
		}
	}
	return out, nil
}

// GuestEvent records a Warning event on the guest's mirror pods on node.
func (a *KubeActions) GuestEvent(ctx context.Context, group, job, node, reason, message string) error {
	pods, err := a.jobPods(group, job, node, true)
	if err != nil {
		return err
	}
	return a.podEvents(ctx, pods, reason, message)
}

// ForegroundEvent records a Warning event on the pods of a foreground job.
func (a *KubeActions) ForegroundEvent(ctx context.Context, group, job, reason, message string) error {
	pods, err := a.jobPods(group, job, "", false)
	if err != nil {
		return err
	}
	return a.podEvents(ctx, pods, reason, message)
}

func (a *KubeActions) podEvents(ctx context.Context, pods []*corev1.Pod, reason, message string) error {
	errs := make([]error, 0, len(pods))
	for _, pod := range pods {
		ref := &corev1.ObjectReference{
			APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID,
		}
		errs = append(errs, a.event(ctx, pod.Namespace, ref, reason, message))
	}
	return errors.Join(errs...)
}

func (a *KubeActions) event(ctx context.Context, namespace string, ref *corev1.ObjectReference, reason, message string) error {
	now := metav1.NewTime(time.Now())
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%x", ref.Name, now.UnixNano()),
			Namespace: namespace,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		Type:           corev1.EventTypeWarning,
		Source:         corev1.EventSource{Component: EventComponent},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	if _, err := a.cs.CoreV1().Events(namespace).Create(ctx, ev, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create %s event on %s %s/%s: %w", reason, ref.Kind, ref.Namespace, ref.Name, err)
	}
	return nil
}
