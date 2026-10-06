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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// EventComponent is the source component of the events the orchestrator
// records.
const EventComponent = "timeslice-orchestrator"

// nodeEventNamespace is where events about Nodes (cluster-scoped) are
// recorded, as the kubelet does.
const nodeEventNamespace = metav1.NamespaceDefault

// KubeActions implements controller.UnconfirmedKube (decision D-NS-6): Warning
// events on mirror pods, foreground pods and Nodes, and the graceful delete of
// a guest's mirror pods. Pods are found through the orchestrator's pod
// listers. It never deletes with grace period 0 and never deletes a Node.
//
// RBAC: events create (all --unconfirmed-kill values); pods delete
// (--unconfirmed-kill=escalate only).
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
	selector := GroupSelector(group, labels.Set{JobLabelKey: job})
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

// NodeEvent records a Warning event on a Node.
func (a *KubeActions) NodeEvent(ctx context.Context, node, reason, message string) error {
	ref := &corev1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: node}
	if n, err := a.orch.nodeLister.Get(node); err == nil {
		ref.UID = n.UID
	}
	return a.event(ctx, nodeEventNamespace, ref, reason, message)
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

// DeleteGuestMirror deletes the guest's mirror pods on node with their own
// grace period (DeleteOptions without GracePeriodSeconds, never 0). A pod
// already being deleted is left alone. It reports how many deletes it issued.
func (a *KubeActions) DeleteGuestMirror(ctx context.Context, group, job, node string) (int, error) {
	pods, err := a.jobPods(group, job, node, true)
	if err != nil {
		return 0, err
	}
	deleted := 0
	var errs []error
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		uid := pod.UID
		err := a.cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		switch {
		case err == nil:
			deleted++
		case apierrors.IsNotFound(err):
		default:
			errs = append(errs, fmt.Errorf("delete mirror pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	return deleted, errors.Join(errs...)
}
