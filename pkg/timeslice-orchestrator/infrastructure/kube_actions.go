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
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// eventSource is the component name on the events the orchestrator writes.
const eventSource = "timeslice-orchestrator"

// KubeActions implements controller.KubeActions with a clientset: Warning
// events on pods, and graceful pod deletes for the unconfirmed-kill
// escalation (decision D-NS-6). It needs "create" on events and, for
// --unconfirmed-kill=escalate, "delete" on pods, in the watched namespaces.
type KubeActions struct {
	client     kubernetes.Interface
	namespaces []string
}

var _ controller.KubeActions = (*KubeActions)(nil)

// NewKubeActions returns KubeActions over client, looking for pods in
// namespaces (empty means all namespaces).
func NewKubeActions(client kubernetes.Interface, namespaces []string) *KubeActions {
	return &KubeActions{client: client, namespaces: namespaces}
}

// pods lists the pods labelled with the group and job, on node if not empty.
func (k *KubeActions) pods(ctx context.Context, groupID, jobID, node string) ([]corev1.Pod, error) {
	selector := labels.SelectorFromSet(labels.Set{PodLabelKey: groupID, JobLabelKey: jobID}).String()
	namespaces := k.namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}
	var out []corev1.Pod
	for _, ns := range namespaces {
		list, err := k.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return nil, fmt.Errorf("list pods of job %s in %q: %w", jobID, ns, err)
		}
		for i := range list.Items {
			if node == "" || list.Items[i].Spec.NodeName == node {
				out = append(out, list.Items[i])
			}
		}
	}
	return out, nil
}

// WarnPods records a Warning event on each pod of the job.
func (k *KubeActions) WarnPods(ctx context.Context, groupID, jobID, node, reason, message string) error {
	pods, err := k.pods(ctx, groupID, jobID, node)
	if err != nil {
		return err
	}
	errs := make([]error, 0, len(pods))
	now := metav1.NewTime(time.Now())
	for i := range pods {
		pod := &pods[i]
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name + "." + strconv.FormatInt(now.UnixNano(), 16),
				Namespace: pod.Namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:            "Pod",
				APIVersion:      "v1",
				Namespace:       pod.Namespace,
				Name:            pod.Name,
				UID:             pod.UID,
				ResourceVersion: pod.ResourceVersion,
			},
			Reason:         reason,
			Message:        message,
			Type:           corev1.EventTypeWarning,
			Source:         corev1.EventSource{Component: eventSource},
			FirstTimestamp: now,
			LastTimestamp:  now,
			Count:          1,
		}
		if _, err := k.client.CoreV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
			errs = append(errs, fmt.Errorf("event %s on pod %s/%s: %w", reason, pod.Namespace, pod.Name, err))
		}
	}
	return errors.Join(errs...)
}

// DeletePodsGracefully deletes each pod of the job on node with the pod's
// own grace period; it never sets a grace period, so never 0. Pods already
// being deleted are left alone.
func (k *KubeActions) DeletePodsGracefully(ctx context.Context, groupID, jobID, node string) (int, error) {
	pods, err := k.pods(ctx, groupID, jobID, node)
	if err != nil {
		return 0, err
	}
	deleted := 0
	errs := make([]error, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		uid := pod.UID
		err := k.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		switch {
		case err == nil:
			deleted++
		case apierrors.IsNotFound(err):
		default:
			errs = append(errs, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	return deleted, errors.Join(errs...)
}
