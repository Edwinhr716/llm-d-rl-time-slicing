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
	"k8s.io/apimachinery/pkg/labels"
	corev1informers "k8s.io/client-go/informers/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// WatchedJobs answers whether a job has a pod in the watched namespaces, from
// the caches of the pod informers the orchestrator already runs. It backs
// --reject-unwatched-jobs.
//
// It looks only at namespaces: a pod counts whether or not it is bound, and
// whether or not its node matches --node-selector.
type WatchedJobs struct {
	listers []corev1listers.PodLister
}

// NewWatchedJobs returns a WatchedJobs over the given pod informers. Call it
// before the informer factories start, so the listers share their caches.
func NewWatchedJobs(podInformers ...corev1informers.PodInformer) *WatchedJobs {
	w := &WatchedJobs{listers: make([]corev1listers.PodLister, 0, len(podInformers))}
	for _, pi := range podInformers {
		w.listers = append(w.listers, pi.Lister())
	}
	return w
}

// Observed reports whether a pod labelled timeslice.io/group=groupID and
// timeslice.io/job-id=jobID is in any of the informers' caches.
func (w *WatchedJobs) Observed(groupID, jobID string) bool {
	selector := labels.SelectorFromSet(labels.Set{PodLabelKey: groupID, JobLabelKey: jobID})
	for _, lister := range w.listers {
		pods, err := lister.List(selector)
		if err == nil && len(pods) > 0 {
			return true
		}
	}
	return false
}
