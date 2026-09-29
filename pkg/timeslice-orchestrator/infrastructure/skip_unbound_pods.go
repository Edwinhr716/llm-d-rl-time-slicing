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
	"fmt"
	"log/slog"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// skipUnboundPods holds the state of the --skip-unbound-pods option.
type skipUnboundPods struct {
	enabled bool
	// logged holds the UIDs of pods already reported as skipped, so each pod
	// is logged once. An entry is dropped when the pod is deleted.
	logged sync.Map // keyed by pod UID
}

// WithSkipUnboundPods drops pods that are not yet bound to a node
// (spec.nodeName empty) until they bind, instead of counting them toward
// their group. It takes effect only together with WithNodeScopedPods (that
// is, with --node-selector set); without node scoping every pod is kept, as
// before. A pod created already bound, like a virtual kubelet mirror pod, is
// never affected.
func WithSkipUnboundPods() Option {
	return func(k *KubernetesOrchestrator) {
		k.skipUnbound.enabled = true
	}
}

// skipsUnboundPod reports whether the pod is dropped because it is not yet
// bound to a node. The first time it drops a pod that carries a group label
// it logs it.
func (k *KubernetesOrchestrator) skipsUnboundPod(pod *corev1.Pod) bool {
	if !k.skipUnbound.enabled || !k.nodeScopedPods || pod.Spec.NodeName != "" {
		return false
	}
	group := k.getGroupFromPod(pod)
	if group == "" {
		return true
	}
	if _, seen := k.skipUnbound.logged.LoadOrStore(pod.UID, struct{}{}); !seen {
		slog.Info("Skipping unbound pod until it is bound",
			"pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name), "group", group)
	}
	return true
}

// forgetSkippedUnboundPod drops the log-once entry of a deleted pod.
func (k *KubernetesOrchestrator) forgetSkippedUnboundPod(obj interface{}) {
	if !k.skipUnbound.enabled {
		return
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		k.skipUnbound.logged.Delete(pod.UID)
	}
}
