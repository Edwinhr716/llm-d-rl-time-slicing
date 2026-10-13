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
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
)

// The contract's role label and its background value, read literally here
// rather than through a shared constant, so this file stands alone.
const (
	exemptRoleLabelKey   = "timeslice.io/role"
	exemptRoleBackground = "background"
)

// WithNodeSelectorExemptBackground keeps pods labelled
// timeslice.io/role=background in their group even when they are bound to a
// node outside --node-selector. It has an effect only together with
// WithNodeScopedPods. It exempts pods only: the node informer, the group's node
// list and every check that a node belongs to a group are unchanged, so a
// node outside the selector still contributes to no group.
func WithNodeSelectorExemptBackground() Option {
	return func(k *KubernetesOrchestrator) {
		k.exemptBackground = true
	}
}

// keepExemptBackgroundPod reports whether a pod bound to a node outside the
// node informer's scope is kept anyway, because the exemption is on and the
// pod is a background pod. It logs each time it keeps one.
func (k *KubernetesOrchestrator) keepExemptBackgroundPod(ctx context.Context, pod *corev1.Pod) bool {
	if !k.exemptBackground || pod.Labels[exemptRoleLabelKey] != exemptRoleBackground {
		return false
	}
	slog.InfoContext(ctx, "Keeping background pod bound to a node outside --node-selector",
		"pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
		"node", pod.Spec.NodeName,
		"group", pod.Labels[PodLabelKey])
	return true
}
