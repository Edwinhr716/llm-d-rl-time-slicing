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
	"crypto/sha256"
	"encoding/hex"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

// GroupTokenPrefix starts every opaque group token.
const GroupTokenPrefix = "gt-"

// GroupToken is the opaque value a guest-kubelet started with --mirror-group-label=token
// writes in PodLabelKey on its mirrors, so a mirror in the guest's namespace does not name the
// owner's namespace. It must match guest-kubelet's
// internal/group.Token; both tests pin the same vector.
func GroupToken(group string) string {
	sum := sha256.Sum256([]byte("timeslice.io/group-token/v1:" + group))
	return GroupTokenPrefix + hex.EncodeToString(sum[:])[:32]
}

// IsGroupToken reports whether a PodLabelKey value is a GroupToken.
func IsGroupToken(v string) bool {
	return strings.HasPrefix(v, GroupTokenPrefix) && len(v) == len(GroupTokenPrefix)+32
}

// GroupSelector matches the pods of group: by name (donors, plain mirrors) or by token
// (token mirrors). extra adds equality terms.
func GroupSelector(group string, extra labels.Set) labels.Selector {
	req, err := labels.NewRequirement(PodLabelKey, selection.In, []string{group, GroupToken(group)})
	if err != nil {
		// group is not a valid label value: nothing carries it, keep the old equality match.
		set := labels.Set{PodLabelKey: group}
		for k, v := range extra {
			set[k] = v
		}
		return labels.SelectorFromSet(set)
	}
	sel := labels.NewSelector().Add(*req)
	for k, v := range extra {
		r, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			return labels.Nothing()
		}
		sel = sel.Add(*r)
	}
	return sel
}

// groupOfToken maps a token label back to a group through the pod's node: a mirror runs on
// the host whose labels name the group. "" when the node is unknown or names another group.
func (k *KubernetesOrchestrator) groupOfToken(pod *corev1.Pod, token string) string {
	if k.nodeLister == nil || pod.Spec.NodeName == "" {
		return ""
	}
	node, err := k.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return ""
	}
	for _, g := range GroupsFromNodeLabels(k.nodeGroupLabelMode(), node.Labels) {
		if GroupToken(g) == token {
			return g
		}
	}
	return ""
}
