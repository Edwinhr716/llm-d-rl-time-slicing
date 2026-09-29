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
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// A node joins a group through either of two label forms:
//
//   - the donor pair: timeslice.io/donor=true plus timeslice.io/group=<group>.
//     <group> is the same value the group's pods carry in PodLabelKey,
//     normally <namespace>.<job-id>.<worker group>. It is read verbatim, never
//     parsed or shortened; a label value is at most 63 characters.
//   - the older prefix form: group.timeslice.io/<group>=true.
//
// Both forms are always accepted, so nodes labelled either way keep working.
const (
	// DonorLabelKey marks a node that lends its accelerator. Only "true" counts.
	DonorLabelKey = "timeslice.io/donor"
	// NodeGroupLabelKey carries the group on a node. It is the same key the
	// orchestrator reads on pods.
	NodeGroupLabelKey = PodLabelKey
)

// GroupsFromNodeLabels returns the group a node with these labels belongs to,
// as a slice of zero or one element. It fails closed, so the node is in no
// group when:
//   - its labels name more than one group (two prefix labels, or a donor pair
//     and a prefix label that disagree);
//   - either key of the donor pair is present but the pair is incomplete,
//     timeslice.io/donor is not "true", or the group value is not a valid
//     label value, even if a prefix label is also present;
//   - its only prefix labels have a value other than "true".
//
// A donor pair and a prefix label that name the same group give that group.
func GroupsFromNodeLabels(nodeLabels map[string]string) []string {
	named := map[string]bool{}
	if hasDonorPairKey(nodeLabels) {
		group, ok := donorPairGroup(nodeLabels)
		if !ok {
			return nil
		}
		named[group] = true
	}
	for key, value := range nodeLabels {
		if group, ok := strings.CutPrefix(key, NodeLabelPrefix); ok && group != "" && value == "true" {
			named[group] = true
		}
	}
	if len(named) != 1 {
		return nil
	}
	for group := range named {
		return []string{group}
	}
	return nil
}

// NodeInGroup reports whether a node with these labels belongs to group.
func NodeInGroup(nodeLabels map[string]string, group string) bool {
	groups := GroupsFromNodeLabels(nodeLabels)
	return len(groups) == 1 && groups[0] == group
}

// hasDonorPairKey reports whether either key of the donor pair is present.
func hasDonorPairKey(nodeLabels map[string]string) bool {
	_, hasDonor := nodeLabels[DonorLabelKey]
	_, hasGroup := nodeLabels[NodeGroupLabelKey]
	return hasDonor || hasGroup
}

// donorPairGroup returns the group of a complete, valid donor pair.
func donorPairGroup(nodeLabels map[string]string) (string, bool) {
	group := nodeLabels[NodeGroupLabelKey]
	if nodeLabels[DonorLabelKey] != "true" || group == "" || len(validation.IsValidLabelValue(group)) > 0 {
		return "", false
	}
	return group, true
}
