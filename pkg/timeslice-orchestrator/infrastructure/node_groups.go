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
	"strings"
)

// Node group label modes, selected with --node-group-labels. They decide which
// node labels make a node a member of a group. PENDING LEAD DECISION D-NS-1.
const (
	// NodeGroupLabelsPrefix reads group.timeslice.io/<group>=true (the default).
	NodeGroupLabelsPrefix = "prefix"
	// NodeGroupLabelsNS reads timeslice.io/donor=true plus
	// timeslice.io/group=<group>. The group value is taken as written; it is
	// expected to be the full <namespace>.<job-id>.<group> form, the same value
	// the donor pods carry in PodLabelKey.
	NodeGroupLabelsNS = "ns"
	// NodeGroupLabelsEither accepts either form. A node whose two forms name
	// different groups belongs to no group.
	NodeGroupLabelsEither = "either"
)

const (
	// DonorLabelKey marks a node that lends its accelerator (ns and either modes).
	DonorLabelKey = "timeslice.io/donor"
	// NodeGroupLabelKey carries the group on a node (ns and either modes). It is
	// the same key the orchestrator reads on pods.
	NodeGroupLabelKey = PodLabelKey
)

// ValidateNodeGroupLabels returns an error unless mode is a known mode.
func ValidateNodeGroupLabels(mode string) error {
	switch mode {
	case NodeGroupLabelsPrefix, NodeGroupLabelsNS, NodeGroupLabelsEither:
		return nil
	}
	return fmt.Errorf("unknown node group label mode %q (want %s, %s or %s)",
		mode, NodeGroupLabelsPrefix, NodeGroupLabelsNS, NodeGroupLabelsEither)
}

// RecommendedNodeSelector is the --node-selector value to pair with a mode.
// A label selector cannot match a key prefix, so prefix and either watch every
// node; ns can narrow the node watch to donor nodes.
func RecommendedNodeSelector(mode string) string {
	if mode == NodeGroupLabelsNS {
		return DonorLabelKey + "=true"
	}
	return ""
}

// GroupsFromNodeLabels returns the group a node with these labels belongs to
// under mode, as a slice of zero or one element. It fails closed: a node whose
// labels name more than one group, a half-written ns pair, a value other than
// "true" on a prefix label, or an unknown mode yields no group.
func GroupsFromNodeLabels(mode string, l map[string]string) []string {
	var groups []string
	switch mode {
	case NodeGroupLabelsPrefix, "":
		groups = prefixNodeGroups(l)
	case NodeGroupLabelsNS:
		groups = nsNodeGroups(l)
	case NodeGroupLabelsEither:
		groups = eitherNodeGroups(l)
	}
	if len(groups) != 1 {
		return nil
	}
	return groups
}

// NodeInGroup reports whether a node with these labels belongs to group under mode.
func NodeInGroup(mode string, l map[string]string, group string) bool {
	for _, g := range GroupsFromNodeLabels(mode, l) {
		if g == group {
			return true
		}
	}
	return false
}

func prefixNodeGroups(l map[string]string) []string {
	var groups []string
	for k, v := range l {
		g, ok := strings.CutPrefix(k, NodeLabelPrefix)
		if ok && g != "" && v == "true" {
			groups = append(groups, g)
		}
	}
	return groups
}

func nsNodeGroups(l map[string]string) []string {
	if l[DonorLabelKey] != "true" || l[NodeGroupLabelKey] == "" {
		return nil
	}
	return []string{l[NodeGroupLabelKey]}
}

func eitherNodeGroups(l map[string]string) []string {
	p, n := prefixNodeGroups(l), nsNodeGroups(l)
	if len(p) == 1 && len(n) == 1 && p[0] == n[0] {
		return n
	}
	return append(p, n...)
}
