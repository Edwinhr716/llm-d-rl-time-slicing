// Package donorcontroller is a minimal donor controller. It labels a real node when a donor pod
// lands on it, and removes those labels after a TTL with no donor pods on the node and no
// foreground lock activity in the group (an era ends). It touches only Node objects: it reads
// pods, patches node labels and annotations, and reads GetGroupStatus from the orchestrator.
// Optionally (--release-dead-hosts) it also releases the virtual Node of a host that is gone.
package donorcontroller

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Label key shapes, in the vocabulary of the orchestrator's --node-group-labels flag.
const (
	// LabelKeysPrefix writes group.timeslice.io/<group>=true.
	LabelKeysPrefix = "prefix"
	// LabelKeysNS writes timeslice.io/donor=true and timeslice.io/group=<group>.
	LabelKeysNS = "ns"
)

// Kubernetes names the controller reads or writes.
const (
	// PrefixGroupKeyPrefix is the key prefix of the prefix shape.
	PrefixGroupKeyPrefix = "group.timeslice.io/"
	// DonorLabelKey marks a donor host in the ns shape.
	DonorLabelKey = "timeslice.io/donor"
	// GroupLabelKey carries the group: on donor pods always, on nodes in the ns shape.
	GroupLabelKey = "timeslice.io/group"
	// VirtualNodeLabel marks a virtual (VK) Node. Pods bound to such a Node never count as donors.
	VirtualNodeLabel = "timeslice.io/virtual-node"
	// VirtualNodeFinalizer is the VK's finalizer on its virtual Node (D-VK-2 option c).
	VirtualNodeFinalizer = "timeslice.io/virtual-node-protection"

	// AnnotationLabelledBy marks a node whose labels this controller owns. Nodes without it are
	// never unlabelled (hand labels stay).
	AnnotationLabelledBy = "timeslice.io/labelled-by"
	// LabelledByValue is the value of AnnotationLabelledBy.
	LabelledByValue = "donor-controller"
	// AnnotationLabelledGroup records the group the controller labelled the node for.
	AnnotationLabelledGroup = "timeslice.io/labelled-group"
	// AnnotationLabelledKeys records the label keys the controller wrote, comma separated. Only
	// these keys are removed at era end.
	AnnotationLabelledKeys = "timeslice.io/labelled-keys"
	// AnnotationEraIdleSince persists the era condition start (RFC3339), so a restart resumes the
	// countdown from it and never from an earlier point.
	AnnotationEraIdleSince = "timeslice.io/era-idle-since"
)

// ValidateLabelKeys checks a --label-keys value.
func ValidateLabelKeys(mode string) error {
	switch mode {
	case LabelKeysPrefix, LabelKeysNS:
		return nil
	default:
		return fmt.Errorf("--label-keys=%q: want %q or %q", mode, LabelKeysPrefix, LabelKeysNS)
	}
}

// NodeLabelsFor returns the node labels that mark a donor host of group in the given shape.
func NodeLabelsFor(mode, group string) map[string]string {
	if mode == LabelKeysNS {
		return map[string]string{DonorLabelKey: "true", GroupLabelKey: group}
	}
	return map[string]string{PrefixGroupKeyPrefix + group: "true"}
}

// ValidateGroup reports whether group can be written in the given shape.
func ValidateGroup(mode, group string) error {
	if group == "" {
		return fmt.Errorf("empty group")
	}
	if errs := validation.IsValidLabelValue(group); len(errs) > 0 {
		return fmt.Errorf("group %q is not a valid label value: %s", group, strings.Join(errs, "; "))
	}
	if mode == LabelKeysPrefix {
		if errs := validation.IsQualifiedName(PrefixGroupKeyPrefix + group); len(errs) > 0 {
			return fmt.Errorf("group %q is not valid in a label key: %s", group, strings.Join(errs, "; "))
		}
	}
	return nil
}

// IsFamilyKey reports whether a node label key belongs to the donor label family (either shape).
func IsFamilyKey(key string) bool {
	return key == DonorLabelKey || key == GroupLabelKey || strings.HasPrefix(key, PrefixGroupKeyPrefix)
}

// FamilyGroups returns the groups named by donor-family labels on a node, sorted.
func FamilyGroups(nodeLabels map[string]string) []string {
	seen := map[string]bool{}
	for key, val := range nodeLabels {
		switch {
		case key == GroupLabelKey && val != "":
			seen[val] = true
		case strings.HasPrefix(key, PrefixGroupKeyPrefix):
			seen[strings.TrimPrefix(key, PrefixGroupKeyPrefix)] = true
		}
	}
	out := make([]string, 0, len(seen))
	for grp := range seen {
		out = append(out, grp)
	}
	sort.Strings(out)
	return out
}

// hasFamilyKey reports whether any donor-family key is on the node.
func hasFamilyKey(nodeLabels map[string]string) bool {
	for key := range nodeLabels {
		if IsFamilyKey(key) {
			return true
		}
	}
	return false
}

// sortedKeys returns the keys of m, sorted.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
