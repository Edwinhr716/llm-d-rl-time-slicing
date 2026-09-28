// Package group is the only place the guest-kubelet reads which RL group its real node belongs
// to. Option a of decision D-VK-3: the real node carries the label
// group.timeslice.io/<group>=true, applied by hand (the demo manifests), and the group is <group>.
//
// Fail closed: a node that resolves to anything other than exactly one group gets no mirror.
package group

import (
	"sort"
	"strings"
)

const (
	// NodeLabelPrefix is the real node's group label prefix; the group is the key's name part.
	NodeLabelPrefix = "group.timeslice.io/"
	// MirrorLabel is the mirror pod label that carries the resolved group (contract §3).
	MirrorLabel = "timeslice.io/group"
	// Source names this branch's label form in the "group resolved" log line.
	Source = "prefix"
)

// Reason says why a host resolved the way it did. Each value is one token, so the log line
// stays greppable.
type Reason string

// Reasons returned by FromNodeLabels and Resolver.Group.
const (
	ReasonOK             Reason = "ok"
	ReasonNoLabel        Reason = "no-group-label"
	ReasonPartial        Reason = "partial-label"   // a group.timeslice.io/* key whose value is not "true", or with no name
	ReasonMultipleGroups Reason = "multiple-groups" // more than one group.timeslice.io/<g>=true: refused
	ReasonHostNotFound   Reason = "host-node-not-found"
	ReasonNotStarted     Reason = "not-started"
)

// FromNodeLabels returns the groups a real node belongs to and the reason (hook V1: groups, reason). source is the
// --group-source value and is ignored on this branch (there is only one label form).
//
// A key counts only when its value is exactly "true" and its name part is not empty; any other
// group.timeslice.io/* key is a partial label and is not a membership. Two or more groups are
// refused: groups is empty and reason is ReasonMultipleGroups. So a non-empty result always has
// exactly one element.
func FromNodeLabels(_ string, l map[string]string) ([]string, Reason) {
	found := make([]string, 0, 1)
	partial := false
	for k, v := range l {
		name, ok := strings.CutPrefix(k, NodeLabelPrefix)
		if !ok {
			continue
		}
		if name == "" || v != "true" {
			partial = true
			continue
		}
		found = append(found, name)
	}
	sort.Strings(found)
	switch {
	case len(found) == 1:
		return found, ReasonOK
	case len(found) > 1:
		return nil, ReasonMultipleGroups
	case partial:
		return nil, ReasonPartial
	default:
		return nil, ReasonNoLabel
	}
}

// One returns the single group, or "" when the labels do not resolve to exactly one.
func One(groups []string) string {
	if len(groups) != 1 {
		return ""
	}
	return groups[0]
}
