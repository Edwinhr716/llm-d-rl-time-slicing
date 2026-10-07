// Package group is the only place the guest kubelet reads which group its real node yields to.
//
// The group comes from two labels on the real (host) node, as a donor controller writes them
// when a donor pod lands there:
//
//	timeslice.io/donor: "true"
//	timeslice.io/group: <group>
//
// <group> is the same value as the donor pods' timeslice.io/group label, in the full
// "<namespace>.<job-id>.<worker group>" form. It is read verbatim: never parsed, truncated or
// hashed, so the VK, the orchestrator and the donor pods always compare the same string. A
// label value is at most 63 characters (the API server rejects longer ones); shortening a
// longer "<namespace>.<job-id>.<worker group>" is the writer's job, not the reader's.
//
// Anything ambiguous or half-written resolves to no group (fail closed): the caller then
// starts no mirror.
package group

import (
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// LabelDonor marks a node that runs donor pods. Only the value "true" counts.
	LabelDonor = "timeslice.io/donor"
	// LabelGroup names the group that yields on the node. Also the mirror pod label.
	LabelGroup = "timeslice.io/group"
	// PrefixKey is the older form, group.timeslice.io/<group>=true. Read only with SourceEither.
	PrefixKey = "group.timeslice.io/"

	// SourceNS reads only the donor/group pair (the default).
	SourceNS = "ns"
	// SourceEither also accepts the prefix form; both forms together must name the same group.
	SourceEither = "either"
)

// Reasons returned with the groups. They contain no spaces, so they log as one token.
const (
	ReasonOK                = "ok"
	ReasonNotShared         = "no-group-labels"
	ReasonGroupWithoutDonor = "group-without-donor"
	ReasonDonorWithoutGroup = "donor-without-group"
	ReasonDonorNotTrue      = "donor-not-true"
	ReasonInvalidValue      = "invalid-group-value"
	ReasonPrefixNotTrue     = "prefix-not-true"
	ReasonTwoGroups         = "two-groups"
	ReasonUnknownSource     = "unknown-source"
	ReasonHostNodeGone      = "host-node-gone"
)

// EventGroupUnresolved is the reason of the Warning event on the virtual Node while the host
// node resolves to no group.
const EventGroupUnresolved = "GroupUnresolved"

// Sources lists the valid --group-source values.
var Sources = []string{SourceNS, SourceEither}

// ValidSource reports whether s is a valid --group-source value.
func ValidSource(s string) bool { return s == SourceNS || s == SourceEither }

// FromNodeLabels returns the group the node with labels l yields to, as a list of zero or one
// element, and why. A node in two groups, or with a half-written pair, gets no group.
//
//nolint:gocritic // unnamedResult; the (groups, reason) pair is the harness API and nonamedreturns forbids names
func FromNodeLabels(source string, l map[string]string) ([]string, string) {
	var res Result
	switch source {
	case SourceNS:
		res = fromNSLabels(l)
	case SourceEither:
		res = fromEither(l)
	default:
		res = Result{Reason: ReasonUnknownSource}
	}
	return res.Groups, res.Reason
}

// fromNSLabels reads the donor/group pair: one group only if both are set and valid.
func fromNSLabels(l map[string]string) Result {
	donor, hasDonor := l[LabelDonor]
	grp, hasGroup := l[LabelGroup]
	switch {
	case !hasDonor && !hasGroup:
		return Result{Reason: ReasonNotShared}
	case !hasDonor:
		return Result{Reason: ReasonGroupWithoutDonor}
	case donor != "true":
		return Result{Reason: ReasonDonorNotTrue}
	case !hasGroup || grp == "":
		return Result{Reason: ReasonDonorWithoutGroup}
	case len(validation.IsValidLabelValue(grp)) > 0:
		return Result{Reason: ReasonInvalidValue}
	}
	return Result{Groups: []string{grp}, Reason: ReasonOK}
}

// hasNSLabels reports whether either key of the pair is present.
func hasNSLabels(l map[string]string) bool {
	_, d := l[LabelDonor]
	_, g := l[LabelGroup]
	return d || g
}

func fromEither(l map[string]string) Result {
	set := map[string]bool{}
	if hasNSLabels(l) {
		ns := fromNSLabels(l)
		if len(ns.Groups) == 0 {
			// A half-written pair next to a prefix label is ambiguous too: fail closed.
			return ns
		}
		set[ns.Groups[0]] = true
	}
	notTrue := false
	for k, v := range l {
		name, ok := strings.CutPrefix(k, PrefixKey)
		if !ok || name == "" {
			continue
		}
		if v != "true" {
			notTrue = true
			continue
		}
		set[name] = true
	}
	names := make([]string, 0, len(set))
	for g := range set {
		names = append(names, g)
	}
	sort.Strings(names)
	switch {
	case len(names) == 1:
		return Result{Groups: names, Reason: ReasonOK}
	case len(names) > 1:
		return Result{Reason: ReasonTwoGroups + ":" + strings.Join(names, ",")}
	case notTrue:
		return Result{Reason: ReasonPrefixNotTrue}
	}
	return Result{Reason: ReasonNotShared}
}
