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
// The older form group.timeslice.io/<group>=true is read too; if both forms are present they
// must name the same group. The orchestrator applies the same rule, so both name the same group
// for the same labels.
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
	// PrefixKey is the older form, group.timeslice.io/<group>=true.
	PrefixKey = "group.timeslice.io/"
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
	ReasonHostNodeGone      = "host-node-gone"
)

// EventGroupUnresolved is the reason of the Warning event on the virtual Node while the host
// node resolves to no group.
const EventGroupUnresolved = "GroupUnresolved"

// FromNodeLabels returns the group the node with labels l yields to, as a list of zero or one
// element, and why. It reads the donor/group pair and the prefix form: exactly one group
// between them resolves; two groups, a half-written pair (even next to a prefix label), a donor
// value other than "true" or an invalid group value resolve to none.
//
//nolint:gocritic // unnamedResult; the (groups, reason) pair is the harness API and nonamedreturns forbids names
func FromNodeLabels(l map[string]string) ([]string, string) {
	set := map[string]bool{}
	if hasPairLabels(l) {
		pair := fromPair(l)
		if len(pair.Groups) == 0 {
			// A half-written pair next to a prefix label is ambiguous too: fail closed.
			return nil, pair.Reason
		}
		set[pair.Groups[0]] = true
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
		return names, ReasonOK
	case len(names) > 1:
		return nil, ReasonTwoGroups + ":" + strings.Join(names, ",")
	case notTrue:
		return nil, ReasonPrefixNotTrue
	}
	return nil, ReasonNotShared
}

// fromPair reads the donor/group pair: one group only if both are set and valid.
func fromPair(l map[string]string) Result {
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

// hasPairLabels reports whether either key of the pair is present.
func hasPairLabels(l map[string]string) bool {
	_, d := l[LabelDonor]
	_, g := l[LabelGroup]
	return d || g
}
