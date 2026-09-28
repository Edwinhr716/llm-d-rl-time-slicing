package group_test

import (
	"strings"
	"testing"

	"github.com/edwinhr716/guest-kubelet/internal/group"
)

const (
	full  = "team-a.rc-7f2k9.trainers" // <namespace>.<job-id>.<worker group>
	other = "team-b.rc-11111.trainers"

	donor = group.LabelDonor
	grp   = group.LabelGroup
	pfx   = group.PrefixKey
)

// long63 is a full-form value of exactly 63 characters, the label value limit (case C10).
var long63 = func() string {
	s := "team-a-namespace.rc-7f2k9x.trainers"
	return s + strings.Repeat("x", 63-len(s))
}()

type labels = map[string]string

// pairAndTwoPrefixes has the pair and two prefix labels: two groups, full and other.
var pairAndTwoPrefixes = labels{pfx + full: "true", pfx + other: "true", donor: "true", grp: full}

// The same case table as the orchestrator's node group tests (plus "unrelated labels"): for the
// same node labels both sides must name the same group, or both none.
func TestFromNodeLabels(t *testing.T) {
	cases := []struct {
		name   string
		labels labels
		want   string // want group, "" = none
		reason string // want reason; two-groups is followed by ":<names>"
	}{
		{"C1 prefix only", labels{pfx + full: "true"}, full, group.ReasonOK},
		{"C2 donor and group", labels{donor: "true", grp: full}, full, group.ReasonOK},
		{"C3 both forms same group", labels{pfx + full: "true", donor: "true", grp: full}, full, group.ReasonOK},
		{"C4 both forms, two groups", labels{pfx + other: "true", donor: "true", grp: full}, "", group.ReasonTwoGroups},
		{"C5 group without donor", labels{grp: full}, "", group.ReasonGroupWithoutDonor},
		{"C6 donor without group", labels{donor: "true"}, "", group.ReasonDonorWithoutGroup},
		{"C7 prefix false", labels{pfx + full: "false"}, "", group.ReasonPrefixNotTrue},
		{"C8 two prefix groups", labels{pfx + full: "true", pfx + other: "true"}, "", group.ReasonTwoGroups},
		{"C9 no labels", nil, "", group.ReasonNotShared},
		{"C10 63-char value", labels{donor: "true", grp: long63}, long63, group.ReasonOK},
		{"64-char value refused", labels{donor: "true", grp: long63 + "x"}, "", group.ReasonInvalidValue},
		{"donor not true", labels{donor: "false", grp: full}, "", group.ReasonDonorNotTrue},
		{"empty group value", labels{donor: "true", grp: ""}, "", group.ReasonDonorWithoutGroup},
		{"empty prefix suffix", labels{pfx: "true"}, "", group.ReasonNotShared},
		{"group without donor next to prefix", labels{pfx + full: "true", grp: full}, "", group.ReasonGroupWithoutDonor},
		{"donor without group next to prefix", labels{pfx + full: "true", donor: "true"}, "", group.ReasonDonorWithoutGroup},
		{"donor not true next to prefix", labels{pfx + full: "true", donor: "false", grp: full}, "", group.ReasonDonorNotTrue},
		{"prefix false next to donor pair", labels{pfx + other: "false", donor: "true", grp: full}, full, group.ReasonOK},
		{"donor pair plus two prefix groups", pairAndTwoPrefixes, "", group.ReasonTwoGroups},
		{"unrelated labels", labels{"kubernetes.io/hostname": "n1"}, "", group.ReasonNotShared},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := group.FromNodeLabels(c.labels)
			if c.want == "" {
				if len(got) != 0 {
					t.Errorf("want no group, got %v (%s)", got, reason)
				}
			} else if len(got) != 1 || got[0] != c.want {
				t.Errorf("want [%s], got %v (%s)", c.want, got, reason)
			}
			if reason != c.reason && !strings.HasPrefix(reason, c.reason+":") {
				t.Errorf("reason: want %s, got %s", c.reason, reason)
			}
			if strings.ContainsAny(reason, " \t\n") {
				t.Errorf("reason %q must be one token", reason)
			}
		})
	}
}

// The value is read verbatim: never parsed, shortened or hashed.
func TestValueIsVerbatim(t *testing.T) {
	for _, v := range []string{full, "short", long63, "a.b.c.d"} {
		got, _ := group.FromNodeLabels(labels{donor: "true", grp: v})
		if len(got) != 1 || got[0] != v {
			t.Errorf("%q -> %v", v, got)
		}
	}
}

// The two-groups reason names both groups, sorted.
func TestTwoGroupsReason(t *testing.T) {
	_, r := group.FromNodeLabels(labels{pfx + other: "true", donor: "true", grp: full})
	if r != group.ReasonTwoGroups+":"+full+","+other {
		t.Errorf("C4 reason: %s", r)
	}
}
