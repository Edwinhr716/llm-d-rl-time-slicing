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
	s := "ev-d-vk-3-r01.ev-rc-r01.trainers"
	return s + strings.Repeat("x", 63-len(s))
}()

type labels = map[string]string

func TestFromNodeLabels(t *testing.T) {
	cases := []struct {
		name   string
		labels labels
		ns     string // want group for --group-source=ns, "" = none
		either string // want group for --group-source=either
		reason string // want reason for --group-source=ns
	}{
		{"C1 prefix only", labels{pfx + full: "true"}, "", full, group.ReasonNotShared},
		{"C2 donor and group", labels{donor: "true", grp: full}, full, full, group.ReasonOK},
		{"C3 both forms same group", labels{pfx + full: "true", donor: "true", grp: full}, full, full, group.ReasonOK},
		{"C4 both forms, two groups", labels{pfx + other: "true", donor: "true", grp: full}, full, "", group.ReasonOK},
		{"C5 group without donor", labels{grp: full}, "", "", group.ReasonGroupWithoutDonor},
		{"C6 donor without group", labels{donor: "true"}, "", "", group.ReasonDonorWithoutGroup},
		{"C7 prefix false", labels{pfx + full: "false"}, "", "", group.ReasonNotShared},
		{"C8 two prefix groups", labels{pfx + full: "true", pfx + other: "true"}, "", "", group.ReasonNotShared},
		{"C9 no labels", nil, "", "", group.ReasonNotShared},
		{"C10 63-char value", labels{donor: "true", grp: long63}, long63, long63, group.ReasonOK},
		{"64-char value refused", labels{donor: "true", grp: long63 + "x"}, "", "", group.ReasonInvalidValue},
		{"donor not true", labels{donor: "false", grp: full}, "", "", group.ReasonDonorNotTrue},
		{"empty group value", labels{donor: "true", grp: ""}, "", "", group.ReasonDonorWithoutGroup},
		{"half pair next to prefix", labels{pfx + full: "true", grp: full}, "", "", group.ReasonGroupWithoutDonor},
		{"unrelated labels", labels{"kubernetes.io/hostname": "n1"}, "", "", group.ReasonNotShared},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, s := range []struct{ source, want string }{{group.SourceNS, c.ns}, {group.SourceEither, c.either}} {
				got, reason := group.FromNodeLabels(s.source, c.labels)
				if s.want == "" {
					if len(got) != 0 {
						t.Errorf("%s: want no group, got %v (%s)", s.source, got, reason)
					}
					if reason == group.ReasonOK {
						t.Errorf("%s: no group must come with a reason, got ok", s.source)
					}
				} else if len(got) != 1 || got[0] != s.want || reason != group.ReasonOK {
					t.Errorf("%s: want [%s], got %v (%s)", s.source, s.want, got, reason)
				}
				if s.source == group.SourceNS && reason != c.reason {
					t.Errorf("ns reason: want %s, got %s", c.reason, reason)
				}
				if strings.ContainsAny(reason, " \t\n") {
					t.Errorf("reason %q must be one token", reason)
				}
			}
		})
	}
}

// The value is read verbatim: never parsed, shortened or hashed.
func TestValueIsVerbatim(t *testing.T) {
	for _, v := range []string{full, "short", long63, "a.b.c.d"} {
		got, _ := group.FromNodeLabels(group.SourceNS, labels{donor: "true", grp: v})
		if len(got) != 1 || got[0] != v {
			t.Errorf("%q -> %v", v, got)
		}
	}
}

func TestEitherReasons(t *testing.T) {
	_, r := group.FromNodeLabels(group.SourceEither, labels{pfx + other: "true", donor: "true", grp: full})
	if r != group.ReasonTwoGroups+":"+full+","+other {
		t.Errorf("C4 reason: %s", r)
	}
	_, r = group.FromNodeLabels(group.SourceEither, labels{pfx + full: "false"})
	if r != group.ReasonPrefixNotTrue {
		t.Errorf("C7 reason: %s", r)
	}
}

func TestUnknownSource(t *testing.T) {
	for _, s := range []string{"", "prefix", "NS"} {
		g, r := group.FromNodeLabels(s, labels{donor: "true", grp: full})
		if len(g) != 0 || r != group.ReasonUnknownSource {
			t.Errorf("source %q: %v %s", s, g, r)
		}
		if group.ValidSource(s) {
			t.Errorf("source %q must be invalid", s)
		}
	}
	for _, s := range group.Sources {
		if !group.ValidSource(s) {
			t.Errorf("source %q must be valid", s)
		}
	}
}
