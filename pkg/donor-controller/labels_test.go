package donorcontroller_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
)

func TestNodeLabelsFor(t *testing.T) {
	group := "ns1.job1.trainers"
	if got, want := donorcontroller.NodeLabelsFor(donorcontroller.LabelKeysPrefix, group),
		map[string]string{"group.timeslice.io/" + group: "true"}; !maps.Equal(got, want) {
		t.Errorf("prefix labels = %v, want %v", got, want)
	}
	if got, want := donorcontroller.NodeLabelsFor(donorcontroller.LabelKeysNS, group),
		map[string]string{"timeslice.io/donor": "true", "timeslice.io/group": group}; !maps.Equal(got, want) {
		t.Errorf("ns labels = %v, want %v", got, want)
	}
}

func TestValidateGroup(t *testing.T) {
	long := strings.Repeat("a", 60)
	cases := []struct {
		mode, group string
		ok          bool
	}{
		{donorcontroller.LabelKeysPrefix, "ns1.job1.trainers", true},
		{donorcontroller.LabelKeysNS, "ns1.job1.trainers", true},
		{donorcontroller.LabelKeysNS, "", false},
		{donorcontroller.LabelKeysNS, "bad/group", false},
		{donorcontroller.LabelKeysNS, long, true},
		{donorcontroller.LabelKeysPrefix, long, true},
		{donorcontroller.LabelKeysPrefix, strings.Repeat("a", 63) + "b", false},
	}
	for _, tc := range cases {
		if err := donorcontroller.ValidateGroup(tc.mode, tc.group); (err == nil) != tc.ok {
			t.Errorf("ValidateGroup(%s, %q) = %v, want ok=%v", tc.mode, tc.group, err, tc.ok)
		}
	}
	if err := donorcontroller.ValidateLabelKeys("both"); err == nil {
		t.Error("ValidateLabelKeys accepted an unknown shape")
	}
}

func TestFamilyGroups(t *testing.T) {
	got := donorcontroller.FamilyGroups(map[string]string{
		"group.timeslice.io/b": "true", "timeslice.io/group": "a", "timeslice.io/donor": "true", "other": "x",
	})
	if want := []string{"a", "b"}; !slices.Equal(got, want) {
		t.Errorf("FamilyGroups = %v, want %v", got, want)
	}
	if donorcontroller.IsFamilyKey("timeslice.io/virtual-node") {
		t.Error("the virtual-node label is not a donor-family key")
	}
}

func TestArgsFromEnv(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"__ARGS__", nil},
		{"  --a=1  ", []string{"--a=1"}},
		{
			"--label-keys=ns --era-ttl=90s --group-filter=^ev-run\\.",
			[]string{"--label-keys=ns", "--era-ttl=90s", "--group-filter=^ev-run\\."},
		},
	}
	for _, tc := range cases {
		if got := donorcontroller.ArgsFromEnv(tc.in); !slices.Equal(got, tc.want) {
			t.Errorf("ArgsFromEnv(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if !donorcontroller.IsPlaceholder("__ORCH_ADDR__") || donorcontroller.IsPlaceholder("orch:50051") {
		t.Error("IsPlaceholder")
	}
	if got, want := donorcontroller.SplitNamespaces(" a, ,b "), []string{"a", "b"}; !slices.Equal(got, want) {
		t.Errorf("SplitNamespaces = %q, want %q", got, want)
	}
}
