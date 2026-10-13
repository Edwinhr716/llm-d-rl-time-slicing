package webhook_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

func TestGroupName_Short(t *testing.T) {
	if got := webhook.GroupName("team-a", "rc-7f2x9", "trainers"); got != "team-a.rc-7f2x9.trainers" {
		t.Errorf("GroupName = %q", got)
	}
	exact := strings.Repeat("n", 20) + "." + strings.Repeat("j", 21) + "." + strings.Repeat("g", 20)
	if got := webhook.GroupName(strings.Repeat("n", 20), strings.Repeat("j", 21), strings.Repeat("g", 20)); got != exact {
		t.Errorf("63-character group changed: %q", got)
	}
}

func TestGroupName_LongIsValidStableAndDistinct(t *testing.T) {
	ns := strings.Repeat("n", 40)
	seen := map[string]string{}
	for _, jobID := range []string{strings.Repeat("j", 30), strings.Repeat("j", 30) + "x", strings.Repeat("j", 7) + "-"} {
		for _, group := range []string{"trainers", "trainers-2", strings.Repeat("g", 80)} {
			got := webhook.GroupName(ns, jobID, group)
			if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
				t.Errorf("GroupName(%s, %s) = %q: %v", jobID, group, got, errs)
			}
			if again := webhook.GroupName(ns, jobID, group); again != got {
				t.Errorf("not stable: %q vs %q", got, again)
			}
			key := jobID + "/" + group
			if prev, dup := seen[got]; dup {
				t.Errorf("%s and %s both map to %q", prev, key, got)
			}
			seen[got] = key
		}
	}
}
