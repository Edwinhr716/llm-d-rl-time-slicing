package mirror

import (
	"strings"
	"testing"

	"github.com/edwinhr716/guest-kubelet/internal/group"
)

// With GroupToken the mirror carries no owner-namespace text.
func TestBuild_GroupTokenHidesOwnerNamespace(t *testing.T) {
	cfg := testConfig()
	cfg.Group = "owner-ns.trainer.workers"
	plain, err := Build(testGuest(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Labels[LabelGroup] != cfg.Group {
		t.Fatalf("plain label = %q", plain.Labels[LabelGroup])
	}
	cfg.GroupToken = true
	mir, err := BuildWithGPU(testGuest(), &cfg, testAttachment())
	if err != nil {
		t.Fatal(err)
	}
	if got := mir.Labels[LabelGroup]; got != group.Token(cfg.Group) {
		t.Fatalf("token label = %q", got)
	}
	for k, v := range mir.Labels {
		if strings.Contains(v, "owner-ns") || strings.Contains(k, "owner-ns") {
			t.Errorf("label %s=%s names the owner namespace", k, v)
		}
	}
	for k, v := range mir.Annotations {
		if strings.Contains(v, "owner-ns") {
			t.Errorf("annotation %s=%s names the owner namespace", k, v)
		}
	}
}
