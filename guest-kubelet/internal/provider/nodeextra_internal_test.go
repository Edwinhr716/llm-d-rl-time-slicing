package provider

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParseNodeLabels(t *testing.T) {
	got, err := ParseNodeLabels(" example.com/run=r1 , team=a ,,")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"example.com/run": "r1", "team": "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("labels = %v, want %v", got, want)
	}
	if got, err := ParseNodeLabels(""); err != nil || len(got) != 0 {
		t.Fatalf("empty: got %v, %v", got, err)
	}
	for _, bad := range []string{
		"novalue",
		"bad key=v",
		"k=bad value",
		VirtualNodeLabel + "=false",
		GuestNodeLabel + "=true",
		"kubernetes.io/hostname=x",
		"a=1,a=2",
	} {
		if _, err := ParseNodeLabels(bad); err == nil {
			t.Errorf("ParseNodeLabels(%q): want error", bad)
		}
	}
}

func TestParseNodeTaints(t *testing.T) {
	got, err := ParseNodeTaints("example.com/run=r1:NoSchedule, dedicated:NoExecute")
	if err != nil {
		t.Fatal(err)
	}
	want := []corev1.Taint{
		{Key: "example.com/run", Value: "r1", Effect: corev1.TaintEffectNoSchedule},
		{Key: "dedicated", Effect: corev1.TaintEffectNoExecute},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("taints = %v, want %v", got, want)
	}
	if got, err := ParseNodeTaints(""); err != nil || len(got) != 0 {
		t.Fatalf("empty: got %v, %v", got, err)
	}
	for _, bad := range []string{
		"k=v",
		"k=v:Sometimes",
		"bad key:NoSchedule",
		GuestTaintKey + "=true:NoSchedule",
		"a:NoSchedule,a=x:NoSchedule",
	} {
		if _, err := ParseNodeTaints(bad); err == nil {
			t.Errorf("ParseNodeTaints(%q): want error", bad)
		}
	}
}

func extraCfg() NodeConfig {
	return NodeConfig{
		Name: "vk-test", InternalIP: "10.0.0.1", KubeletPort: 10260,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
	}
}

// The default (no extras) registers the Node exactly as before.
func TestNewNodeSpec_NoExtras(t *testing.T) {
	spec := NewNodeSpec(extraCfg())
	if want := nodeLabels("vk-test", false); !reflect.DeepEqual(spec.Labels, want) {
		t.Fatalf("labels = %v, want %v", spec.Labels, want)
	}
	want := []corev1.Taint{{Key: GuestTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	if !reflect.DeepEqual(spec.Spec.Taints, want) {
		t.Fatalf("taints = %v, want %v", spec.Spec.Taints, want)
	}
}

func TestNewNodeSpec_Extras(t *testing.T) {
	cfg := extraCfg()
	cfg.ExtraLabels = map[string]string{"example.com/run": "r1"}
	cfg.ExtraTaints = []corev1.Taint{{Key: "example.com/run", Value: "r1", Effect: corev1.TaintEffectNoSchedule}}
	spec := NewNodeSpec(cfg)
	if spec.Labels["example.com/run"] != "r1" || spec.Labels[VirtualNodeLabel] != "true" {
		t.Fatalf("labels = %v", spec.Labels)
	}
	if len(spec.Spec.Taints) != 2 || spec.Spec.Taints[0].Key != GuestTaintKey || spec.Spec.Taints[1] != cfg.ExtraTaints[0] {
		t.Fatalf("taints = %v", spec.Spec.Taints)
	}
	// An extra label never overrides one the VK sets.
	l := withExtraLabels(map[string]string{VirtualNodeLabel: "true"}, map[string]string{VirtualNodeLabel: "false"})
	if l[VirtualNodeLabel] != "true" {
		t.Fatalf("override: %v", l)
	}
}
