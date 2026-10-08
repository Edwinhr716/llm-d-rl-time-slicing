package provider

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Extra labels and taints on the virtual Node (--node-labels, --node-taints). They are part of the
// Node the VK registers, so a guest meant for another install or test run can never bind to the
// Node in the gap between registration and a later `kubectl label/taint`. Both are empty by
// default, which leaves the Node exactly as before.

// ParseNodeLabels parses "k=v,k2=v2" into a map. Keys the VK sets itself (nodeLabels) are refused,
// so an extra label never changes what the virtual Node is.
func ParseNodeLabels(s string) (map[string]string, error) {
	out := map[string]string{}
	reserved := nodeLabels("x", true)
	for _, item := range splitList(s) {
		k, v, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("node label %q: want key=value", item)
		}
		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			return nil, fmt.Errorf("node label key %q: %s", k, strings.Join(errs, "; "))
		}
		if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
			return nil, fmt.Errorf("node label value %q: %s", v, strings.Join(errs, "; "))
		}
		if _, r := reserved[k]; r {
			return nil, fmt.Errorf("node label %q is set by the guest kubelet itself", k)
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("node label %q given twice", k)
		}
		out[k] = v
	}
	return out, nil
}

// ParseNodeTaints parses "k=v:Effect,k2:Effect" into taints. The guest taint key is refused: the
// VK always sets it, and a second value would make guests unschedulable.
func ParseNodeTaints(s string) ([]corev1.Taint, error) {
	var out []corev1.Taint
	seen := map[string]bool{}
	for _, item := range splitList(s) {
		kv, effect, ok := strings.Cut(item, ":")
		if !ok {
			return nil, fmt.Errorf("node taint %q: want key[=value]:Effect", item)
		}
		k, v, _ := strings.Cut(kv, "=")
		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			return nil, fmt.Errorf("node taint key %q: %s", k, strings.Join(errs, "; "))
		}
		if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
			return nil, fmt.Errorf("node taint value %q: %s", v, strings.Join(errs, "; "))
		}
		e := corev1.TaintEffect(effect)
		switch e {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
		default:
			return nil, fmt.Errorf("node taint %q: effect %q is not NoSchedule, PreferNoSchedule or NoExecute", item, effect)
		}
		if k == GuestTaintKey {
			return nil, fmt.Errorf("node taint %q: %s is set by the guest kubelet itself", item, GuestTaintKey)
		}
		if seen[k+":"+effect] {
			return nil, fmt.Errorf("node taint %q given twice", item)
		}
		seen[k+":"+effect] = true
		out = append(out, corev1.Taint{Key: k, Value: v, Effect: e})
	}
	return out, nil
}

func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
