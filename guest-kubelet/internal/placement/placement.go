// Package placement answers which nodes a workload's pod template can land on, from its
// nodeSelector and tolerations only (no resource fit, no affinity). It backs the tests of pending
// lead decision D-NS-11 (option ns-ds: guest kubelet and snapshot agent as DaemonSets that select
// timeslice.io/donor=true).
package placement

import corev1 "k8s.io/api/core/v1"

// DonorLabel is the real-node label (value "true") that the ns-ds DaemonSets select on.
// The virtual Node never carries it.
const DonorLabel = "timeslice.io/donor"

// Eligible reports whether a pod with this spec could be placed on the node: every nodeSelector
// entry matches a node label, and every NoSchedule and NoExecute taint is tolerated.
func Eligible(spec *corev1.PodSpec, node *corev1.Node) bool {
	for k, v := range spec.NodeSelector {
		if got, ok := node.Labels[k]; !ok || got != v {
			return false
		}
	}
	for i := range node.Spec.Taints {
		taint := &node.Spec.Taints[i]
		if taint.Effect == corev1.TaintEffectPreferNoSchedule {
			continue
		}
		tolerated := false
		for j := range spec.Tolerations {
			if tolerates(&spec.Tolerations[j], taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

// tolerates is the Exists/Equal toleration match: an empty key with Exists matches every taint,
// and an empty effect matches every effect.
func tolerates(tol *corev1.Toleration, taint *corev1.Taint) bool {
	if tol.Effect != "" && tol.Effect != taint.Effect {
		return false
	}
	if tol.Key != "" && tol.Key != taint.Key {
		return false
	}
	switch tol.Operator {
	case corev1.TolerationOpExists:
		return true
	case corev1.TolerationOpEqual, "":
		return tol.Key != "" && tol.Value == taint.Value
	default:
		return false
	}
}
