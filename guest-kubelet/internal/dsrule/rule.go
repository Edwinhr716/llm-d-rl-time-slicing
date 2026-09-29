// Package dsrule is the "rule" option of D-VK-7 (PENDING LEAD DECISION): a mutating admission
// webhook that gives every DaemonSet a required node affinity excluding virtual nodes, so a
// DaemonSet never gets a pod on the virtual Node and its rollouts never wait for one.
//
// It is served by the guest-kubelet image (`guest-kubelet daemonset-rule-webhook`) and
// installed by deploy/daemonset-rule/install.sh. It uses only admissionregistration.k8s.io/v1
// (no MutatingAdmissionPolicy) and makes its own serving certificate, so there is no manual
// certificate step.
package dsrule

import (
	"encoding/json"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// Exclusion is the requirement added to every node selector term.
func Exclusion() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: provider.VirtualNodeLabel, Operator: corev1.NodeSelectorOpDoesNotExist}
}

func hasExclusion(t corev1.NodeSelectorTerm) bool {
	for _, r := range t.MatchExpressions {
		if r.Key == provider.VirtualNodeLabel && r.Operator == corev1.NodeSelectorOpDoesNotExist {
			return true
		}
	}
	return false
}

// ExcludeVirtualNodes adds Exclusion to every term of the pod spec's required node affinity.
// Terms are alternatives, so the requirement has to be in each of them; a spec with no required node
// affinity gets one term holding only the exclusion. Preferred terms, pod (anti-)affinity and
// nodeSelector are left as they are. It reports whether it changed anything.
func ExcludeVirtualNodes(spec *corev1.PodSpec) bool {
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := spec.Affinity.NodeAffinity
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}
	req := na.RequiredDuringSchedulingIgnoredDuringExecution
	if len(req.NodeSelectorTerms) == 0 {
		req.NodeSelectorTerms = []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{Exclusion()}}}
		return true
	}
	changed := false
	for i := range req.NodeSelectorTerms {
		if !hasExclusion(req.NodeSelectorTerms[i]) {
			req.NodeSelectorTerms[i].MatchExpressions = append(req.NodeSelectorTerms[i].MatchExpressions, Exclusion())
			changed = true
		}
	}
	return changed
}

type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// Patch returns the JSON patch that applies ExcludeVirtualNodes to the DaemonSet's pod
// template, or nil if it already excludes virtual nodes. The patch sets
// /spec/template/spec/affinity as a whole ("add" replaces an existing member).
func Patch(ds *appsv1.DaemonSet) ([]byte, error) {
	spec := ds.Spec.Template.Spec.DeepCopy()
	if !ExcludeVirtualNodes(spec) {
		return nil, nil
	}
	return json.Marshal([]jsonPatchOp{{Op: "add", Path: "/spec/template/spec/affinity", Value: spec.Affinity}})
}
