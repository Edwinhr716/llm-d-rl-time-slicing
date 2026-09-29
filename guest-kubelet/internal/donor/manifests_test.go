package donor_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/edwinhr716/guest-kubelet/internal/donor"
)

// Option d manifests (D-VK-2): the delete policy and the donor-controller stand-in.
const optDir = "../../deploy/opt-d"

const (
	vkSA      = "system:serviceaccount:guest-kubelet-proto:guest-kubelet"
	standinSA = "system:serviceaccount:guest-kubelet-proto:donor-standin"
	// The NS stack's donor controller (deploy/donor-controller, --release-dead-hosts).
	donorCtlSA = "system:serviceaccount:__DONOR_NS__:donor-controller"
)

// docs decodes every YAML document in the file into the object of its kind.
func docs(t *testing.T, file string) map[string][]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(optDir, file))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]json.RawMessage{}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		var raw json.RawMessage
		err := dec.Decode(&raw)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		var meta struct{ Kind string }
		if err := json.Unmarshal(raw, &meta); err != nil {
			t.Fatal(err)
		}
		out[meta.Kind] = append(out[meta.Kind], raw)
	}
}

func one[T any](t *testing.T, all map[string][]json.RawMessage, kind string) *T {
	t.Helper()
	if len(all[kind]) != 1 {
		t.Fatalf("want exactly one %s, got %d", kind, len(all[kind]))
	}
	obj := new(T)
	if err := json.Unmarshal(all[kind][0], obj); err != nil {
		t.Fatalf("%s: %v", kind, err)
	}
	return obj
}

func TestOptD_PolicyDeniesDeleteOfOwnVirtualNodeOnly(t *testing.T) {
	all := docs(t, "policy.yaml")
	pol := one[admissionv1.ValidatingAdmissionPolicy](t, all, "ValidatingAdmissionPolicy")
	bind := one[admissionv1.ValidatingAdmissionPolicyBinding](t, all, "ValidatingAdmissionPolicyBinding")

	if pol.Spec.FailurePolicy == nil || *pol.Spec.FailurePolicy != admissionv1.Fail {
		t.Errorf("failurePolicy = %v; want Fail", pol.Spec.FailurePolicy)
	}
	mc := pol.Spec.MatchConstraints
	if mc == nil || len(mc.ResourceRules) != 1 {
		t.Fatalf("matchConstraints = %+v; want one rule", mc)
	}
	rule := mc.ResourceRules[0]
	if !slices.Equal(rule.Operations, []admissionv1.OperationType{admissionv1.Delete}) ||
		!slices.Equal(rule.Resources, []string{"nodes"}) {
		t.Errorf("policy rule = %+v; want DELETE nodes only", rule)
	}
	if mc.ObjectSelector == nil || mc.ObjectSelector.MatchLabels[donor.VirtualNodeLabel] != "true" {
		t.Errorf("policy objectSelector = %+v; want %s=true", mc.ObjectSelector, donor.VirtualNodeLabel)
	}

	if bind.Spec.PolicyName != pol.Name {
		t.Errorf("binding policyName = %q; want %q", bind.Spec.PolicyName, pol.Name)
	}
	if !slices.Contains(bind.Spec.ValidationActions, admissionv1.Deny) {
		t.Errorf("binding validationActions = %v; want Deny", bind.Spec.ValidationActions)
	}
	// Scoped to __VK_NODE__ (plan hook H5): runs on other hosts must not be protected.
	br := bind.Spec.MatchResources
	if br == nil || len(br.ResourceRules) != 1 ||
		!slices.Equal(br.ResourceRules[0].ResourceNames, []string{"__VK_NODE__"}) {
		t.Errorf("binding matchResources = %+v; want resourceNames [__VK_NODE__]", br)
	}

	if len(pol.Spec.Validations) != 1 {
		t.Fatalf("validations = %d; want 1", len(pol.Spec.Validations))
	}
	v := pol.Spec.Validations[0]
	vars := ""
	for _, pv := range pol.Spec.Variables {
		vars += pv.Expression
	}
	for _, sa := range []string{vkSA, standinSA, donorCtlSA} {
		if !strings.Contains(vars+v.Expression, sa) {
			t.Errorf("policy does not allow %s", sa)
		}
	}
	// The denial names the policy, so audit logs show which policy denied (plan, option d hook 3).
	if !strings.Contains(v.MessageExpression, pol.Name) {
		t.Errorf("messageExpression %q does not name the policy %q", v.MessageExpression, pol.Name)
	}
}

func TestOptD_VKServiceAccountMatchesPolicy(t *testing.T) {
	rbac, err := os.ReadFile("../../deploy/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// The ClusterRoleBinding in rbac.yaml binds this service account; the policy allows it by name.
	if !strings.Contains(string(rbac), "name: guest-kubelet\n    namespace: guest-kubelet-proto") {
		t.Fatalf("deploy/rbac.yaml no longer binds guest-kubelet-proto/guest-kubelet; update %s in policy.yaml", vkSA)
	}
}

func TestOptD_StandinHooks(t *testing.T) {
	all := docs(t, "donor-standin.yaml")
	dep := one[appsv1.Deployment](t, all, "Deployment")
	sa := one[corev1.ServiceAccount](t, all, "ServiceAccount")
	role := one[rbacv1.ClusterRole](t, all, "ClusterRole")
	crb := one[rbacv1.ClusterRoleBinding](t, all, "ClusterRoleBinding")

	if "system:serviceaccount:"+sa.Namespace+":"+sa.Name != standinSA {
		t.Errorf("stand-in service account %s/%s is not the one the policy allows (%s)", sa.Namespace, sa.Name, standinSA)
	}
	pod := dep.Spec.Template.Spec
	if pod.ServiceAccountName != sa.Name {
		t.Errorf("serviceAccountName = %q; want %q", pod.ServiceAccountName, sa.Name)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != sa.Name || crb.RoleRef.Name != role.Name {
		t.Errorf("ClusterRoleBinding = %+v; want %s bound to %s", crb, sa.Name, role.Name)
	}
	// Plan hook H7: collected by label, runs on a CPU node, never on the host.
	if dep.Spec.Template.Labels["timeslice.io/eval-role"] != "donor-standin" {
		t.Errorf("pod labels = %v; want timeslice.io/eval-role=donor-standin", dep.Spec.Template.Labels)
	}
	if pod.NodeSelector["cloud.google.com/gke-nodepool"] != "default-pool" {
		t.Errorf("nodeSelector = %v; want the default pool", pod.NodeSelector)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d; want 1", len(pod.Containers))
	}
	c := pod.Containers[0]
	if !slices.Equal(c.Command, []string{"/donor-standin"}) || c.Image != "__IMAGE__" {
		t.Errorf("container = %s %v; want __IMAGE__ /donor-standin", c.Image, c.Command)
	}
	for _, arg := range []string{"--vk-node=__VK_NODE__", "--host-node=__HOST__"} {
		if !slices.Contains(c.Args, arg) {
			t.Errorf("args = %v; missing %s", c.Args, arg)
		}
	}
	// Node objects only: get the host and the virtual Node, delete only the virtual Node.
	for _, r := range role.Rules {
		if !slices.Equal(r.Resources, []string{"nodes"}) {
			t.Errorf("rule %+v touches more than nodes", r)
		}
		if slices.Contains(r.Verbs, "delete") && !slices.Equal(r.ResourceNames, []string{"__VK_NODE__"}) {
			t.Errorf("delete rule %+v is not limited to __VK_NODE__", r)
		}
	}
}
