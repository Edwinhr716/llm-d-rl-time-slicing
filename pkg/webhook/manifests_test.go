package webhook_test

import (
	"io/fs"
	"os"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

// render substitutes the manifest placeholders the way an installer does, including the args
// placeholder line, which becomes one "- flag" item per flag at the placeholder's indentation.
func render(t *testing.T, name string, flags []string) [][]byte {
	t.Helper()
	raw, err := fs.ReadFile(os.DirFS("../../deploy/timeslice-webhook/manifests"), name)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	var out []string
	for _, line := range lines {
		if idx := strings.Index(line, "__EXTRA_ARGS__"); idx >= 0 && strings.TrimSpace(line) == "__EXTRA_ARGS__" {
			for _, flag := range flags {
				out = append(out, line[:idx]+"- "+flag)
			}
			continue
		}
		out = append(out, line)
	}
	text := strings.NewReplacer(
		"__NS__", "timeslice-system", "__IMAGE__", "example.com/timeslice-webhook:dev", "__PORT__", "10250",
		"__CA_BUNDLE__", "Y2E=", "__TLS_SECRET__", "timeslice-webhook-tls",
		"__VK_USERNAME__", vkUsername,
	).Replace(strings.Join(out, "\n"))
	if strings.Contains(text, "__") {
		t.Fatalf("%s: unrendered placeholder left", name)
	}
	docs := strings.Split(text, "\n---\n")
	result := make([][]byte, 0, len(docs))
	for _, doc := range docs {
		result = append(result, []byte(doc))
	}
	return result
}

func unmarshal(t *testing.T, doc []byte, obj any) {
	t.Helper()
	if err := yaml.UnmarshalStrict(doc, obj); err != nil {
		t.Fatalf("decode %T: %v", obj, err)
	}
}

func TestManifests_Deployment(t *testing.T) {
	docs := render(t, "deployment.yaml", nsFlags)
	var dep appsv1.Deployment
	unmarshal(t, docs[0], &dep)
	spec := &dep.Spec.Template.Spec
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Errorf("replicas = %v, want 2", dep.Spec.Replicas)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || spec.ServiceAccountName != "" {
		t.Errorf("the webhook must run without a ServiceAccount token: %v %q",
			spec.AutomountServiceAccountToken, spec.ServiceAccountName)
	}
	ctr := &spec.Containers[0]
	args := append(append([]string{}, ctr.Command[1:]...), ctr.Args...)
	cfg, err := webhook.ConfigFromFlags(args)
	if err != nil {
		t.Fatalf("rendered flags %v do not parse: %v", args, err)
	}
	if cfg.Port != 10250 || cfg.GuestSteering != webhook.SteeringPreferred || int(ctr.Ports[0].ContainerPort) != cfg.Port {
		t.Errorf("rendered config = %+v, ports %+v", cfg, ctr.Ports)
	}
	if cfg.CertDir != ctr.VolumeMounts[0].MountPath || spec.Volumes[0].Secret.SecretName != "timeslice-webhook-tls" {
		t.Errorf("cert dir %q vs mount %+v", cfg.CertDir, ctr.VolumeMounts)
	}
	if sc := ctr.SecurityContext; sc == nil || sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Error("container root filesystem is not read-only")
	}

	var svc corev1.Service
	unmarshal(t, render(t, "service.yaml", nil)[0], &svc)
	if svc.Spec.Ports[0].Port != 443 || svc.Spec.Ports[0].TargetPort.StrVal != ctr.Ports[0].Name {
		t.Errorf("service ports = %+v", svc.Spec.Ports)
	}
}

func TestManifests_MutatingWebhook(t *testing.T) {
	var mwc admissionregistrationv1.MutatingWebhookConfiguration
	unmarshal(t, render(t, "mutatingwebhook.yaml", nil)[0], &mwc)
	want := map[string]admissionregistrationv1.FailurePolicyType{
		"donor.timeslice.io": admissionregistrationv1.Fail,
		"guest.timeslice.io": admissionregistrationv1.Ignore,
	}
	if len(mwc.Webhooks) != len(want) {
		t.Fatalf("webhooks = %d, want %d", len(mwc.Webhooks), len(want))
	}
	for i := range mwc.Webhooks {
		hook := &mwc.Webhooks[i]
		policy, ok := want[hook.Name]
		if !ok || hook.FailurePolicy == nil || *hook.FailurePolicy != policy {
			t.Errorf("%s: failurePolicy %v, want %v", hook.Name, hook.FailurePolicy, policy)
		}
		if hook.TimeoutSeconds == nil || *hook.TimeoutSeconds != 3 {
			t.Errorf("%s: timeoutSeconds %v, want 3", hook.Name, hook.TimeoutSeconds)
		}
		if hook.SideEffects == nil || *hook.SideEffects != admissionregistrationv1.SideEffectClassNone {
			t.Errorf("%s: sideEffects %v", hook.Name, hook.SideEffects)
		}
		if hook.ReinvocationPolicy == nil || *hook.ReinvocationPolicy != admissionregistrationv1.IfNeededReinvocationPolicy {
			t.Errorf("%s: reinvocationPolicy %v", hook.Name, hook.ReinvocationPolicy)
		}
		ops := hook.Rules[0].Operations
		if len(hook.Rules) != 1 || len(ops) != 1 || ops[0] != admissionregistrationv1.Create ||
			hook.Rules[0].Resources[0] != "pods" {
			t.Errorf("%s: rules %+v, want CREATE on pods only", hook.Name, hook.Rules)
		}
		label := strings.TrimSuffix(hook.Name, ".timeslice.io")
		if hook.ObjectSelector == nil || hook.ObjectSelector.MatchLabels["timeslice.io/"+label] != "true" {
			t.Errorf("%s: objectSelector %+v", hook.Name, hook.ObjectSelector)
		}
		svc := hook.ClientConfig.Service
		if svc == nil || svc.Path == nil || *svc.Path != "/mutate" || string(hook.ClientConfig.CABundle) != "ca" {
			t.Errorf("%s: clientConfig %+v", hook.Name, hook.ClientConfig)
		}
	}
}

func TestManifests_Policy(t *testing.T) {
	docs := render(t, "policy.yaml", nil)
	if len(docs) != 2 {
		t.Fatalf("policy.yaml has %d documents, want 2", len(docs))
	}
	var vap admissionregistrationv1.ValidatingAdmissionPolicy
	unmarshal(t, docs[0], &vap)
	var binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
	unmarshal(t, docs[1], &binding)
	if vap.Spec.FailurePolicy == nil || *vap.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Errorf("policy failurePolicy = %v", vap.Spec.FailurePolicy)
	}
	rules := make([]string, 0, len(vap.Spec.Validations))
	for i := range vap.Spec.Validations {
		rules = append(rules, strings.Fields(vap.Spec.Validations[i].Message)[0])
	}
	if strings.Join(rules, ",") != "[W10],[W6],[W9],[W2],[W12]" {
		t.Errorf("validations = %v", rules)
	}
	if !strings.Contains(vap.Spec.MatchConditions[0].Expression, "'"+vkUsername+"'") {
		t.Errorf("match condition %q does not exempt the virtual kubelet", vap.Spec.MatchConditions[0].Expression)
	}
	if binding.Spec.PolicyName != vap.Name || len(binding.Spec.ValidationActions) != 1 ||
		binding.Spec.ValidationActions[0] != admissionregistrationv1.Deny {
		t.Errorf("binding = %+v", binding.Spec)
	}
}

// The W11 shadow policy is cluster-wide (no namespace or label match), fails closed, exempts only
// the virtual kubelet and covers both the pooled and the per-GPU shadow resource names.
func TestManifests_ShadowPolicy(t *testing.T) {
	docs := render(t, "shadow-policy.yaml", nil)
	if len(docs) != 2 {
		t.Fatalf("shadow-policy.yaml has %d documents, want 2", len(docs))
	}
	var vap admissionregistrationv1.ValidatingAdmissionPolicy
	unmarshal(t, docs[0], &vap)
	var binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
	unmarshal(t, docs[1], &binding)
	if vap.Spec.FailurePolicy == nil || *vap.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Errorf("failurePolicy = %v", vap.Spec.FailurePolicy)
	}
	if len(vap.Spec.Validations) != 1 || !strings.HasPrefix(vap.Spec.Validations[0].Message, "[W11] ") {
		t.Errorf("validations = %+v", vap.Spec.Validations)
	}
	if len(vap.Spec.MatchConditions) != 1 ||
		!strings.Contains(vap.Spec.MatchConditions[0].Expression, "'"+vkUsername+"'") {
		t.Errorf("match conditions %+v do not exempt exactly the virtual kubelet", vap.Spec.MatchConditions)
	}
	expr := vap.Spec.Variables[0].Expression
	for _, want := range []string{"'" + webhook.ShadowResource + "'", "'" + webhook.ShadowResourcePrefix + "'", "initContainers"} {
		if !strings.Contains(expr, want) {
			t.Errorf("asksShadow does not cover %s: %s", want, expr)
		}
	}
	if binding.Spec.PolicyName != vap.Name || binding.Spec.MatchResources != nil ||
		len(binding.Spec.ValidationActions) != 1 || binding.Spec.ValidationActions[0] != admissionregistrationv1.Deny {
		t.Errorf("binding must deny cluster-wide: %+v", binding.Spec)
	}
}
