package provider

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// Tests for pending lead decision D-NS-3: the guest manifests in deploy/guests (options keep and
// ns-preferred) and the W9 policy variants in deploy/admission, which carry the guest predicate
// of each --guest-marker option (D-VK-4) as the CEL variable isGuest.

const deployDir = "../../deploy"

// decodeYAML decodes the YAML documents of a file, in order, into objs.
func decodeYAML(t *testing.T, path string, objs ...any) {
	t.Helper()
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for _, obj := range objs {
		if err := dec.Decode(obj); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	var extra map[string]any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("%s: want %d documents, err=%v", path, len(objs), err)
	}
}

func readGuest(t *testing.T, manifest string) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	decodeYAML(t, filepath.Join(deployDir, "guests", "guest-"+manifest+".yaml"), &pod)
	if pod.Kind != "Pod" {
		t.Fatalf("guest-%s.yaml: kind %q", manifest, pod.Kind)
	}
	return &pod
}

// tolerates reports whether the pod tolerates every NoSchedule taint of the node.
func tolerates(pod *corev1.Pod, node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		ok := false
		for _, tol := range pod.Spec.Tolerations {
			keyOK := tol.Key == taint.Key && (tol.Operator == corev1.TolerationOpExists || tol.Value == taint.Value)
			ok = ok || (keyOK && (tol.Effect == "" || tol.Effect == taint.Effect))
		}
		if !ok {
			return false
		}
	}
	return true
}

// selects reports whether the node labels satisfy the pod's nodeSelector.
func selects(pod *corev1.Pod, labels map[string]string) bool {
	for k, v := range pod.Spec.NodeSelector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// preferred returns the summed weight of the pod's preferred node terms that the labels satisfy.
// Only the In operator is used by the manifests.
func preferred(pod *corev1.Pod, labels map[string]string) int32 {
	var sum int32
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return 0
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		match := true
		for _, req := range term.Preference.MatchExpressions {
			in := false
			for _, v := range req.Values {
				in = in || (req.Operator == corev1.NodeSelectorOpIn && labels[req.Key] == v)
			}
			match = match && in
		}
		if match {
			sum += term.Weight
		}
	}
	return sum
}

func hasTimesliceLabel(pod *corev1.Pod) bool {
	for k := range pod.Labels {
		if strings.HasPrefix(k, "timeslice.io/") {
			return true
		}
	}
	return false
}

func noRequiredAffinity(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	aff := pod.Spec.Affinity
	if aff != nil && aff.NodeAffinity != nil && aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		t.Errorf("%s: a required node affinity would block the fallback to real nodes", pod.Name)
	}
}

func vkNode(guestNodeLabel bool) *corev1.Node {
	node := NewNodeSpec(NodeConfig{Name: "vk-test", GuestNodeLabel: guestNodeLabel})
	return &node
}

// guestMarkers returns which --guest-marker values treat the pod as a guest.
func guestMarkers(t *testing.T, pod *corev1.Pod) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for _, marker := range []string{GuestMarkerToleration, GuestMarkerLabel, GuestMarkerBoth} {
		match, err := newGuestMatcher(marker)
		if err != nil {
			t.Fatal(err)
		}
		got[marker] = match(pod) != ""
	}
	return got
}

// keep: today's manifest requires a virtual Node through the contract's nodeSelector, so it
// lands on the VK Node with either --guest-node-label value, and is a guest for the toleration
// and both markers only.
func TestGuestManifest_Today(t *testing.T) {
	pod := readGuest(t, "today")
	noRequiredAffinity(t, pod)
	if hasTimesliceLabel(pod) {
		t.Errorf("today's guest carries a timeslice.io label: %v", pod.Labels)
	}
	if len(pod.Spec.NodeSelector) != 1 || pod.Spec.NodeSelector[VirtualNodeLabel] != "true" {
		t.Errorf("nodeSelector %v, want only %s=true", pod.Spec.NodeSelector, VirtualNodeLabel)
	}
	for _, label := range []bool{false, true} {
		node := vkNode(label)
		if !tolerates(pod, node) || !selects(pod, node.Labels) {
			t.Errorf("guest-node-label=%v: today's guest cannot land on the VK Node", label)
		}
	}
	realNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"kubernetes.io/hostname": "real"}}}
	if selects(pod, realNode.Labels) {
		t.Error("today's guest selects a real node")
	}
	markers := guestMarkers(t, pod)
	want := map[string]bool{GuestMarkerToleration: true, GuestMarkerLabel: false, GuestMarkerBoth: true}
	for marker, isGuestPod := range want {
		if markers[marker] != isGuestPod {
			t.Errorf("--guest-marker=%s: guest=%v, want %v", marker, markers[marker], isGuestPod)
		}
	}
}

// ns-preferred: the north-star manifest has no nodeSelector, so it can run on any node, and
// prefers the VK Node only when the Node carries timeslice.io/guest=true (D-NS-2 ns-label).
// Every marker treats it as a guest.
func TestGuestManifest_NS(t *testing.T) {
	pod := readGuest(t, "ns")
	noRequiredAffinity(t, pod)
	if len(pod.Spec.NodeSelector) != 0 {
		t.Errorf("nodeSelector %v would block the fallback to real nodes", pod.Spec.NodeSelector)
	}
	if pod.Labels[GuestPodLabel] != "true" {
		t.Errorf("labels %v, want %s=true", pod.Labels, GuestPodLabel)
	}
	if !tolerates(pod, vkNode(true)) {
		t.Error("the north-star guest does not tolerate the VK Node's taint")
	}
	if got := preferred(pod, vkNode(true).Labels); got != 100 {
		t.Errorf("guest-node-label=true: preferred weight %d on the VK Node, want 100", got)
	}
	if got := preferred(pod, vkNode(false).Labels); got != 0 {
		t.Errorf("guest-node-label=false: preferred weight %d on the VK Node, want 0 (no steering)", got)
	}
	for marker, isGuestPod := range guestMarkers(t, pod) {
		if !isGuestPod {
			t.Errorf("--guest-marker=%s does not treat the north-star guest as a guest", marker)
		}
	}
}

// w9Case is one row of the W9 table: labels, tolerations and the verdict of each variant.
type w9Case struct {
	id     string
	labels string
	tols   []corev1.Toleration
	admit  map[string]bool // by variant
}

var (
	tolGuest = []corev1.Toleration{
		{Key: GuestTaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
	tolGuestEq = []corev1.Toleration{
		{Key: GuestTaintKey, Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
	}
	tolUniversal = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
)

// verdicts builds the admit map from "a" (admit) and "r" (reject) for toleration, label, both, either.
func verdicts(s string) map[string]bool {
	return map[string]bool{"toleration": s[0] == 'a', "label": s[1] == 'a', "both": s[2] == 'a', "either": s[3] == 'a'}
}

func w9Cases() []w9Case {
	return []w9Case{
		{"W01", "app=zz", tolGuest, verdicts("aaaa")},
		{"W02", "app=zz,timeslice.io/guest=true", tolGuest, verdicts("aaaa")},
		{"W03", "app=zz,timeslice.io/group=g", tolGuest, verdicts("rarr")},
		{"W04", "app=zz,timeslice.io/guest=true,timeslice.io/group=g", tolGuest, verdicts("rrrr")},
		{"W05", "app=zz,timeslice.io/guest=true,timeslice.io/job-id=j", tolGuest, verdicts("rrrr")},
		{"W06", "app=zz,timeslice.io/guest=true,timeslice.io/role=background", tolGuest, verdicts("rrrr")},
		{"W07", "app=zz,timeslice.io/guest=true,timeslice.io/group=g", nil, verdicts("arrr")},
		{"W08", "app=tr,timeslice.io/donor=true,timeslice.io/group=g,timeslice.io/job-id=j", tolUniversal, verdicts("aaaa")},
		{"W09", "app=tr,timeslice.io/group=g,timeslice.io/job-id=j", nil, verdicts("aaaa")},
		{"W10", "app=zz,timeslice.io/job-id=j", tolGuestEq, verdicts("rarr")},
		{"W11", "timeslice.io/guest=false,timeslice.io/role=x", nil, verdicts("aaaa")},
		{"W12", "", tolGuest, verdicts("aaaa")},
	}
}

func (c w9Case) pod() *corev1.Pod {
	pod := guestPod(strings.ToLower(c.id))
	pod.Spec.Tolerations = c.tols
	if c.labels != "" {
		pod.Labels = map[string]string{}
		for _, kv := range strings.Split(c.labels, ",") {
			k, v, _ := strings.Cut(kv, "=")
			pod.Labels[k] = v
		}
	}
	return pod
}

// w9Policy is a W9 variant compiled with cel-go the way the API server evaluates it.
type w9Policy struct {
	isGuest, allowed cel.Program
	message          string
}

func compileCEL(t *testing.T, env *cel.Env, expr string) cel.Program {
	t.Helper()
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		t.Fatalf("%q: %v", expr, iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatal(err)
	}
	return prg
}

// readW9 reads deploy/admission/w9-<variant>.yaml, checks its shape and compiles its CEL.
func readW9(t *testing.T, variant string) w9Policy {
	t.Helper()
	var policy admissionv1.ValidatingAdmissionPolicy
	var binding admissionv1.ValidatingAdmissionPolicyBinding
	decodeYAML(t, filepath.Join(deployDir, "admission", "w9-"+variant+".yaml"), &policy, &binding)
	name := "w9-" + variant + "-__NS__"
	spec, bspec := policy.Spec, binding.Spec
	switch {
	case policy.Kind != "ValidatingAdmissionPolicy" || binding.Kind != "ValidatingAdmissionPolicyBinding":
		t.Fatalf("%s: kinds %s, %s", variant, policy.Kind, binding.Kind)
	case policy.Name != name || binding.Name != name || bspec.PolicyName != name:
		t.Errorf("%s: names %s, %s, policyName %s, want %s", variant, policy.Name, binding.Name, bspec.PolicyName, name)
	case len(bspec.ValidationActions) != 1 || bspec.ValidationActions[0] != admissionv1.Deny:
		t.Errorf("%s: validationActions %v, want [Deny]", variant, bspec.ValidationActions)
	case bspec.MatchResources == nil || bspec.MatchResources.NamespaceSelector == nil ||
		bspec.MatchResources.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "__NS__":
		t.Errorf("%s: the binding is not scoped to the namespace __NS__", variant)
	case len(spec.Variables) != 1 || spec.Variables[0].Name != "isGuest":
		t.Errorf("%s: variables %v, want the one variable isGuest", variant, spec.Variables)
	case len(spec.Validations) != 1 || !strings.HasPrefix(spec.Validations[0].Message, "W9:"):
		t.Errorf("%s: want one validation with a message starting W9:", variant)
	case spec.FailurePolicy == nil || *spec.FailurePolicy != admissionv1.Fail:
		t.Errorf("%s: failurePolicy must be Fail", variant)
	}
	if t.Failed() {
		t.FailNow()
	}
	env, err := cel.NewEnv(cel.OptionalTypes(),
		cel.Variable("object", cel.DynType), cel.Variable("variables", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		t.Fatal(err)
	}
	return w9Policy{
		isGuest: compileCEL(t, env, spec.Variables[0].Expression),
		allowed: compileCEL(t, env, spec.Validations[0].Expression),
		message: spec.Validations[0].Message,
	}
}

func evalBool(t *testing.T, prg cel.Program, vars map[string]any) bool {
	t.Helper()
	out, _, err := prg.Eval(vars)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.Value().(bool)
	if !ok {
		t.Fatalf("CEL returned %v, want a bool", out)
	}
	return got
}

// w9Verdict is what a W9 policy says about a pod.
type w9Verdict struct {
	guest bool // the isGuest variable
	admit bool // every validation passed
}

// eval evaluates the policy on the pod as the API server sees the object.
func (p w9Policy) eval(t *testing.T, pod *corev1.Pod) w9Verdict {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	guest := evalBool(t, p.isGuest, map[string]any{"object": obj})
	admit := evalBool(t, p.allowed, map[string]any{"object": obj, "variables": map[string]any{"isGuest": guest}})
	return w9Verdict{guest: guest, admit: admit}
}

// testW9 checks one variant: every W case gets its verdict, and isGuest agrees with the VK's
// matcher for marker on the W cases, the marker cases and both guest manifests.
func testW9(t *testing.T, variant, marker string) {
	t.Helper()
	policy := readW9(t, variant)
	match, err := evalGuestMatcher(marker)
	if err != nil {
		t.Fatal(err)
	}
	mcs, wcs := markerCases(), w9Cases()
	pods := make([]*corev1.Pod, 0, 2+len(mcs)+len(wcs))
	pods = append(pods, readGuest(t, "today"), readGuest(t, "ns"))
	for _, mc := range mcs {
		pods = append(pods, mc.pod)
	}
	for _, wc := range wcs {
		got := policy.eval(t, wc.pod())
		if got.admit != wc.admit[variant] {
			t.Errorf("%s %s: admit=%v, want %v (isGuest=%v)", variant, wc.id, got.admit, wc.admit[variant], got.guest)
		}
		pods = append(pods, wc.pod())
	}
	for _, pod := range pods {
		if got := policy.eval(t, pod); got.guest != match(pod) {
			t.Errorf("%s: %s: isGuest=%v but --guest-marker=%s says %v", variant, pod.Name, got.guest, marker, match(pod))
		}
	}
}

func TestW9_Toleration(t *testing.T) { testW9(t, "toleration", GuestMarkerToleration) }

func TestW9_Label(t *testing.T) { testW9(t, "label", GuestMarkerLabel) }

func TestW9_Both(t *testing.T) { testW9(t, "both", GuestMarkerBoth) }

// either is the both predicate, applied whatever --guest-marker is.
func TestW9_Either(t *testing.T) { testW9(t, "either", GuestMarkerBoth) }
