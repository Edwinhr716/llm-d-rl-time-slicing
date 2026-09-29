package dsrule

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	admregv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

func ds(affinity *corev1.Affinity, tols ...corev1.Toleration) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "ds"},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Affinity: affinity, Tolerations: tols,
			Containers: []corev1.Container{{Name: "c", Image: "pause"}},
		}}},
	}
}

func req(key string, op corev1.NodeSelectorOperator, values ...string) []corev1.NodeSelectorRequirement {
	return []corev1.NodeSelectorRequirement{{Key: key, Operator: op, Values: values}}
}

func twoTerms() *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{MatchExpressions: req("kubernetes.io/hostname", corev1.NodeSelectorOpIn, "a", "b")},
			{MatchExpressions: req(provider.VirtualNodeLabel, corev1.NodeSelectorOpIn, "true")},
			{MatchFields: req("metadata.name", corev1.NodeSelectorOpIn, "n")},
		}},
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight:     1,
			Preference: corev1.NodeSelectorTerm{MatchExpressions: req("x", corev1.NodeSelectorOpExists)},
		}},
	}}
}

func mustPatch(t *testing.T, dset *appsv1.DaemonSet) []byte {
	t.Helper()
	patch, err := Patch(dset)
	if err != nil {
		t.Fatal(err)
	}
	return patch
}

// applyPatch applies the single "add /spec/template/spec/affinity" op Patch produces.
func applyPatch(t *testing.T, dset *appsv1.DaemonSet, patch []byte) *appsv1.DaemonSet {
	t.Helper()
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(patch, &ops); err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Op != "add" || ops[0].Path != "/spec/template/spec/affinity" {
		t.Fatalf("unexpected patch %s", patch)
	}
	out := dset.DeepCopy()
	out.Spec.Template.Spec.Affinity = &corev1.Affinity{}
	if err := json.Unmarshal(ops[0].Value, out.Spec.Template.Spec.Affinity); err != nil {
		t.Fatal(err)
	}
	return out
}

func requiredTerms(a *corev1.Affinity) []corev1.NodeSelectorTerm {
	return a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func requireExcludedEverywhere(t *testing.T, a *corev1.Affinity) {
	t.Helper()
	terms := requiredTerms(a)
	if len(terms) == 0 {
		t.Fatal("no terms")
	}
	for i := range terms {
		if !hasExclusion(terms[i]) {
			t.Errorf("term %d lacks the exclusion: %+v", i, terms[i])
		}
	}
}

func TestDaemonSetPolicy_Rule_NoAffinityGetsOneTerm(t *testing.T) {
	dset := ds(nil)
	p := mustPatch(t, dset)
	if p == nil {
		t.Fatal("no patch")
	}
	terms := requiredTerms(applyPatch(t, dset, p).Spec.Template.Spec.Affinity)
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 || !hasExclusion(terms[0]) {
		t.Errorf("terms: %+v", terms)
	}
	// Other affinity kinds without node affinity: pod anti-affinity is kept.
	d2 := ds(&corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}})
	got2 := applyPatch(t, d2, mustPatch(t, d2)).Spec.Template.Spec.Affinity
	if got2.PodAntiAffinity == nil {
		t.Error("pod anti-affinity dropped")
	}
	requireExcludedEverywhere(t, got2)
}

func TestDaemonSetPolicy_Rule_EveryTermGetsExclusion(t *testing.T) {
	dset := ds(twoTerms())
	p := mustPatch(t, dset)
	if p == nil {
		t.Fatal("no patch")
	}
	got := applyPatch(t, dset, p).Spec.Template.Spec.Affinity
	requireExcludedEverywhere(t, got)
	terms := requiredTerms(got)
	if len(terms) != 3 || terms[0].MatchExpressions[0].Key != "kubernetes.io/hostname" || len(terms[2].MatchFields) != 1 {
		t.Errorf("existing requirements must be kept: %+v", terms)
	}
	pref := got.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 || len(pref[0].Preference.MatchExpressions) != 1 {
		t.Error("preferred terms must be untouched")
	}
	// The input object is not modified.
	if hasExclusion(requiredTerms(dset.Spec.Template.Spec.Affinity)[0]) {
		t.Error("Patch modified its input")
	}
}

func TestDaemonSetPolicy_Rule_Idempotent(t *testing.T) {
	dset := ds(twoTerms())
	again := mustPatch(t, applyPatch(t, dset, mustPatch(t, dset)))
	if again != nil {
		t.Errorf("second patch should be empty, got %s", again)
	}
	// Only the terms that lack it change.
	aff := twoTerms()
	first := &aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0]
	first.MatchExpressions = append(first.MatchExpressions, Exclusion())
	spec := ds(aff).Spec.Template.Spec
	if !ExcludeVirtualNodes(&spec) {
		t.Fatal("expected a change")
	}
	if n := len(requiredTerms(spec.Affinity)[0].MatchExpressions); n != 2 {
		t.Errorf("exclusion duplicated: %d expressions", n)
	}
}

// A DaemonSet whose pods tolerate the guest taint by key (so the VK would treat them as guests)
// is excluded like any other: no DaemonSet pod reaches a virtual node under the rule.
func TestDaemonSetPolicy_Rule_GuestTolerationStillExcluded(t *testing.T) {
	dset := ds(nil, corev1.Toleration{Key: provider.GuestTaintKey, Operator: corev1.TolerationOpExists})
	if mustPatch(t, dset) == nil {
		t.Fatal("a DaemonSet tolerating the guest taint must still be excluded")
	}
}

var dsKind = metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "DaemonSet"}

func request(
	t *testing.T, op admissionv1.Operation, kind metav1.GroupVersionKind, obj runtime.Object,
) *admissionv1.AdmissionRequest {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return &admissionv1.AdmissionRequest{UID: "u1", Kind: kind, Operation: op, Object: runtime.RawExtension{Raw: raw}}
}

func TestDaemonSetPolicy_Rule_ReviewCreateAndUpdate(t *testing.T) {
	for _, op := range []admissionv1.Operation{admissionv1.Create, admissionv1.Update} {
		resp := Review(request(t, op, dsKind, ds(twoTerms())))
		if !resp.Allowed || resp.UID != "u1" || resp.Patch == nil || resp.PatchType == nil ||
			*resp.PatchType != admissionv1.PatchTypeJSONPatch {
			t.Errorf("%s: %+v", op, resp)
		}
	}
	if resp := Review(request(t, admissionv1.Delete, dsKind, ds(nil))); !resp.Allowed || resp.Patch != nil {
		t.Errorf("delete: %+v", resp)
	}
	dep := metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	if resp := Review(request(t, admissionv1.Create, dep, ds(nil))); !resp.Allowed || resp.Patch != nil {
		t.Errorf("deployment: %+v", resp)
	}
	badReq := request(t, admissionv1.Create, dsKind, ds(nil))
	badReq.Object.Raw = []byte("{")
	if bad := Review(badReq); !bad.Allowed || bad.Patch != nil || len(bad.Warnings) == 0 {
		t.Errorf("undecodable: %+v", bad)
	}
}

func TestDaemonSetPolicy_Rule_HandlerRoundTrip(t *testing.T) {
	in := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request:  request(t, admissionv1.Create, dsKind, ds(nil)),
	}
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	Handler{}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader(body)))
	var out admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%v: %s", err, rec.Body.String())
	}
	if out.APIVersion != "admission.k8s.io/v1" || out.Kind != "AdmissionReview" || out.Request != nil ||
		out.Response == nil || out.Response.UID != "u1" || !out.Response.Allowed || out.Response.Patch == nil {
		t.Errorf("response: %+v", out)
	}
	rec = httptest.NewRecorder()
	Handler{}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader([]byte("{}"))))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("empty review: code %d", rec.Code)
	}
}

func TestDaemonSetPolicy_Rule_CertVerifiesForService(t *testing.T) {
	cfg := &Config{Namespace: "ns1", Service: "daemonset-rule"}
	kp, err := GenerateCert(cfg.DNSNames(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(kp.CertPEM)
	if blk == nil {
		t.Fatal("no PEM block")
	}
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(kp.CertPEM)
	if _, err := cert.Verify(x509.VerifyOptions{DNSName: "daemonset-rule.ns1.svc", Roots: pool}); err != nil {
		t.Errorf("verify: %v", err)
	}
}

func TestDaemonSetPolicy_Rule_EnsureCertSharedAndCABundleSet(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{Namespace: "ns1", Service: "daemonset-rule", Secret: "daemonset-rule-tls", WebhookConfig: "ns1-daemonset-rule"}
	client := fake.NewClientset(&admregv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.WebhookConfig},
		Webhooks:   []admregv1.MutatingWebhook{{Name: "daemonset-rule.timeslice.io"}},
	})
	first, err := EnsureCert(ctx, client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EnsureCert(ctx, client, cfg) // a second replica reuses the Secret
	if err != nil || !bytes.Equal(first.CertPEM, second.CertPEM) || !bytes.Equal(first.KeyPEM, second.KeyPEM) {
		t.Fatalf("second replica got a different certificate (err=%v)", err)
	}
	if err := SetCABundle(ctx, client, cfg.WebhookConfig, first.CertPEM); err != nil {
		t.Fatal(err)
	}
	mwc, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, cfg.WebhookConfig, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mwc.Webhooks[0].ClientConfig.CABundle, first.CertPEM) {
		t.Error("caBundle not set")
	}
	if err := SetCABundle(ctx, client, "missing", first.CertPEM); err == nil {
		t.Error("missing webhook configuration must be an error")
	}
}
