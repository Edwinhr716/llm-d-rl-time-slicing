package webhook_test

import (
	"encoding/json"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

const (
	testNS     = "team-a"
	testUser   = "alice"
	vkUsername = "system:serviceaccount:vk-system:guest-kubelet"
	orchAddr   = "timeslice-orchestrator.timeslice-system.svc:50051"
)

// todayFlags and nsFlags are the two stack profiles of the evaluation (TODAY and NS).
var (
	todayFlags = []string{
		"--guest-steering=required", "--virtual-node-label=timeslice.io/virtual-node",
		"--donor-client-wiring=env", "--orchestrator-addr=" + orchAddr,
		"--group-format=ns.job.group", "--vk-service-account=vk-system:guest-kubelet",
	}
	nsFlags = []string{
		"--guest-steering=preferred", "--virtual-node-label=timeslice.io/guest",
		"--donor-client-wiring=downward", "--podinfo-path=/etc/timeslice/podinfo",
		"--group-format=ns.job.group", "--vk-service-account=vk-system:guest-kubelet",
	}
)

func mustConfig(t *testing.T, args []string) *webhook.Config {
	t.Helper()
	cfg, err := webhook.ConfigFromFlags(args)
	if err != nil {
		t.Fatalf("ConfigFromFlags(%v): %v", args, err)
	}
	return &cfg
}

// newPod is a minimal pause pod with the given labels; opts edit it further.
func newPod(labels map[string]string, opts ...func(*corev1.Pod)) *corev1.Pod {
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "p", Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10"}},
		},
	}
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

// result is one admission: the response and, when allowed, the pod with the patch applied.
type result struct {
	resp *admissionv1.AdmissionResponse
	pod  *corev1.Pod
	raw  []byte
}

func (r *result) message() string {
	if r.resp.Result == nil {
		return ""
	}
	return r.resp.Result.Message
}

func (r *result) patchOps(t *testing.T) int {
	t.Helper()
	if len(r.resp.Patch) == 0 {
		return 0
	}
	var ops []map[string]any
	if err := json.Unmarshal(r.resp.Patch, &ops); err != nil {
		t.Fatalf("patch is not a JSON array: %v", err)
	}
	return len(ops)
}

// admit runs webhook.Admit on pod as user and applies the returned JSONPatch with an RFC 6902
// implementation, so a patch that does not apply fails the test.
func admit(t *testing.T, cfg *webhook.Config, pod *corev1.Pod, user string, op admissionv1.Operation) *result {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		UID:       "uid-1",
		Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
		Resource:  metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
		Namespace: testNS,
		Operation: op,
		UserInfo:  authv1.UserInfo{Username: user},
		Object:    runtime.RawExtension{Raw: raw},
	}
	resp := webhook.Admit(t.Context(), req, *cfg)
	if resp == nil {
		t.Fatal("nil response")
	}
	if resp.UID != req.UID {
		t.Fatalf("response UID %q, want %q", resp.UID, req.UID)
	}
	res := &result{resp: resp}
	if !resp.Allowed {
		return res
	}
	res.raw = raw
	if len(resp.Patch) > 0 {
		if resp.PatchType == nil || *resp.PatchType != admissionv1.PatchTypeJSONPatch {
			t.Fatalf("patch without PatchType JSONPatch")
		}
		patch, err := jsonpatch.DecodePatch(resp.Patch)
		if err != nil {
			t.Fatalf("decode patch %s: %v", resp.Patch, err)
		}
		if res.raw, err = patch.Apply(raw); err != nil {
			t.Fatalf("apply patch %s: %v", resp.Patch, err)
		}
	}
	res.pod = &corev1.Pod{}
	if err := json.Unmarshal(res.raw, res.pod); err != nil {
		t.Fatal(err)
	}
	return res
}

func create(t *testing.T, cfg *webhook.Config, pod *corev1.Pod) *result {
	t.Helper()
	return admit(t, cfg, pod, testUser, admissionv1.Create)
}

func mustAllow(t *testing.T, res *result) *corev1.Pod {
	t.Helper()
	if !res.resp.Allowed {
		t.Fatalf("denied: %s", res.message())
	}
	return res.pod
}

func env(ctr *corev1.Container, name string) (string, bool) {
	for _, ev := range ctr.Env {
		if ev.Name == name {
			return ev.Value, true
		}
	}
	return "", false
}

func countTolerations(pod *corev1.Pod, key string) int {
	n := 0
	for _, tol := range pod.Spec.Tolerations {
		if tol.Key == key {
			n++
		}
	}
	return n
}

func preferredTerms(pod *corev1.Pod) []corev1.PreferredSchedulingTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func podsGVR(resource string) metav1.GroupVersionResource {
	return metav1.GroupVersionResource{Version: "v1", Resource: resource}
}
