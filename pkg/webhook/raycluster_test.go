package webhook_test

import (
	"encoding/json"
	"strings"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

// rayClusterJSON is a RayCluster as KubeRay creates it for a RayJob: a head without template
// metadata, a donor trainer group and a rollout group.
const rayClusterJSON = `{
  "apiVersion": "ray.io/v1", "kind": "RayCluster",
  "metadata": {"name": "longcot-abc12", "namespace": "team-a"},
  "spec": {
    "headGroupSpec": {"rayStartParams": {}, "template": {"spec": {"containers": [{"name": "ray-head", "image": "rayproject/ray"}]}}},
    "workerGroupSpecs": [
      {"groupName": "trainer", "template": {"metadata": {"labels": {"timeslice.io/donor": "true"}},
        "spec": {"containers": [{"name": "ray-worker", "image": "verl", "env": [{"name": "HF_HOME", "value": "/w"}]}]}}},
      {"groupName": "rollout", "template": {"metadata": {},
        "spec": {"containers": [{"name": "ray-worker", "image": "verl"}]}}}
    ]
  }
}`

type rcTemplate struct {
	Metadata struct {
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Containers []struct {
			Env []struct{ Name, Value string } `json:"env"`
		} `json:"containers"`
	} `json:"spec"`
}

type rcObj struct {
	Spec struct {
		HeadGroupSpec struct {
			Template rcTemplate `json:"template"`
		} `json:"headGroupSpec"`
		WorkerGroupSpecs []struct {
			Template rcTemplate `json:"template"`
		} `json:"workerGroupSpecs"`
	} `json:"spec"`
}

// admitRayCluster admits raw and returns the response and the patched object.
func admitRayCluster(t *testing.T, cfg *webhook.Config, raw []byte, op admissionv1.Operation) (*admissionv1.AdmissionResponse, []byte) {
	t.Helper()
	req := &admissionv1.AdmissionRequest{
		UID:       "uid-rc",
		Kind:      metav1.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"},
		Resource:  metav1.GroupVersionResource{Group: "ray.io", Version: "v1", Resource: "rayclusters"},
		Namespace: testNS,
		Operation: op,
		UserInfo:  authv1.UserInfo{Username: "system:serviceaccount:kuberay:kuberay-operator"},
		Object:    runtime.RawExtension{Raw: raw},
	}
	resp := webhook.Admit(t.Context(), req, *cfg)
	if resp == nil || !resp.Allowed {
		t.Fatalf("not allowed: %+v", resp)
	}
	if len(resp.Patch) == 0 {
		return resp, raw
	}
	p, err := jsonpatch.DecodePatch(resp.Patch)
	if err != nil {
		t.Fatalf("decode patch %s: %v", resp.Patch, err)
	}
	out, err := p.Apply(raw)
	if err != nil {
		t.Fatalf("apply patch %s: %v", resp.Patch, err)
	}
	return resp, out
}

func rcEnv(tmpl *rcTemplate) map[string]string {
	m := map[string]string{}
	for _, c := range tmpl.Spec.Containers {
		for _, e := range c.Env {
			m[e.Name] = e.Value
		}
	}
	return m
}

func TestRayCluster_WiresEveryGroup(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	_, out := admitRayCluster(t, cfg, []byte(rayClusterJSON), admissionv1.Create)
	var rc rcObj
	if err := json.Unmarshal(out, &rc); err != nil {
		t.Fatal(err)
	}
	wantGroup := webhook.GroupName(testNS, "longcot-abc12", "trainer")
	templates := map[string]*rcTemplate{
		"head":    &rc.Spec.HeadGroupSpec.Template,
		"trainer": &rc.Spec.WorkerGroupSpecs[0].Template,
		"rollout": &rc.Spec.WorkerGroupSpecs[1].Template,
	}
	for name, tmpl := range templates {
		env := rcEnv(tmpl)
		if env[webhook.EnvFullyAsync] != "1" || env[webhook.EnvJobID] != "longcot-abc12" ||
			env[webhook.EnvGroup] != wantGroup || env[webhook.EnvOrchAddr] != orchAddr ||
			env[webhook.EnvNCCLCuMem] != "0" || env[webhook.EnvNCCLNVLS] != "0" {
			t.Errorf("%s env = %v", name, env)
		}
		rl := tmpl.Metadata.Labels[webhook.LabelRLIntegration]
		if name == "trainer" {
			if rl != "" || tmpl.Metadata.Labels[webhook.LabelDonor] != "true" {
				t.Errorf("trainer labels = %v", tmpl.Metadata.Labels)
			}
			if env["HF_HOME"] != "/w" {
				t.Errorf("trainer lost its env: %v", env)
			}
		} else if rl != "true" {
			t.Errorf("%s labels = %v, want %s=true", name, tmpl.Metadata.Labels, webhook.LabelRLIntegration)
		}
	}
	// Reinvocation: the wired object yields no patch.
	resp, _ := admitRayCluster(t, cfg, out, admissionv1.Create)
	if len(resp.Patch) != 0 {
		t.Errorf("second admission patched again: %s", resp.Patch)
	}
}

func TestRayCluster_ExplicitIdentityAndUserEnvKept(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	raw := strings.Replace(rayClusterJSON, `{"timeslice.io/donor": "true"}`,
		`{"timeslice.io/donor": "true", "timeslice.io/job-id": "job-x", "timeslice.io/group": "grp-y"}`, 1)
	raw = strings.Replace(raw, `"env": [{"name": "HF_HOME", "value": "/w"}]`,
		`"env": [{"name": "TIMESLICE_FULLY_ASYNC", "value": "0"}]`, 1)
	_, out := admitRayCluster(t, cfg, []byte(raw), admissionv1.Create)
	var rc rcObj
	if err := json.Unmarshal(out, &rc); err != nil {
		t.Fatal(err)
	}
	head := rcEnv(&rc.Spec.HeadGroupSpec.Template)
	if head[webhook.EnvJobID] != "job-x" || head[webhook.EnvGroup] != "grp-y" {
		t.Errorf("head env = %v", head)
	}
	if v := rcEnv(&rc.Spec.WorkerGroupSpecs[0].Template)[webhook.EnvFullyAsync]; v != "0" {
		t.Errorf("user-set %s = %q, want kept 0", webhook.EnvFullyAsync, v)
	}
}

func TestRayCluster_Unchanged(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	noDonor := strings.Replace(rayClusterJSON, `"timeslice.io/donor": "true"`, `"app": "x"`, 1)
	cases := map[string]struct {
		raw string
		op  admissionv1.Operation
	}{
		"no donor group": {noDonor, admissionv1.Create},
		"update":         {rayClusterJSON, admissionv1.Update},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, _ := admitRayCluster(t, cfg, []byte(tc.raw), tc.op)
			if len(resp.Patch) != 0 {
				t.Errorf("patched: %s", resp.Patch)
			}
		})
	}
}

func TestRayCluster_TwoDonorGroupsLabelOnly(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	raw := strings.Replace(rayClusterJSON, `"template": {"metadata": {},`,
		`"template": {"metadata": {"labels": {"timeslice.io/donor": "true"}},`, 1)
	_, out := admitRayCluster(t, cfg, []byte(raw), admissionv1.Create)
	var rc rcObj
	if err := json.Unmarshal(out, &rc); err != nil {
		t.Fatal(err)
	}
	if env := rcEnv(&rc.Spec.HeadGroupSpec.Template); len(env) != 0 {
		t.Errorf("head env with two donor groups = %v, want none (each donor pod derives its own group)", env)
	}
	if rc.Spec.HeadGroupSpec.Template.Metadata.Labels[webhook.LabelRLIntegration] != "true" {
		t.Errorf("head not labelled for the RL integration")
	}
}
