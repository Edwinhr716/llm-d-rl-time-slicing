// Tests for render.sh (decision D-NS-18, hooks M1, M2, M4 and M6). They run the script with
// bash and decode what it prints with the client-go scheme, so a GROUP_NODES=2 manifest that
// is not valid YAML or not a valid object fails here, not on a cluster.
package demo_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	deployDir = "../../deploy"
	rlDir     = "../../../guides/rl-batch-interleaving/examples"
	image     = "registry.example/guest-kubelet/guest-kubelet:abc1234"
	host0     = "gke-demo-pool-a1b2"
	host1     = "gke-demo-pool-c3d4"
	vnode0    = "vk-a1b2"
	vnode1    = "vk-c3d4"
	twoHosts  = host0 + " " + host1
)

// render runs render.sh with only PATH and env set.
func render(t *testing.T, part string, env ...string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", "render.sh", part)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), errors.New(strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

func mustRender(t *testing.T, part string, env ...string) string {
	t.Helper()
	out, err := render(t, part, env...)
	if err != nil {
		t.Fatalf("render.sh %s %v: %v", part, env, err)
	}
	return out
}

// today is the Makefile's SUBST: sed replaces the first match of each placeholder per line.
func today(t *testing.T, path, host, vnode string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(raw), "\n")
	for i, line := range lines {
		line = strings.Replace(line, "__IMAGE__", image, 1)
		line = strings.Replace(line, "__HOST__", host, 1)
		lines[i] = strings.Replace(line, "__VNODE__", vnode, 1)
	}
	return strings.Join(lines, "")
}

func decode(t *testing.T, out string) []runtime.Object {
	t.Helper()
	reader := utilyaml.NewYAMLReader(bufio.NewReader(strings.NewReader(out)))
	var objs []runtime.Object
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return objs
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(doc, nil, nil)
		if err != nil {
			t.Fatalf("decode %q: %v", doc, err)
		}
		objs = append(objs, obj)
	}
}

func ofType[T runtime.Object](objs []runtime.Object) []T {
	var out []T
	for _, o := range objs {
		if v, ok := o.(T); ok {
			out = append(out, v)
		}
	}
	return out
}

func needRL(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(rlDir + "/rl-job.yaml"); err != nil {
		t.Skipf("RL manifests not in this checkout (%v); the repo-root CI runs this", err)
	}
}

func TestRender_OneNode_ByteIdenticalToMakefile(t *testing.T) {
	for part, file := range map[string]string{"vk": "deployment.yaml", "claim": "m1/claim.yaml", "guest": "m1/vllm-guest.yaml"} {
		want := today(t, deployDir+"/"+file, host0, vnode0)
		for _, env := range [][]string{
			{"IMAGE=" + image, "HOST=" + host0},
			{"IMAGE=" + image, "HOSTS=" + host0, "GROUP_NODES=1", "CLAIM_MODE=static"},
		} {
			if got := mustRender(t, part, env...); got != want {
				t.Errorf("%s %v: not byte-identical to the Makefile's render\n got: %q\nwant: %q", part, env, got, want)
			}
		}
	}
}

func TestRender_OneNode_RLUnchanged(t *testing.T) {
	needRL(t)
	for part, file := range map[string]string{"rl-job": "rl-job.yaml", "rl-claims": "resource-claims.yaml"} {
		want, err := os.ReadFile(rlDir + "/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustRender(t, part, "GROUP_NODES=1"); got != string(want) {
			t.Errorf("%s: GROUP_NODES=1 must print %s unchanged", part, file)
		}
	}
}

func TestRender_OneNode_DonorModeChangesOnlyTheClaimFlag(t *testing.T) {
	got := mustRender(t, "vk", "IMAGE="+image, "HOST="+host0, "CLAIM_MODE=donor")
	want := strings.Replace(today(t, deployDir+"/deployment.yaml", host0, vnode0),
		"- --gpu-claim=shared-m1-gpu-claim\n", "- --gpu-claim-mode=donor\n", 1)
	if got != want {
		t.Errorf("GROUP_NODES=1 donor vk:\n got: %q\nwant: %q", got, want)
	}
}

// oneNodeVK is today's VK Deployment for host, decoded.
func oneNodeVK(t *testing.T, host, vnode string) *appsv1.Deployment {
	t.Helper()
	deps := ofType[*appsv1.Deployment](decode(t, today(t, deployDir+"/deployment.yaml", host, vnode)))
	if len(deps) != 1 {
		t.Fatalf("deployment.yaml: %d Deployments", len(deps))
	}
	return deps[0]
}

func TestRender_TwoNodes_OneVKPerHost(t *testing.T) {
	for _, mode := range []string{"static", "donor"} {
		deps := ofType[*appsv1.Deployment](decode(t, mustRender(t, "vk",
			"IMAGE="+image, "HOSTS="+twoHosts, "GROUP_NODES=2", "CLAIM_MODE="+mode)))
		if len(deps) != 2 {
			t.Fatalf("%s: want 2 Deployments, got %d", mode, len(deps))
		}
		for i, hv := range [][2]string{{host0, vnode0}, {host1, vnode1}} {
			// Everything but these fields must equal today's Deployment for that host.
			want := oneNodeVK(t, hv[0], hv[1])
			want.Name = "guest-kubelet-" + hv[1]
			labels := map[string]string{"app": "guest-kubelet", "timeslice.io/vk-node": hv[1]}
			want.Spec.Selector.MatchLabels = labels
			want.Spec.Template.Labels = labels
			claim := "--gpu-claim=shared-m1-gpu-claim-" + strconv.Itoa(i)
			if mode == "donor" {
				claim = "--gpu-claim-mode=donor"
			}
			want.Spec.Template.Spec.Containers[0].Args = []string{
				"--leader-elect=true", "--host-node=" + hv[0], "--node-name=" + hv[1], claim,
				"--mirror-cpu-headroom=1", "--mirror-memory-headroom=4Gi", "--mirror-owner-ref=true",
			}
			if !reflect.DeepEqual(deps[i], want) {
				t.Errorf("%s host %d:\n got: %+v\nwant: %+v", mode, i, deps[i], want)
			}
		}
	}
}

func TestRender_TwoNodes_ClaimAndStandinPerHost(t *testing.T) {
	for _, mode := range []string{"static", "donor"} {
		objs := decode(t, mustRender(t, "claim", "HOSTS="+twoHosts, "GROUP_NODES=2", "CLAIM_MODE="+mode))
		claims, pods := ofType[*resourcev1.ResourceClaim](objs), ofType[*corev1.Pod](objs)
		if len(claims) != 2 || len(pods) != 2 {
			t.Fatalf("%s: want 2 claims and 2 pods, got %d and %d", mode, len(claims), len(pods))
		}
		for i, host := range []string{host0, host1} {
			suffix := "-" + strconv.Itoa(i)
			if claims[i].Name != "shared-m1-gpu-claim"+suffix {
				t.Errorf("%s claim %d: name %q", mode, i, claims[i].Name)
			}
			pod := pods[i]
			if pod.Name != "trainer-standin"+suffix || pod.Spec.NodeSelector["kubernetes.io/hostname"] != host ||
				*pod.Spec.ResourceClaims[0].ResourceClaimName != claims[i].Name {
				t.Errorf("%s stand-in %d: %s on %v with claim %s", mode, i, pod.Name, pod.Spec.NodeSelector,
					*pod.Spec.ResourceClaims[0].ResourceClaimName)
			}
			_, labelled := pod.Labels["timeslice.io/job-id"]
			if labelled != (mode == "donor") {
				t.Errorf("%s stand-in %d: labels %v (the donor selector needs timeslice.io/job-id in donor mode only)",
					mode, i, pod.Labels)
			}
		}
	}
}

func TestRender_TwoNodes_GuestDeploymentOnePerVKNode(t *testing.T) {
	deps := ofType[*appsv1.Deployment](decode(t, mustRender(t, "guest", "HOSTS="+twoHosts, "GROUP_NODES=2")))
	if len(deps) != 1 {
		t.Fatalf("want 1 Deployment, got %d", len(deps))
	}
	dep := deps[0]
	if dep.Name != "vllm-guest" || dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Errorf("name %q replicas %v", dep.Name, dep.Spec.Replicas)
	}
	app := map[string]string{"app": "vllm-guest"}
	if !reflect.DeepEqual(dep.Spec.Selector.MatchLabels, app) || !reflect.DeepEqual(dep.Spec.Template.Labels, app) {
		t.Errorf("selector %v, pod labels %v", dep.Spec.Selector.MatchLabels, dep.Spec.Template.Labels)
	}
	aff := dep.Spec.Template.Spec.Affinity
	if aff == nil || aff.NodeAffinity == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("affinity: %+v", aff)
	}
	expr := aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0]
	if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn ||
		!slices.Equal(expr.Values, []string{vnode0, vnode1}) {
		t.Errorf("node affinity: %+v", expr)
	}
	anti := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(anti) != 1 || anti[0].TopologyKey != "kubernetes.io/hostname" ||
		!reflect.DeepEqual(anti[0].LabelSelector.MatchLabels, app) {
		t.Errorf("pod anti-affinity: %+v", anti)
	}
	// The pod spec is today's guest Pod spec with the node selector moved into the affinity.
	pods := ofType[*corev1.Pod](decode(t, today(t, deployDir+"/m1/vllm-guest.yaml", host0, vnode0)))
	want := pods[0].Spec
	want.NodeSelector = nil
	want.Affinity = aff
	if !reflect.DeepEqual(dep.Spec.Template.Spec, want) {
		t.Errorf("pod spec differs from today's guest:\n got: %+v\nwant: %+v", dep.Spec.Template.Spec, want)
	}
}

func TestRender_TwoNodes_RLTwoTrainers(t *testing.T) {
	needRL(t)
	env := []string{"HOSTS=" + twoHosts, "GROUP_NODES=2"}
	claims := ofType[*resourcev1.ResourceClaim](decode(t, mustRender(t, "rl-claims", env...)))
	if len(claims) != 2 || claims[0].Name != "shared-trainers-gpu-claim-0" || claims[1].Name != "shared-trainers-gpu-claim-1" {
		t.Fatalf("rl claims: %d", len(claims))
	}
	objs := decode(t, mustRender(t, "rl-job", env...))
	raw, err := os.ReadFile(rlDir + "/rl-job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	base := decode(t, string(raw))

	cm := ofType[*corev1.ConfigMap](objs)[0]
	baseCM := ofType[*corev1.ConfigMap](base)[0]
	for _, want := range []string{"trainer.nnodes=2 \\", "gpus >= 3", `--resources='{"trainer_node": 100}'`} {
		if !strings.Contains(cm.Data["run_head.sh"], want) {
			t.Errorf("run_head.sh lacks %q", want)
		}
	}
	worker := cm.Data["run_trainer_worker.sh"]
	if !strings.Contains(worker, `--resources='{"trainer_node": 100}'`) || !strings.Contains(worker, "rl-head.default.svc") {
		t.Errorf("run_trainer_worker.sh: %q", worker)
	}
	for _, key := range []string{"data_prep.py", "install_common.sh", "run_rollout.sh"} {
		if cm.Data[key] != baseCM.Data[key] {
			t.Errorf("ConfigMap key %s changed", key)
		}
	}

	pods := map[string]*corev1.Pod{}
	for _, pod := range ofType[*corev1.Pod](objs) {
		pods[pod.Name] = pod
	}
	basePods := map[string]*corev1.Pod{}
	for _, pod := range ofType[*corev1.Pod](base) {
		basePods[pod.Name] = pod
	}
	if len(pods) != 3 {
		t.Fatalf("want rl-head, rl-trainer-1, rl-rollout; got %v", slices.Sorted(maps.Keys(pods)))
	}
	if !reflect.DeepEqual(pods["rl-rollout"], basePods["rl-rollout"]) {
		t.Error("rl-rollout must be unchanged (its anti-affinity to the group label keeps it off both hosts)")
	}
	for name, want := range map[string][2]string{
		"rl-head":      {host0, "shared-trainers-gpu-claim-0"},
		"rl-trainer-1": {host1, "shared-trainers-gpu-claim-1"},
	} {
		pod := pods[name]
		if pod == nil {
			t.Fatalf("no pod %s", name)
		}
		sel := pod.Spec.NodeSelector
		if sel["kubernetes.io/hostname"] != want[0] || sel["group.timeslice.io/trainers"] != "true" {
			t.Errorf("%s node selector %v", name, pod.Spec.NodeSelector)
		}
		if *pod.Spec.ResourceClaims[0].ResourceClaimName != want[1] {
			t.Errorf("%s claim %s, want %s", name, *pod.Spec.ResourceClaims[0].ResourceClaimName, want[1])
		}
		if pod.Labels["timeslice.io/job-id"] != "rl-trainer" || pod.Labels["timeslice.io/group"] != "trainers" {
			t.Errorf("%s labels %v", name, pod.Labels)
		}
	}
	if pods["rl-trainer-1"].Labels["app"] != "rl-trainer-1" ||
		!strings.Contains(pods["rl-trainer-1"].Spec.Containers[0].Args[0], "run_trainer_worker.sh") {
		t.Error("rl-trainer-1 must run run_trainer_worker.sh and stay out of the rl-head Service")
	}
	if !reflect.DeepEqual(ofType[*corev1.Service](objs), ofType[*corev1.Service](base)) {
		t.Error("Service rl-head must be unchanged")
	}
}

func TestRender_Labels(t *testing.T) {
	got := mustRender(t, "labels", "HOSTS="+twoHosts, "GROUP_NODES=2")
	want := "kubectl label node " + host0 + " group.timeslice.io/trainers=true --overwrite\n" +
		"kubectl label node " + host1 + " group.timeslice.io/trainers=true --overwrite\n"
	if got != want {
		t.Errorf("labels:\n got: %q\nwant: %q", got, want)
	}
}

func TestRender_Errors(t *testing.T) {
	for name, tc := range map[string]struct {
		part string
		env  []string
		want string
	}{
		"three nodes":     {"vk", []string{"IMAGE=x", "HOSTS=a-1 b-2 c-3", "GROUP_NODES=3"}, "GROUP_NODES must be 1 or 2"},
		"host count":      {"claim", []string{"HOSTS=" + host0, "GROUP_NODES=2"}, "HOSTS has 1 hosts"},
		"same VK name":    {"vk", []string{"IMAGE=x", "HOSTS=pool-a-x1 pool-b-x1", "GROUP_NODES=2"}, "share the VK node name vk-x1"},
		"claim mode":      {"vk", []string{"IMAGE=x", "HOSTS=" + host0, "CLAIM_MODE=per-host"}, "CLAIM_MODE must be"},
		"no image":        {"vk", []string{"HOSTS=" + host0}, "IMAGE is not set"},
		"unknown part":    {"all", nil, "usage"},
		"missing anchors": {"rl-job", []string{"HOSTS=" + twoHosts, "GROUP_NODES=2", "RL_DIR=" + deployDir}, ""},
	} {
		_, err := render(t, tc.part, tc.env...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want an error containing %q, got %v", name, tc.want, err)
		}
	}
}
