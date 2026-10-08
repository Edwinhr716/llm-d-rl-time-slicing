package webhook_test

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

const rlImage = "registry.example/rl-integration:v1"

var rlFlags = append(append([]string{}, todayFlags...), "--rl-integration-image="+rlImage)

func rlPython() string { return webhook.DefaultRLIntegrationPath + "/python" }

// checkInjected asserts the volume, the single init container (last), the mount and PYTHONPATH
// first on every container.
func checkInjected(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	vols := 0
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if v.Name == webhook.RLIntegrationVolume {
			vols++
			if v.EmptyDir == nil {
				t.Errorf("volume %s is not an emptyDir", v.Name)
			}
		}
	}
	if vols != 1 {
		t.Errorf("%d %s volumes, want 1", vols, webhook.RLIntegrationVolume)
	}
	inits := pod.Spec.InitContainers
	if len(inits) == 0 || inits[len(inits)-1].Name != webhook.RLIntegrationInit {
		t.Fatalf("init containers %v: want %s last", inits, webhook.RLIntegrationInit)
	}
	ic := inits[len(inits)-1]
	if ic.Image != rlImage || strings.Join(ic.Command, " ") != "cp -R "+webhook.RLIntegrationSource+" /timeslice-rl/" {
		t.Errorf("init container = %s %v", ic.Image, ic.Command)
	}
	sc := ic.SecurityContext
	if sc == nil || sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || sc.AllowPrivilegeEscalation == nil ||
		*sc.AllowPrivilegeEscalation || sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 {
		t.Errorf("init container security context = %+v", sc)
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		mounted := false
		for _, m := range c.VolumeMounts {
			if m.Name == webhook.RLIntegrationVolume {
				mounted = m.MountPath == webhook.DefaultRLIntegrationPath && m.ReadOnly
			}
		}
		if !mounted {
			t.Errorf("container %s: no read-only mount at %s", c.Name, webhook.DefaultRLIntegrationPath)
		}
		pp, ok := env(c, webhook.EnvPythonPath)
		if !ok || strings.Split(pp, ":")[0] != rlPython() {
			t.Errorf("container %s: PYTHONPATH = %q, want %s first", c.Name, pp, rlPython())
		}
	}
}

func TestAdmit_RLIntegration_Donor(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	pod := kuberayDonor()
	pod.Spec.InitContainers = []corev1.Container{{Name: "user-init", Image: "busybox"}}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name: "ray-worker", Image: "verl:latest",
		Env: []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: webhook.EnvPythonPath, Value: "/app:/lib"}},
	})
	res := create(t, cfg, pod)
	got := mustAllow(t, res)
	checkInjected(t, got)
	// The donor wiring is still there next to PYTHONPATH (both edit env in one patch).
	for i := range got.Spec.Containers {
		if v, _ := env(&got.Spec.Containers[i], webhook.EnvOrchAddr); v != orchAddr {
			t.Errorf("container %d: %s = %q", i, webhook.EnvOrchAddr, v)
		}
	}
	if pp, _ := env(&got.Spec.Containers[1], webhook.EnvPythonPath); pp != rlPython()+":/app:/lib" {
		t.Errorf("existing PYTHONPATH = %q, want prepended", pp)
	}
	if v, _ := env(&got.Spec.Containers[1], "A"); v != "1" {
		t.Errorf("user env lost: A = %q", v)
	}
	if got.Spec.InitContainers[0].Name != "user-init" {
		t.Errorf("user init container moved: %v", got.Spec.InitContainers)
	}
	// Reinvocation: an admitted pod is not patched again.
	again := create(t, cfg, got)
	mustAllow(t, again)
	if n := again.patchOps(t); n != 0 {
		t.Errorf("reinvocation patched %d ops: %s", n, again.resp.Patch)
	}
}

func TestAdmit_RLIntegration_OptInLabel(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	sampler := newPod(map[string]string{
		"app": "rl", webhook.LabelRLIntegration: "true",
		webhook.LabelRayCluster: "rc-7f2x9", webhook.LabelRayGroup: "samplers",
	})
	got := mustAllow(t, create(t, cfg, sampler))
	checkInjected(t, got)
	// Only the injection: no identity labels, no client wiring.
	if _, ok := got.Labels[webhook.LabelGroup]; ok {
		t.Errorf("opt-in pod got a group label: %v", got.Labels)
	}
	if _, ok := env(&got.Spec.Containers[0], webhook.EnvOrchAddr); ok {
		t.Error("opt-in pod got the donor client wiring")
	}
}

func TestAdmit_RLIntegration_Off(t *testing.T) {
	// Without --rl-integration-image nothing is injected, into donors or opt-in pods.
	cfg := mustConfig(t, todayFlags)
	donor := mustAllow(t, create(t, cfg, kuberayDonor()))
	if len(donor.Spec.InitContainers) != 0 || len(donor.Spec.Volumes) != 0 {
		t.Errorf("donor injected without an image: %+v", donor.Spec)
	}
	optIn := newPod(map[string]string{webhook.LabelRLIntegration: "true"})
	res := create(t, cfg, optIn)
	mustAllow(t, res)
	if len(res.resp.Patch) != 0 {
		t.Errorf("opt-in pod patched without an image: %s", res.resp.Patch)
	}
}

func TestAdmit_RLIntegration_NotGuests(t *testing.T) {
	// A guest (even one carrying the opt-in label) never gets the RL packages.
	cfg := mustConfig(t, rlFlags)
	g := guest()
	g.Labels[webhook.LabelRLIntegration] = "true"
	got := mustAllow(t, create(t, cfg, g))
	if len(got.Spec.InitContainers) != 0 {
		t.Errorf("guest got an init container: %v", got.Spec.InitContainers)
	}
	if _, ok := env(&got.Spec.Containers[0], webhook.EnvPythonPath); ok {
		t.Error("guest got PYTHONPATH")
	}
}

func TestAdmit_RLIntegration_PythonPathEdges(t *testing.T) {
	cfg := mustConfig(t, rlFlags)
	pod := explicitDonor()
	pod.Spec.Containers = []corev1.Container{
		{Name: "empty", Image: "i", Env: []corev1.EnvVar{{Name: webhook.EnvPythonPath}}},
		{Name: "first", Image: "i", Env: []corev1.EnvVar{{Name: webhook.EnvPythonPath, Value: rlPython() + ":/x"}}},
		{Name: "from", Image: "i", Env: []corev1.EnvVar{{Name: webhook.EnvPythonPath, ValueFrom: &corev1.EnvVarSource{
			ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Key: "k"},
		}}}},
	}
	got := mustAllow(t, create(t, cfg, pod))
	want := map[string]string{"empty": rlPython(), "first": rlPython() + ":/x", "from": ""}
	for i := range got.Spec.Containers {
		c := &got.Spec.Containers[i]
		if pp, _ := env(c, webhook.EnvPythonPath); pp != want[c.Name] {
			t.Errorf("container %s: PYTHONPATH = %q, want %q", c.Name, pp, want[c.Name])
		}
		n := 0
		for _, ev := range c.Env {
			if ev.Name == webhook.EnvPythonPath {
				n++
			}
		}
		if n != 1 {
			t.Errorf("container %s: %d PYTHONPATH entries", c.Name, n)
		}
	}
	// The patch only adds, plus one replace for the "empty" literal.
	var ops []struct{ Op, Path string }
	if err := json.Unmarshal(create(t, cfg, pod).resp.Patch, &ops); err != nil {
		t.Fatal(err)
	}
	replaces := 0
	for _, op := range ops {
		switch op.Op {
		case "replace":
			replaces++
			if op.Path != "/spec/containers/0/env/0/value" {
				t.Errorf("replace at %s", op.Path)
			}
		case "add":
		default:
			t.Errorf("unexpected op %s %s", op.Op, op.Path)
		}
	}
	if replaces != 1 {
		t.Errorf("%d replace ops, want 1", replaces)
	}
}

func TestAdmit_RLIntegration_Verl(t *testing.T) {
	cfg := mustConfig(t, append(append([]string{}, rlFlags...), "--rl-integration-verl"))
	pod := kuberayDonor()
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: webhook.EnvPythonPath, Value: "/app"}}
	got := mustAllow(t, create(t, cfg, pod))
	inits := got.Spec.InitContainers
	ic := inits[len(inits)-1]
	want := "cp -R " + webhook.RLIntegrationSource + " " + webhook.RLIntegrationVerlSource + " /timeslice-rl/"
	if strings.Join(ic.Command, " ") != want {
		t.Errorf("init command = %v, want %s", ic.Command, want)
	}
	verl := webhook.DefaultRLIntegrationPath + "/verl"
	if pp, _ := env(&got.Spec.Containers[0], webhook.EnvPythonPath); pp != rlPython()+":"+verl+":/app" {
		t.Errorf("PYTHONPATH = %q, want %s:%s:/app", pp, rlPython(), verl)
	}
	again := create(t, cfg, got)
	if n := again.patchOps(t); n != 0 {
		t.Errorf("reinvocation patched %d ops: %s", n, again.resp.Patch)
	}
}
