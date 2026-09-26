package mirror

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testConfig() Config {
	return Config{
		HostNode: "real-node", VirtualNode: "vk-x",
		CPUHeadroom: resource.MustParse("1"), MemoryHeadroom: resource.MustParse("4Gi"),
		GPUClaim:      "shared-gpu",
		HostTaints:    []corev1.Taint{{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}},
		GuestTaintKey: "timeslice.io/guest",
		OwnerRef:      true,
	}
}

func testGuest() *corev1.Pod {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "vllm", UID: types.UID("guest-uid"),
			Labels: map[string]string{"app": "vllm"}, Annotations: map[string]string{"a": "b"},
		},
		Spec: corev1.PodSpec{
			NodeName:     "vk-x",
			NodeSelector: map[string]string{"timeslice.io/virtual-node": "true"},
			Affinity:     &corev1.Affinity{},
			Tolerations: []corev1.Toleration{
				{Key: "timeslice.io/guest", Operator: corev1.TolerationOpExists},
				{Key: "other", Operator: corev1.TolerationOpExists},
			},
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: "timeslice.io/serving"}},
			Containers: []corev1.Container{{
				Name: "vllm", Image: "vllm/vllm-openai:v0.9.2",
				ReadinessProbe: probe, LivenessProbe: probe, StartupProbe: probe,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("24Gi"),
						GPUResource: resource.MustParse("1"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("30Gi"), GPUResource: resource.MustParse("1")},
				},
				Env: []corev1.EnvVar{
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "APP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['app']"}}},
					{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
				},
			}},
		},
	}
}

func TestBuildMirror(t *testing.T) {
	g := testGuest()
	m, err := Build(g, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "vllm-m" || m.Namespace != "ns" {
		t.Errorf("name %s/%s", m.Namespace, m.Name)
	}
	if len(m.Labels) != 2 || m.Labels[LabelMirrorOf] != "guest-uid" || m.Labels[LabelMirrorNode] != "vk-x" {
		t.Errorf("labels: %v (guest labels must not be copied)", m.Labels)
	}
	if m.Annotations[AnnotationGuestName] != "vllm" || m.Annotations[AnnotationGuestSpecHash] != SpecHash(g) {
		t.Errorf("annotations: %v", m.Annotations)
	}
	if len(m.OwnerReferences) != 1 || m.OwnerReferences[0].UID != "guest-uid" {
		t.Errorf("ownerRefs: %v", m.OwnerReferences)
	}
	s := m.Spec
	if s.NodeName != "real-node" || s.NodeSelector != nil || s.Affinity != nil || s.ReadinessGates != nil {
		t.Errorf("placement not rewritten: node=%s sel=%v aff=%v", s.NodeName, s.NodeSelector, s.Affinity)
	}
	if s.Hostname != "vllm" {
		t.Errorf("hostname %q", s.Hostname)
	}
	c := s.Containers[0]
	if c.ReadinessProbe != nil || c.LivenessProbe != nil || c.StartupProbe != nil {
		t.Error("probes must be stripped")
	}
	if _, ok := c.Resources.Requests[GPUResource]; ok {
		t.Error("nvidia.com/gpu request must be removed")
	}
	if _, ok := c.Resources.Limits[GPUResource]; ok {
		t.Error("nvidia.com/gpu limit must be removed")
	}
	if q := c.Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("cpu request not capped: %s", q.String())
	}
	if q := c.Resources.Requests[corev1.ResourceMemory]; q.Cmp(resource.MustParse("4Gi")) != 0 {
		t.Errorf("memory request not capped: %s", q.String())
	}
	if q := c.Resources.Limits[corev1.ResourceMemory]; q.Cmp(resource.MustParse("30Gi")) != 0 {
		t.Errorf("memory limit must be kept: %s", q.String())
	}
	if len(c.Resources.Claims) != 1 || c.Resources.Claims[0].Name != ClaimRefName {
		t.Errorf("container claims: %v", c.Resources.Claims)
	}
	if len(s.ResourceClaims) != 1 || *s.ResourceClaims[0].ResourceClaimName != "shared-gpu" {
		t.Errorf("pod claims: %v", s.ResourceClaims)
	}
	var keys []string
	for _, tol := range s.Tolerations {
		keys = append(keys, tol.Key)
	}
	if len(keys) != 2 || keys[0] != "other" || keys[1] != "nvidia.com/gpu" {
		t.Errorf("tolerations: %v", keys)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	if env["POD_NAME"].Value != "vllm" || env["APP"].Value != "vllm" || env["POD_IP"].ValueFrom == nil {
		t.Errorf("env: %+v", c.Env)
	}
	// The guest must be untouched.
	if g.Spec.NodeName != "vk-x" || g.Spec.Containers[0].ReadinessProbe == nil {
		t.Error("Build modified the guest")
	}
}

func TestBuildNoOwnerRefAndNoGPU(t *testing.T) {
	g := testGuest()
	c := &g.Spec.Containers[0]
	delete(c.Resources.Requests, GPUResource)
	delete(c.Resources.Limits, GPUResource)
	cfg := testConfig()
	cfg.OwnerRef = false
	cfg.GPUClaim = ""
	m, err := Build(g, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if m.OwnerReferences != nil || m.Spec.ResourceClaims != nil || m.Spec.Containers[0].Resources.Claims != nil {
		t.Errorf("unexpected owner/claims: %v %v", m.OwnerReferences, m.Spec.ResourceClaims)
	}
}

func TestBuildRefusesGPUWithoutClaim(t *testing.T) {
	cfg := testConfig()
	cfg.GPUClaim = ""
	if _, err := Build(testGuest(), cfg); err == nil {
		t.Error("want an error for a GPU guest without a claim")
	}
}

func TestLimitOnlyIsCapped(t *testing.T) {
	g := testGuest()
	g.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("7")},
	}
	m, _ := Build(g, testConfig())
	r := m.Spec.Containers[0].Resources
	if q := r.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("limit-only cpu should get an explicit capped request, got %v", r.Requests)
	}
}

func TestNoCapWhenHeadroomZero(t *testing.T) {
	cfg := testConfig()
	cfg.CPUHeadroom = resource.Quantity{}
	m, _ := Build(testGuest(), cfg)
	if q := m.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("6")) != 0 {
		t.Errorf("cpu must not be capped: %s", q.String())
	}
}

func TestSpecHashIgnoresTokenMount(t *testing.T) {
	a, b := testGuest(), testGuest()
	a.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "kube-api-access-abcde", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"}}
	b.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "kube-api-access-vwxyz", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount"}}
	if SpecHash(a) != SpecHash(b) {
		t.Error("two pods from one template must hash the same")
	}
	b.Spec.Containers[0].Image = "other:2"
	if SpecHash(a) == SpecHash(b) {
		t.Error("a different image must change the hash")
	}
	if len(a.Spec.Containers[0].VolumeMounts) != 1 {
		t.Error("SpecHash modified the guest")
	}
}
