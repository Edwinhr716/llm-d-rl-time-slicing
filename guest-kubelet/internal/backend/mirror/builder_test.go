package mirror

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// ref returns a pointer to a copy of v.
func ref[T any](v T) *T { return &v }

func testConfig() Config {
	return Config{
		HostNode: "real-node", VirtualNode: "vk-x",
		CPUHeadroom: resource.MustParse("1"), MemoryHeadroom: resource.MustParse("4Gi"),
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
	m, err := Build(g, ref(testConfig()))
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
	if q := c.Resources.Limits[api.PooledResource]; q.Value() != 1 {
		t.Errorf("want %s: 1, got %v", api.PooledResource, c.Resources.Limits)
	}
	if len(c.Resources.Claims) != 0 || len(s.ResourceClaims) != 0 {
		t.Errorf("no DRA claim: %v %v", c.Resources.Claims, s.ResourceClaims)
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
	m, err := Build(g, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if m.OwnerReferences != nil || PooledQtyOf(m) != 0 {
		t.Errorf("unexpected owner/GPU: %v %v", m.OwnerReferences, m.Spec.Containers[0].Resources)
	}
}

func TestLimitOnlyIsCapped(t *testing.T) {
	g := testGuest()
	g.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("7")},
	}
	m, _ := Build(g, ref(testConfig()))
	r := m.Spec.Containers[0].Resources
	if q := r.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("limit-only cpu should get an explicit capped request, got %v", r.Requests)
	}
}

func TestNoCapWhenHeadroomZero(t *testing.T) {
	cfg := testConfig()
	cfg.CPUHeadroom = resource.Quantity{}
	m, _ := Build(testGuest(), &cfg)
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

func TestDeviceReserve(t *testing.T) {
	// L4: 23034 MiB of device memory; x1.1 = 25337.4 MiB, rounded up to 25338 MiB.
	got, err := DeviceReserve(resource.MustParse("23034Mi"), 1.1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cmp(resource.MustParse("25338Mi")) != 0 {
		t.Errorf("reserve = %s, want 25338Mi", got.String())
	}
	if _, err := DeviceReserve(resource.MustParse("1Gi"), 0.9); err == nil {
		t.Error("a factor below 1 must be refused")
	}
	if z, err := DeviceReserve(resource.Quantity{}, 1.1); err != nil || !z.IsZero() {
		t.Errorf("zero device memory: %s, %v", z.String(), err)
	}
}

func TestMirrorMemoryLimitAddsDeviceReserve(t *testing.T) {
	cfg := testConfig()
	cfg.DeviceMemoryReserve = resource.MustParse("25338Mi")
	build := func(guest *corev1.Pod) *corev1.Pod {
		t.Helper()
		mirror, err := Build(guest, &cfg)
		if err != nil {
			t.Fatal(err)
		}
		return mirror
	}
	limit := func(mirror *corev1.Pod, i int) resource.Quantity {
		return mirror.Spec.Containers[i].Resources.Limits[corev1.ResourceMemory]
	}

	// GPU container with a 30Gi limit: limit = 30Gi + reserve; request still capped.
	mirror := build(testGuest())
	want := resource.MustParse("30Gi")
	want.Add(resource.MustParse("25338Mi"))
	if q := limit(mirror, 0); q.Cmp(want) != 0 {
		t.Errorf("gpu limit = %s, want %s", q.String(), want.String())
	}
	if q := mirror.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]; q.Cmp(resource.MustParse("4Gi")) != 0 {
		t.Errorf("request = %s, want the 4Gi headroom", q.String())
	}

	// No limit: the base is the (uncapped) request.
	guest := testGuest()
	delete(guest.Spec.Containers[0].Resources.Limits, corev1.ResourceMemory)
	want = resource.MustParse("24Gi")
	want.Add(resource.MustParse("25338Mi"))
	if q := limit(build(guest), 0); q.Cmp(want) != 0 {
		t.Errorf("no-limit gpu limit = %s, want %s", q.String(), want.String())
	}

	// Neither limit nor request: the reserve alone (never unlimited).
	guest = testGuest()
	delete(guest.Spec.Containers[0].Resources.Limits, corev1.ResourceMemory)
	delete(guest.Spec.Containers[0].Resources.Requests, corev1.ResourceMemory)
	if q := limit(build(guest), 0); q.Cmp(resource.MustParse("25338Mi")) != 0 {
		t.Errorf("bare gpu limit = %s, want the reserve", q.String())
	}

	// A sidecar without the GPU keeps its own limit; a CPU-only guest is unchanged.
	guest = testGuest()
	side := corev1.Container{Name: "side", Image: "busybox"}
	side.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
	guest.Spec.Containers = append(guest.Spec.Containers, side)
	if q := limit(build(guest), 1); q.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("sidecar limit = %s, want 1Gi", q.String())
	}
	echo := corev1.Container{Name: "echo", Image: "agnhost"}
	echo.Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")}
	cpuOnly := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "echo"}}
	cpuOnly.Spec.Containers = []corev1.Container{echo}
	if got := build(cpuOnly).Spec.Containers[0].Resources.Limits; got != nil {
		t.Errorf("cpu-only guest got limits %v", got)
	}
}
