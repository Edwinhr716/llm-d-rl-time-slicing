package provider

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

func guestPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		Spec: corev1.PodSpec{
			Containers:  []corev1.Container{{Name: "c", Image: "busybox"}},
			Tolerations: []corev1.Toleration{{Key: GuestTaintKey, Operator: corev1.TolerationOpExists}},
		},
	}
}

func dsPod(name string) *corev1.Pod {
	p := guestPod(name)
	p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	return p
}

// fakeBackend records calls.
type fakeBackend struct {
	created, deleted []string
	cb               func(*corev1.Pod)
}

func (f *fakeBackend) Create(_ context.Context, g *corev1.Pod) error {
	f.created = append(f.created, g.Name)
	return nil
}
func (f *fakeBackend) Delete(_ context.Context, g *corev1.Pod) error {
	f.deleted = append(f.deleted, g.Name)
	return nil
}
func (f *fakeBackend) Get(ns, name string) (*corev1.Pod, error) {
	return nil, errdefs.NotFoundf("%s/%s", ns, name)
}
func (f *fakeBackend) List() ([]*corev1.Pod, error)           { return nil, nil }
func (f *fakeBackend) SetStatusCallback(cb func(*corev1.Pod)) { f.cb = cb }

func TestIsGuest(t *testing.T) {
	if IsGuest(dsPod("ds")) {
		t.Error("a blanket {operator: Exists} toleration must not make a pod a guest")
	}
	if !IsGuest(guestPod("g")) {
		t.Error("a pod tolerating the guest taint by key is a guest")
	}
}

func TestCreateAndDeleteOnlyGuests(t *testing.T) {
	b := &fakeBackend{}
	p := New(b)
	ctx := context.Background()
	for _, pod := range []*corev1.Pod{guestPod("g"), dsPod("ds")} {
		if err := p.CreatePod(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.created) != 1 || b.created[0] != "g" {
		t.Errorf("only the guest should reach the backend, got %v", b.created)
	}
	if err := p.DeletePod(ctx, dsPod("ds")); !errdefs.IsNotFound(err) {
		t.Errorf("deleting a non-guest: want NotFound, got %v", err)
	}
	if err := p.DeletePod(ctx, guestPod("g")); err != nil || len(b.deleted) != 1 {
		t.Errorf("guest delete: err=%v deleted=%v", err, b.deleted)
	}
}

type closedGate struct{ refused []string }

func (g *closedGate) Admit(_ context.Context, pod *corev1.Pod) bool {
	g.refused = append(g.refused, pod.Name)
	return false
}

func TestCreatePodGate(t *testing.T) {
	b := &fakeBackend{}
	g := &closedGate{}
	p := New(b).WithGate(g)
	if err := p.CreatePod(context.Background(), guestPod("g")); err != nil {
		t.Fatal(err)
	}
	if len(b.created) != 0 || len(g.refused) != 1 {
		t.Errorf("closed gate: created %v refused %v", b.created, g.refused)
	}
	if err := New(b).WithGate(nil).CreatePod(context.Background(), guestPod("g")); err != nil || len(b.created) != 1 {
		t.Errorf("no gate: err %v created %v", err, b.created)
	}
}

func TestNotifyPodsFiltersNonGuests(t *testing.T) {
	b := &fakeBackend{}
	p := New(b)
	var got []string
	p.NotifyPods(context.Background(), func(pod *corev1.Pod) { got = append(got, pod.Name) })
	b.cb(guestPod("g"))
	b.cb(dsPod("ds"))
	if len(got) != 1 || got[0] != "g" {
		t.Errorf("notified %v", got)
	}
}

func TestGuestOnlyRecorder(t *testing.T) {
	fake := record.NewFakeRecorder(10)
	r := GuestOnlyRecorder{EventRecorder: fake}
	r.Event(dsPod("ds"), corev1.EventTypeNormal, "ProviderCreateSuccess", "x")
	r.Eventf(guestPod("g"), corev1.EventTypeNormal, "ProviderCreateSuccess", "%s", "x")
	r.Event(&corev1.Node{}, corev1.EventTypeNormal, "NodeReady", "x")
	if n := len(fake.Events); n != 2 {
		t.Errorf("want 2 events (guest + node), got %d", n)
	}
}

func TestNewNodeSpec(t *testing.T) {
	n := NewNodeSpec(NodeConfig{
		Name: "vk-test", InternalIP: "10.0.0.1", KubeletPort: 10260, GPUs: 1,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"),
		ProviderID: "gce://p/z/i",
	})
	if n.Labels[VirtualNodeLabel] != "true" || n.Labels["type"] != "virtual-kubelet" {
		t.Errorf("labels: %v", n.Labels)
	}
	// The group labels belong to the real node only; an orchestrator must never see the
	// virtual Node as a member of a group.
	for l := range n.Labels {
		if strings.HasPrefix(l, "group.timeslice.io/") || l == "timeslice.io/group" || l == "timeslice.io/donor" {
			t.Errorf("the virtual node must not have group label %s", l)
		}
	}
	for _, l := range []string{"cloud.google.com/gke-nodepool", "kubernetes.io/os"} {
		if _, ok := n.Labels[l]; ok {
			t.Errorf("the virtual node must not have label %s", l)
		}
	}
	if n.Spec.ProviderID != "gce://p/z/i" {
		t.Errorf("providerID: %q", n.Spec.ProviderID)
	}
	if n.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] != "true" {
		t.Error("scale-down must be disabled")
	}
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != GuestTaintKey {
		t.Errorf("taints: %v", n.Spec.Taints)
	}
	if g := n.Status.Capacity[GPUResource]; g.Value() != 1 {
		t.Errorf("gpu capacity: %v", g)
	}
}

// Admission (VK-A7). In this file so the tests share its helpers.

func l4Policy() AdmissionPolicy {
	return AdmissionPolicy{GPUAllowlist: ParseGPUAllowlist("nvidia-l4"), HostGPUModel: "nvidia-l4"}
}

func withGPU(p *corev1.Pod, res corev1.ResourceName) *corev1.Pod {
	p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{res: resource.MustParse("1")}
	return p
}

func httpProbe() *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health"}}}
}

func execProbe() *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
}

func TestAdmitProbesOptionA(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*corev1.Pod)
		rule string // "" = admitted
	}{
		{"plain", func(*corev1.Pod) {}, ""},
		{"httpGet readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = httpProbe() }, ""},
		{"tcpSocket readiness", func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{}}}
		}, ""},
		{"exec readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = execProbe() }, "readiness-probe-exec"},
		{"grpc readiness", func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 9000}}}
		}, "readiness-probe-grpc"},
		{"httpGet liveness", func(p *corev1.Pod) { p.Spec.Containers[0].LivenessProbe = httpProbe() }, "liveness-probe"},
		{"startup", func(p *corev1.Pod) { p.Spec.Containers[0].StartupProbe = httpProbe() }, "startup-probe"},
		{"sidecar liveness", func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "side", LivenessProbe: httpProbe()}}
		}, "liveness-probe"},
		{"readiness gate", func(p *corev1.Pod) {
			p.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "x/ready"}}
		}, "readiness-gates"},
	}
	for _, c := range cases {
		p := guestPod("g")
		c.mut(p)
		r := Admit(p, l4Policy())
		switch {
		case c.rule == "" && r != nil:
			t.Errorf("%s: rejected (%v), want admitted", c.name, r)
		case c.rule != "" && (r == nil || r.Rule != c.rule):
			t.Errorf("%s: got %v, want rule %s", c.name, r, c.rule)
		}
	}
}

func TestAdmitGPU(t *testing.T) {
	if r := Admit(withGPU(guestPod("g"), GPUResource), l4Policy()); r != nil {
		t.Errorf("L4 host, L4 allowlist: rejected %v", r)
	}
	pol := l4Policy()
	pol.HostGPUModel = "nvidia-tesla-a100"
	rej := Admit(withGPU(guestPod("g"), GPUResource), pol)
	if rej == nil || rej.Rule != "gpu-allowlist" || !strings.Contains(rej.Message, "nvidia-tesla-a100") {
		t.Errorf("host off the allowlist: got %v", rej)
	}
	if r := Admit(guestPod("g"), pol); r != nil {
		t.Errorf("a CPU-only guest is admitted on any host: %v", r)
	}
	pol.HostGPUModel = ""
	if r := Admit(withGPU(guestPod("g"), GPUResource), pol); r == nil || r.Rule != "gpu-allowlist" {
		t.Errorf("unknown host model must fail closed: got %v", r)
	}
	if r := Admit(withGPU(guestPod("g"), "nvidia.com/mig-1g.5gb"), l4Policy()); r == nil || r.Rule != "gpu-resource" {
		t.Errorf("MIG slice: got %v", r)
	}
	if r := Admit(withGPU(guestPod("g"), "amd.com/gpu"), l4Policy()); r == nil || r.Rule != "gpu-resource" {
		t.Errorf("other vendor: got %v", r)
	}
	if r := Admit(withGPU(guestPod("g"), "timeslice.io/gpu-shadow-1"), l4Policy()); r == nil || r.Rule != "gpu-resource" {
		t.Errorf("shadow resource named by a guest: got %v", r)
	}
	p := withGPU(guestPod("g"), GPUResource)
	p.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-accelerator": "nvidia-h100-80gb"}
	if r := Admit(p, l4Policy()); r == nil || r.Rule != "gpu-allowlist" {
		t.Errorf("selector naming another model: got %v", r)
	}
	p.Spec.NodeSelector = map[string]string{"nvidia.com/gpu.product": "NVIDIA-L4"}
	if r := Admit(p, l4Policy()); r != nil {
		t.Errorf("selector naming the allowed model: %v", r)
	}
}

func TestHostGPUModelAndAllowlist(t *testing.T) {
	node := &corev1.Node{}
	node.Labels = map[string]string{"nvidia.com/gpu.product": "NVIDIA L4"}
	if got := HostGPUModel(node); got != "nvidia-l4" {
		t.Errorf("gpu.product: %q", got)
	}
	node.Labels["cloud.google.com/gke-accelerator"] = "nvidia-l4"
	if got := HostGPUModel(node); got != "nvidia-l4" {
		t.Errorf("gke-accelerator: %q", got)
	}
	got := ParseGPUAllowlist(" NVIDIA L4, nvidia_tesla_t4 ,,")
	if len(got) != 2 || got[0] != "nvidia-l4" || got[1] != "nvidia-tesla-t4" {
		t.Errorf("allowlist: %v", got)
	}
}

// TestCreatePodRejects: a refused guest never reaches the backend, gets exactly one Warning
// event, is reported Failed through the status callback, and the library's
// ProviderCreateSuccess for it is dropped.
func TestCreatePodRejects(t *testing.T) {
	backend := &fakeBackend{}
	fake := record.NewFakeRecorder(10)
	rejected := NewRejectedSet()
	rec := GuestOnlyRecorder{EventRecorder: fake, Rejected: rejected}
	prov := New(backend).WithAdmission(&Admission{Policy: l4Policy(), Recorder: rec, Rejected: rejected})
	got := make(chan *corev1.Pod, 4)
	prov.NotifyPods(context.Background(), func(pod *corev1.Pod) { got <- pod })

	bad := guestPod("bad")
	bad.UID = types.UID("bad-uid")
	bad.Spec.Containers[0].LivenessProbe = httpProbe()
	ctx := context.Background()
	for i := 0; i < 2; i++ { // a second offer must not repeat the event
		if err := prov.CreatePod(ctx, bad); err != nil {
			t.Fatal(err)
		}
		rec.Event(bad, corev1.EventTypeNormal, "ProviderCreateSuccess", "Create pod in provider successfully")
	}
	if len(backend.created) != 0 {
		t.Fatalf("rejected guest reached the backend: %v", backend.created)
	}
	for i := 0; i < 2; i++ {
		select {
		case st := <-got:
			status := st.Status
			failed := status.Phase == corev1.PodFailed && status.Reason == ReasonGuestRejected
			if !failed || !strings.Contains(status.Message, "liveness-probe") {
				t.Errorf("status: %+v", st.Status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no Failed status pushed")
		}
	}
	if n := len(fake.Events); n != 1 {
		t.Fatalf("want exactly 1 event, got %d", n)
	}
	if e := <-fake.Events; !strings.HasPrefix(e, "Warning GuestRejected liveness-probe:") {
		t.Errorf("event %q", e)
	}

	good := guestPod("good")
	good.UID = types.UID("good-uid")
	good.Spec.Containers[0].ReadinessProbe = httpProbe()
	if err := prov.CreatePod(ctx, good); err != nil || len(backend.created) != 1 {
		t.Fatalf("good guest: err=%v created=%v", err, backend.created)
	}
	rec.Event(good, corev1.EventTypeNormal, "ProviderCreateSuccess", "x")
	if n := len(fake.Events); n != 1 {
		t.Errorf("an admitted guest keeps its ProviderCreateSuccess; events=%d", n)
	}
}

// Lead decision D-VK-5 (--guest-probe-policy). Each case is one probe shape; want holds the rule
// each option rejects it with ("" = admitted), in the order a, b, c.

func grpcProbe() *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 9000}}}
}

type probeCase struct {
	name string
	mut  func(*corev1.Pod)
	want [3]string
}

func probeCases() []probeCase {
	return []probeCase{
		{"no probes", func(*corev1.Pod) {}, [3]string{"", "", ""}},
		{"httpGet readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = httpProbe() },
			[3]string{"", "readiness-probe", ""}},
		{"tcpSocket readiness", func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{}}}
		}, [3]string{"", "readiness-probe", ""}},
		{"exec readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = execProbe() },
			[3]string{"readiness-probe-exec", "readiness-probe", ""}},
		{"grpc readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = grpcProbe() },
			[3]string{"readiness-probe-grpc", "readiness-probe", ""}},
		{"liveness", func(p *corev1.Pod) { p.Spec.Containers[0].LivenessProbe = httpProbe() },
			[3]string{"liveness-probe", "liveness-probe", ""}},
		{"startup", func(p *corev1.Pod) { p.Spec.Containers[0].StartupProbe = httpProbe() },
			[3]string{"startup-probe", "startup-probe", ""}},
		{"stock chart: startup, liveness and readiness", func(p *corev1.Pod) {
			p.Spec.Containers[0].StartupProbe = httpProbe()
			p.Spec.Containers[0].LivenessProbe = httpProbe()
			p.Spec.Containers[0].ReadinessProbe = httpProbe()
		}, [3]string{"liveness-probe", "liveness-probe", ""}},
		{"sidecar readiness", func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "side", ReadinessProbe: httpProbe()}}
		}, [3]string{"", "readiness-probe", ""}},
		{"readiness gate", func(p *corev1.Pod) {
			p.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "x/ready"}}
		}, [3]string{"readiness-gates", "readiness-gates", ""}},
	}
}

func checkProbeCases(t *testing.T, pol ProbePolicy, idx int) {
	t.Helper()
	for _, tc := range probeCases() {
		pod := guestPod("g")
		tc.mut(pod)
		policy := l4Policy()
		policy.Probes = pol
		rej, want := Admit(pod, policy), tc.want[idx]
		switch {
		case want == "" && rej != nil:
			t.Errorf("policy %q, %s: rejected (%v), want admitted", pol, tc.name, rej)
		case want != "" && (rej == nil || rej.Rule != want):
			t.Errorf("policy %q, %s: got %v, want rule %s", pol, tc.name, rej, want)
		}
	}
}

func TestGuestProbePolicy_A_Admit(t *testing.T) {
	checkProbeCases(t, ProbePolicyA, 0)
	checkProbeCases(t, "", 0) // unset is the default, option a
}

func TestGuestProbePolicy_B_Admit(t *testing.T) {
	checkProbeCases(t, ProbePolicyB, 1)
	pod := guestPod("g")
	pod.Spec.Containers[0].ReadinessProbe = httpProbe()
	policy := l4Policy()
	policy.Probes = ProbePolicyB
	if rej := Admit(pod, policy); rej == nil || !strings.Contains(rej.Message, `container "c" has a readinessProbe`) {
		t.Errorf("the message must name the container and the field: %v", rej)
	}
}

func TestGuestProbePolicy_C_Admit(t *testing.T) {
	checkProbeCases(t, ProbePolicyC, 2)
	// Option c still applies the GPU rules.
	policy := l4Policy()
	policy.Probes = ProbePolicyC
	if rej := Admit(withGPU(guestPod("g"), "nvidia.com/mig-1g.10gb"), policy); rej == nil {
		t.Error("option c must not bypass the GPU checks")
	}
}

func TestGuestProbePolicy_Parse(t *testing.T) {
	for in, want := range map[string]ProbePolicy{"": ProbePolicyA, "a": ProbePolicyA, "b": ProbePolicyB, "c": ProbePolicyC} {
		if got, err := ParseProbePolicy(in); err != nil || got != want {
			t.Errorf("%q: got %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"A", "d", "none"} {
		if _, err := ParseProbePolicy(in); err == nil {
			t.Errorf("%q must be refused", in)
		}
	}
}
