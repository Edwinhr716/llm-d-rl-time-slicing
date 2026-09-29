package placement_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/edwinhr716/guest-kubelet/internal/placement"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// Tests for pending lead decision D-NS-11. Option keep (the default) is deploy/deployment.yaml
// plus the snapshot-agent chart as it is. Option ns-ds is deploy/daemonset.yaml plus the chart
// with deploy/opt-d-ns-11/snapshot-agent-values.yaml.

const (
	deployDir   = "../../deploy"
	vkDaemonSet = deployDir + "/daemonset.yaml"
	vkDeploy    = deployDir + "/deployment.yaml"
	agentValues = deployDir + "/opt-d-ns-11/snapshot-agent-values.yaml"
	// The chart lives outside the module. The module's own Cloud Build uploads only the module,
	// so tests that read it skip there and run in CI parity (which uploads the whole repo).
	chartValues = "../../../deploy/snapshot-agent/values.yaml"
)

// chartTolerations are the snapshot-agent chart's default tolerations, which the ns-ds overlay
// leaves unchanged. TestPlacementNSDS_ChartTolerationsCurrent checks them against the chart.
var chartTolerations = []corev1.Toleration{
	{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpEqual, Value: "present", Effect: corev1.TaintEffectNoSchedule},
	{Key: "timeslice.io/shared", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
}

// agentValuesDoc is the part of the chart values that decides placement.
type agentValuesDoc struct {
	NodeSelector map[string]string   `json:"nodeSelector"`
	Tolerations  []corev1.Toleration `json:"tolerations"`
}

// decodeAll decodes every YAML document of a file into objs, and fails unless the file has
// exactly len(objs) documents.
func decodeAll(t *testing.T, path string, objs ...any) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for _, obj := range objs {
		if err := dec.Decode(obj); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	var extra map[string]any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("%s: want %d documents, err=%v", path, len(objs), err)
	}
	return raw
}

func readDaemonSet(t *testing.T) *appsv1.DaemonSet {
	t.Helper()
	var ds appsv1.DaemonSet
	decodeAll(t, vkDaemonSet, &ds)
	if ds.Kind != "DaemonSet" || ds.APIVersion != "apps/v1" {
		t.Fatalf("daemonset.yaml: %s %s", ds.APIVersion, ds.Kind)
	}
	return &ds
}

func readDeployment(t *testing.T) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	decodeAll(t, vkDeploy, &d)
	if d.Kind != "Deployment" {
		t.Fatalf("deployment.yaml: kind %q", d.Kind)
	}
	return &d
}

func readAgentOverlay(t *testing.T) agentValuesDoc {
	t.Helper()
	var doc agentValuesDoc
	decodeAll(t, agentValues, &doc)
	return doc
}

// agentSpec is the pod spec the chart renders, as far as placement goes: nodeSelector and
// tolerations from the values (the chart copies both as they are).
func agentSpec(v agentValuesDoc) *corev1.PodSpec {
	return &corev1.PodSpec{NodeSelector: v.NodeSelector, Tolerations: v.Tolerations}
}

func vkContainer(t *testing.T, spec *corev1.PodSpec) *corev1.Container {
	t.Helper()
	if len(spec.Containers) != 1 || spec.Containers[0].Name != "guest-kubelet" {
		t.Fatalf("want one guest-kubelet container, got %d", len(spec.Containers))
	}
	return &spec.Containers[0]
}

var gpuTaint = corev1.Taint{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}

func realNode(name string, donor bool, taints ...corev1.Taint) *corev1.Node {
	n := &corev1.Node{Spec: corev1.NodeSpec{Taints: taints}}
	n.Name = name
	n.Labels = map[string]string{"kubernetes.io/hostname": name, "kubernetes.io/os": "linux"}
	if donor {
		n.Labels[placement.DonorLabel] = "true"
	}
	return n
}

// virtualNode is the Node the guest kubelet registers for a host.
func virtualNode(host string) *corev1.Node {
	n := provider.NewNodeSpec(provider.NodeConfig{
		Name: "vk-" + host[strings.LastIndex(host, "-")+1:], InternalIP: "10.0.0.1", KubeletPort: 10260,
		CPU: resource.MustParse("8"), Memory: resource.MustParse("32Gi"), Pods: resource.MustParse("20"), GPUs: 1,
	})
	return &n
}

// testNodes: donor and non-donor GPU hosts, a CPU node, and the donor host's virtual Node.
func testNodes() []*corev1.Node {
	return []*corev1.Node{
		realNode("gpu-donor-a", true, gpuTaint),
		realNode("gpu-donor-shared-b", true, gpuTaint,
			corev1.Taint{Key: "timeslice.io/shared", Value: "g1", Effect: corev1.TaintEffectNoSchedule}),
		realNode("gpu-plain-c", false, gpuTaint),
		realNode("cpu-plain-d", false),
		virtualNode("gpu-donor-a"),
	}
}

func eligibleNames(spec *corev1.PodSpec, nodes []*corev1.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if placement.Eligible(spec, n) {
			out = append(out, n.Name)
		}
	}
	return out
}

// H3: the VK DaemonSet is one object in its own file, selects donor=true, gets its host from the
// downward API, derives the Node name (no --node-name) and keeps leader election.
func TestPlacementNSDS_VKDaemonSetShape(t *testing.T) {
	ds := readDaemonSet(t)
	raw := decodeAll(t, vkDaemonSet, &appsv1.DaemonSet{})
	if ds.Name != "guest-kubelet" || ds.Namespace != "guest-kubelet-proto" {
		t.Errorf("object %s/%s, want guest-kubelet-proto/guest-kubelet", ds.Namespace, ds.Name)
	}
	if bytes.Contains(raw, []byte("__HOST__")) {
		t.Error("daemonset.yaml must not be pinned to a host (__HOST__)")
	}
	spec := &ds.Spec.Template.Spec
	if want := map[string]string{placement.DonorLabel: "true"}; !reflect.DeepEqual(spec.NodeSelector, want) {
		t.Errorf("nodeSelector %v, want %v", spec.NodeSelector, want)
	}
	if spec.Affinity != nil {
		t.Errorf("affinity %+v, want none (selectors go in nodeSelector)", spec.Affinity)
	}
	if spec.ServiceAccountName != "guest-kubelet" || !spec.HostNetwork {
		t.Errorf("serviceAccount %q hostNetwork %v", spec.ServiceAccountName, spec.HostNetwork)
	}
	if got := ds.Spec.Template.Labels["app"]; got != "guest-kubelet" {
		t.Errorf("pod label app=%q, want guest-kubelet", got)
	}
	if !reflect.DeepEqual(ds.Spec.Selector.MatchLabels, map[string]string{"app": "guest-kubelet"}) {
		t.Errorf("selector %v", ds.Spec.Selector.MatchLabels)
	}

	ctr := vkContainer(t, spec)
	if ctr.Image != "__IMAGE__" {
		t.Errorf("image %q, want __IMAGE__", ctr.Image)
	}
	leader := 0
	for _, a := range ctr.Args {
		if a == "--leader-elect=true" {
			leader++
		}
		if strings.HasPrefix(a, "--node-name") || strings.HasPrefix(a, "--host-node") {
			t.Errorf("arg %q: the Node name must derive from NODE_NAME", a)
		}
	}
	if leader != 1 {
		t.Errorf("want exactly one arg line --leader-elect=true, got %d", leader)
	}
	fields := map[string]string{}
	for _, e := range ctr.Env {
		if e.ValueFrom != nil && e.ValueFrom.FieldRef != nil {
			fields[e.Name] = e.ValueFrom.FieldRef.FieldPath
		}
	}
	want := map[string]string{
		"NODE_NAME": "spec.nodeName", "HOST_IP": "status.hostIP",
		"POD_NAME": "metadata.name", "POD_NAMESPACE": "metadata.namespace",
	}
	if !reflect.DeepEqual(fields, want) {
		t.Errorf("downward API env %v, want %v", fields, want)
	}
}

// H6: a rollout surges one pod per node and leader election hands the per-Node Lease over.
func TestPlacementNSDS_VKDaemonSetRollingUpdate(t *testing.T) {
	ds := readDaemonSet(t)
	u := ds.Spec.UpdateStrategy
	if u.Type != appsv1.RollingUpdateDaemonSetStrategyType || u.RollingUpdate == nil {
		t.Fatalf("updateStrategy %+v, want RollingUpdate", u)
	}
	if s := u.RollingUpdate.MaxSurge; s == nil || s.String() != "1" {
		t.Errorf("maxSurge %v, want 1", s)
	}
	if m := u.RollingUpdate.MaxUnavailable; m == nil || m.String() != "0" {
		t.Errorf("maxUnavailable %v, want 0", m)
	}
}

// H3: everything but placement is carried over from the Deployment of the same branch (flags,
// env, tolerations, security, resources), so the two options differ only in where they run.
func TestPlacementNSDS_VKDaemonSetMatchesDeployment(t *testing.T) {
	ds := readDaemonSet(t)
	d := readDeployment(t)
	got, want := ds.Spec.Template.DeepCopy(), d.Spec.Template.DeepCopy()
	got.Spec.NodeSelector, want.Spec.NodeSelector = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DaemonSet pod template differs from the Deployment's beyond nodeSelector:\nds:  %+v\ndep: %+v", got, want)
	}
}

// H5: the agent overlay only sets the nodeSelector; tolerations stay the chart's.
func TestPlacementNSDS_AgentOverlay(t *testing.T) {
	doc := readAgentOverlay(t)
	var keys map[string]any
	decodeAll(t, agentValues, &keys)
	if want := map[string]string{placement.DonorLabel: "true"}; !reflect.DeepEqual(doc.NodeSelector, want) {
		t.Errorf("nodeSelector %v, want %v", doc.NodeSelector, want)
	}
	if len(keys) != 1 {
		t.Errorf("overlay keys %v, want nodeSelector only", keys)
	}
}

func TestPlacementNSDS_ChartTolerationsCurrent(t *testing.T) {
	if _, err := os.Stat(chartValues); err != nil {
		t.Skipf("snapshot-agent chart not in this build context: %v", err)
	}
	var chart agentValuesDoc
	var rest map[string]any
	raw, err := os.ReadFile(chartValues)
	if err != nil {
		t.Fatal(err)
	}
	if err := utilyaml.Unmarshal(raw, &chart); err != nil {
		t.Fatal(err)
	}
	if err := utilyaml.Unmarshal(raw, &rest); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(chart.Tolerations, chartTolerations) {
		t.Errorf("chart tolerations %+v, want %+v (update chartTolerations)", chart.Tolerations, chartTolerations)
	}
	if len(chart.NodeSelector) != 0 {
		t.Errorf("chart default nodeSelector %v, want {} (option keep)", chart.NodeSelector)
	}
	if affinity, ok := rest["affinity"].(map[string]any); !ok || len(affinity) != 0 {
		t.Errorf("chart default affinity %v, want {}", rest["affinity"])
	}
}

// P1 / S1 in miniature: both ns-ds workloads can land on donor nodes only, never on an unlabelled
// GPU node, a CPU node or a virtual Node.
func TestPlacementNSDS_OnlyDonorNodes(t *testing.T) {
	nodes := testNodes()
	donors := []string{"gpu-donor-a", "gpu-donor-shared-b"}

	ds := readDaemonSet(t)
	if got := eligibleNames(&ds.Spec.Template.Spec, nodes); !slices.Equal(got, donors) {
		t.Errorf("VK DaemonSet eligible on %v, want %v", got, donors)
	}
	doc := readAgentOverlay(t)
	doc.Tolerations = chartTolerations
	if got := eligibleNames(agentSpec(doc), nodes); !slices.Equal(got, donors) {
		t.Errorf("agent (chart + overlay) eligible on %v, want %v", got, donors)
	}
}

// P6 in miniature: wherever the VK DaemonSet can run, the agent can run too.
func TestPlacementNSDS_AgentWhereverVK(t *testing.T) {
	ds := readDaemonSet(t)
	doc := readAgentOverlay(t)
	doc.Tolerations = chartTolerations
	for _, n := range testNodes() {
		if placement.Eligible(&ds.Spec.Template.Spec, n) && !placement.Eligible(agentSpec(doc), n) {
			t.Errorf("node %s: VK eligible but agent not", n.Name)
		}
	}
}

// H7 / P5: the virtual Node never carries the donor label, even for a donor host, so no donor
// DaemonSet targets it (its taint would also keep them off).
func TestPlacementNSDS_VirtualNodeNeverDonor(t *testing.T) {
	vn := virtualNode("gpu-donor-a")
	if _, ok := vn.Labels[placement.DonorLabel]; ok {
		t.Fatalf("virtual Node labels %v carry %s", vn.Labels, placement.DonorLabel)
	}
	// Even a DaemonSet that tolerated every taint would not select it.
	ds := readDaemonSet(t)
	spec := ds.Spec.Template.Spec.DeepCopy()
	spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	if placement.Eligible(spec, vn) {
		t.Error("VK DaemonSet with all tolerations selects the virtual Node")
	}
}

// Option keep (the default) is unchanged: a 2-replica Deployment pinned to one host, and an agent
// that lands on every node it tolerates, labelled or not.
func TestPlacementKeep_DefaultUnchanged(t *testing.T) {
	dep := readDeployment(t)
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Errorf("replicas %v, want 2", dep.Spec.Replicas)
	}
	pinned := map[string]string{"kubernetes.io/hostname": "__HOST__"}
	if got := dep.Spec.Template.Spec.NodeSelector; !reflect.DeepEqual(got, pinned) {
		t.Errorf("nodeSelector %v, want %v", got, pinned)
	}
	nodes := testNodes()
	spec := dep.Spec.Template.Spec.DeepCopy()
	spec.NodeSelector["kubernetes.io/hostname"] = "gpu-plain-c"
	if got := eligibleNames(spec, nodes); !slices.Equal(got, []string{"gpu-plain-c"}) {
		t.Errorf("keep VK eligible on %v, want only its pinned host", got)
	}
	agent := agentSpec(agentValuesDoc{Tolerations: chartTolerations})
	want := []string{"gpu-donor-a", "gpu-donor-shared-b", "gpu-plain-c", "cpu-plain-d"}
	if got := eligibleNames(agent, nodes); !slices.Equal(got, want) {
		t.Errorf("keep agent eligible on %v, want %v", got, want)
	}
}

func TestEligible(t *testing.T) {
	shared := corev1.Taint{Key: "timeslice.io/shared", Value: "g1", Effect: corev1.TaintEffectNoSchedule}
	execute := corev1.Taint{Key: "k", Value: "v", Effect: corev1.TaintEffectNoExecute}
	prefer := corev1.Taint{Key: "p", Effect: corev1.TaintEffectPreferNoSchedule}
	donorSel := map[string]string{placement.DonorLabel: "true"}
	otherHost := map[string]string{"kubernetes.io/hostname": "m"}
	tols := func(tt ...corev1.Toleration) corev1.PodSpec { return corev1.PodSpec{Tolerations: tt} }
	gpuOther := corev1.Toleration{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpEqual, Value: "absent"}
	sharedAny := corev1.Toleration{Key: "timeslice.io/shared", Operator: corev1.TolerationOpExists}
	kNoSched := corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
	all := corev1.Toleration{Operator: corev1.TolerationOpExists}
	cases := []struct {
		name string
		spec corev1.PodSpec
		node *corev1.Node
		want bool
	}{
		{"no selector, no taint", corev1.PodSpec{}, realNode("n", false), true},
		{"selector matches", corev1.PodSpec{NodeSelector: donorSel}, realNode("n", true), true},
		{"selector missing label", corev1.PodSpec{NodeSelector: donorSel}, realNode("n", false), false},
		{"selector wrong value", corev1.PodSpec{NodeSelector: otherHost}, realNode("n", false), false},
		{"untolerated taint", corev1.PodSpec{}, realNode("n", false, gpuTaint), false},
		{"equal toleration", tols(chartTolerations[0]), realNode("n", false, gpuTaint), true},
		{"equal toleration, other value", tols(gpuOther), realNode("n", false, gpuTaint), false},
		{"exists toleration, any effect", tols(sharedAny), realNode("n", false, shared), true},
		{"effect mismatch", tols(kNoSched), realNode("n", false, execute), false},
		{"empty key with exists tolerates all", tols(all), realNode("n", false, gpuTaint, shared, execute), true},
		{"prefer-no-schedule ignored", corev1.PodSpec{}, realNode("n", false, prefer), true},
		{"one of two taints tolerated", tols(chartTolerations[0]), realNode("n", false, gpuTaint, shared), false},
	}
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			if got := placement.Eligible(&tc.spec, tc.node); got != tc.want {
				t.Errorf("Eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

// TODAY stack (VK-A7 outage guard): the node keeper follows the guest kubelet onto donor nodes,
// with the pinned Deployment's pod template except for the nodeSelector, one keeper per node.
func TestPlacementNSDS_KeeperDaemonSetMatchesDeployment(t *testing.T) {
	var ds appsv1.DaemonSet
	var dep appsv1.Deployment
	decodeAll(t, deployDir+"/guard/node-keeper-daemonset.yaml", &ds)
	decodeAll(t, deployDir+"/guard/node-keeper.yaml", &dep)
	if ds.Kind != "DaemonSet" || dep.Kind != "Deployment" {
		t.Fatalf("kinds %q, %q", ds.Kind, dep.Kind)
	}
	if ds.Name != dep.Name || ds.Namespace != dep.Namespace {
		t.Errorf("DaemonSet %s/%s, Deployment %s/%s", ds.Namespace, ds.Name, dep.Namespace, dep.Name)
	}
	if want := map[string]string{placement.DonorLabel: "true"}; !reflect.DeepEqual(ds.Spec.Template.Spec.NodeSelector, want) {
		t.Errorf("nodeSelector %v, want %v", ds.Spec.Template.Spec.NodeSelector, want)
	}
	got, want := ds.Spec.Template.DeepCopy(), dep.Spec.Template.DeepCopy()
	got.Spec.NodeSelector, want.Spec.NodeSelector = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("keeper DaemonSet pod template differs from the Deployment's beyond nodeSelector:\nds:  %+v\ndep: %+v", got, want)
	}
	u := ds.Spec.UpdateStrategy
	if u.RollingUpdate == nil || u.RollingUpdate.MaxSurge == nil || u.RollingUpdate.MaxSurge.String() != "0" ||
		u.RollingUpdate.MaxUnavailable == nil || u.RollingUpdate.MaxUnavailable.String() != "1" {
		t.Errorf("updateStrategy %+v, want RollingUpdate maxSurge 0 maxUnavailable 1 (Recreate per node)", u)
	}
	vkDS := readDaemonSet(t)
	for _, n := range testNodes() {
		if placement.Eligible(&vkDS.Spec.Template.Spec, n) != placement.Eligible(&ds.Spec.Template.Spec, n) {
			t.Errorf("node %s: VK and keeper DaemonSets disagree", n.Name)
		}
	}
}
