package mirror

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

type fakeHolders struct {
	holders *api.Holders
	err     error
}

func (f fakeHolders) Holders(context.Context) (*api.Holders, error) { return f.holders, f.err }

// twoGPUHolders: the neighbor holds GPU 0, the donor holds GPU 1 (the point: pick the donor's).
func twoGPUHolders() *api.Holders {
	return &api.Holders{
		GPUs: []api.GPU{
			{Minor: 0, UUID: "GPU-0000", Device: "nvidia0", Resource: api.ShadowResource(0)},
			{Minor: 1, UUID: "GPU-1111", Device: "nvidia1", Resource: api.ShadowResource(1)},
		},
		Holders: []api.Holder{
			{
				Namespace: "ns", Name: "neighbor", Container: "hold", Resource: "nvidia.com/gpu",
				DeviceIDs: []string{"nvidia0"}, Minors: []int{0},
			},
			{
				Namespace: "ns", Name: "donor", Container: "load", Resource: "nvidia.com/gpu",
				DeviceIDs: []string{"nvidia1"}, Minors: []int{1},
			},
		},
	}
}

func hostNode(shadows ...int) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "real-node"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule},
		}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}},
	}
	for _, minor := range shadows {
		node.Status.Allocatable[api.ShadowResource(minor)] = resource.MustParse("1")
	}
	return node
}

func donorPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "donor", UID: "donor-uid",
			Labels: map[string]string{"timeslice.io/donor": "true"},
		},
		Spec:   corev1.PodSpec{NodeName: "real-node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func dpOptions(holders HoldersSource, rec record.EventRecorder) Options {
	cfg := testConfig()
	cfg.GPUClaim = ""
	return Options{
		Config: cfg, OrphanGrace: time.Minute,
		GPUMode: GPUModeDevicePlugin, DonorSelector: DefaultDonorSelector, Holders: holders, Recorder: rec,
		FenceInterval: 50 * time.Millisecond,
	}
}

func noDRACalls(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		if action.GetResource().Group == "resource.k8s.io" {
			t.Errorf("deviceplugin mode made a resource.k8s.io call: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func getNode(t *testing.T, hn *harness) *corev1.Node {
	t.Helper()
	node, err := hn.client.CoreV1().Nodes().Get(context.Background(), "real-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func TestDevicePluginMirrorGetsDonorsGPU(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	hn := newHarness(t, dpOptions(fakeHolders{holders: twoGPUHolders()}, rec), hostNode(0, 1), donorPod())
	guest := testGuest()
	hn.addGuest(guest)
	if err := hn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	mir := hn.mirror("vllm-m")
	if mir == nil {
		t.Fatal("mirror not created")
	}
	if ShadowResourceOf(mir) != api.ShadowResource(1) || mir.Annotations[AnnotationGPUUUID] != "GPU-1111" ||
		mir.Annotations[AnnotationGPUDonorUID] != "donor-uid" {
		t.Errorf("want the donor's GPU 1, got %s uuid=%s donor=%s",
			ShadowResourceOf(mir), mir.Annotations[AnnotationGPUUUID], mir.Annotations[AnnotationGPUDonorUID])
	}
	if len(mir.Spec.ResourceClaims) != 0 {
		t.Errorf("no claim in deviceplugin mode: %v", mir.Spec.ResourceClaims)
	}
	// A retry is a no-op.
	hn.waitInformer("guest-uid")
	if err := hn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	noDRACalls(t, hn.client.Actions())
}

func usedGPU1(t *testing.T) runtime.Object {
	t.Helper()
	cfg := dpOptions(nil, nil).Config
	other, err := BuildWithGPU(cpuGuest("other-uid"), &cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	other.Name = "other-m"
	other.Spec.Containers[0].Resources.Limits = corev1.ResourceList{api.ShadowResource(1): resource.MustParse("1")}
	other.Status.Phase = corev1.PodRunning
	return other
}

func TestDevicePluginRefusals(t *testing.T) {
	two := fakeHolders{holders: twoGPUHolders()}
	cases := map[string]struct {
		holders HoldersSource
		objs    []runtime.Object
		want    string
	}{
		"no donor": {
			holders: two,
			objs:    []runtime.Object{hostNode(0, 1)},
			want:    "no running donor pod",
		},
		"plugin down": {
			holders: fakeHolders{err: errors.New("connection refused")},
			objs:    []runtime.Object{hostNode(0, 1), donorPod()},
			want:    "holders endpoint",
		},
		"shadow not on host": {
			holders: two,
			objs:    []runtime.Object{hostNode(0), donorPod()},
			want:    "does not advertise timeslice.io/gpu-shadow-1",
		},
		"GPU already shared": {
			holders: two,
			objs:    []runtime.Object{hostNode(0, 1), donorPod(), usedGPU1(t)},
			want:    "already shared",
		},
		"donor holds no GPU": {
			holders: fakeHolders{holders: &api.Holders{GPUs: twoGPUHolders().GPUs}},
			objs:    []runtime.Object{hostNode(0, 1), donorPod()},
			want:    "holds no nvidia.com/gpu",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := record.NewFakeRecorder(10)
			hn := newHarness(t, dpOptions(tc.holders, rec), tc.objs...)
			if name == "GPU already shared" {
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if _, err := hn.b.mirrors.Pods("ns").Get("other-m"); err == nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			guest := testGuest()
			hn.addGuest(guest)
			err := hn.b.Create(context.Background(), guest)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want refusal containing %q, got %v", tc.want, err)
			}
			if hn.mirror("vllm-m") != nil {
				t.Error("fail closed: no mirror when no donor GPU is found")
			}
			select {
			case event := <-rec.Events:
				if !strings.Contains(event, EventGPUUnavailable) {
					t.Errorf("event %q", event)
				}
			default:
				t.Error("want a GPUUnavailable event on the guest")
			}
			noDRACalls(t, hn.client.Actions())
		})
	}
}

func fenceTaint(node *corev1.Node) string {
	for _, taint := range node.Spec.Taints {
		if taint.Key == FenceTaintKey {
			return taint.Value
		}
	}
	return ""
}

// firstAction returns the index of the first action that matches, or -1.
func firstAction(actions []k8stesting.Action, match func(k8stesting.Action) bool) int {
	for i, action := range actions {
		if match(action) {
			return i
		}
	}
	return -1
}

func isFenceTaintPatch(action k8stesting.Action) bool {
	patch, isPatch := action.(k8stesting.PatchAction)
	return isPatch && action.GetResource().Resource == "nodes" &&
		strings.Contains(string(patch.GetPatch()), FenceTaintKey)
}

func isMirrorDelete(action k8stesting.Action) bool {
	del, isDelete := action.(k8stesting.DeleteAction)
	return isDelete && del.GetName() == "vllm-m"
}

func TestDonorGoneFencesAndStopsMirror(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	hn := newHarness(t, dpOptions(fakeHolders{holders: twoGPUHolders()}, rec), hostNode(0, 1), donorPod())
	guest := testGuest()
	hn.addGuest(guest)
	if err := hn.b.Create(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	hn.waitInformer("guest-uid")
	ctx := context.Background()
	hn.client.ClearActions()

	// The donor is deleted (grace period running): deletionTimestamp is set.
	donor, err := hn.client.CoreV1().Pods("ns").Get(ctx, "donor", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	donor.DeletionTimestamp = &now
	if _, err := hn.client.CoreV1().Pods("ns").Update(ctx, donor, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && hn.mirror("vllm-m") != nil {
		time.Sleep(20 * time.Millisecond)
	}
	if hn.mirror("vllm-m") != nil {
		t.Fatal("mirror must be stopped when its donor goes")
	}
	// The taint must have gone on before the mirror delete.
	actions := hn.client.Actions()
	taintAt, deleteAt := firstAction(actions, isFenceTaintPatch), firstAction(actions, isMirrorDelete)
	if taintAt < 0 || deleteAt < 0 || taintAt > deleteAt {
		t.Errorf("want the fence taint (action %d) before the mirror delete (action %d)", taintAt, deleteAt)
	}
	// Once no mirror holds the GPU, the taint comes off.
	for time.Now().Before(deadline) {
		if node := getNode(t, hn); fenceTaint(node) == "" && len(node.Spec.Taints) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if node := getNode(t, hn); fenceTaint(node) != "" || len(node.Spec.Taints) != 1 {
		t.Errorf("fence taint must be removed and other taints kept: %v", node.Spec.Taints)
	}
	var sawDonorGone bool
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, EventDonorGone) {
			sawDonorGone = true
		}
	}
	if !sawDonorGone {
		t.Error("want a DonorGone event on the guest")
	}
	noDRACalls(t, hn.client.Actions())
}

func TestStaleFenceTaintRemovedAtStart(t *testing.T) {
	node := hostNode(0, 1)
	node.Spec.Taints = append(node.Spec.Taints,
		corev1.Taint{Key: FenceTaintKey, Value: "vk-x", Effect: corev1.TaintEffectNoSchedule})
	hn := newHarness(t, dpOptions(fakeHolders{holders: twoGPUHolders()}, nil), node, donorPod())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fenceTaint(getNode(t, hn)) == "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("a fence taint left by an earlier run must be removed when nothing is fenced")
}

func TestForeignFenceTaintLeftAlone(t *testing.T) {
	node := hostNode(0, 1)
	node.Spec.Taints = append(node.Spec.Taints,
		corev1.Taint{Key: FenceTaintKey, Value: "vk-other", Effect: corev1.TaintEffectNoSchedule})
	hn := newHarness(t, dpOptions(fakeHolders{holders: twoGPUHolders()}, nil), node, donorPod())
	hn.b.reconcileFence(context.Background())
	if cur := getNode(t, hn); fenceTaint(cur) != "vk-other" {
		t.Errorf("another guest kubelet's fence must stay: %v", cur.Spec.Taints)
	}
}
