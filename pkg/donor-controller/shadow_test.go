package donorcontroller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addGPUPod creates a running pod of group on node holding gpus nvidia.com/gpu; extra labels
// are added (a mirror carries timeslice.io/role).
func (env *testEnv) addGPUPod(name, node, group string, gpus int64, extra map[string]string) {
	env.t.Helper()
	lbls := map[string]string{}
	if group != "" {
		lbls[GroupLabelKey] = group
	}
	for k, v := range extra {
		lbls[k] = v
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: lbls},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{
			Name: "c", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{GPUResource: *resource.NewQuantity(gpus, resource.DecimalSI)}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := env.cs.CoreV1().Pods(testNS).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		env.t.Fatal(err)
	}
}

func (env *testEnv) lendable(name string) string {
	return env.node(name).Annotations[AnnotationLendableGPUs]
}

// The lendable set follows the group's donor pod; a second GPU-holding donor pod on the host
// makes it 0 (fail closed) with a Warning event; era end removes the annotation.
func TestLendableGPUs(t *testing.T) {
	env := newEnv(t, LabelKeysNS, false, realNode("w1", nil))
	env.addGPUPod("donor", "w1", testGroup, 2, nil)
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.eventually("lendable 2", func() bool { return env.lendable("w1") == "2" })

	env.addGPUPod("donor-b", "w1", testGroup, 1, nil)
	env.eventually("lendable 0 with two holders", func() bool { return env.lendable("w1") == "0" })
	env.eventually("LendableAmbiguous event", func() bool { return env.hasEvent(EventLendableAmbiguous, corev1.EventTypeWarning) })

	if err := env.cs.CoreV1().Pods(testNS).Delete(context.Background(), "donor-b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	env.eventually("lendable 2 again", func() bool { return env.lendable("w1") == "2" })

	env.deleteDonor()
	env.eventually("lendable 0 without donor", func() bool { return env.lendable("w1") == "0" })
	env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w1") })
	env.advance(testTTL + 2*testStep)
	env.eventually("w1 unlabelled", func() bool { return env.clean("w1") })
	if _, ok := env.node("w1").Annotations[AnnotationLendableGPUs]; ok {
		t.Fatal("lendable annotation left after era end")
	}
}

func TestLendableOf(t *testing.T) {
	pod := func(name string, ctr, init int64) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: name}}
		p.Spec.Containers = []corev1.Container{{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{GPUResource: *resource.NewQuantity(ctr, resource.DecimalSI)},
		}}}
		if init > 0 {
			p.Spec.InitContainers = []corev1.Container{{Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{GPUResource: *resource.NewQuantity(init, resource.DecimalSI)},
			}}}
		}
		return p
	}
	for _, tc := range []struct {
		name    string
		donors  donorSet
		want    int64
		holders int
	}{
		{"none", donorSet{}, 0, 0},
		{"one holder", donorSet{"g": {pod("a", 4, 0), pod("cpu", 0, 0)}}, 4, 1},
		{"init larger", donorSet{"g": {pod("a", 1, 2)}}, 2, 1},
		{"two holders, two groups", donorSet{"g": {pod("a", 1, 0)}, "h": {pod("b", 1, 0)}}, 0, 2},
	} {
		got, holders := lendable(tc.donors)
		if got != tc.want || len(holders) != tc.holders {
			t.Errorf("%s: lendable %d holders %v, want %d and %d holders", tc.name, got, holders, tc.want, tc.holders)
		}
	}
}

func fencedNode(name, vk string) *corev1.Node {
	n := realNode(name, nil)
	n.Spec.Taints = []corev1.Taint{
		{Key: "other", Value: "x", Effect: corev1.TaintEffectNoSchedule},
		{Key: FenceTaintKey, Value: vk, Effect: corev1.TaintEffectNoSchedule},
	}
	return n
}

func (env *testEnv) fenced(name string) bool {
	return fenceTaint(env.node(name)) != nil
}

func TestClearStaleFences(t *testing.T) {
	cases := []struct {
		name   string
		fences bool
		objs   []runtime.Object
		mirror bool
		want   bool // fence removed
	}{
		{"virtual node gone", true, []runtime.Object{fencedNode("h1", "vk-h1")}, false, true},
		{"virtual node present", true, []runtime.Object{fencedNode("h1", "vk-h1"), virtualNode("vk-h1", "h1", "uid-h1")}, false, false},
		{"mirror still on host", true, []runtime.Object{fencedNode("h1", "vk-h1")}, true, false},
		{"flag off", false, []runtime.Object{fencedNode("h1", "vk-h1")}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnvWith(t, LabelKeysNS, false, tc.fences, tc.objs...)
			if tc.mirror {
				env.addGPUPod("mirror", "h1", "", 1, map[string]string{RoleLabelKey: "background"})
			}
			settleTicks()
			env.advance(testGrace - 2*testStep)
			if !env.fenced("h1") {
				t.Fatal("fence removed before the grace")
			}
			env.advance(3 * testStep)
			if tc.want {
				env.eventually("fence removed", func() bool { return !env.fenced("h1") })
				if taints := env.node("h1").Spec.Taints; len(taints) != 1 || taints[0].Key != "other" {
					t.Fatalf("other taints must stay: %+v", taints)
				}
				env.eventually("StaleFenceCleared event", func() bool { return env.hasEvent(EventStaleFenceCleared, corev1.EventTypeNormal) })
				return
			}
			env.advance(3 * testStep)
			if !env.fenced("h1") {
				t.Fatal("fence removed")
			}
		})
	}
}
