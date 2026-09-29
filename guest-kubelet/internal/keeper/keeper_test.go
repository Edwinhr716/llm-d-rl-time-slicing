package keeper_test

import (
	"context"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/edwinhr716/guest-kubelet/internal/keeper"
)

var start = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func hostNode(ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "host"},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}},
	}
}

func vkLease(renew time.Time) *coordinationv1.Lease {
	holder := "vk-x"
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "vk-x", Namespace: keeper.NodeLeaseNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder,
			RenewTime:      &metav1.MicroTime{Time: renew.UTC().Truncate(time.Microsecond)},
		},
	}
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func config(clk *clock) keeper.Config {
	return keeper.Config{
		HostNode:    "host",
		VirtualNode: "vk-x",
		StaleAfter:  15 * time.Second,
		OutageGrace: 15 * time.Minute,
		Now:         clk.now,
	}
}

func setup(objs ...runtime.Object) (*fake.Clientset, *keeper.Keeper, *clock) {
	clk := &clock{t: start}
	client := fake.NewClientset(objs...)
	return client, keeper.New(client, config(clk)), clk
}

func getLease(t *testing.T, client *fake.Clientset) *coordinationv1.Lease {
	t.Helper()
	lease, err := client.CoordinationV1().Leases(keeper.NodeLeaseNamespace).Get(context.Background(), "vk-x", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func tick(t *testing.T, kp *keeper.Keeper) keeper.Result {
	t.Helper()
	res, err := kp.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestTwoMinuteOutage walks a 2-minute outage in 5 s ticks: the Lease never gets older than
// StaleAfter + one tick, which is well under the 40 s node-monitor grace period.
func TestTwoMinuteOutage(t *testing.T) {
	client, kp, clk := setup(hostNode(corev1.ConditionTrue), vkLease(start))
	var maxAge time.Duration
	renewals := 0
	for s := 0; s <= 120; s += 5 {
		clk.t = start.Add(time.Duration(s) * time.Second)
		if age := clk.t.Sub(getLease(t, client).Spec.RenewTime.Time); age > maxAge {
			maxAge = age
		}
		res := tick(t, kp)
		if res.Action == keeper.ActionRenewed {
			renewals++
			if !res.VKLastSeen.Equal(start) {
				t.Errorf("vkLastSeen = %v, want the guest kubelet's last renewal %v", res.VKLastSeen, start)
			}
		}
	}
	if renewals == 0 || maxAge > 20*time.Second {
		t.Errorf("renewals=%d maxAge=%v", renewals, maxAge)
	}

	// The guest kubelet comes back and renews: the keeper sees it and stays out of the way.
	back := clk.t.Add(time.Second)
	lease := getLease(t, client)
	lease.Spec.RenewTime = &metav1.MicroTime{Time: back}
	leases := client.CoordinationV1().Leases(keeper.NodeLeaseNamespace)
	if _, err := leases.Update(context.Background(), lease, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	clk.t = back.Add(5 * time.Second)
	if res := tick(t, kp); res.Action != keeper.ActionFresh || !res.VKLastSeen.Equal(back) {
		t.Errorf("after recovery: %+v", res)
	}
}

func TestGivesUpAfterGrace(t *testing.T) {
	client, kp, clk := setup(hostNode(corev1.ConditionTrue), vkLease(start))
	var last keeper.Result
	for s := 0; s <= int((17 * time.Minute).Seconds()); s += 5 {
		clk.t = start.Add(time.Duration(s) * time.Second)
		last = tick(t, kp)
	}
	if last.Action != keeper.ActionGaveUp {
		t.Fatalf("after 17 min: %+v", last)
	}
	if age := clk.t.Sub(getLease(t, client).Spec.RenewTime.Time); age < time.Minute {
		t.Errorf("the Lease must go stale after the grace; age %v", age)
	}

	// A keeper restart (new object, same Lease) keeps counting from the annotation.
	if res := tick(t, keeper.New(client, config(clk))); res.Action != keeper.ActionGaveUp {
		t.Errorf("restarted keeper: %+v", res)
	}
}

func TestNeverCoversForAHostThatIsDown(t *testing.T) {
	_, kp, clk := setup(hostNode(corev1.ConditionUnknown), vkLease(start))
	clk.t = start.Add(time.Minute)
	if res := tick(t, kp); res.Action != keeper.ActionHostNotReady {
		t.Errorf("host NotReady: %+v", res)
	}
	_, kp, clk = setup(vkLease(start)) // no host Node at all
	clk.t = start.Add(time.Minute)
	if res := tick(t, kp); res.Action != keeper.ActionHostNotReady {
		t.Errorf("host gone: %+v", res)
	}
}

func TestLeaseEdgeCases(t *testing.T) {
	_, kp, _ := setup(hostNode(corev1.ConditionTrue))
	if res := tick(t, kp); res.Action != keeper.ActionNoLease {
		t.Errorf("no lease: %+v", res)
	}
	foreign := vkLease(start)
	other := "someone-else"
	foreign.Spec.HolderIdentity = &other
	_, kp, clk := setup(hostNode(corev1.ConditionTrue), foreign)
	clk.t = start.Add(time.Minute)
	if res := tick(t, kp); res.Action != keeper.ActionForeign {
		t.Errorf("foreign holder: %+v", res)
	}
	_, kp, clk = setup(hostNode(corev1.ConditionTrue), vkLease(start))
	clk.t = start.Add(10 * time.Second)
	if res := tick(t, kp); res.Action != keeper.ActionFresh {
		t.Errorf("10 s old lease: %+v", res)
	}
}

func TestConflictBacksOff(t *testing.T) {
	client, kp, clk := setup(hostNode(corev1.ConditionTrue), vkLease(start))
	leases := schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}
	client.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(leases, "vk-x", nil)
	})
	clk.t = start.Add(30 * time.Second)
	if res := tick(t, kp); res.Action != keeper.ActionConflict {
		t.Errorf("conflict: %+v", res)
	}
}
