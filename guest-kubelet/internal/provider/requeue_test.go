package provider_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

func strandedPod(name string, phase corev1.PodPhase, owner string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "batch", Name: name, UID: types.UID("uid-" + name)},
		Spec: corev1.PodSpec{
			NodeName:    "vk-x",
			Tolerations: []corev1.Toleration{{Key: provider.GuestTaintKey, Operator: corev1.TolerationOpExists}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if owner != "" {
		yes := true
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: owner, UID: "rs-uid", Controller: &yes}}
	}
	return p
}

type requeueRig struct {
	cs    *fake.Clientset
	rec   *record.FakeRecorder
	donor atomic.Bool
	now   time.Time
	rq    *provider.Requeuer
}

func newRequeueRig(t *testing.T, pods ...*corev1.Pod) *requeueRig {
	t.Helper()
	objs := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	r := &requeueRig{cs: fake.NewClientset(objs...), rec: record.NewFakeRecorder(20), now: time.Unix(1000, 0)}
	r.donor.Store(true)
	r.rq = &provider.Requeuer{
		Pods: r.cs.CoreV1(),
		Bound: func() ([]*corev1.Pod, error) {
			l, err := r.cs.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			out := make([]*corev1.Pod, 0, len(l.Items))
			for i := range l.Items {
				out = append(out, &l.Items[i])
			}
			return out, nil
		},
		IsGuest: provider.IsGuest,
		HasDonor: func() (bool, string) {
			if r.donor.Load() {
				return true, "1 donor pod(s) on the host"
			}
			return false, "no donor pod on the host"
		},
		After:    10 * time.Second,
		Recorder: r.rec,
		Now:      func() time.Time { return r.now },
	}
	return r
}

func (r *requeueRig) exists(name string) bool {
	_, err := r.cs.CoreV1().Pods("batch").Get(context.Background(), name, metav1.GetOptions{})
	return err == nil
}

func TestRequeueDeletesOnlyStrandedOwnedGuests(t *testing.T) {
	terminating := strandedPod("terminating", corev1.PodPending, "rs")
	now := metav1.Now()
	terminating.DeletionTimestamp, terminating.Finalizers = &now, []string{"test/hold"}
	notGuest := strandedPod("ds-pod", corev1.PodPending, "rs")
	notGuest.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	r := newRequeueRig(t,
		strandedPod("pending", corev1.PodPending, "rs"),
		strandedPod("unset", "", "rs"),
		strandedPod("running", corev1.PodRunning, "rs"),
		strandedPod("bare", corev1.PodPending, ""),
		terminating, notGuest,
	)
	ctx := context.Background()

	if n := r.rq.Tick(ctx); n != 0 {
		t.Fatalf("donor present: deleted %d", n)
	}
	r.donor.Store(false)
	if n := r.rq.Tick(ctx); n != 0 {
		t.Fatalf("donor just gone: deleted %d before After", n)
	}
	r.now = r.now.Add(9 * time.Second)
	if n := r.rq.Tick(ctx); n != 0 {
		t.Fatalf("9s without a donor: deleted %d before After (10s)", n)
	}
	r.now = r.now.Add(2 * time.Second)
	if n := r.rq.Tick(ctx); n != 2 {
		t.Fatalf("11s without a donor: deleted %d, want 2 (pending, unset)", n)
	}
	for name, want := range map[string]bool{
		"pending": false, "unset": false, "running": true, "bare": true, "terminating": true, "ds-pod": true,
	} {
		if got := r.exists(name); got != want {
			t.Errorf("%s exists = %v, want %v", name, got, want)
		}
	}
	var requeued, stranded int
	for len(r.rec.Events) > 0 {
		ev := <-r.rec.Events
		switch {
		case strings.Contains(ev, provider.EventRequeued):
			requeued++
		case strings.Contains(ev, provider.EventStrandedNoOwn):
			stranded++
		}
	}
	if requeued != 2 || stranded != 1 {
		t.Errorf("events: %d %s, %d %s; want 2 and 1", requeued, provider.EventRequeued, stranded, provider.EventStrandedNoOwn)
	}
	// The bare pod is warned about once, not on every pass.
	r.rq.Tick(ctx)
	if len(r.rec.Events) != 0 {
		t.Errorf("second pass recorded %d more events", len(r.rec.Events))
	}
}

func TestRequeueDonorBackResetsTheClock(t *testing.T) {
	r := newRequeueRig(t, strandedPod("pending", corev1.PodPending, "rs"))
	ctx := context.Background()
	r.donor.Store(false)
	r.rq.Tick(ctx)
	r.now = r.now.Add(8 * time.Second)
	r.donor.Store(true) // a donor pod replaced in place
	r.rq.Tick(ctx)
	r.donor.Store(false)
	r.rq.Tick(ctx)
	r.now = r.now.Add(8 * time.Second)
	if n := r.rq.Tick(ctx); n != 0 || !r.exists("pending") {
		t.Fatalf("16s in total but only 8s since the donor left: deleted %d", n)
	}
	r.now = r.now.Add(3 * time.Second)
	if n := r.rq.Tick(ctx); n != 1 || r.exists("pending") {
		t.Fatalf("11s since the donor left: deleted %d, want 1", n)
	}
}

func TestWithoutDonor(t *testing.T) {
	donor := true
	hasDonor := func() (bool, string) {
		if donor {
			return true, "1 donor pod(s) on the host"
		}
		return false, "no donor pod on the host"
	}
	suspended := false
	held := func() (bool, string) {
		if suspended {
			return true, "1 guest(s) suspended"
		}
		return false, "no guest suspended"
	}
	for _, tc := range []struct {
		donor, suspended bool
		next             bool
		want             bool
		why              string
	}{
		{donor: true, next: false, want: false, why: "donor present"},
		{donor: false, next: false, want: true, why: "no donor pod on the host"},
		{donor: true, next: true, want: false, why: "no guest suspended"},
		{donor: true, suspended: true, next: true, want: true, why: "1 guest(s) suspended"},
		{donor: false, suspended: true, next: true, want: true, why: "no donor pod on the host"},
	} {
		donor, suspended = tc.donor, tc.suspended
		var next provider.HoldFunc
		if tc.next {
			next = held
		}
		got, why := provider.WithoutDonor(hasDonor, next)()
		if got != tc.want || why != tc.why {
			t.Errorf("donor=%v suspended=%v next=%v: got %v (%s), want %v (%s)",
				tc.donor, tc.suspended, tc.next, got, why, tc.want, tc.why)
		}
	}
}

func TestCordonWithoutDonor_CordonThenUncordonWhenDonorBack(t *testing.T) {
	cs := cordonCluster(t, false, nil)
	c := provider.NewCordoner(cs.CoreV1().Nodes(), cordonNode, nil)
	c.Resync = time.Nanosecond
	ctx := context.Background()
	var donor atomic.Bool
	donor.Store(true)
	hold := provider.WithoutDonor(func() (bool, string) {
		if donor.Load() {
			return true, "donor"
		}
		return false, "no donor pod on the host"
	}, nil)

	set := func() { held, why := hold(); c.Set(ctx, held, why) }
	set()
	if cordonGet(t, cs).Spec.Unschedulable {
		t.Fatal("donor present: cordoned")
	}
	donor.Store(false)
	set()
	if n := cordonGet(t, cs); !n.Spec.Unschedulable || n.Annotations[provider.CordonedByAnnotation] != provider.CordonedByValue {
		t.Fatalf("donor gone: unschedulable=%v annotations=%v", n.Spec.Unschedulable, n.Annotations)
	}
	donor.Store(true)
	set()
	if n := cordonGet(t, cs); n.Spec.Unschedulable || n.Annotations[provider.CordonedByAnnotation] != "" {
		t.Fatalf("donor back: unschedulable=%v annotations=%v", n.Spec.Unschedulable, n.Annotations)
	}
}
