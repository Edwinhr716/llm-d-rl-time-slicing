package era //nolint:testpackage // drives the unexported step, caches and seams

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
	"github.com/edwinhr716/guest-kubelet/internal/testutil"
)

const (
	tHost  = "host-1"
	tVK    = "vk-1"
	tGroup = "g1"
	tTTL   = time.Minute
	tNS    = "ev"
)

var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func TestLastLockActivity(t *testing.T) {
	now := t0.Add(time.Hour)
	st := func(s GroupState, locking, active string) *GroupStatus {
		return &GroupStatus{GroupState: s, LockingJob: locking, ActiveJob: active}
	}
	cases := []struct {
		name      string
		prev, cur *GroupStatus
		want      bool
	}{
		{"unknown group", st(StateIdle, "", ""), nil, false},
		{"first observation idle", nil, st(StateIdle, "", ""), false},
		{"first observation locked", nil, st(StateLocked, "j1", "j1"), true},
		{"locked stays locked", st(StateLocked, "j1", "j1"), st(StateLocked, "j1", "j1"), true},
		{"switching", st(StateIdleYielded, "", "j1"), st(StateSwitching, "j2", "j1"), true},
		{"vacating", st(StateBackground, "vk/h", "vk/h"), st(StateVacating, "vk/h", "vk/h"), false},
		{"trainer yields", st(StateLocked, "j1", "j1"), st(StateIdleYielded, "", "j1"), true},
		{"context released", st(StateIdleYielded, "", "j1"), st(StateIdle, "", ""), true},
		{"no change", st(StateIdleYielded, "", "j1"), st(StateIdleYielded, "", "j1"), false},
		{"vk acquires", st(StateIdleYielded, "", "j1"), st(StateBackground, "vk/h", "vk/h"), false},
		{"vk yields", st(StateBackground, "vk/h", "vk/h"), st(StateIdleYielded, "", "j1"), false},
		{"vk heartbeat", st(StateBackground, "vk/h", "vk/h"), st(StateBackground, "vk/h", "vk/h"), false},
		{"trainer takes it back from vk", st(StateBackground, "vk/h", "vk/h"), st(StateLocked, "j1", "j1"), true},
		{"vk locked is not foreground", nil, st(StateLocked, "vk/h", "vk/h"), false},
	}
	for _, tc := range cases {
		got := LastLockActivity(tc.prev, tc.cur, now)
		if got.Equal(now) != tc.want || (!tc.want && !got.IsZero()) {
			t.Errorf("%s: got %v, want activity=%v", tc.name, got, tc.want)
		}
	}
}

func TestGroupFromHost(t *testing.T) {
	n := func(l map[string]string) *corev1.Node { return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: l}} }
	cases := []struct {
		name, keys, want string
		node             *corev1.Node
	}{
		{"prefix", LabelKeysPrefix, "g1", n(map[string]string{"group.timeslice.io/g1": "true"})},
		{"prefix false value", LabelKeysPrefix, "", n(map[string]string{"group.timeslice.io/g1": "false"})},
		{"prefix two groups", LabelKeysPrefix, "", n(map[string]string{
			"group.timeslice.io/g1": "true", "group.timeslice.io/g2": "true",
		})},
		{"ns", LabelKeysNS, "ns.job.g", n(map[string]string{"timeslice.io/donor": "true", "timeslice.io/group": "ns.job.g"})},
		{"ns without donor", LabelKeysNS, "", n(map[string]string{"timeslice.io/group": "ns.job.g"})},
		{"ns ignores prefix form", LabelKeysNS, "", n(map[string]string{"group.timeslice.io/g1": "true"})},
		{"nil host", LabelKeysPrefix, "", nil},
	}
	for _, tc := range cases {
		if got := GroupFromHost(tc.node, tc.keys); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// fixture drives a Controller step by step on a fake clientset and a fake clock.
type fixture struct {
	t     *testing.T
	cs    kubernetes.Interface
	c     *Controller
	now   time.Time
	locks *scriptedLocks
	set   func(state, job string)
	calls []string // hook calls, in order
	mu    sync.Mutex
	ctx   context.Context
}

type shape bool

const (
	shapeNS    shape = true  // virtual Node with the D-VK-2 c finalizer
	shapeToday shape = false // plain virtual Node
)

func (s shape) String() string {
	if s {
		return "ns-finalizer"
	}
	return "today-plain"
}

func newFixture(t *testing.T, sh shape, in *Config, hookEdit func(*fixture, *Hooks)) *fixture {
	t.Helper()
	host := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: tHost, UID: "host-uid",
		Labels: map[string]string{"group.timeslice.io/" + tGroup: "true"},
	}}
	vk := vkNode(sh, "vk-uid-1")
	cs := testutil.NewClient(host, vk)
	fx := &fixture{t: t, cs: cs, now: t0}
	ls, set := evalLocks()
	sl, ok := ls.(*scriptedLocks)
	if !ok {
		t.Fatal("evalLocks returned an unexpected type")
	}
	fx.locks, fx.set = sl, set
	cfg := *in
	cfg.Host, cfg.VKNode = tHost, tVK
	if cfg.EraTTL == 0 {
		cfg.EraTTL = tTTL
	}
	cfg.MirrorWait = time.Second
	hooks := Hooks{
		Stop: func(ctx context.Context) {
			fx.record("stop:" + fx.nodeState(ctx))
		},
		Start: func(ctx context.Context) error {
			fx.record("start")
			_, err := cs.CoreV1().Nodes().Create(ctx, vkNode(sh, "vk-uid-2"), metav1.CreateOptions{})
			return err
		},
		Yield: func(context.Context) error { fx.record("yield"); return nil },
	}
	if hookEdit != nil {
		hookEdit(fx, &hooks)
	}
	c, err := New(cs, ls, func() time.Time { return fx.now }, &cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	fx.c = c
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fx.ctx = ctx
	for _, fac := range c.factories {
		fac.Start(ctx.Done())
	}
	if !cache.WaitForCacheSync(ctx.Done(), c.synced...) {
		t.Fatal("informers did not sync")
	}
	c.restore(ctx)
	return fx
}

func vkNode(sh shape, uid types.UID) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: tVK, UID: uid, Labels: map[string]string{provider.VirtualNodeLabel: "true"},
	}}
	if sh {
		n.Finalizers = []string{provider.NodeFinalizer}
	}
	return n
}

func (fx *fixture) record(s string) {
	fx.mu.Lock()
	fx.calls = append(fx.calls, s)
	fx.mu.Unlock()
}

// step waits for the informers to catch up with the API, then evaluates once.
func (fx *fixture) step() {
	fx.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fx.cachesCurrent() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	fx.c.step(fx.ctx)
}

func (fx *fixture) cachesCurrent() bool {
	h, err := fx.cs.CoreV1().Nodes().Get(context.Background(), tHost, metav1.GetOptions{})
	if err != nil {
		return false
	}
	ch, err := fx.c.hostNodes.Get(tHost)
	if err != nil || ch.ResourceVersion != h.ResourceVersion || len(ch.Labels) != len(h.Labels) {
		return false
	}
	api, err := fx.cs.CoreV1().Pods(corev1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return false
	}
	var want, got []string
	for i := range api.Items {
		if p := &api.Items[i]; fx.c.isDonor(p) {
			want = append(want, p.Name+string(p.Status.Phase))
		}
	}
	cached, err := fx.c.donorPods.List(labels.Everything())
	if err != nil {
		return false
	}
	for _, p := range cached {
		if fx.c.isDonor(p) {
			got = append(got, p.Name+string(p.Status.Phase))
		}
	}
	sort.Strings(want)
	sort.Strings(got)
	return slices.Equal(want, got)
}

func (fx *fixture) advance(d time.Duration) {
	for s := time.Duration(0); s < d; s += 5 * time.Second {
		fx.now = fx.now.Add(5 * time.Second)
		fx.step()
	}
}

func (fx *fixture) pod(name, node string, uid types.UID, l map[string]string) {
	fx.t.Helper()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tNS, UID: uid, Labels: l},
		Spec:       corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "c", Image: "i"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := fx.cs.CoreV1().Pods(tNS).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
		fx.t.Fatal(err)
	}
}

func (fx *fixture) donor(name string) {
	fx.pod(name, tHost, types.UID(name+"-uid"), map[string]string{"timeslice.io/group": tGroup})
}

func (fx *fixture) del(name string) {
	if err := fx.cs.CoreV1().Pods(tNS).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		fx.t.Fatal(err)
	}
}

// guest on the virtual Node with its mirror on the host (labels as builder.go writes them).
func (fx *fixture) guest(name string) {
	uid := types.UID(name + "-uid")
	fx.pod(name, tVK, uid, map[string]string{"app": "guest"})
	fx.pod(mirror.Name(name), tHost, "", map[string]string{
		mirror.LabelMirrorOf: string(uid), mirror.LabelMirrorNode: tVK, "timeslice.io/role": "background",
	})
}

func (fx *fixture) vkState() string { return fx.nodeState(fx.ctx) }

func (fx *fixture) nodeState(ctx context.Context) string {
	n, err := fx.cs.CoreV1().Nodes().Get(ctx, tVK, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return "absent"
	case err != nil:
		return "error"
	case n.DeletionTimestamp != nil:
		return "terminating"
	}
	return "present:" + string(n.UID)
}

func (fx *fixture) podGone(name string) bool {
	_, err := fx.cs.CoreV1().Pods(tNS).Get(context.Background(), name, metav1.GetOptions{})
	return apierrors.IsNotFound(err)
}

func (fx *fixture) expired(name string) bool {
	p, err := fx.cs.CoreV1().Pods(tNS).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil && p.Status.Phase == corev1.PodFailed && p.Status.Reason == ReasonEraEnded
}

func (fx *fixture) annotation() string {
	n, err := fx.cs.CoreV1().Nodes().Get(context.Background(), tVK, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return n.Annotations[AnnotationIdleSince]
}

func forShapes(t *testing.T, fn func(*testing.T, shape)) {
	t.Helper()
	for _, sh := range []shape{shapeNS, shapeToday} {
		t.Run(sh.String(), func(t *testing.T) { fn(t, sh) })
	}
}

func TestEra_EndAtTTL(t *testing.T) {
	forShapes(t, func(t *testing.T, sh shape) {
		t.Helper()
		fx := newFixture(t, sh, &Config{Trigger: TriggerTTL}, nil)
		fx.donor("rl-0")
		fx.guest("guest-0")
		fx.pod("other-m", tHost, "o", map[string]string{mirror.LabelMirrorNode: "vk-other"})
		fx.step()
		if fx.c.state != StateActive {
			t.Fatalf("state %s with a donor pod, want active", fx.c.state)
		}
		fx.advance(2 * tTTL)
		if fx.vkState() != "present:vk-uid-1" || fx.expired("guest-0") {
			t.Fatalf("idle donor pod: era ended (node %s)", fx.vkState())
		}
		fx.del("rl-0")
		fx.step()
		startAt := fx.now
		if fx.c.state != StateCounting || fx.annotation() != startAt.Format(time.RFC3339) {
			t.Fatalf("after the last donor: state %s annotation %q, want counting %s",
				fx.c.state, fx.annotation(), startAt.Format(time.RFC3339))
		}
		fx.advance(tTTL - 5*time.Second)
		if fx.vkState() == "absent" {
			t.Fatal("era ended before the TTL")
		}
		fx.advance(5 * time.Second)
		if fx.vkState() != "absent" {
			t.Fatalf("at the TTL: vk node %s, want absent", fx.vkState())
		}
		if !fx.expired("guest-0") || !fx.podGone("guest-0-m") {
			t.Fatalf("guest expired=%v mirror gone=%v", fx.expired("guest-0"), fx.podGone("guest-0-m"))
		}
		if fx.podGone("other-m") {
			t.Fatal("a mirror of another virtual Node was deleted")
		}
		want := []string{"yield", "stop:present:vk-uid-1"}
		if !slices.Equal(fx.calls, want) {
			t.Fatalf("hook calls %v, want %v (stop before deregistration)", fx.calls, want)
		}
		if fx.c.state != StateEnded || fx.c.MirrorDeletedReason() != ReasonEraEnded {
			t.Fatalf("state %s reason %q", fx.c.state, fx.c.MirrorDeletedReason())
		}
		// Stays gone while no donor pod is on the host.
		fx.advance(3 * tTTL)
		if fx.vkState() != "absent" {
			t.Fatalf("node came back without a donor pod: %s", fx.vkState())
		}
		// A new donor pod starts a fresh era with a new Node.
		fx.donor("rl-1")
		fx.step()
		if fx.vkState() != "present:vk-uid-2" || fx.c.state != StateActive || fx.c.MirrorDeletedReason() != "" {
			t.Fatalf("fresh era: node %s state %s", fx.vkState(), fx.c.state)
		}
		if !fx.c.Admit(fx.ctx, &corev1.Pod{}) {
			t.Fatal("admission closed in a fresh era")
		}
	})
}

func TestEra_LockActivityKeeps(t *testing.T) {
	forShapes(t, func(t *testing.T, sh shape) {
		t.Helper()
		fx := newFixture(t, sh, &Config{Trigger: TriggerTTL}, nil)
		fx.set("IDLE", "")
		fx.step()
		fx.set("LOCKED", "job1")
		fx.advance(2 * tTTL)
		if fx.c.state != StateActive || fx.vkState() == "absent" {
			t.Fatalf("lock held: state %s node %s", fx.c.state, fx.vkState())
		}
		fx.set("IDLE_YIELDED", "")
		fx.step()
		yieldAt := fx.now
		fx.set("BACKGROUND", "vk/"+tHost)
		fx.advance(tTTL - 5*time.Second)
		if fx.vkState() == "absent" {
			t.Fatal("VK's own background grant counted as no activity too early, or yield not counted")
		}
		fx.set("IDLE_YIELDED", "")
		fx.advance(5 * time.Second)
		if fx.vkState() != "absent" {
			t.Fatalf("TTL after the trainer's yield (%s): node %s", yieldAt, fx.vkState())
		}
	})
}

func TestEra_LabelLoss(t *testing.T) {
	forShapes(t, func(t *testing.T, sh shape) {
		t.Helper()
		for _, trig := range []string{TriggerEither, TriggerLabel, TriggerTTL} {
			fx := newFixture(t, sh, &Config{Trigger: trig, EraTTL: time.Hour}, nil)
			fx.donor("rl-0")
			fx.guest("guest-0")
			fx.step()
			hostNode, err := fx.cs.CoreV1().Nodes().Get(context.Background(), tHost, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			delete(hostNode.Labels, "group.timeslice.io/"+tGroup)
			if _, err := fx.cs.CoreV1().Nodes().Update(context.Background(), hostNode, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			fx.step()
			ended := fx.vkState() == "absent" && fx.expired("guest-0")
			if ended != (trig != TriggerTTL) {
				t.Fatalf("trigger %s: ended=%v after label loss (donor pod still present)", trig, ended)
			}
			if trig == TriggerTTL {
				continue
			}
			// A donor pod without the label does not start a fresh era; with it, it does.
			fx.step()
			if fx.vkState() != "absent" {
				t.Fatalf("trigger %s: fresh era without the host label", trig)
			}
			if hostNode, err = fx.cs.CoreV1().Nodes().Get(context.Background(), tHost, metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}
			hostNode.Labels["group.timeslice.io/"+tGroup] = "true"
			if _, err := fx.cs.CoreV1().Nodes().Update(context.Background(), hostNode, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			fx.step()
			if fx.vkState() != "present:vk-uid-2" {
				t.Fatalf("trigger %s: no fresh era after relabel: %s", trig, fx.vkState())
			}
		}
	})
}

func TestEra_LabelOnlyIgnoresTTL(t *testing.T) {
	fx := newFixture(t, shapeNS, &Config{Trigger: TriggerLabel}, nil)
	fx.step()
	fx.advance(3 * tTTL)
	if fx.vkState() == "absent" {
		t.Fatal("trigger label ended the era on the TTL")
	}
}

func TestEra_TTLZeroNeverEnds(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: tVK}})
	c, err := New(cs, nil, nil, &Config{Host: tHost, VKNode: tVK}, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Enabled() || c.cfg.Trigger != TriggerEither {
		t.Fatalf("enabled=%v trigger=%s", c.Enabled(), c.cfg.Trigger)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if acts := cs.Actions(); len(acts) != 0 {
		t.Fatalf("era off made API calls: %v", acts)
	}
}

func TestEra_RestartResumesCountdown(t *testing.T) {
	forShapes(t, func(t *testing.T, sh shape) {
		t.Helper()
		fx := newFixture(t, sh, &Config{Trigger: TriggerTTL}, nil)
		fx.step() // no donor pod: the countdown starts at t0
		fx.advance(tTTL / 2)
		if fx.annotation() != t0.Format(time.RFC3339) {
			t.Fatalf("annotation %q, want %s", fx.annotation(), t0.Format(time.RFC3339))
		}
		// A new VK process (same API state, clock later).
		cfg := fx.c.cfg
		c2, err := New(fx.cs, nil, func() time.Time { return fx.now }, &cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		fx.c = c2
		for _, fac := range c2.factories {
			fac.Start(fx.ctx.Done())
		}
		cache.WaitForCacheSync(fx.ctx.Done(), c2.synced...)
		c2.restore(fx.ctx)
		fx.advance(tTTL/2 - 5*time.Second)
		if fx.vkState() == "absent" {
			t.Fatal("restart made the era end early")
		}
		fx.advance(5 * time.Second)
		if fx.vkState() != "absent" {
			t.Fatalf("restart restarted the countdown: node %s at t0+TTL", fx.vkState())
		}
	})
}

func TestEra_DonorBackClearsAnnotation(t *testing.T) {
	fx := newFixture(t, shapeNS, &Config{Trigger: TriggerTTL}, nil)
	fx.step()
	if fx.annotation() == "" {
		t.Fatal("no annotation while counting")
	}
	fx.donor("rl-0")
	fx.step()
	if fx.annotation() != "" || fx.c.state != StateActive {
		t.Fatalf("donor back: annotation %q state %s", fx.annotation(), fx.c.state)
	}
	// Mirrors and finished pods are never donors.
	fx.del("rl-0")
	fx.pod("m", tHost, "m", map[string]string{"timeslice.io/group": tGroup, mirror.LabelMirrorNode: tVK})
	fx.pod("bg", tHost, "bg", map[string]string{"timeslice.io/group": tGroup, "timeslice.io/role": "background"})
	fx.step()
	if fx.c.state != StateCounting {
		t.Fatalf("mirror or background pod counted as a donor: state %s", fx.c.state)
	}
}

func TestEra_KillPathAndRefusedGuest(t *testing.T) {
	fx := newFixture(t, shapeNS, &Config{Trigger: TriggerTTL}, func(fx *fixture, h *Hooks) {
		h.Kill = func(ctx context.Context, g, m *corev1.Pod) (bool, error) {
			if g.Name != "frozen" {
				return false, nil
			}
			fx.record("kill:" + g.Name)
			return true, fx.cs.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, metav1.DeleteOptions{})
		}
	})
	fx.guest("frozen")
	fx.guest("running")
	fx.step()
	fx.advance(tTTL)
	if !fx.expired("frozen") || !fx.expired("running") || !fx.podGone("frozen-m") || !fx.podGone("running-m") {
		t.Fatal("guests not expired")
	}
	if !slices.Contains(fx.calls, "kill:frozen") || slices.Contains(fx.calls, "kill:running") {
		t.Fatalf("calls %v", fx.calls)
	}
	// A guest bound after the end gets no mirror; it is failed instead.
	fx.pod("late", tVK, "late-uid", nil)
	late, err := fx.cs.CoreV1().Pods(tNS).Get(context.Background(), "late", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if fx.c.Admit(fx.ctx, late) {
		t.Fatal("admitted a guest after the era ended")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !fx.expired("late") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !fx.expired("late") {
		t.Fatal("late guest not failed")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	cs := fake.NewSimpleClientset()
	for _, cfg := range []Config{
		{Host: tHost},
		{Host: tHost, VKNode: tVK, Trigger: "sometimes"},
		{Host: tHost, VKNode: tVK, LabelKeys: "both"},
		{Host: tHost, VKNode: tVK, DonorSelector: "a b c"},
		{Host: tHost, VKNode: tVK, EraTTL: -time.Second},
	} {
		if _, err := New(cs, nil, nil, &cfg, Hooks{}); err == nil {
			t.Errorf("config %+v accepted", cfg)
		}
	}
}
