package donorcontroller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

const (
	testTTL   = 30 * time.Second
	testGrace = 10 * time.Second
	testGroup = "ns1.job1.trainers"
	testNS    = "ns1"

	// testStep is one clock step of advance; it is the lock poll interval, so every step polls.
	testStep = 2 * time.Second
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (tc *testClock) Now() time.Time {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.now
}

func (tc *testClock) add(d time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.now = tc.now.Add(d)
}

type testLocks struct {
	mu  sync.Mutex
	st  map[string]*v1alpha1.GroupStatus
	err error
}

func (tl *testLocks) GroupStatus(_ context.Context, group string) (*v1alpha1.GroupStatus, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if tl.err != nil {
		return nil, tl.err
	}
	st, ok := tl.st[group]
	if !ok {
		return nil, nil //nolint:nilnil // LockSource contract: nil, nil is an unknown group
	}
	return &v1alpha1.GroupStatus{GroupId: st.GetGroupId(), GroupState: st.GetGroupState(), LockingJob: st.GetLockingJob()}, nil
}

// set sets the status of testGroup and clears any failure.
func (tl *testLocks) set(state v1alpha1.GroupStatus_State, locking string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.err = nil
	tl.st[testGroup] = &v1alpha1.GroupStatus{GroupId: testGroup, GroupState: state, LockingJob: locking}
}

func (tl *testLocks) fail(err error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.err = err
}

type testEnv struct {
	t     *testing.T
	cs    *fake.Clientset
	clk   *testClock
	locks *testLocks
	mode  string
	stop  func()
}

// newEnv starts a controller on a fake clientset holding objs. The group starts IDLE.
func newEnv(t *testing.T, mode string, release bool, objs ...runtime.Object) *testEnv {
	t.Helper()
	env := &testEnv{
		t:     t,
		cs:    fake.NewClientset(objs...),
		mode:  mode,
		clk:   &testClock{now: time.Unix(1_800_000_000, 0)},
		locks: &testLocks{st: map[string]*v1alpha1.GroupStatus{}},
	}
	env.locks.set(v1alpha1.GroupStatus_STATE_IDLE, "")
	env.stop = env.start(release)
	t.Cleanup(func() { env.stop() })
	return env
}

// start runs a controller on the env's cluster, clock and lock source. The returned function stops
// it and waits for it; calling it again does nothing.
func (env *testEnv) start(release bool) func() {
	env.t.Helper()
	run, err := evalController(env.cs, env.locks, env.clk.Now, Config{
		LabelKeys: env.mode, EraTTL: testTTL, DonorSelector: DefaultDonorSelector, GroupFilter: `^ns1\.`,
		VKDeregisterGrace: testGrace, ReleaseDeadHosts: release,
	})
	if err != nil {
		env.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				env.t.Errorf("run: %v", err)
			}
		})
	}
}

func realNode(name string, lbls map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), Labels: lbls}}
}

func virtualNode(name, host string, hostUID types.UID) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:            name,
		UID:             types.UID("uid-" + name),
		Labels:          map[string]string{VirtualNodeLabel: "true"},
		Finalizers:      []string{VirtualNodeFinalizer},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: host, UID: hostUID}},
	}}
}

func (env *testEnv) addPod(name, node, group string) {
	env.t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: map[string]string{GroupLabelKey: group}},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := env.cs.CoreV1().Pods(testNS).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		env.t.Fatal(err)
	}
}

// deleteDonor deletes the pod named donor.
func (env *testEnv) deleteDonor() {
	env.t.Helper()
	if err := env.cs.CoreV1().Pods(testNS).Delete(context.Background(), "donor", metav1.DeleteOptions{}); err != nil {
		env.t.Fatal(err)
	}
}

func (env *testEnv) node(name string) *corev1.Node {
	env.t.Helper()
	node, err := env.cs.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		env.t.Fatal(err)
	}
	return node
}

// labelled reports whether the node carries this controller's labels for testGroup.
func (env *testEnv) labelled(name string) bool {
	group := testGroup
	node := env.node(name)
	for k, v := range NodeLabelsFor(env.mode, group) {
		if node.Labels[k] != v {
			return false
		}
	}
	return node.Annotations[AnnotationLabelledBy] == LabelledByValue && node.Annotations[AnnotationLabelledGroup] == group
}

// clean reports whether the node has no donor-family label and no ownership annotation.
func (env *testEnv) clean(name string) bool {
	node := env.node(name)
	if hasFamilyKey(node.Labels) {
		return false
	}
	for _, key := range []string{AnnotationLabelledBy, AnnotationLabelledGroup, AnnotationLabelledKeys, AnnotationEraIdleSince} {
		if _, ok := node.Annotations[key]; ok {
			return false
		}
	}
	return true
}

func (env *testEnv) eventually(what string, cond func() bool) {
	env.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			env.t.Fatalf("timed out waiting: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// settleTicks gives the loop a few real-time ticks without moving the clock.
func settleTicks() { time.Sleep(10 * evalTick) }

// advance moves the clock in testStep steps, letting the loop run after each.
func (env *testEnv) advance(d time.Duration) {
	for moved := time.Duration(0); moved < d; moved += testStep {
		env.clk.add(testStep)
		time.Sleep(3 * evalTick)
	}
	settleTicks()
}

func (env *testEnv) hasEvent(reason, eventType string) bool {
	env.t.Helper()
	list, err := env.cs.CoreV1().Events(metav1.NamespaceDefault).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		env.t.Fatal(err)
	}
	for i := range list.Items {
		if list.Items[i].Reason == reason && list.Items[i].Type == eventType {
			return true
		}
	}
	return false
}

// idleSinceSet reports whether the era countdown has been persisted on the node.
func (env *testEnv) idleSinceSet(name string) bool {
	_, ok := env.node(name).Annotations[AnnotationEraIdleSince]
	return ok
}

func TestLabelOnBindAndEraEnd(t *testing.T) {
	for _, mode := range []string{LabelKeysPrefix, LabelKeysNS} {
		t.Run(mode, func(t *testing.T) {
			env := newEnv(t, mode, false, realNode("w1", nil), realNode("w2", nil))
			env.addPod("donor", "w1", testGroup)
			env.addPod("other-group", "w2", "ns2.job9.trainers") // outside --group-filter
			env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
			env.eventually("DonorLabelled event", func() bool { return env.hasEvent(EventDonorLabelled, corev1.EventTypeNormal) })
			if keys := env.node("w1").Annotations[AnnotationLabelledKeys]; keys == "" {
				t.Error("no labelled-keys annotation")
			}
			settleTicks()
			if !env.clean("w2") {
				t.Error("w2 labelled for a group outside the filter")
			}

			env.deleteDonor()
			env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w1") })
			env.advance(testTTL - 2*testStep)
			if !env.labelled("w1") {
				t.Fatal("labels removed before the TTL")
			}
			env.advance(3 * testStep)
			env.eventually("w1 unlabelled", func() bool { return env.clean("w1") })
			env.eventually("EraEnded event", func() bool { return env.hasEvent(EventEraEnded, corev1.EventTypeNormal) })
		})
	}
}

func TestDonorBackWithinTTLKeepsLabels(t *testing.T) {
	env := newEnv(t, LabelKeysNS, false, realNode("w1", nil))
	env.addPod("donor", "w1", testGroup)
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.deleteDonor()
	env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w1") })
	env.advance(testTTL / 2)
	env.addPod("donor-b", "w1", testGroup)
	env.eventually("countdown cleared", func() bool { return !env.idleSinceSet("w1") })
	env.advance(2 * testTTL)
	if !env.labelled("w1") {
		t.Fatal("labels removed while a donor pod is on the node")
	}
}

func TestLockActivityHoldsEraAndVKDoesNot(t *testing.T) {
	env := newEnv(t, LabelKeysPrefix, false, realNode("w1", nil))
	env.addPod("donor", "w1", testGroup)
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.deleteDonor()
	env.locks.set(v1alpha1.GroupStatus_STATE_LOCKED, "job1")
	env.advance(2 * testTTL)
	if !env.labelled("w1") {
		t.Fatal("labels removed while the group is locked")
	}
	// The VK takes the group: BACKGROUND with locking job vk/w1 is not foreground activity, so the
	// countdown runs from the last LOCKED observation.
	env.locks.set(v1alpha1.GroupStatus_STATE_BACKGROUND, BackgroundJobPrefix+"w1")
	env.advance(testTTL - 3*testStep)
	if !env.labelled("w1") {
		t.Fatal("labels removed before TTL after the last lock activity")
	}
	env.advance(4 * testStep)
	env.eventually("w1 unlabelled despite the VK", func() bool { return env.clean("w1") })
}

func TestLockStatusErrorHoldsEra(t *testing.T) {
	env := newEnv(t, LabelKeysNS, false, realNode("w1", nil))
	env.locks.fail(errors.New("orchestrator unavailable"))
	env.addPod("donor", "w1", testGroup)
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.deleteDonor()
	env.advance(2 * testTTL)
	if !env.labelled("w1") {
		t.Fatal("labels removed while the lock status was unknown")
	}
	env.locks.set(v1alpha1.GroupStatus_STATE_IDLE, "")
	env.advance(testStep)
	env.eventually("w1 unlabelled once the status is known and idle", func() bool { return env.clean("w1") })
}

func TestGroupConflictDoesNotFlip(t *testing.T) {
	for _, mode := range []string{LabelKeysPrefix, LabelKeysNS} {
		t.Run(mode, func(t *testing.T) {
			env := newEnv(t, mode, false, realNode("w1", nil))
			env.addPod("a-donor", "w1", testGroup)
			env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
			env.addPod("b-donor", "w1", "ns1.job2.trainers")
			env.eventually("GroupConflict event", func() bool { return env.hasEvent(EventGroupConflict, corev1.EventTypeWarning) })
			settleTicks()
			if !env.labelled("w1") {
				t.Fatal("labels changed on a conflict")
			}
			if groups := FamilyGroups(env.node("w1").Labels); len(groups) != 1 || groups[0] != testGroup {
				t.Fatalf("family groups on w1 = %v, want only %s", groups, testGroup)
			}
		})
	}
}

func TestHandLabelledNodeIsNotAdopted(t *testing.T) {
	admin := PrefixGroupKeyPrefix + "ns1.admin"
	env := newEnv(t, LabelKeysPrefix, false, realNode("w1", map[string]string{admin: "true"}))
	env.addPod("donor", "w1", testGroup)
	env.eventually("GroupConflict event", func() bool { return env.hasEvent(EventGroupConflict, corev1.EventTypeWarning) })
	env.deleteDonor()
	env.advance(2 * testTTL)
	node := env.node("w1")
	if node.Labels[admin] != "true" {
		t.Fatal("hand-set label removed")
	}
	if _, ok := node.Annotations[AnnotationLabelledBy]; ok {
		t.Fatal("hand-labelled node adopted")
	}
	if node.Labels[PrefixGroupKeyPrefix+testGroup] != "" {
		t.Fatal("hand-labelled node got a second group label")
	}
}

func TestEraEndWaitsForVirtualNode(t *testing.T) {
	env := newEnv(t, LabelKeysNS, false, realNode("w1", nil), virtualNode("vk-w1", "w1", "uid-w1"))
	env.addPod("donor", "w1", testGroup)
	env.addPod("guest", "vk-w1", testGroup) // pods on a virtual Node are not donors of a real node
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.deleteDonor()
	env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w1") })
	env.advance(testTTL + testStep)
	if !env.labelled("w1") {
		t.Fatal("labels removed while the host's virtual Node is registered")
	}
	if !env.clean("vk-w1") {
		t.Fatal("virtual Node labelled")
	}
	if err := env.cs.CoreV1().Nodes().Delete(context.Background(), "vk-w1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	env.eventually("w1 unlabelled after the virtual Node went", func() bool { return env.clean("w1") })
}

func TestEraEndGrace(t *testing.T) {
	env := newEnv(t, LabelKeysPrefix, false, realNode("w2", nil), virtualNode("vk-w2", "w2", "uid-w2"))
	env.addPod("donor", "w2", testGroup)
	env.eventually("w2 labelled", func() bool { return env.labelled("w2") })
	env.deleteDonor()
	env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w2") })
	env.advance(testTTL + testStep)
	if !env.labelled("w2") {
		t.Fatal("labels removed before the grace")
	}
	env.advance(testGrace + testStep)
	env.eventually("w2 unlabelled after the grace", func() bool { return env.clean("w2") })
}

func TestRestartResumesCountdown(t *testing.T) {
	env := newEnv(t, LabelKeysNS, false, realNode("w1", nil))
	env.addPod("donor", "w1", testGroup)
	env.eventually("w1 labelled", func() bool { return env.labelled("w1") })
	env.deleteDonor()
	env.eventually("countdown persisted", func() bool { return env.idleSinceSet("w1") })
	start := env.clk.Now()
	env.advance(testTTL / 2)

	// Restart: stop the controller and start a new one on the same cluster and clock.
	env.stop()
	env.stop = env.start(false)
	env.advance(start.Add(testTTL).Sub(env.clk.Now()) - 2*testStep)
	if !env.labelled("w1") {
		t.Fatal("restart shortened the countdown")
	}
	env.advance(3 * testStep)
	env.eventually("w1 unlabelled at the persisted expiry", func() bool { return env.clean("w1") })
}

// released reports whether the virtual Node is gone or no longer carries the VK finalizer.
func (env *testEnv) released(name string) bool {
	node, err := env.cs.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		env.t.Fatal(err)
	}
	return !slices.Contains(node.Finalizers, VirtualNodeFinalizer)
}

func TestReleaseDeadHosts(t *testing.T) {
	terminating := virtualNode("vk-term", "gone", "uid-gone")
	stamp := metav1.NewTime(time.Unix(1_700_000_000, 0))
	terminating.DeletionTimestamp = &stamp
	cases := []struct {
		name    string
		release bool
		objs    []runtime.Object
		want    bool
	}{
		{"host gone", true, []runtime.Object{virtualNode("vk", "gone", "uid-gone")}, true},
		{"host recreated", true, []runtime.Object{realNode("h1", nil), virtualNode("vk", "h1", "uid-old")}, true},
		{"host present", true, []runtime.Object{realNode("h1", nil), virtualNode("vk", "h1", "uid-h1")}, false},
		{"flag off", false, []runtime.Object{virtualNode("vk", "gone", "uid-gone")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv(t, LabelKeysNS, tc.release, tc.objs...)
			if tc.want {
				env.eventually("virtual Node released", func() bool { return env.released("vk") })
				return
			}
			env.advance(5 * testStep)
			if env.released("vk") {
				t.Fatal("virtual Node released")
			}
		})
	}
	t.Run("already terminating", func(t *testing.T) {
		env := newEnv(t, LabelKeysNS, true, terminating)
		env.eventually("finalizer removed", func() bool { return env.released("vk-term") })
	})
}

func TestReleaseRefusesRealNode(t *testing.T) {
	node := realNode("w1", nil)
	node.Finalizers = []string{VirtualNodeFinalizer}
	cs := fake.NewClientset(node)
	ctl, err := New(cs, nil, nil, &Config{LabelKeys: LabelKeysNS, EraTTL: testTTL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.releaseVirtualNode(context.Background(), node); err == nil {
		t.Fatal("released a Node without the virtual-node label")
	}
	got, err := cs.CoreV1().Nodes().Get(context.Background(), "w1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got.Finalizers, VirtualNodeFinalizer) {
		t.Fatal("finalizer removed from a real Node")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	cs := fake.NewClientset()
	for _, cfg := range []Config{
		{LabelKeys: "both", EraTTL: testTTL},
		{LabelKeys: LabelKeysNS},
		{LabelKeys: LabelKeysNS, EraTTL: testTTL, DonorSelector: "a in (b"},
		{LabelKeys: LabelKeysNS, EraTTL: testTTL, GroupFilter: "("},
	} {
		if _, err := New(cs, nil, nil, &cfg); err == nil {
			t.Errorf("New(%+v) accepted a bad config", cfg)
		}
	}
}
