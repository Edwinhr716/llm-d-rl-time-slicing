package provider_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

func journalNode(annotations map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vk-x", Annotations: annotations}}
}

func TestNodeJournal_RoundTrip(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset(journalNode(map[string]string{"keep": "me"}))
	j := &provider.NodeJournal{Nodes: client.CoreV1().Nodes(), Name: "vk-x"}
	if rec, err := j.Load(ctx); rec != nil || err != nil {
		t.Fatalf("no journal yet: %+v, %v", rec, err)
	}
	want := &hostcmd.Record{
		Epoch: 7, Command: "COMMAND_VACATE", Phase: hostcmd.PhaseDone, Outcome: "OUTCOME_VACATED",
		Deadline: time.Now().Add(time.Minute).UTC().Truncate(time.Second),
		Guests:   []hostcmd.Guest{{Guest: "ns/g", Outcome: "OUTCOME_VACATED"}},
		Attempts: map[types.UID]int{"g-uid": 2},
	}
	if err := j.Save(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := j.Load(ctx)
	if err != nil || got == nil {
		t.Fatalf("load: %+v, %v", got, err)
	}
	if got.Epoch != 7 || got.Command != want.Command || got.Phase != want.Phase || got.Outcome != want.Outcome ||
		!got.Deadline.Equal(want.Deadline) || len(got.Guests) != 1 || got.Attempts["g-uid"] != 2 {
		t.Fatalf("round trip: got %+v, want %+v", got, want)
	}
	n, err := client.CoreV1().Nodes().Get(ctx, "vk-x", metav1.GetOptions{})
	if err != nil || n.Annotations["keep"] != "me" {
		t.Fatalf("the journal write must keep the other annotations: %v, %v", n, err)
	}
}

func TestNodeJournal_MissingAndUnreadable(t *testing.T) {
	ctx := context.Background()
	gone := &provider.NodeJournal{Nodes: fake.NewClientset().CoreV1().Nodes(), Name: "vk-x"}
	if rec, err := gone.Load(ctx); rec != nil || err != nil {
		t.Fatalf("no Node: %+v, %v", rec, err)
	}
	bad := &provider.NodeJournal{
		Nodes: fake.NewClientset(journalNode(map[string]string{provider.AnnotationHostCommand: "{"})).CoreV1().Nodes(),
		Name:  "vk-x",
	}
	if rec, err := bad.Load(ctx); rec != nil || err == nil {
		t.Fatalf("an unreadable journal is an error: %+v, %v", rec, err)
	}
}

type recoverFake struct {
	mu         sync.Mutex
	mirrors    map[string]*corev1.Pod // by guest name
	mirrorErr  error
	attempts   map[types.UID]int
	attemptsOf []string
	restored   *hostcmd.Record
	restoreFor []string
	restores   int
	reconciled int
	killsFor   []string
	killBudget time.Duration
	killSet    bool
	killsLate  bool // FinishKills ran after Restore
}

func (f *recoverFake) MirrorNow(_ context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error) {
	if f.mirrorErr != nil {
		return nil, false, f.mirrorErr
	}
	m, ok := f.mirrors[guest.Name]
	return m, ok, nil
}

func (f *recoverFake) RestoreAttempts(guests []mirror.Guest, saved map[types.UID]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = saved
	for _, g := range guests {
		f.attemptsOf = append(f.attemptsOf, g.Pod.Name)
	}
}

func (f *recoverFake) ReconcileSuspend(context.Context) ([]mirror.Reconciled, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconciled++
	return nil, nil
}

func (f *recoverFake) FinishKills(
	_ context.Context, guests []mirror.Guest, kill mirror.KillFunc, budget time.Duration,
) ([]mirror.FinishedKill, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killBudget, f.killSet = budget, kill != nil
	var out []mirror.FinishedKill
	for _, g := range guests {
		f.killsFor = append(f.killsFor, g.Pod.Name)
		if g.Mirror != nil && g.Mirror.Annotations[mirror.AnnotationKilling] != "" {
			cause := g.Mirror.Annotations[mirror.AnnotationKilling]
			out = append(out, mirror.FinishedKill{Mirror: g.Mirror.Name, Cause: cause})
		}
	}
	f.killsLate = f.restores > 0
	return out, nil
}

func (f *recoverFake) Restore(_ context.Context, rec *hostcmd.Record, guests []mirror.Guest) hostcmd.Restored {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores++
	f.restored = rec
	for _, g := range guests {
		name := g.Pod.Name
		if g.Mirror != nil {
			name += "+mirror"
		}
		f.restoreFor = append(f.restoreFor, name)
	}
	return hostcmd.Restored{}
}

func podOn(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name + "-uid")},
		Spec:       corev1.PodSpec{NodeName: "vk-x"},
	}
}

// recoverClient serves two pods bound to the virtual node, one of them a guest, and checks that
// Recover lists only that node's pods.
func recoverClient(t *testing.T, node *corev1.Node) *fake.Clientset {
	t.Helper()
	objs := []runtime.Object{podOn("guest"), podOn("daemon")}
	if node != nil {
		objs = append(objs, node)
	}
	client := fake.NewClientset(objs...)
	client.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if la, ok := a.(k8stesting.ListAction); ok {
			if got := la.GetListRestrictions().Fields.String(); got != "spec.nodeName=vk-x" {
				t.Errorf("relist field selector %q", got)
			}
		}
		return false, nil, nil
	})
	return client
}

func onlyGuest(p *corev1.Pod) bool { return p.Name == "guest" }

func TestRecover_HostCommand_RestoresFromJournal(t *testing.T) {
	ctx := context.Background()
	rec := &hostcmd.Record{
		Epoch: 4, Command: "COMMAND_RESUME", Phase: hostcmd.PhaseRunning,
		Attempts: map[types.UID]int{"guest-uid": 3},
	}
	client := recoverClient(t, journalNode(nil))
	j := &provider.NodeJournal{Nodes: client.CoreV1().Nodes(), Name: "vk-x"}
	if err := j.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	f := &recoverFake{mirrors: map[string]*corev1.Pod{"guest": {ObjectMeta: metav1.ObjectMeta{Name: "guest-m"}}}}
	out, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(""), NodeName: "vk-x", Host: f, Server: f, Journal: j, IsGuest: onlyGuest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Guests != 1 || out.Mirrors != 1 || out.Journal != "COMMAND_RESUME epoch 4 running" || out.Restored == nil {
		t.Fatalf("recovery %+v", out)
	}
	if f.restores != 1 || f.restored == nil || f.restored.Epoch != 4 || len(f.restoreFor) != 1 || f.restoreFor[0] != "guest+mirror" {
		t.Fatalf("restore: %d calls, record %+v, guests %v", f.restores, f.restored, f.restoreFor)
	}
	if f.attempts["guest-uid"] != 3 || len(f.attemptsOf) != 1 {
		t.Fatalf("attempts restored %v for %v", f.attempts, f.attemptsOf)
	}
	if f.reconciled != 0 {
		t.Fatal("host-command mode must not reconcile with a local freezer")
	}
}

// An unreadable journal is not fatal: the server restores with none (fail closed).
func TestRecover_HostCommand_UnreadableJournalFailsClosed(t *testing.T) {
	ctx := context.Background()
	client := recoverClient(t, journalNode(map[string]string{provider.AnnotationHostCommand: "not json"}))
	f := &recoverFake{}
	out, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(""), NodeName: "vk-x", Host: f, Server: f, IsGuest: onlyGuest,
		Journal: &provider.NodeJournal{Nodes: client.CoreV1().Nodes(), Name: "vk-x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Journal != "unreadable" || f.restores != 1 || f.restored != nil || f.attempts != nil {
		t.Fatalf("recovery %+v; restores %d with %+v, attempts %v", out, f.restores, f.restored, f.attempts)
	}
}

func TestRecover_M3_Reconciles(t *testing.T) {
	ctx := context.Background()
	client := recoverClient(t, nil)
	f := &recoverFake{}
	out, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(""), NodeName: "vk-x", Host: f, IsGuest: onlyGuest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.reconciled != 1 || f.restores != 0 || out.Journal != "none" || out.Mirrors != 0 {
		t.Fatalf("recovery %+v; reconciled %d, restores %d", out, f.reconciled, f.restores)
	}
}

// A guest whose mirror cannot be read stops the start: running half blind could act twice.
func TestRecover_MirrorReadErrorIsFatal(t *testing.T) {
	ctx := context.Background()
	client := recoverClient(t, journalNode(nil))
	f := &recoverFake{mirrorErr: errors.New("apiserver down")}
	if _, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(""), NodeName: "vk-x", Host: f, Server: f, IsGuest: onlyGuest,
	}); err == nil {
		t.Fatal("want an error")
	}
	if f.restores != 0 {
		t.Fatal("nothing may be restored from a partial relist")
	}
}

// A kill sequence cut short by the restart (M4's timeslice.io/killing mark) is finished during
// the relist, before the host command state is restored, with the configured agent Kill.
func TestRecover_FinishesInterruptedKill(t *testing.T) {
	ctx := context.Background()
	client := recoverClient(t, journalNode(nil))
	marked := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "guest-m", Annotations: map[string]string{mirror.AnnotationKilling: "agent Resume: DEADLINE_EXCEEDED"},
	}}
	f := &recoverFake{mirrors: map[string]*corev1.Pod{"guest": marked}}
	out, err := provider.Recover(ctx, &provider.RecoverConfig{
		Pods: client.CoreV1().Pods(""), NodeName: "vk-x", Host: f, Server: f, IsGuest: onlyGuest,
		Journal:    &provider.NodeJournal{Nodes: client.CoreV1().Nodes(), Name: "vk-x"},
		Kill:       func(context.Context, string, string) error { return nil },
		KillBudget: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Killed) != 1 || out.Killed[0].Cause != "agent Resume: DEADLINE_EXCEEDED" {
		t.Fatalf("recovery killed %+v", out.Killed)
	}
	if len(f.killsFor) != 1 || f.killsFor[0] != "guest" || !f.killSet || f.killBudget != 3*time.Second {
		t.Fatalf("FinishKills saw guests %v, kill set %t, budget %s", f.killsFor, f.killSet, f.killBudget)
	}
	if f.restores != 1 || f.killsLate {
		t.Fatalf("restores %d; kills finished after the restore: %t", f.restores, f.killsLate)
	}
}
