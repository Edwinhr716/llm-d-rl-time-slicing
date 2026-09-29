package provider

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/freeze"
)

type adoptCall struct {
	attempt  int
	released bool
	reason   string
}

// fakeRecoverHost is an in-memory RecoverHost.
type fakeRecoverHost struct {
	mirrors  []*corev1.Pod
	guests   map[string]*corev1.Pod // by guest name
	recorded map[types.UID]string
	adopted  map[types.UID]adoptCall
}

func newFakeRecoverHost() *fakeRecoverHost {
	return &fakeRecoverHost{guests: map[string]*corev1.Pod{}, recorded: map[types.UID]string{}, adopted: map[types.UID]adoptCall{}}
}

func (host *fakeRecoverHost) ListMirrors(context.Context) ([]*corev1.Pod, error) {
	return host.mirrors, nil
}

func (host *fakeRecoverHost) GuestNow(_ context.Context, m *corev1.Pod) (*corev1.Pod, error) {
	return host.guests[m.Annotations[mirror.AnnotationGuestName]], nil
}

func (host *fakeRecoverHost) RecordSuspendState(_ context.Context, guest *corev1.Pod, state string) (*corev1.Pod, error) {
	host.recorded[guest.UID] = state
	return guest, nil
}

func (host *fakeRecoverHost) Adopt(guest types.UID, attempt int, released bool, reason string) {
	host.adopted[guest] = adoptCall{attempt: attempt, released: released, reason: reason}
}

// add creates a guest and its running mirror with the given recorded state and attempt.
func (host *fakeRecoverHost) add(name, state, attempt string, ready bool) addedPods {
	uid := types.UID(name + "-uid")
	guest := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: uid}}
	if ready {
		guest.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	mp := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        mirror.Name(name),
			UID:         types.UID(name + "-m-uid"),
			Labels:      map[string]string{mirror.LabelMirrorOf: string(uid), mirror.LabelJobID: string(uid) + "-" + attempt},
			Annotations: map[string]string{mirror.AnnotationGuestName: name},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if state != "" {
		mp.Annotations[mirror.AnnotationSuspendState] = state
	}
	host.guests[name] = guest
	host.mirrors = append(host.mirrors, mp)
	return addedPods{guest: guest, mp: mp}
}

// frozenSet is a host fact: which mirrors are frozen, and which have no cgroup.
func frozenSet(frozen map[string]bool, noCgroup ...string) func(*corev1.Pod) (bool, error) {
	return func(m *corev1.Pod) (bool, error) {
		for _, n := range noCgroup {
			if m.Name == n {
				return false, freeze.ErrNoCgroup
			}
		}
		return frozen[m.Name], nil
	}
}

func TestRecover_HostWinsOverTheAnnotation(t *testing.T) {
	cases := []struct {
		name         string
		recorded     string
		frozen       bool
		wantState    string
		wantRecord   bool
		wantSuspend  bool
		wantReleased bool
		wantReason   string
	}{
		{
			name: "suspended-frozen", recorded: mirror.StateSuspended, frozen: true,
			wantState: mirror.StateSuspended, wantSuspend: true, wantReason: mirror.ReasonSuspended,
		},
		{
			name: "suspending-frozen", recorded: mirror.StateSuspending, frozen: true,
			wantState: mirror.StateSuspended, wantRecord: true, wantSuspend: true, wantReason: mirror.ReasonSuspended,
		},
		{name: "suspending-thawed", recorded: mirror.StateSuspending, wantRecord: true},
		{name: "resuming-thawed", recorded: mirror.StateResuming, wantRecord: true},
		{
			name: "resuming-frozen", recorded: mirror.StateResuming, frozen: true,
			wantState: mirror.StateSuspended, wantRecord: true, wantSuspend: true, wantReason: mirror.ReasonSuspended,
		},
		{name: "suspended-thawed", recorded: mirror.StateSuspended, wantRecord: true},
		{name: "running-thawed", wantReleased: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := newFakeRecoverHost()
			added := host.add("guest", tc.recorded, "2", true)
			guest, m := added.guest, added.mp
			var seen []Adopted
			got, err := Recover(context.Background(), host, RecoverOptions{
				Frozen:      frozenSet(map[string]bool{m.Name: tc.frozen}),
				RecordState: true,
				OnAdopt:     func(a Adopted) { seen = append(seen, a) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || len(seen) != 1 {
				t.Fatalf("want one adopted guest, got %d (OnAdopt %d)", len(got), len(seen))
			}
			a := got[0]
			if a.State != tc.wantState || a.Suspended != tc.wantSuspend || a.Released != tc.wantReleased || a.Attempt != 2 {
				t.Errorf("adopted %+v", a)
			}
			state, recorded := host.recorded[guest.UID]
			if recorded != tc.wantRecord || (recorded && state != tc.wantState) {
				t.Errorf("recorded %q (%t), want %q (%t)", state, recorded, tc.wantState, tc.wantRecord)
			}
			if (a.Mismatch != "") != tc.wantRecord {
				t.Errorf("mismatch %q", a.Mismatch)
			}
			want := adoptCall{attempt: 2, released: tc.wantReleased, reason: tc.wantReason}
			if host.adopted[guest.UID] != want {
				t.Errorf("Adopt got %+v, want %+v", host.adopted[guest.UID], want)
			}
		})
	}
}

func TestRecover_NoHostFactTrustsTheAnnotation(t *testing.T) {
	host := newFakeRecoverHost()
	guest := host.add("guest", mirror.StateSuspended, "0", false).guest
	got, err := Recover(context.Background(), host, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Suspended || len(host.recorded) != 0 {
		t.Fatalf("got %+v, recorded %v", got, host.recorded)
	}
	if host.adopted[guest.UID].reason != mirror.ReasonSuspended {
		t.Errorf("Adopt %+v", host.adopted[guest.UID])
	}
}

func TestRecover_UnreadableHostTrustsTheAnnotation(t *testing.T) {
	host := newFakeRecoverHost()
	host.add("guest", mirror.StateSuspended, "0", false)
	got, err := Recover(context.Background(), host, RecoverOptions{
		Frozen:      func(*corev1.Pod) (bool, error) { return false, errors.New("permission denied") },
		RecordState: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Suspended || len(host.recorded) != 0 {
		t.Fatalf("got %+v, recorded %v", got, host.recorded)
	}
}

func TestRecover_NoCgroupMeansNotFrozen(t *testing.T) {
	host := newFakeRecoverHost()
	added := host.add("guest", mirror.StateSuspended, "1", false)
	guest, m := added.guest, added.mp
	got, err := Recover(context.Background(), host, RecoverOptions{Frozen: frozenSet(nil, m.Name), RecordState: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Suspended || got[0].Released {
		t.Fatalf("got %+v", got)
	}
	if s, ok := host.recorded[guest.UID]; !ok || s != "" {
		t.Errorf("want Suspended cleared, recorded %q (%t)", s, ok)
	}
}

func TestRecover_RecordStateOffLeavesTheAnnotation(t *testing.T) {
	host := newFakeRecoverHost()
	m := host.add("guest", "", "0", false).mp
	got, err := Recover(context.Background(), host, RecoverOptions{Frozen: frozenSet(map[string]bool{m.Name: true})})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Suspended || len(host.recorded) != 0 {
		t.Fatalf("got %+v, recorded %v", got, host.recorded)
	}
}

func TestRecover_SkipsOrphansAndTerminalMirrors(t *testing.T) {
	host := newFakeRecoverHost()
	host.add("live", "", "0", true)
	host.add("orphan", "", "0", true)
	delete(host.guests, "orphan")
	done := host.add("done", "", "0", false).mp
	done.Status.Phase = corev1.PodFailed
	deleting := host.add("deleting", "", "0", false).mp
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	got, err := Recover(context.Background(), host, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Guest.Name != "live" || len(host.adopted) != 1 {
		t.Fatalf("want only the live guest adopted, got %+v", got)
	}
}

func TestRecover_NotReadyGuestIsNotReleased(t *testing.T) {
	host := newFakeRecoverHost()
	guest := host.add("guest", "", "4", false).guest
	if _, err := Recover(context.Background(), host, RecoverOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := host.adopted[guest.UID]; got.released || got.attempt != 4 {
		t.Errorf("Adopt %+v", got)
	}
}

// addedPods is a guest and its mirror, as add made them.
type addedPods struct {
	guest, mp *corev1.Pod
}
