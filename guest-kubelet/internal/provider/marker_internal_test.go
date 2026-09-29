package provider

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Tests for pending lead decision D-VK-4 (--guest-marker): options toleration (the default),
// label and both.

// evalGuestMatcher is evaluation hook H6a for D-VK-4: the predicate the provider and
// GuestOnlyRecorder use with --guest-marker=marker. An unknown marker is an error.
func evalGuestMatcher(marker string) (func(*corev1.Pod) bool, error) {
	match, err := newGuestMatcher(marker)
	if err != nil {
		return nil, err
	}
	return func(pod *corev1.Pod) bool { return match(pod) != "" }, nil
}

// evalMirrorLabels is evaluation hook H6b for D-VK-4: the labels of the mirror the builder makes
// for guest, with the mirror config of deploy/deployment.yaml and main.go.
func evalMirrorLabels(guest *corev1.Pod) (map[string]string, error) {
	m, err := mirror.Build(guest, mirror.Config{
		HostNode: "host", VirtualNode: "vk-test", GPUClaim: "claim", GuestTaintKey: GuestTaintKey, OwnerRef: true,
		CPUHeadroom: resource.MustParse("1"), MemoryHeadroom: resource.MustParse("4Gi"),
	})
	if err != nil {
		return nil, err
	}
	return m.Labels, nil
}

// withMarker sets the process-wide marker for one test and restores the default afterwards.
func withMarker(t *testing.T, marker string) {
	t.Helper()
	if err := SetGuestMarker(marker); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := SetGuestMarker(GuestMarkerToleration); err != nil {
			t.Error(err)
		}
	})
}

func labelled(pod *corev1.Pod, value string) *corev1.Pod {
	pod.Labels = map[string]string{"app": "x", GuestPodLabel: value}
	return pod
}

// labelOnlyPod carries the guest label and no guest toleration.
func labelOnlyPod(name string) *corev1.Pod {
	pod := labelled(guestPod(name), "true")
	pod.Spec.Tolerations = nil
	return pod
}

type markerCase struct {
	name                    string
	pod                     *corev1.Pod
	toleration, label, both bool // the verdict of each option, from the option texts
}

func markerCases() []markerCase {
	equal := guestPod("equal")
	equal.Spec.Tolerations = []corev1.Toleration{{Key: GuestTaintKey, Operator: corev1.TolerationOpEqual, Value: "true"}}
	return []markerCase{
		{"today's guest (toleration)", guestPod("g"), true, false, true},
		{"today's guest (Equal toleration)", equal, true, false, true},
		{"north-star guest (label and toleration)", labelled(guestPod("ns"), "true"), true, true, true},
		{"label only", labelOnlyPod("l"), false, true, true},
		{"label false", labelled(guestPod("f"), "false"), true, false, true},
		{"label True (not exactly true)", labelled(guestPod("T"), "True"), true, false, true},
		{"DaemonSet (tolerates everything)", dsPod("ds"), false, false, false},
		{"plain pod", labelled(dsPod("p"), "no"), false, false, false},
	}
}

func testMatcher(t *testing.T, marker string, want func(markerCase) bool) {
	t.Helper()
	isGuestPod, err := evalGuestMatcher(marker)
	if err != nil {
		t.Fatal(err)
	}
	withMarker(t, marker)
	for _, tc := range markerCases() {
		if got := isGuestPod(tc.pod); got != want(tc) {
			t.Errorf("%s: %s: got guest=%v, want %v", marker, tc.name, got, want(tc))
		}
		if got := isGuest(tc.pod); got != want(tc) {
			t.Errorf("%s: %s: the provider's predicate says guest=%v, want %v", marker, tc.name, got, want(tc))
		}
	}
}

func TestGuestMarker_Parse(t *testing.T) {
	if GuestMarker() != GuestMarkerToleration {
		t.Errorf("default marker %q, want toleration", GuestMarker())
	}
	for _, marker := range []string{GuestMarkerToleration, GuestMarkerLabel, GuestMarkerBoth} {
		if _, err := evalGuestMatcher(marker); err != nil {
			t.Errorf("%s: %v", marker, err)
		}
	}
	for _, bad := range []string{"", "bogus", "Label", "either"} {
		if _, err := evalGuestMatcher(bad); err == nil {
			t.Errorf("marker %q must be rejected", bad)
		}
	}
	withMarker(t, GuestMarkerLabel)
	if err := SetGuestMarker("bogus"); err == nil || GuestMarker() != GuestMarkerLabel {
		t.Errorf("a bad marker must fail and leave the marker as it was: err=%v marker=%q", err, GuestMarker())
	}
}

func TestGuestMarker_Toleration_Matcher(t *testing.T) {
	testMatcher(t, GuestMarkerToleration, func(c markerCase) bool { return c.toleration })
}

func TestGuestMarker_Label_Matcher(t *testing.T) {
	testMatcher(t, GuestMarkerLabel, func(c markerCase) bool { return c.label })
}

func TestGuestMarker_Both_Matcher(t *testing.T) {
	testMatcher(t, GuestMarkerBoth, func(c markerCase) bool { return c.both })
}

// testProviderAndRecorder checks that CreatePod, DeletePod, NotifyPods and GuestOnlyRecorder all
// follow the active marker, for a toleration-only guest, a label-only guest and a DaemonSet pod.
func testProviderAndRecorder(t *testing.T, marker string, wantTol, wantLabel bool) {
	t.Helper()
	withMarker(t, marker)
	tolPod, labelPod, ds := guestPod("tol"), labelOnlyPod("label"), dsPod("ds")
	var want []string
	if wantTol {
		want = append(want, "tol")
	}
	if wantLabel {
		want = append(want, "label")
	}

	backend := &fakeBackend{}
	prov := New(backend)
	ctx := context.Background()
	var notified []string
	prov.NotifyPods(ctx, func(pod *corev1.Pod) { notified = append(notified, pod.Name) })
	fake := record.NewFakeRecorder(10)
	rec := GuestOnlyRecorder{EventRecorder: fake}
	for _, pod := range []*corev1.Pod{tolPod, labelPod, ds} {
		if err := prov.CreatePod(ctx, pod); err != nil {
			t.Fatal(err)
		}
		backend.cb(pod)
		rec.Event(pod, corev1.EventTypeNormal, "ProviderCreateSuccess", "x")
		err := prov.DeletePod(ctx, pod)
		if isGuestPod := isGuest(pod); isGuestPod == errdefs.IsNotFound(err) {
			t.Errorf("%s: DeletePod of %s: guest=%v err=%v", marker, pod.Name, isGuestPod, err)
		}
	}
	for what, got := range map[string][]string{"created": backend.created, "deleted": backend.deleted, "notified": notified} {
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: %s %v, want %v", marker, what, got, want)
		}
	}
	if len(fake.Events) != len(want) {
		t.Errorf("%s: %d events recorded, want %d (guests only)", marker, len(fake.Events), len(want))
	}
}

func TestGuestMarker_Toleration_ProviderAndRecorder(t *testing.T) {
	testProviderAndRecorder(t, GuestMarkerToleration, true, false)
}

func TestGuestMarker_Label_ProviderAndRecorder(t *testing.T) {
	testProviderAndRecorder(t, GuestMarkerLabel, false, true)
}

func TestGuestMarker_Both_ProviderAndRecorder(t *testing.T) {
	testProviderAndRecorder(t, GuestMarkerBoth, true, true)
}

// The verdict lines are logged at Info once per pod UID, whatever the marker.
func testVerdictLogs(t *testing.T, marker string, guest, other *corev1.Pod, wantBy string) {
	t.Helper()
	withMarker(t, marker)
	var buf bytes.Buffer
	ctx := log.WithLogger(context.Background(), vkslog.FromSlog(slog.New(slog.NewJSONHandler(&buf, nil))))
	guest.UID, other.UID = types.UID("uid-guest"), types.UID("uid-other")
	prov := New(&fakeBackend{})
	for range 3 {
		for _, pod := range []*corev1.Pod{guest, other} {
			if err := prov.CreatePod(ctx, pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	out := buf.String()
	accepted := `"msg":"guest accepted","pod":"ns/` + guest.Name + `","uid":"uid-guest","by":"` + wantBy + `"`
	ignored := `"msg":"ignoring non-guest pod","pod":"ns/` + other.Name + `","uid":"uid-other","marker":"` + marker + `"`
	if strings.Count(out, `"level":"INFO"`) != 2 || strings.Count(out, accepted) != 1 || strings.Count(out, ignored) != 1 {
		t.Errorf("%s: want one Info line each:\n%s\n%s\ngot:\n%s", marker, accepted, ignored, out)
	}
}

func TestGuestMarker_Toleration_Logs(t *testing.T) {
	testVerdictLogs(t, GuestMarkerToleration, guestPod("g"), labelOnlyPod("l"), "toleration")
}

func TestGuestMarker_Label_Logs(t *testing.T) {
	testVerdictLogs(t, GuestMarkerLabel, labelOnlyPod("l"), guestPod("g"), "label")
}

func TestGuestMarker_Both_Logs(t *testing.T) {
	testVerdictLogs(t, GuestMarkerBoth, labelled(guestPod("ns"), "true"), dsPod("ds"), "toleration,label")
}

// H7: in every option the mirror carries no guest label and no user label.
func TestGuestMarker_MirrorHasNoGuestLabel(t *testing.T) {
	guest := labelled(guestPod("ns"), "true")
	guest.UID = types.UID("uid-ns")
	labels, err := evalMirrorLabels(guest)
	if err != nil {
		t.Fatal(err)
	}
	for _, leaked := range []string{GuestPodLabel, "app"} {
		if _, ok := labels[leaked]; ok {
			t.Errorf("the mirror carries label %s: %v", leaked, labels)
		}
	}
	if labels[mirror.LabelMirrorOf] != "uid-ns" {
		t.Errorf("mirror labels %v", labels)
	}
}
