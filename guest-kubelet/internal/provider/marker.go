package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Pending lead decision D-VK-4 (--guest-marker): what marks a pod bound to the virtual Node as
// a guest, so that it gets a mirror, status and events. Each option is one matcher below.
const (
	// GuestMarkerToleration (default, today): the pod tolerates the guest taint by key.
	GuestMarkerToleration = "toleration"
	// GuestMarkerLabel: the pod carries GuestPodLabel with the value exactly "true".
	GuestMarkerLabel = "label"
	// GuestMarkerBoth: either one.
	GuestMarkerBoth = "both"

	// GuestPodLabel is the guest label on a pod. Same key as the taint.
	GuestPodLabel = GuestTaintKey
)

// A guestMatcher returns which marker makes the pod a guest ("toleration", "label", or
// "toleration,label"), or "" if the pod is not a guest.
type guestMatcher func(*corev1.Pod) string

func hasGuestLabel(pod *corev1.Pod) bool { return pod.Labels[GuestPodLabel] == "true" }

func matchToleration(pod *corev1.Pod) string {
	if IsGuest(pod) {
		return GuestMarkerToleration
	}
	return ""
}

func matchLabel(pod *corev1.Pod) string {
	if hasGuestLabel(pod) {
		return GuestMarkerLabel
	}
	return ""
}

func matchBoth(pod *corev1.Pod) string {
	tol, label := matchToleration(pod), matchLabel(pod)
	switch {
	case tol != "" && label != "":
		return tol + "," + label
	case tol != "":
		return tol
	default:
		return label
	}
}

func newGuestMatcher(marker string) (guestMatcher, error) {
	switch marker {
	case GuestMarkerToleration:
		return matchToleration, nil
	case GuestMarkerLabel:
		return matchLabel, nil
	case GuestMarkerBoth:
		return matchBoth, nil
	default:
		return nil, fmt.Errorf("unknown guest marker %q: want %s, %s or %s",
			marker, GuestMarkerToleration, GuestMarkerLabel, GuestMarkerBoth)
	}
}

// The marker is process-wide: the provider and GuestOnlyRecorder must agree on it, and both are
// built in main. It is set once, while flags are parsed, before any pod is seen.
var (
	activeMarker = GuestMarkerToleration
	matchGuest   = guestMatcher(matchToleration)
)

// SetGuestMarker selects the guest marker (--guest-marker). An unknown value is an error and
// leaves the marker unchanged. Call it before the provider starts.
func SetGuestMarker(marker string) error {
	m, err := newGuestMatcher(marker)
	if err != nil {
		return err
	}
	activeMarker, matchGuest = marker, m
	return nil
}

// GuestMarker returns the active guest marker.
func GuestMarker() string { return activeMarker }

// isGuest is the one guest predicate of the provider and GuestOnlyRecorder.
func isGuest(pod *corev1.Pod) bool { return matchGuest(pod) != "" }
