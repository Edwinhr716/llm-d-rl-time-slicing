package era

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Label key forms for the host's group (decision D-NS-1 vocabulary).
const (
	// LabelKeysPrefix: group.timeslice.io/<group>=true (today's demo manifests).
	LabelKeysPrefix = "prefix"
	// LabelKeysNS: timeslice.io/donor=true plus timeslice.io/group=<group> (north star).
	LabelKeysNS = "ns"

	groupPrefix  = "group.timeslice.io/"
	nsDonorLabel = "timeslice.io/donor"
	nsGroupLabel = "timeslice.io/group"
)

// GroupFromHost returns the one group the host carries in the given key form, or "" when it
// carries none or more than one. It stands in for the VK-A6 loop's O3 lookup (D-VK-3), which
// replaces it when this code moves onto that loop.
func GroupFromHost(host *corev1.Node, keys string) string {
	if host == nil {
		return ""
	}
	if keys == LabelKeysNS {
		if host.Labels[nsDonorLabel] != "true" {
			return ""
		}
		return host.Labels[nsGroupLabel]
	}
	group := ""
	for k, v := range host.Labels {
		if !strings.HasPrefix(k, groupPrefix) || v != "true" {
			continue
		}
		if group != "" {
			return ""
		}
		group = strings.TrimPrefix(k, groupPrefix)
	}
	return group
}

// hostCarries reports whether the host still carries group (the donor controller has not ended
// the era by removing it).
func hostCarries(host *corev1.Node, keys, group string) bool {
	if host == nil || group == "" {
		return false
	}
	if keys == LabelKeysNS {
		return host.Labels[nsDonorLabel] == "true" && host.Labels[nsGroupLabel] == group
	}
	return host.Labels[groupPrefix+group] == "true"
}

// isDonor reports whether a pod on the host counts as a donor pod: it matches the selector, is
// not a mirror and has not finished. A pod being deleted still counts until it is gone.
func (c *Controller) isDonor(p *corev1.Pod) bool {
	if p.Spec.NodeName != c.cfg.Host {
		return false
	}
	if _, isMirror := p.Labels[mirror.LabelMirrorNode]; isMirror {
		return false
	}
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return false
	}
	return c.selector.Matches(labels.Set(p.Labels))
}
