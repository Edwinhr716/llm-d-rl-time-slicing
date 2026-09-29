package mirror

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

// This file is where the mirror's GPU claim comes from (decision D-NS-18, hook M3).
//
// ClaimModeStatic is today's behaviour: one claim name for every guest, from --gpu-claim.
// ClaimModeDonor finds the claim on the host instead: the donor pod (the trainer that holds the
// GPU) on the real node, in the guest's namespace, and the one ResourceClaim it uses. It is
// what a group spanning several hosts needs when the trainers come from one pod template (a
// RayJob worker group with a ResourceClaimTemplate): each trainer pod gets its own generated
// claim, so no single name fits every host, and the VK on host X must use the claim of the
// trainer on host X.

// ClaimMode selects where the mirror's GPU claim name comes from.
type ClaimMode string

const (
	// ClaimModeStatic uses Config.GPUClaim for every guest (today, the default).
	ClaimModeStatic ClaimMode = "static"
	// ClaimModeDonor uses the claim of the donor pod on the real node.
	ClaimModeDonor ClaimMode = "donor"
)

// DefaultDonorSelector matches the trainer pods of the demo (they carry timeslice.io/job-id)
// and leaves out mirrors, which the contract labels timeslice.io/role=background.
const DefaultDonorSelector = "timeslice.io/job-id,timeslice.io/role!=background"

// ParseClaimMode validates a --gpu-claim-mode value. Empty means static.
func ParseClaimMode(s string) (ClaimMode, error) {
	switch ClaimMode(s) {
	case "", ClaimModeStatic:
		return ClaimModeStatic, nil
	case ClaimModeDonor:
		return ClaimModeDonor, nil
	default:
		return "", fmt.Errorf("unknown GPU claim mode %q (want %s or %s)", s, ClaimModeStatic, ClaimModeDonor)
	}
}

// claimConfig returns the builder config for this guest: the static config, or, in donor mode
// and for a guest that asks for a GPU, the config with the donor's claim filled in.
func (b *Backend) claimConfig(ctx context.Context, guest *corev1.Pod) (Config, error) {
	cfg := b.opts.Config
	if b.opts.ClaimMode != ClaimModeDonor || !RequestsGPU(guest) {
		return cfg, nil
	}
	name, err := b.donorClaim(ctx, guest.Namespace)
	if err != nil {
		return cfg, err
	}
	cfg.GPUClaim = name
	return cfg, nil
}

// donorClaim lists the donor pods on the real node in namespace and returns the one claim they
// use. A shared claim can only be used from its own namespace, so the donor must be in the
// guest's namespace. It fails when there is no donor claim, when the claim of a template is not
// generated yet, or when the donors use more than one claim (the VK cannot tell which GPU is
// meant; the guest is retried and the error shows up in its events).
func (b *Backend) donorClaim(ctx context.Context, namespace string) (string, error) {
	sel := b.opts.DonorSelector
	if sel == nil {
		var err error
		if sel, err = labels.Parse(DefaultDonorSelector); err != nil {
			return "", err
		}
	}
	pods, err := b.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sel.String(),
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", b.opts.HostNode).String(),
	})
	if err != nil {
		return "", fmt.Errorf("list donor pods on %s: %w", b.opts.HostNode, err)
	}
	var names []string
	for i := range pods.Items {
		p := &pods.Items[i]
		if !isDonor(p, b.opts.HostNode, sel) {
			continue
		}
		for _, name := range podClaimNames(p) {
			if name == "" {
				return "", fmt.Errorf("donor %s/%s: claim from template not generated yet", p.Namespace, p.Name)
			}
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	switch len(names) {
	case 0:
		return "", fmt.Errorf("no donor pod with a ResourceClaim on %s in namespace %s (selector %q)", b.opts.HostNode, namespace, sel)
	case 1:
		return names[0], nil
	default:
		slices.Sort(names)
		return "", fmt.Errorf("donor pods on %s use more than one claim (%s); cannot pick one",
			b.opts.HostNode, strings.Join(names, ", "))
	}
}

// isDonor re-checks what the list asked for (a fake or a cache may ignore the field selector)
// and drops pods that are not running a GPU context: mirrors, finished pods, pods being deleted.
func isDonor(p *corev1.Pod, host string, sel labels.Selector) bool {
	if p.Spec.NodeName != host || !sel.Matches(labels.Set(p.Labels)) {
		return false
	}
	if _, mirror := p.Labels[LabelMirrorOf]; mirror {
		return false
	}
	if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return false
	}
	return true
}

// podClaimNames returns the ResourceClaim names a pod uses: the named claims as they are, and
// for claims from a template, the generated name from status (empty while not generated).
func podClaimNames(p *corev1.Pod) []string {
	var out []string
	for _, rc := range p.Spec.ResourceClaims {
		switch {
		case rc.ResourceClaimName != nil:
			out = append(out, *rc.ResourceClaimName)
		case rc.ResourceClaimTemplateName != nil:
			name := ""
			for _, st := range p.Status.ResourceClaimStatuses {
				if st.Name == rc.Name && st.ResourceClaimName != nil {
					name = *st.ResourceClaimName
				}
			}
			out = append(out, name)
		}
	}
	return out
}
