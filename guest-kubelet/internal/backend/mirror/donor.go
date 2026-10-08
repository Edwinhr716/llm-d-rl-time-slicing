package mirror

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// DefaultGPUDonorSelector selects donor pods (typically the RL trainer) on the host.
const DefaultGPUDonorSelector = "timeslice.io/donor=true"

// Event reasons on the guest.
const (
	EventGPUUnavailable = "GPUUnavailable"
	EventDonorGone      = "DonorGone"
)

// refusalError is a reason the mirror cannot get a GPU. The create fails (no mirror: fail closed)
// and the library retries it.
//
// The library writes Error() into the guest's status.message
// (ProviderFailed) and a ProviderCreateFailed event, and refused() records an event on the
// guest. The guest's owner can read all three, and the reason names the donor pod, the donor
// selector and other tenants' mirrors. So Error() carries only public; reason goes to the VK
// log. public is set only for reasons about the guest itself (guestRefusal).
type refusalError struct{ reason, public string }

// PublicRefusal is what a guest sees when no GPU can be lent to its mirror.
const PublicRefusal = "no GPU is free for this guest on its node right now; it stays pending and is retried"

func (r *refusalError) Error() string {
	if r.public != "" {
		return "gpu attach refused: " + r.public
	}
	return "gpu attach refused: " + PublicRefusal
}

// guestRefusal is a refusal whose reason is about the guest's own spec, safe to show it.
func guestRefusal(err error) error {
	return &refusalError{reason: err.Error(), public: err.Error()}
}

func refuse(format string, args ...any) error {
	return &refusalError{reason: fmt.Sprintf(format, args...)}
}

func podTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// liveDonors returns the donor pods on the host that still hold their GPU, oldest first.
func (b *Backend) liveDonors() ([]*corev1.Pod, error) {
	all, err := b.donors.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(all))
	for _, p := range all {
		if _, isMirror := p.Labels[LabelMirrorOf]; isMirror {
			continue
		}
		if p.DeletionTimestamp == nil && !podTerminal(p) && p.Spec.NodeName == b.opts.HostNode {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i].CreationTimestamp, out[j].CreationTimestamp
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		return out[i].Namespace+"/"+out[i].Name < out[j].Namespace+"/"+out[j].Name
	})
	return out, nil
}

// HasDonor reports whether a donor pod on the host still holds its GPU, and why (for logs and
// events). While the donor informer is not set up or not synced it answers true, so an empty
// cache never looks like a departed donor. It feeds the no-donor cordon (--cordon-without-donor).
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func (b *Backend) HasDonor() (bool, string) {
	if b.donors == nil || b.donorsSynced == nil || !b.donorsSynced() {
		return true, "donor cache not synced"
	}
	donors, err := b.liveDonors()
	if err != nil {
		return true, "donor list failed: " + err.Error()
	}
	if len(donors) == 0 {
		return false, "no donor pod on the host"
	}
	return true, fmt.Sprintf("%d donor pod(s) on the host", len(donors))
}

// BoundPods lists every pod bound to the virtual Node (guests and anything else the scheduler
// put there), from the library's informer.
func (b *Backend) BoundPods() ([]*corev1.Pod, error) {
	return b.guests.List(labels.Everything())
}

// assignmentTTL is how long a just-created mirror's GPUs count on their own; after it, the
// informer decides.
const assignmentTTL = 30 * time.Second

func (b *Backend) refused(ctx context.Context, guest *corev1.Pod, err error) error {
	reason, public := err.Error(), PublicRefusal
	var r *refusalError
	if errors.As(err, &r) {
		reason = r.reason
		if r.public != "" {
			public = r.public
		}
	} else {
		err = &refusalError{reason: reason} // never hand an unredacted error to the library
	}
	log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("reason", reason).Warn("gpu attach refused")
	if b.opts.Recorder != nil {
		b.opts.Recorder.Event(guest, corev1.EventTypeWarning, EventGPUUnavailable, public)
	}
	return err
}
