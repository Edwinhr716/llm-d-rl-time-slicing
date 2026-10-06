package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// DevicePluginDonorSelector selects donor pods (the RL trainer in the north star) on the host.
const DevicePluginDonorSelector = "timeslice.io/donor=true"

// Event reasons on the guest (deviceplugin mode).
const (
	EventGPUUnavailable = "GPUUnavailable"
	EventDonorGone      = "DonorGone"
)

// HoldersSource returns which pod holds which GPU on the host (the shadow plugin's endpoint).
type HoldersSource interface {
	Holders(ctx context.Context) (*api.Holders, error)
}

// HTTPHolders reads the Holders document from the shadow plugin on the host loopback. The
// guest kubelet runs with hostNetwork, like the plugin.
type HTTPHolders struct {
	URL    string
	Client *http.Client
}

// Holders implements HoldersSource.
func (h HTTPHolders) Holders(ctx context.Context) (*api.Holders, error) {
	c := h.Client
	if c == nil {
		c = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.G(ctx).WithError(err).Debug("holders response close")
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", h.URL, resp.StatusCode)
	}
	var out api.Holders
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", h.URL, err)
	}
	return &out, nil
}

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

// assignment is a GPU given to a mirror that the informer may not show yet.
type assignment struct {
	guest  types.UID
	mirror string
	at     time.Time
}

// assignmentTTL is how long an assignment counts on its own; after it, the informer decides.
const assignmentTTL = 30 * time.Second

// shadowsInUse returns the shadow resources held by this node's other mirrors. A mirror that
// is terminating still holds its device until the real kubelet has stopped it. Callers hold
// attachMu.
func (b *Backend) shadowsInUse(except *corev1.Pod) (map[corev1.ResourceName]string, error) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	used := map[corev1.ResourceName]string{}
	now := time.Now()
	for r, a := range b.assigned {
		switch {
		case now.Sub(a.at) > assignmentTTL:
			delete(b.assigned, r)
		case a.guest != except.UID:
			used[r] = a.mirror
		}
	}
	for _, m := range ms {
		if podTerminal(m) || m.Labels[LabelMirrorOf] == string(except.UID) {
			continue
		}
		if r := ShadowResourceOf(m); r != "" {
			used[r] = m.Namespace + "/" + m.Name
		}
	}
	return used, nil
}

// resolveGPU finds the donor GPU for a guest's mirror: a live donor on the host, the GPU the
// real kubelet gave it (pod-resources, through the shadow plugin), not already shared with
// another mirror, and advertised by the shadow plugin on the host.
func (b *Backend) resolveGPU(ctx context.Context, guest *corev1.Pod) (*GPUAttachment, error) {
	donors, err := b.liveDonors()
	if err != nil {
		return nil, refuse("list donors: %v", err)
	}
	if len(donors) == 0 {
		return nil, refuse("no running donor pod on host %s matches %q", b.opts.HostNode, b.opts.GPUDonorSelector)
	}
	hs, err := b.opts.Holders.Holders(ctx)
	if err != nil {
		return nil, refuse("shadow plugin holders endpoint: %v", err)
	}
	used, err := b.shadowsInUse(guest)
	if err != nil {
		return nil, refuse("list mirrors: %v", err)
	}
	host, err := b.client.CoreV1().Nodes().Get(ctx, b.opts.HostNode, metav1.GetOptions{})
	if err != nil {
		return nil, refuse("get host node: %v", err)
	}
	why := make([]string, 0, len(donors))
	for _, donor := range donors {
		key := donor.Namespace + "/" + donor.Name
		gpus := hs.HeldBy(donor.Namespace, donor.Name, string(GPUResource))
		if len(gpus) == 0 {
			why = append(why, key+" holds no "+string(GPUResource)+" in pod-resources")
			continue
		}
		for _, gpu := range gpus {
			log.G(ctx).WithField("pod", key).WithField("uuid", gpu.UUID).WithField("minor", gpu.Minor).
				WithField("source", "podresources").Info("donor device")
			if by, ok := used[gpu.Resource]; ok {
				why = append(why, fmt.Sprintf("%s GPU %s is already shared with mirror %s", key, gpu.UUID, by))
				continue
			}
			if q, ok := host.Status.Allocatable[gpu.Resource]; !ok || q.IsZero() {
				why = append(why, fmt.Sprintf("host does not advertise %s (is the shadow plugin running?)", gpu.Resource))
				continue
			}
			if gpu.UUID == "" {
				why = append(why, fmt.Sprintf("%s GPU minor %d has no UUID", key, gpu.Minor))
				continue
			}
			return &GPUAttachment{Resource: gpu.Resource, UUID: gpu.UUID, DonorUID: donor.UID, Donor: key}, nil
		}
	}
	return nil, refuse("%s", strings.Join(why, "; "))
}

// attachGPU resolves the donor GPU for a GPU guest in deviceplugin mode. On failure it logs
// "gpu attach refused", records GPUUnavailable on the guest and returns the error: no mirror.
func (b *Backend) attachGPU(ctx context.Context, guest *corev1.Pod) (*GPUAttachment, error) {
	if err := CheckDevicePluginGuest(guest); err != nil {
		return nil, b.refused(ctx, guest, guestRefusal(err))
	}
	att, err := b.resolveGPU(ctx, guest)
	if err != nil {
		return nil, b.refused(ctx, guest, err)
	}
	return att, nil
}

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
