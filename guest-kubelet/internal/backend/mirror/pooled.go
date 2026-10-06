package mirror

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// pooledAssignment is GPUs counted for a just-created mirror the informer may not show yet.
type pooledAssignment struct {
	mirror string
	qty    int64
	at     time.Time
}

// pooledInUse returns the pooled shadow devices held by this node's mirrors other than the
// guest's own. A terminating mirror still holds its devices until the real kubelet has
// stopped it, so it counts (the admission race). Callers hold attachMu.
func (b *Backend) pooledInUse(except types.UID) (int64, error) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return 0, err
	}
	seen := map[string]bool{}
	var used int64
	for _, m := range ms {
		if podTerminal(m) || m.Labels[LabelMirrorOf] == string(except) {
			continue
		}
		used += PooledQtyOf(m)
		seen[m.Namespace+"/"+m.Name] = true
	}
	now := time.Now()
	for uid, a := range b.pooledAssigned {
		switch {
		case now.Sub(a.at) > assignmentTTL:
			delete(b.pooledAssigned, uid)
		case uid != except && !seen[a.mirror]:
			used += a.qty
		}
	}
	return used, nil
}

// attachPooled admits a GPU guest's mirror in pooled mode: no GPU pick, only the check that
// the mirrors on the node plus this one fit the host's gpu-shadow allocatable. That
// allocatable is what the shadow plugin advertises: the GPUs of the node's one donor, or 0
// (no donor, or more than one GPU pod: fail closed). On failure no mirror is created and the
// library retries the create.
func (b *Backend) attachPooled(ctx context.Context, guest *corev1.Pod) (*GPUAttachment, error) {
	k, err := CheckPooledGuest(guest)
	if err != nil {
		return nil, b.refused(ctx, guest, guestRefusal(err))
	}
	host, err := b.client.CoreV1().Nodes().Get(ctx, b.opts.HostNode, metav1.GetOptions{})
	if err != nil {
		return nil, b.refused(ctx, guest, refuse("get host node: %v", err))
	}
	var alloc int64
	if q, ok := host.Status.Allocatable[api.PooledResource]; ok {
		alloc = q.Value()
	}
	used, err := b.pooledInUse(guest.UID)
	if err != nil {
		return nil, b.refused(ctx, guest, refuse("list mirrors: %v", err))
	}
	if used+k > alloc {
		return nil, b.refused(ctx, guest, refuse("host %s %s allocatable %d; mirrors hold %d (terminating included); guest needs %d",
			b.opts.HostNode, api.PooledResource, alloc, used, k))
	}
	att := &GPUAttachment{Pooled: true, Qty: k}
	// The fence (fence.go) stops mirrors whose donor is gone; it needs the donor's UID. With
	// one owner per node there is exactly one; with none or several the plugin advertises 0
	// and the check above has already refused.
	if b.donors != nil {
		if donors, err := b.liveDonors(); err == nil && len(donors) == 1 {
			att.DonorUID, att.Donor = donors[0].UID, donors[0].Namespace+"/"+donors[0].Name
		} else {
			log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("donors", len(donors)).
				WithError(err).Warn("pooled attach: not exactly one live donor; mirror is not fenced to a donor")
		}
	}
	return att, nil
}
