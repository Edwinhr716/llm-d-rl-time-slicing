// Package keeper is the virtual Node's outage guard. When every guest-kubelet replica is down
// (a crash loop, a bad rollout), nothing renews the virtual Node's Lease. About 40-50 s later
// kube-controller-manager marks the Node NotReady, and because the Node has no cloud instance
// the cloud node lifecycle controller deletes it a few seconds after that; the pod garbage
// collector then deletes every guest bound to it, and with mirror owner references on, the
// mirrors too (measured in M0/M1: Node gone at +54-60 s, guests at +96 s).
//
// The keeper is a separate, tiny process pinned to the host. While the host is Ready and the
// guest kubelet has been seen alive within OutageGrace, it renews the virtual Node's Lease
// whenever the guest kubelet has stopped renewing it. The Node stays Ready, so no controller
// acts on it; the mirrors keep running on the real kubelet; when a guest kubelet comes back it
// re-adopts them. Past OutageGrace it stops, and the normal NotReady/delete path runs.
//
// It never touches Node status or objects other than that one Lease. All of its state is in the
// Lease's annotations, so a keeper restart in the middle of an outage does not extend the grace.
package keeper

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// AnnotationRenewedAt is the RenewTime the keeper last wrote. A RenewTime that differs
	// was written by the guest kubelet.
	AnnotationRenewedAt = "timeslice.io/node-keeper-renewed-at"
	// AnnotationVKLastSeen is when the guest kubelet last renewed the Lease itself, as the
	// keeper saw it. OutageGrace counts from here.
	AnnotationVKLastSeen = "timeslice.io/node-keeper-vk-last-seen"
	// NodeLeaseNamespace holds every Node's heartbeat Lease.
	NodeLeaseNamespace = "kube-node-lease"
)

// Config is the keeper's settings.
type Config struct {
	HostNode    string // the real node; the keeper does nothing while it is not Ready
	VirtualNode string // whose Lease is kept
	// StaleAfter is how old the Lease's RenewTime must be before the keeper renews it. The
	// guest kubelet renews every 10 s, and a leader failover takes about 2 s.
	StaleAfter time.Duration
	// OutageGrace is how long after the guest kubelet was last seen the keeper keeps renewing.
	OutageGrace time.Duration
	// Interval is the time between checks (Run only).
	Interval time.Duration
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Action is what one Tick did.
type Action string

const (
	ActionHostNotReady Action = "host-not-ready" // host gone or not Ready: never cover for a dead host
	ActionNoLease      Action = "no-lease"       // the virtual Node (and so its Lease) does not exist
	ActionForeign      Action = "foreign-lease"  // the Lease is not held by the virtual Node
	ActionFresh        Action = "fresh"          // the Lease is recent: nothing to do
	ActionRenewed      Action = "renewed"        // the keeper renewed the Lease
	ActionGaveUp       Action = "gave-up"        // outage longer than OutageGrace: let the Node go NotReady
	ActionConflict     Action = "conflict"       // someone (the guest kubelet) wrote the Lease first
)

// Keeper keeps one virtual Node's Lease fresh through a guest-kubelet outage.
type Keeper struct {
	client kubernetes.Interface
	cfg    Config
}

// New returns a keeper.
func New(client kubernetes.Interface, cfg Config) *Keeper {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Keeper{client: client, cfg: cfg}
}

// Result is one Tick's outcome, for logging and tests.
type Result struct {
	Action     Action
	LeaseAge   time.Duration // now - RenewTime before any write
	VKLastSeen time.Time     // zero if unknown
}

func fmtTime(t time.Time) string { return t.UTC().Format(metav1.RFC3339Micro) }

// Tick checks once and renews the Lease if the guest kubelet is down within the grace.
func (k *Keeper) Tick(ctx context.Context) (Result, error) {
	now := k.cfg.Now().UTC().Truncate(time.Microsecond)
	host, err := k.client.CoreV1().Nodes().Get(ctx, k.cfg.HostNode, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Result{Action: ActionHostNotReady}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("get host node: %w", err)
	}
	if !nodeReady(host) {
		return Result{Action: ActionHostNotReady}, nil
	}

	leases := k.client.CoordinationV1().Leases(NodeLeaseNamespace)
	lease, err := leases.Get(ctx, k.cfg.VirtualNode, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Result{Action: ActionNoLease}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("get lease: %w", err)
	}
	if h := lease.Spec.HolderIdentity; h == nil || *h != k.cfg.VirtualNode {
		return Result{Action: ActionForeign}, nil
	}
	if lease.Spec.RenewTime == nil {
		return Result{Action: ActionFresh}, nil // never renewed: the guest kubelet is starting
	}
	rt := lease.Spec.RenewTime.Time
	res := Result{LeaseAge: now.Sub(rt)}

	// Who wrote the current RenewTime? If it is not the value the keeper wrote, the guest
	// kubelet did, and that is when it was last alive.
	if lease.Annotations[AnnotationRenewedAt] != fmtTime(rt) {
		res.VKLastSeen = rt
	} else if t, err := time.Parse(metav1.RFC3339Micro, lease.Annotations[AnnotationVKLastSeen]); err == nil {
		res.VKLastSeen = t
	} else {
		res.VKLastSeen = rt // annotation lost: count from the keeper's last write, at worst one grace more
	}

	if res.LeaseAge < k.cfg.StaleAfter {
		res.Action = ActionFresh
		return res, nil
	}
	if now.Sub(res.VKLastSeen) > k.cfg.OutageGrace {
		res.Action = ActionGaveUp
		return res, nil
	}

	upd := lease.DeepCopy()
	upd.Spec.RenewTime = &metav1.MicroTime{Time: now}
	if upd.Annotations == nil {
		upd.Annotations = map[string]string{}
	}
	upd.Annotations[AnnotationRenewedAt] = fmtTime(now)
	upd.Annotations[AnnotationVKLastSeen] = fmtTime(res.VKLastSeen)
	// Update carries the resourceVersion from the Get, so a concurrent write by a guest kubelet
	// that just came back wins and the keeper backs off.
	if _, err := leases.Update(ctx, upd, metav1.UpdateOptions{}); apierrors.IsConflict(err) {
		res.Action = ActionConflict
		return res, nil
	} else if err != nil {
		return res, fmt.Errorf("renew lease: %w", err)
	}
	res.Action = ActionRenewed
	return res, nil
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Run ticks every Interval until ctx is done. log gets one line per state change and one per
// renewal, so an outage is visible in the keeper's log.
func (k *Keeper) Run(ctx context.Context, log func(msg string, kv ...any)) error {
	interval := k.cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var last Action
	for {
		res, err := k.Tick(ctx)
		switch {
		case err != nil:
			log("keeper check failed", "error", err.Error())
		case res.Action == ActionRenewed:
			log("guest kubelet is not renewing its Node Lease; renewed it", "leaseAge", res.LeaseAge.String(),
				"vkLastSeen", fmtTime(res.VKLastSeen), "grace", k.cfg.OutageGrace.String())
		case res.Action != last:
			kv := []any{"action", string(res.Action), "leaseAge", res.LeaseAge.String()}
			if res.Action == ActionGaveUp {
				kv = append(kv, "vkLastSeen", fmtTime(res.VKLastSeen), "grace", k.cfg.OutageGrace.String())
			}
			log("keeper state", kv...)
		}
		if err == nil {
			last = res.Action
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
