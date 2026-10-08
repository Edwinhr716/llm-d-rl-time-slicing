package provider

// Guests stranded on a virtual Node whose host has no donor pod. The no-donor cordon
// (--cordon-without-donor) stops new guests from binding, but a guest bound before the cordon
// landed, or one still Pending when the donor left, cannot run: no GPU is lent there. A pod
// bound to a Node cannot be moved, so the guest kubelet deletes it and its controller creates a
// replacement, which the scheduler places on a Node that is not cordoned. A bare pod has no
// controller to re-create it, so it is never deleted: it gets a Warning event and a log line,
// once, and stays for its owner to delete.

import (
	"context"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// Event reasons on a stranded guest.
const (
	EventRequeued      = "RequeuedNoDonor"
	EventStrandedNoOwn = "StrandedNoDonor"
)

// DefaultRequeueAfter is how long the host must have had no donor pod before Pending guests
// are re-queued. It rides out a donor pod that is replaced in place.
const DefaultRequeueAfter = 10 * time.Second

// Requeuer deletes Pending, controller-owned guests of a virtual Node whose host has had no
// donor pod for After.
type Requeuer struct {
	// Pods deletes pods.
	Pods corev1client.PodsGetter
	// Bound lists the pods bound to the virtual Node.
	Bound func() ([]*corev1.Pod, error)
	// IsGuest picks the guests among them (the provider's guest predicate).
	IsGuest func(*corev1.Pod) bool
	// HasDonor answers whether a donor pod holds its GPU on the host.
	HasDonor HoldFunc
	// After is how long the donor must be gone first (DefaultRequeueAfter if zero).
	After time.Duration
	// Recorder records events on the guests. Optional.
	Recorder record.EventRecorder
	// Now is the clock (time.Now if nil).
	Now func() time.Time

	mu           sync.Mutex
	noDonorSince time.Time
	warned       map[types.UID]bool
}

// Tick runs one pass and returns how many guests it deleted.
func (r *Requeuer) Tick(ctx context.Context) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	after := r.After
	if after <= 0 {
		after = DefaultRequeueAfter
	}
	if ok, _ := r.HasDonor(); ok {
		r.noDonorSince, r.warned = time.Time{}, nil
		return 0
	}
	if r.noDonorSince.IsZero() {
		r.noDonorSince = now()
	}
	if now().Sub(r.noDonorSince) < after {
		return 0
	}
	pods, err := r.Bound()
	if err != nil {
		log.G(ctx).WithError(err).Warn("requeue: could not list the guests; retrying")
		return 0
	}
	deleted := 0
	for _, p := range pods {
		if !r.stranded(p) {
			continue
		}
		logger := log.G(ctx).WithField("guest", p.Namespace+"/"+p.Name)
		ctrl := metav1.GetControllerOf(p)
		if ctrl == nil {
			if !r.warned[p.UID] {
				if r.warned == nil {
					r.warned = map[types.UID]bool{}
				}
				r.warned[p.UID] = true
				logger.Warn("requeue: bare guest stranded on a node with no donor pod; left for its owner to delete")
				r.event(p, corev1.EventTypeWarning, EventStrandedNoOwn,
					"no GPU can be lent on this node (no donor pod); a bare pod is not re-created, delete it to schedule it elsewhere")
			}
			continue
		}
		uid := p.UID
		err := r.Pods.Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
		switch {
		case apierrors.IsNotFound(err) || apierrors.IsConflict(err):
			continue // gone or replaced already
		case err != nil:
			logger.WithError(err).Warn("requeue: delete failed; retrying on the next pass")
			continue
		}
		deleted++
		logger.WithField("owner", ctrl.Kind+"/"+ctrl.Name).Info("requeue: deleted a Pending guest on a node with no donor pod; its controller re-creates it")
		r.event(p, corev1.EventTypeNormal, EventRequeued,
			"no GPU can be lent on this node (no donor pod); deleted so that "+ctrl.Kind+" "+ctrl.Name+" re-creates it on another node")
	}
	return deleted
}

// stranded is a live guest that has not started: phase Pending (or not yet set).
func (r *Requeuer) stranded(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil || (r.IsGuest != nil && !r.IsGuest(p)) {
		return false
	}
	return p.Status.Phase == corev1.PodPending || p.Status.Phase == ""
}

func (r *Requeuer) event(p *corev1.Pod, kind, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(p, kind, reason, msg)
	}
}

// Run calls Tick every interval until ctx ends.
func (r *Requeuer) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		r.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
