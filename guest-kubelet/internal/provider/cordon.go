package provider

// Option ns-cordon of --cordon-while-held (pending lead decision D-NS-8): the virtual Node is
// unschedulable while the donor holds the GPU. Nothing in this file runs when the flag is false
// (option skip). The cordon mechanics follow opt/D-NS-8/all; the hold signal here is M3's: at
// least one guest of this Node is suspending or suspended (the donor has the GPU back). The
// orchestrator loop (VK-A6) can feed the same Cordoner from the group state instead.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

const (
	// CordonedByAnnotation marks a cordon the guest kubelet set. The guest kubelet only ever
	// removes a cordon that carries it, so an admin's `kubectl cordon` is left alone.
	CordonedByAnnotation = "timeslice.io/cordoned-by"
	// CordonedByValue is the value of CordonedByAnnotation.
	CordonedByValue = "guest-kubelet"

	// DefaultCordonResync is how long a confirmed cordon value is trusted before Set reads the
	// Node again. It repairs a cordon lost to something else (a re-registered Node, an admin
	// uncordon during a hold) without a Node read on every poll.
	DefaultCordonResync = 5 * time.Second
)

// Cordoner keeps the virtual Node's spec.unschedulable equal to "the donor holds the GPU" (and,
// with WithoutDonor, "or the host has no donor pod").
//
// It patches only spec.unschedulable and its own annotation, never the rest of the Node. The
// virtual-kubelet NodeController writes only nodes/status, so it cannot undo the cordon.
type Cordoner struct {
	nodes    corev1client.NodeInterface
	name     string
	recorder record.EventRecorder
	// Resync bounds how long a confirmed value is trusted (DefaultCordonResync if zero).
	Resync time.Duration
	now    func() time.Time

	mu          sync.Mutex
	applied     *bool // last value written or confirmed; nil until the first successful Set
	confirmedAt time.Time
}

// NewCordoner returns a Cordoner for the named Node. recorder may be nil.
func NewCordoner(nodes corev1client.NodeInterface, name string, recorder record.EventRecorder) *Cordoner {
	return &Cordoner{nodes: nodes, name: name, recorder: recorder, now: time.Now}
}

// Set makes the Node unschedulable when held and schedulable again when not held. It does
// nothing when held equals the value confirmed less than Resync ago; after an error it logs and
// tries again on the next call. reason says why (for the log line and the event).
func (c *Cordoner) Set(ctx context.Context, held bool, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resync := c.Resync
	if resync <= 0 {
		resync = DefaultCordonResync
	}
	if c.applied != nil && *c.applied == held && c.now().Sub(c.confirmedAt) < resync {
		return
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error { return c.apply(ctx, held, reason) })
	if err != nil {
		log.G(ctx).WithField("node", c.name).WithField("held", held).WithField("reason", reason).
			WithError(err).Warn("cordon failed; retrying on the next poll")
		c.applied = nil
		return
	}
	c.applied, c.confirmedAt = &held, c.now()
}

func (c *Cordoner) apply(ctx context.Context, held bool, reason string) error {
	cur, err := c.nodes.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	owned := cur.Annotations[CordonedByAnnotation] == CordonedByValue
	logger := log.G(ctx).WithField("node", c.name).WithField("held", held).WithField("reason", reason)
	switch {
	case held && cur.Spec.Unschedulable:
		if !owned && (c.applied == nil || !*c.applied) {
			logger.WithField("action", "none: cordoned by someone else").Info("cordon")
		}
		return nil // already cordoned (by us before a restart, or by an admin)
	case !held && !owned:
		return nil // not ours to remove: never cordoned, or an admin cordon
	}
	var ann any // nil removes the annotation in a JSON merge patch
	evReason, action := "Uncordoned", "uncordoned"
	if held {
		ann, evReason, action = CordonedByValue, "Cordoned", "cordoned"
	}
	patch, err := json.Marshal(map[string]any{
		// resourceVersion makes the patch fail with a conflict if the Node changed since the Get.
		"metadata": map[string]any{
			"resourceVersion": cur.ResourceVersion,
			"annotations":     map[string]any{CordonedByAnnotation: ann},
		},
		"spec": map[string]any{"unschedulable": held},
	})
	if err != nil {
		return err
	}
	if _, err := c.nodes.Patch(ctx, c.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return err
	}
	logger.WithField("action", action).Info("cordon")
	if c.recorder != nil {
		// Same reference shape as the kubelet's node events, so `kubectl describe node` shows them.
		ref := &corev1.ObjectReference{Kind: "Node", Name: c.name, UID: types.UID(c.name)}
		c.recorder.Event(ref, corev1.EventTypeNormal, evReason, fmt.Sprintf("guest-kubelet %s the node (%s)", action, reason))
	}
	return nil
}

// HoldFunc reports whether the donor holds the GPU, and why (for logs and events).
type HoldFunc func() (bool, string)

// WithoutDonor returns a HoldFunc that also cordons while the host has no donor pod
// (--cordon-without-donor): no GPU can be lent there, so a guest bound in that time would only
// wait. hasDonor answers whether a donor pod holds its GPU on the host. While a donor is
// present the answer is next's (nil: not held). A donor that comes back uncordons the Node on
// the next poll, unless next still holds it.
func WithoutDonor(hasDonor, next HoldFunc) HoldFunc {
	return func() (bool, string) {
		if ok, why := hasDonor(); !ok {
			return true, why
		}
		if next == nil {
			return false, "donor present"
		}
		return next()
	}
}

// RunCordon calls Set with hold's answer every interval, and at once whenever poke fires (a
// suspend or resume changed the answer). It returns when ctx ends. It never uncordons on exit:
// a restarted guest kubelet reads the hold from the mirrors again and decides then.
func (c *Cordoner) RunCordon(ctx context.Context, interval time.Duration, hold HoldFunc, poke <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		held, reason := hold()
		c.Set(ctx, held, reason)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-poke:
		}
	}
}
