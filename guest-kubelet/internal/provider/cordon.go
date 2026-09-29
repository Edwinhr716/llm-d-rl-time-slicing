package provider

// The virtual Node is unschedulable while the donor holds the group lock (cordon while held).

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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
)

// Cordoner keeps the virtual Node's spec.unschedulable equal to "the donor holds the lock".
//
// It patches only spec.unschedulable and its own annotation, never the rest of the Node. The
// virtual-kubelet NodeController writes only nodes/status, so it cannot undo the cordon. The
// template (used when the Node has to be registered again) tracks the held value.
type Cordoner struct {
	nodes    corev1client.NodeInterface
	name     string
	recorder record.EventRecorder

	mu       sync.Mutex
	template corev1.Node
	applied  *bool // last value written or confirmed; nil until the first successful Set
}

// NewCordoner returns a Cordoner for the Node described by template.
func NewCordoner(nodes corev1client.NodeInterface, template *corev1.Node, recorder record.EventRecorder) *Cordoner {
	return &Cordoner{nodes: nodes, name: template.Name, recorder: recorder, template: *template.DeepCopy()}
}

// Template returns a copy of the Node to register, with the current cordon applied.
func (c *Cordoner) Template() *corev1.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.template.DeepCopy()
}

// Set makes the Node unschedulable when held and schedulable again when not held. It does
// nothing when held has not changed since the last successful call; after an error it logs
// and tries again on the next call. state is the group state, for the log line and the event.
func (c *Cordoner) Set(ctx context.Context, held bool, state string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applied != nil && *c.applied == held {
		return
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error { return c.apply(ctx, held, state) })
	if err != nil {
		log.G(ctx).WithField("node", c.name).WithField("held", held).WithField("state", state).
			WithError(err).Warn("cordon failed; retrying on the next poll")
		return
	}
	c.applied = &held
	c.template.Spec.Unschedulable = held
	if held {
		if c.template.Annotations == nil {
			c.template.Annotations = map[string]string{}
		}
		c.template.Annotations[CordonedByAnnotation] = CordonedByValue
	} else {
		delete(c.template.Annotations, CordonedByAnnotation)
	}
}

func (c *Cordoner) apply(ctx context.Context, held bool, state string) error {
	cur, err := c.nodes.Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	owned := cur.Annotations[CordonedByAnnotation] == CordonedByValue
	logger := log.G(ctx).WithField("node", c.name).WithField("held", held).WithField("state", state)
	switch {
	case held && cur.Spec.Unschedulable:
		if !owned {
			logger.WithField("action", "none: cordoned by someone else").Info("cordon")
		}
		return nil // already cordoned (by us before a restart, or by an admin)
	case !held && !owned:
		return nil // not ours to remove: never cordoned, or an admin cordon
	}
	var ann any // nil removes the annotation in a JSON merge patch
	reason, action := "Uncordoned", "uncordoned"
	if held {
		ann, reason, action = CordonedByValue, "Cordoned", "cordoned"
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
		msg := fmt.Sprintf("guest-kubelet %s the node (group state %s)", action, state)
		c.recorder.Event(ref, corev1.EventTypeNormal, reason, msg)
	}
	return nil
}
