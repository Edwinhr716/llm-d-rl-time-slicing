package provider

// What a guest kubelet leaves behind when it is stopped. Before
// this, a SIGTERM left the virtual Node Ready, schedulable and finalized until its lease went
// stale, so the scheduler kept binding new guests to a Node no process served.
//
// Two outcomes, chosen by the caller (cmd/guest-kubelet decideStop):
//   - uninstall (the VK's controller is gone or no longer wants this host): ReleaseNode, so the
//     Node and its finalizer go, then CleanupReleasedNode, so no mirror or guest is left on it.
//   - restart (the controller will start a replacement): MarkStopped, so the Node is cordoned
//     and NotReady but kept, with its guests and their mirrors, for the replacement to adopt.
//     The replacement calls ClearStopped when it starts.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

const (
	// StoppedByAnnotation names the VK pod that cordoned the Node on its way out.
	StoppedByAnnotation = "timeslice.io/stopped-by"
	// ReasonStopped is the Ready=False reason on a Node whose guest kubelet stopped.
	ReasonStopped = "GuestKubeletStopped"
	// ReasonUninstall is ReleaseNode's reason when the VK's own controller is gone.
	ReasonUninstall = "uninstall"
)

// MarkStopped cordons the virtual Node (spec.unschedulable, CordonedByAnnotation and
// StoppedByAnnotation=by) and sets its Ready condition False. The finalizer stays: the
// replacement VK is expected within seconds and keeps the guests. It refuses a Node without
// the virtual-node label. A Node that is gone is not an error.
func MarkStopped(ctx context.Context, nodes corev1client.NodeInterface, name, by string) error {
	cur, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if cur.Labels[VirtualNodeLabel] != "true" {
		return fmt.Errorf("refusing to mark Node %s stopped: it has no %s=true label", name, VirtualNodeLabel)
	}
	patch, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{
			CordonedByAnnotation: CordonedByValue, StoppedByAnnotation: by,
		}},
		"spec": map[string]any{"unschedulable": true},
	})
	if _, err := nodes.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("cordon stopped Node %s: %w", name, err)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := nodes.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		setReadyFalse(n, by, time.Now())
		_, err = nodes.UpdateStatus(ctx, n, metav1.UpdateOptions{})
		return err
	})
}

func setReadyFalse(n *corev1.Node, by string, now time.Time) {
	cond := corev1.NodeCondition{
		Type: corev1.NodeReady, Status: corev1.ConditionFalse, Reason: ReasonStopped,
		Message:            "guest kubelet " + by + " stopped; the Node is cordoned until a guest kubelet serves it again",
		LastHeartbeatTime:  metav1.NewTime(now),
		LastTransitionTime: metav1.NewTime(now),
	}
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == corev1.NodeReady {
			n.Status.Conditions[i] = cond
			return
		}
	}
	n.Status.Conditions = append(n.Status.Conditions, cond)
}

// ClearStopped undoes MarkStopped's cordon when a guest kubelet (self) serves the Node again.
// It acts only on a Node that carries StoppedByAnnotation from another pod, and removes the
// cordon only if the guest kubelet set it; --cordon-while-held re-cordons on its next poll if
// the donor holds the GPU. The Ready condition is the library's next status update. It
// reports whether it cleared anything.
func ClearStopped(ctx context.Context, nodes corev1client.NodeInterface, name, self string) (bool, error) {
	cur, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	by, ok := cur.Annotations[StoppedByAnnotation]
	if !ok || (self != "" && by == self) {
		return false, nil
	}
	meta := map[string]any{"annotations": map[string]any{StoppedByAnnotation: nil}}
	p := map[string]any{"metadata": meta}
	if cur.Annotations[CordonedByAnnotation] == CordonedByValue {
		meta["annotations"].(map[string]any)[CordonedByAnnotation] = nil
		p["spec"] = map[string]any{"unschedulable": false}
	}
	patch, _ := json.Marshal(p)
	if _, err := nodes.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return false, err
	}
	log.G(ctx).WithField("node", name).WithField("stoppedBy", by).Info("cleared the stop cordon left by a previous guest kubelet")
	return true, nil
}

// KeepClearingStopped runs ClearStopped now and every interval until ctx ends. The repeat
// covers a leader-election handover, where the old leader's MarkStopped can land after the
// new leader started.
func KeepClearingStopped(ctx context.Context, nodes corev1client.NodeInterface, name, self string, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := ClearStopped(ctx, nodes, name, self); err != nil && ctx.Err() == nil {
			log.G(ctx).WithField("node", name).WithError(err).Warn("could not clear the stop cordon; retrying")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// CleanupReleasedNode removes what is left of a virtual Node that ReleaseNode released on
// uninstall. No guest kubelet serves the Node again, so nothing would ever confirm a guest's
// delete: a guest Terminating there would stay until someone force-deleted it. It deletes
// every mirror of the Node (label timeslice.io/mirror-node; they run on a live host, so with
// their normal grace, and the real kubelet stops them) and force-deletes (grace 0) every pod
// still bound to the Node. It returns how many of each it deleted; a pod that is already gone
// is not an error.
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func CleanupReleasedNode(ctx context.Context, pods corev1client.PodsGetter, name string) (int, int, error) {
	mirrors, guests := 0, 0
	all := pods.Pods(corev1.NamespaceAll)
	ms, err := all.List(ctx, metav1.ListOptions{LabelSelector: mirror.LabelMirrorNode + "=" + name})
	if err != nil {
		return 0, 0, fmt.Errorf("list mirrors of %s: %w", name, err)
	}
	var errs []error
	for i := range ms.Items {
		m := &ms.Items[i]
		if m.DeletionTimestamp != nil {
			continue
		}
		if err := pods.Pods(m.Namespace).Delete(ctx, m.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete mirror %s/%s: %w", m.Namespace, m.Name, err))
			continue
		}
		mirrors++
	}
	bound, err := all.List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", name).String()})
	if err != nil {
		return mirrors, 0, errors.Join(append(errs, fmt.Errorf("list pods on %s: %w", name, err))...)
	}
	zero := int64(0)
	for i := range bound.Items {
		p := &bound.Items[i]
		if p.Spec.NodeName != name {
			continue // a client that ignores the field selector
		}
		uid := p.UID
		err := pods.Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &uid},
		})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			errs = append(errs, fmt.Errorf("force-delete %s/%s: %w", p.Namespace, p.Name, err))
			continue
		}
		guests++
	}
	if mirrors+guests > 0 {
		log.G(ctx).WithField("node", name).WithField("mirrors", mirrors).WithField("guests", guests).
			Info("released node: mirrors deleted and guests force-deleted")
	}
	return mirrors, guests, errors.Join(errs...)
}
