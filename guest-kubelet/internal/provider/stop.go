package provider

// What a guest kubelet leaves behind when it is stopped. Before
// this, a SIGTERM left the virtual Node Ready, schedulable and finalized until its lease went
// stale, so the scheduler kept binding new guests to a Node no process served.
//
// Two outcomes, chosen by the caller (cmd/guest-kubelet decideStop):
//   - uninstall (the VK's controller is gone or no longer wants this host): ReleaseNode, so the
//     Node and its finalizer go.
//   - restart (the controller will start a replacement): MarkStopped, so the Node is cordoned
//     and NotReady but kept, with its guests and their mirrors, for the replacement to adopt.
//     The replacement calls ClearStopped when it starts.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"
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
