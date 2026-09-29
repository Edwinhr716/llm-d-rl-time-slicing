package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// ReasonReclaim is the reason= on "node finalizer removed" when a returning VK replaces its own
// Terminating Node (ReclaimNode). The host is alive: the VK runs on it.
const ReasonReclaim = "reclaim"

// What ReclaimNode found or did.
const (
	ReclaimNone     = "none"     // the Node is missing or not being deleted: nothing to do
	ReclaimReplaced = "replaced" // the held Node was let go and a fresh one registered
	ReclaimSkipped  = "skipped"  // the Node is Terminating, but not ours to reclaim; left held
)

// ReclaimNode replaces the virtual Node when a delete is holding it Terminating on the VK's
// return, for example after the cloud node lifecycle controller deleted it during a VK outage.
// A Node with a deletionTimestamp cannot be un-deleted, so the VK removes its own finalizer,
// waits until the old object is gone, and registers spec again at once (a new UID, the same
// name). Guests are bound by Node name, and pod GC deletes pods of a missing Node only after
// it has been missing for a 40 s quarantine and a fresh lookup, so the guests and their mirrors
// are untouched; the old Node's unreachable taints go with it.
//
// It reclaims only a Node that is plainly the VK's own on a live host: it has the
// virtual-node label, NodeFinalizer is its only finalizer, and its ownerReference names the
// host Node with the UID in spec (the host this VK runs on). A Node owned by an earlier host
// (the host was recreated) is left held for the donor controller, which releases it and so
// deletes its guests, because their mirrors died with the old host. Call it only from the
// leader, before the kubelet role starts.
func ReclaimNode(ctx context.Context, client kubernetes.Interface, spec *corev1.Node, timeout time.Duration) (string, error) {
	nodes := client.CoreV1().Nodes()
	cur, err := nodes.Get(ctx, spec.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ReclaimNone, nil
	}
	if err != nil {
		return "", err
	}
	if cur.DeletionTimestamp == nil {
		return ReclaimNone, nil
	}
	logger := log.G(ctx).WithField("node", spec.Name).WithField("finalizer", NodeFinalizer)
	if why := reclaimRefusal(cur, spec); why != "" {
		logger.WithField("why", why).Warn(
			"terminating node not reclaimed; the finalizer holds it until the donor controller or deregistration releases it")
		return ReclaimSkipped, nil
	}
	oldUID := cur.UID
	cur.Finalizers = nil
	// The Get's resourceVersion fences the update: any change since then is a conflict, and the
	// VK exits and retries on its restart rather than letting go of a Node it did not inspect.
	if _, err := nodes.Update(ctx, cur, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("remove finalizer from terminating Node %s: %w", spec.Name, err)
	}
	logger.WithField("reason", ReasonReclaim).WithField("uid", string(oldUID)).Info("node finalizer removed")

	var lastGetErr error // a failed Get is transient: keep polling, report it on timeout
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		n, getErr := nodes.Get(ctx, spec.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			return true, nil
		case getErr != nil:
			lastGetErr = getErr
			return false, nil //nolint:nilerr // transient: keep polling; reported on timeout
		default:
			return n.UID != oldUID, nil
		}
	})
	if err != nil {
		if lastGetErr != nil {
			err = errors.Join(err, lastGetErr)
		}
		return "", fmt.Errorf("wait for terminating Node %s (uid %s) to go: %w", spec.Name, oldUID, err)
	}

	fresh := spec.DeepCopy()
	fresh.ResourceVersion = ""
	fresh.UID = ""
	created, err := nodes.Create(ctx, fresh, metav1.CreateOptions{})
	var newUID types.UID
	switch {
	case err == nil:
		newUID = created.UID
	case apierrors.IsAlreadyExists(err):
		// Someone registered the name in between; EnsureNodeGuard adds the guard to it.
	default:
		return "", fmt.Errorf("re-register Node %s: %w", spec.Name, err)
	}
	logger.WithField("oldUID", string(oldUID)).WithField("newUID", string(newUID)).
		WithField("owner", ownerNames(spec.OwnerReferences)).Info("node reclaimed")
	return ReclaimReplaced, nil
}

// reclaimRefusal returns why cur (a Terminating Node) is not the VK's to reclaim, or "".
func reclaimRefusal(cur, spec *corev1.Node) string {
	if cur.Labels[VirtualNodeLabel] != "true" {
		return "no " + VirtualNodeLabel + "=true label"
	}
	if !slices.Equal(cur.Finalizers, []string{NodeFinalizer}) {
		return fmt.Sprintf("finalizers %v are not just %s", cur.Finalizers, NodeFinalizer)
	}
	want := nodeOwners(spec.OwnerReferences)
	if len(want) == 0 {
		return "the VK does not know its host Node's UID"
	}
	for _, w := range want {
		if !slices.ContainsFunc(cur.OwnerReferences, func(ref metav1.OwnerReference) bool {
			return isNodeOwner(&ref) && ref.UID == w.UID
		}) {
			return "owned by another host Node (" + w.Name + " was recreated?)"
		}
	}
	return ""
}
