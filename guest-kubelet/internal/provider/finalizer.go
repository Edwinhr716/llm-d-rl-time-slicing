package provider

import (
	"context"
	"fmt"
	"slices"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// What EnsureNodeGuard found or did.
const (
	GuardCreated     = "created"     // the Node did not exist; created with the finalizer
	GuardUpdated     = "updated"     // the finalizer or the host ownerReference was added
	GuardPresent     = "present"     // both were already there
	GuardTerminating = "terminating" // the Node is being deleted; the finalizer holds it
)

// Why the finalizer was removed. Logged as reason= on "node finalizer removed".
const (
	ReasonHostGone   = "host-gone"  // the donor controller saw the real Node disappear
	ReasonDeregister = "deregister" // the VK deregistered its own Node (end of an era)
)

// EnsureNodeGuard makes sure the virtual Node carries NodeFinalizer (only if spec carries it,
// D-VK-2 option c) and the ownerReference to the real Node from spec. It creates the Node from
// spec if it does not exist. A Node that is
// already being deleted cannot take new finalizers; it is left as it is (its finalizer, if
// any, holds it until the donor controller or a deregistration releases it).
func EnsureNodeGuard(ctx context.Context, client kubernetes.Interface, spec *corev1.Node) (string, error) {
	nodes := client.CoreV1().Nodes()
	action := GuardPresent
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := nodes.Get(ctx, spec.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			fresh := spec.DeepCopy()
			fresh.ResourceVersion = ""
			if _, err := nodes.Create(ctx, fresh, metav1.CreateOptions{}); err != nil {
				return err
			}
			action = GuardCreated
			return nil
		}
		if err != nil {
			return err
		}
		if cur.DeletionTimestamp != nil {
			action = GuardTerminating
			return nil
		}
		if !mergeGuard(cur, spec) {
			action = GuardPresent
			return nil
		}
		if _, err := nodes.Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
			return err
		}
		action = GuardUpdated
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("guard virtual Node %s: %w", spec.Name, err)
	}
	if !slices.Contains(spec.Finalizers, NodeFinalizer) {
		log.G(ctx).WithField("node", spec.Name).WithField("action", action).
			WithField("owner", ownerNames(spec.OwnerReferences)).Info("node registered without finalizer")
		return action, nil
	}
	logger := log.G(ctx).WithField("node", spec.Name).WithField("finalizer", NodeFinalizer).WithField("action", action)
	if action == GuardTerminating {
		logger.Warn("node is terminating; the finalizer holds it until the donor controller or deregistration releases it")
	} else {
		logger.WithField("owner", ownerNames(spec.OwnerReferences)).Info("node finalizer set")
	}
	return action, nil
}

// mergeGuard adds NodeFinalizer (if spec carries it) and spec's Node ownerReferences to cur. Node ownerReferences
// on cur that spec does not list (a host that was recreated with a new UID) are replaced.
// It reports whether cur changed.
func mergeGuard(cur, spec *corev1.Node) bool {
	changed := false
	if slices.Contains(spec.Finalizers, NodeFinalizer) && !slices.Contains(cur.Finalizers, NodeFinalizer) {
		cur.Finalizers = append(cur.Finalizers, NodeFinalizer)
		changed = true
	}
	want := nodeOwners(spec.OwnerReferences)
	if len(want) == 0 {
		return changed
	}
	kept := make([]metav1.OwnerReference, 0, len(cur.OwnerReferences)+len(want))
	for _, ref := range cur.OwnerReferences {
		if isNodeOwner(&ref) && !slices.ContainsFunc(want, func(w metav1.OwnerReference) bool { return w.UID == ref.UID }) {
			changed = true
			continue
		}
		kept = append(kept, ref)
	}
	for _, w := range want {
		if !slices.ContainsFunc(kept, func(k metav1.OwnerReference) bool { return k.UID == w.UID }) {
			kept = append(kept, w)
			changed = true
		}
	}
	cur.OwnerReferences = kept
	return changed
}

func isNodeOwner(ref *metav1.OwnerReference) bool {
	return ref.APIVersion == "v1" && ref.Kind == "Node"
}

func nodeOwners(refs []metav1.OwnerReference) []metav1.OwnerReference {
	var out []metav1.OwnerReference
	for _, ref := range refs {
		if isNodeOwner(&ref) {
			out = append(out, ref)
		}
	}
	return out
}

func ownerNames(refs []metav1.OwnerReference) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Kind+"/"+ref.Name)
	}
	return names
}

// ReleaseNode deletes the virtual Node (unless it is already being deleted) and removes
// NodeFinalizer, so the delete completes and its guests are cleaned up. Only the donor
// controller (reason host-gone) and the VK's own deregistration (reason deregister) call it.
// It refuses any Node without the virtual-node label, so a wrong name never touches a real
// Node. It reports whether it found the Node and released it; a Node that is already gone is
// not an error.
func ReleaseNode(ctx context.Context, client kubernetes.Interface, name, reason string) (bool, error) {
	nodes := client.CoreV1().Nodes()
	cur, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if cur.Labels[VirtualNodeLabel] != "true" {
		return false, fmt.Errorf("refusing to release Node %s: it has no %s=true label", name, VirtualNodeLabel)
	}
	uid := cur.UID
	if cur.DeletionTimestamp == nil {
		err := nodes.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete virtual Node %s: %w", name, err)
		}
	}
	removed := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if node.UID != uid {
			return nil // a new Node with the same name: not the one being released
		}
		kept := slices.DeleteFunc(slices.Clone(node.Finalizers), func(f string) bool { return f == NodeFinalizer })
		if len(kept) == len(node.Finalizers) {
			return nil
		}
		node.Finalizers = kept
		if _, err := nodes.Update(ctx, node, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		removed = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("remove finalizer from virtual Node %s: %w", name, err)
	}
	if removed {
		log.G(ctx).WithField("node", name).WithField("finalizer", NodeFinalizer).WithField("reason", reason).
			Info("node finalizer removed")
	}
	return true, nil
}
