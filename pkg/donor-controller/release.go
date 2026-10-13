package donorcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// ReasonHostGone is the release reason logged when a virtual Node's host is gone.
const ReasonHostGone = "host-gone"

// releaseDeadHosts is H11 (--release-dead-hosts): the D-VK-2 option c donor stand-in's duty,
// generalised to every virtual Node. A virtual Node (label timeslice.io/virtual-node=true) whose
// host, named by its ownerReference of kind Node, is gone or was recreated with a new UID is
// released: deleted, and the VK finalizer removed, so its guests are cleaned up and retried. The
// VK cannot do this itself: it ran on the host that died.
func (c *Controller) releaseDeadHosts(ctx context.Context, now time.Time, nodes map[string]*corev1.Node) {
	present := map[types.UID]bool{}
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		vk := nodes[name]
		if vk.Labels[VirtualNodeLabel] != "true" {
			continue
		}
		present[vk.UID] = true
		ref := hostOwnerRef(vk)
		if ref == nil {
			continue
		}
		if host, ok := nodes[ref.Name]; ok && host.UID == ref.UID {
			continue
		}
		if last, ok := c.released[vk.UID]; ok && now.Sub(last) < releaseRetry {
			continue
		}
		c.released[vk.UID] = now
		why, err := c.hostGone(ctx, ref)
		if err != nil {
			c.log.Warn("host check failed", "node", vk.Name, "host", ref.Name, "err", err)
			continue
		}
		if why == "" {
			continue // the cache was behind; the host is there
		}
		done, err := c.releaseVirtualNode(ctx, vk)
		if err != nil {
			c.log.Warn("virtual node release failed; retrying", "node", vk.Name, "host", ref.Name, "err", err)
			continue
		}
		if done {
			c.log.Info("virtual node released", "node", vk.Name, "host", ref.Name, "reason", ReasonHostGone, "detail", why)
		}
	}
	for uid := range c.released {
		if !present[uid] {
			delete(c.released, uid)
		}
	}
}

// hostGone confirms with a live read why the host counts as gone, or returns "" if it is there.
func (c *Controller) hostGone(ctx context.Context, ref *metav1.OwnerReference) (string, error) {
	host, err := c.cs.CoreV1().Nodes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "host Node not found", nil
	}
	if err != nil {
		return "", err
	}
	if host.UID != ref.UID {
		return "host Node was recreated", nil
	}
	return "", nil
}

// releaseVirtualNode deletes the virtual Node (UID precondition) and removes the VK finalizer. It
// refuses any Node without timeslice.io/virtual-node=true. It reports whether it changed anything.
func (c *Controller) releaseVirtualNode(ctx context.Context, vk *corev1.Node) (bool, error) {
	api := c.cs.CoreV1().Nodes()
	cur, err := api.Get(ctx, vk.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if cur.UID != vk.UID {
		return false, nil // a new Node with the same name: not the one to release
	}
	if cur.Labels[VirtualNodeLabel] != "true" {
		return false, fmt.Errorf("refusing to release Node %s: it has no %s=true label", vk.Name, VirtualNodeLabel)
	}
	uid := cur.UID
	acted := false
	if cur.DeletionTimestamp == nil {
		err := api.Delete(ctx, vk.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete virtual Node %s: %w", vk.Name, err)
		}
		acted = true
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := api.Get(ctx, vk.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if node.UID != uid || !slices.Contains(node.Finalizers, VirtualNodeFinalizer) {
			return nil
		}
		kept := slices.DeleteFunc(slices.Clone(node.Finalizers), func(f string) bool { return f == VirtualNodeFinalizer })
		meta := map[string]any{"finalizers": kept}
		if node.ResourceVersion != "" {
			meta["resourceVersion"] = node.ResourceVersion // optimistic lock: a conflict is retried
		}
		body, err := json.Marshal(map[string]any{"metadata": meta})
		if err != nil {
			return err
		}
		opts := metav1.PatchOptions{FieldManager: LabelledByValue}
		if _, err := api.Patch(ctx, vk.Name, types.MergePatchType, body, opts); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		acted = true
		return nil
	})
	return acted, err
}
