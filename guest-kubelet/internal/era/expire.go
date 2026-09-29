package era

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// end runs the era end in order: stop admitting, expire guests, hand back the grant, stop the
// node controller, catch anything created meanwhile, deregister the Node.
func (c *Controller) end(ctx context.Context, trigger string, donors int) {
	log.G(ctx).WithField("host", c.cfg.Host).WithField("node", c.cfg.VKNode).WithField("trigger", trigger).
		Info("era ending")
	c.closed.Store(true)
	c.expireGuests(ctx)
	if c.hooks.Yield != nil {
		if err := c.hooks.Yield(ctx); err != nil {
			log.G(ctx).WithError(err).Warn("era: yield of the background grant failed")
		}
	}
	if c.hooks.Stop != nil {
		c.hooks.Stop(ctx)
	}
	// A create that raced the gate, or a guest bound meanwhile: a second pass finds it.
	c.expireGuests(ctx)
	c.state = StateEnded
	c.deregister(ctx)
	c.idleSince, c.persisted, c.sawLabel = time.Time{}, time.Time{}, false
	c.logState(ctx, donors, time.Time{})
}

type expiry struct {
	guest, mirror *corev1.Pod
	path          string
}

// expireGuests expires every unfinished pod bound to the virtual Node. Running guests: delete
// the mirror (normal grace), wait for it to go (up to MirrorWait), mark the guest Failed with
// reason EraEnded. Suspended or in-doubt guests go through the Kill hook first. Mirrors of
// this Node without a live guest are deleted too.
func (c *Controller) expireGuests(ctx context.Context) {
	pods := c.cs.CoreV1().Pods(corev1.NamespaceAll)
	gl, err := pods.List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", c.cfg.VKNode).String()})
	if err != nil {
		log.G(ctx).WithError(err).Warn("era: list guests failed")
		return
	}
	ml, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labels.Set{mirror.LabelMirrorNode: c.cfg.VKNode}.String()})
	if err != nil {
		log.G(ctx).WithError(err).Warn("era: list mirrors failed")
		return
	}
	mirrors := map[string]*corev1.Pod{}
	for i := range ml.Items {
		m := &ml.Items[i]
		mirrors[m.Namespace+"/"+m.Name] = m
	}
	var todo []expiry
	var deleted []*corev1.Pod
	for i := range gl.Items {
		g := &gl.Items[i]
		if g.Spec.NodeName != c.cfg.VKNode || terminal(g) {
			continue
		}
		e := expiry{guest: g, path: "delete"}
		key := g.Namespace + "/" + mirror.Name(g.Name)
		if m, ok := mirrors[key]; ok && g.UID != "" && m.Labels[mirror.LabelMirrorOf] == string(g.UID) {
			e.mirror = m
			delete(mirrors, key)
		}
		if c.hooks.Kill != nil {
			killed, err := c.hooks.Kill(ctx, g, e.mirror)
			if err != nil {
				log.G(ctx).WithError(err).WithField("pod", g.Namespace+"/"+g.Name).Warn("era: kill sequence failed; deleting the mirror")
			}
			if killed {
				e.path = "kill"
			}
		}
		if e.path == "delete" && e.mirror != nil && c.deleteMirror(ctx, e.mirror) {
			deleted = append(deleted, e.mirror)
		}
		todo = append(todo, e)
	}
	for _, m := range mirrors {
		if c.deleteMirror(ctx, m) {
			deleted = append(deleted, m)
		}
	}
	c.waitGone(ctx, deleted)
	for _, e := range todo {
		c.failGuest(ctx, e.guest, e.mirror, e.path)
	}
}

func terminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded
}

// deleteMirror deletes a mirror with its normal grace period (never force). It reports whether
// the mirror may still exist and must be waited for.
func (c *Controller) deleteMirror(ctx context.Context, m *corev1.Pod) bool {
	uid := m.UID
	opts := metav1.DeleteOptions{}
	if uid != "" {
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	err := c.cs.CoreV1().Pods(m.Namespace).Delete(ctx, m.Name, opts)
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return false
	}
	if err != nil {
		log.G(ctx).WithError(err).WithField("mirror", m.Namespace+"/"+m.Name).Warn("era: delete mirror failed")
		return false
	}
	log.G(ctx).WithField("mirror", m.Namespace+"/"+m.Name).WithField("reason", ReasonEraEnded).Info("mirror deleted")
	return true
}

// waitGone waits, up to MirrorWait, until every mirror is gone (or replaced by another UID).
func (c *Controller) waitGone(ctx context.Context, ms []*corev1.Pod) {
	if len(ms) == 0 {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, c.cfg.MirrorWait)
	defer cancel()
	for {
		ms = slices.DeleteFunc(ms, func(m *corev1.Pod) bool {
			cur, err := c.cs.CoreV1().Pods(m.Namespace).Get(wctx, m.Name, metav1.GetOptions{})
			return apierrors.IsNotFound(err) || (err == nil && cur.UID != m.UID)
		})
		if len(ms) == 0 {
			return
		}
		select {
		case <-wctx.Done():
			for _, m := range ms {
				log.G(ctx).WithField("mirror", m.Namespace+"/"+m.Name).
					Warn("era: mirror still terminating; marking its guest Failed anyway (no force delete)")
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// failGuest marks one guest Failed with reason EraEnded and records an Event on it.
func (c *Controller) failGuest(ctx context.Context, guest, lastMirror *corev1.Pod, path string) {
	msg := fmt.Sprintf("era ended on host %s: no donor pods and no lock activity for %s, or the host lost its group label",
		c.cfg.Host, c.cfg.EraTTL)
	pods := c.cs.CoreV1().Pods(guest.Namespace)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := pods.Get(ctx, guest.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if guest.UID != "" && cur.UID != guest.UID {
			return nil // a replacement pod with the same name; not ours to fail
		}
		if terminal(cur) && cur.Status.Reason == ReasonEraEnded {
			return nil
		}
		out := mirror.TerminalStatus(cur, lastMirror, ReasonEraEnded)
		out.Status.Message = msg
		_, err = pods.UpdateStatus(ctx, out, metav1.UpdateOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		err = nil // deleted meanwhile: expired as well
	}
	if err != nil {
		log.G(ctx).WithError(err).WithField("pod", guest.Namespace+"/"+guest.Name).Warn("era: could not mark guest Failed")
		return
	}
	if c.hooks.Recorder != nil {
		c.hooks.Recorder.Event(guest, corev1.EventTypeWarning, ReasonEraEnded, msg)
	}
	log.G(ctx).WithField("pod", guest.Namespace+"/"+guest.Name).WithField("reason", ReasonEraEnded).WithField("path", path).
		Info("guest expired")
}

// deregister removes the virtual Node: through ReleaseNode (reason deregister) when it carries
// the finalizer (NS shape, D-VK-2 c), a plain delete otherwise (TODAY shape). A failure is
// retried on every step while the era stays ended.
func (c *Controller) deregister(ctx context.Context) {
	nodes := c.cs.CoreV1().Nodes()
	n, err := nodes.Get(ctx, c.cfg.VKNode, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		c.deregPending = false
		return
	}
	if err != nil {
		c.deregPending = true
		log.G(ctx).WithError(err).WithField("node", c.cfg.VKNode).Warn("era: deregistration failed; retrying")
		return
	}
	uid := n.UID
	switch {
	case slices.Contains(n.Finalizers, provider.NodeFinalizer):
		_, err = provider.ReleaseNode(ctx, c.cs, c.cfg.VKNode, provider.ReasonDeregister)
	case n.Labels[provider.VirtualNodeLabel] != "true":
		err = fmt.Errorf("refusing to delete Node %s: it has no %s=true label", c.cfg.VKNode, provider.VirtualNodeLabel)
	default:
		opts := metav1.DeleteOptions{}
		if uid != "" {
			opts.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		err = nodes.Delete(ctx, c.cfg.VKNode, opts)
		if apierrors.IsNotFound(err) {
			err = nil
		}
	}
	if err != nil {
		c.deregPending = true
		log.G(ctx).WithError(err).WithField("node", c.cfg.VKNode).Warn("era: deregistration failed; retrying")
		return
	}
	c.deregPending = false
	log.G(ctx).WithField("node", c.cfg.VKNode).WithField("uid", string(uid)).Info("node deregistered")
}
