package mirror

import (
	"context"
	"encoding/json"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// FenceTaintKey is the NoSchedule taint the guest kubelet puts on the real node while a mirror
// still runs on a GPU whose donor has gone. Its value is the virtual node.
//
// Why: the donor's nvidia.com/gpu booking is what keeps the scheduler off the shared GPU. When
// the donor pod goes, the device plugin frees that device, and a pending pod could be bound to
// it while the mirror still uses it. The taint goes on as soon as the donor is marked for
// deletion (the scheduler only frees the device once the pod object is gone), the mirror is
// stopped, and the taint comes off once no such mirror remains. It blocks every pod that does
// not tolerate it, not just GPU pods, for the few seconds the mirror takes to stop.
const FenceTaintKey = "timeslice.io/gpu-fence"

// fenced returns this node's mirrors that still hold a GPU whose donor is gone (deleted,
// being deleted or finished). Terminating mirrors count until they are gone.
func (b *Backend) fenced() ([]*corev1.Pod, error) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	donors, err := b.donors.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	live := map[types.UID]bool{}
	for _, d := range donors {
		if d.DeletionTimestamp == nil && !podTerminal(d) {
			live[d.UID] = true
		}
	}
	out := make([]*corev1.Pod, 0, len(ms))
	for _, m := range ms {
		uid := m.Annotations[AnnotationGPUDonorUID]
		if uid == "" || podTerminal(m) || live[types.UID(uid)] {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (b *Backend) pokeFence() {
	select {
	case b.fenceKick <- struct{}{}:
	default:
	}
}

// fenceLoop is level-triggered: on every donor or mirror change, and every FenceInterval, it
// recomputes the fenced mirrors and makes the taint and the mirrors match.
func (b *Backend) fenceLoop(ctx context.Context) {
	t := time.NewTicker(b.opts.FenceInterval)
	defer t.Stop()
	for {
		b.reconcileFence(ctx)
		select {
		case <-ctx.Done():
			return
		case <-b.fenceKick:
		case <-t.C:
		}
	}
}

func (b *Backend) reconcileFence(ctx context.Context) {
	fenced, err := b.fenced()
	if err != nil {
		log.G(ctx).WithError(err).Warn("fence: list failed")
		return
	}
	if err := b.ensureFenceTaint(ctx, len(fenced) > 0); err != nil {
		log.G(ctx).WithError(err).Warn("fence: taint update failed; retrying")
		// Still stop the mirrors: without the taint a pending pod may get the GPU, but a
		// mirror left running on it is the worse outcome.
	}
	for _, mir := range fenced {
		if mir.DeletionTimestamp != nil {
			continue
		}
		uid := mir.UID
		err := b.client.CoreV1().Pods(mir.Namespace).Delete(ctx, mir.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			log.G(ctx).WithError(err).WithField("mirror", mir.Namespace+"/"+mir.Name).Warn("fence: mirror delete failed; retrying")
			continue
		}
		log.G(ctx).WithField("mirror", mir.Namespace+"/"+mir.Name).
			WithField("donorUID", mir.Annotations[AnnotationGPUDonorUID]).Warn("donor gone; GPU fenced and mirror stopped")
		if g := b.guestFor(mir); g != nil && b.opts.Recorder != nil {
			b.opts.Recorder.Event(g, corev1.EventTypeWarning, EventDonorGone,
				"the donor pod holding this guest's GPUs is gone; the mirror was stopped")
		}
	}
}

// findFenceTaint returns the node's fence taint, or nil.
func findFenceTaint(node *corev1.Node) *corev1.Taint {
	for i := range node.Spec.Taints {
		if t := &node.Spec.Taints[i]; t.Key == FenceTaintKey && t.Effect == corev1.TaintEffectNoSchedule {
			return t
		}
	}
	return nil
}

// ensureFenceTaint adds or removes the fence taint on the real node. It only calls the API
// to check the taint while mirrors are fenced, or when it may have to come off.
func (b *Backend) ensureFenceTaint(ctx context.Context, want bool) error {
	b.mu.Lock()
	known, state := b.fenceKnown, b.fenceState
	b.mu.Unlock()
	if known && !state && !want {
		return nil
	}
	nodes := b.client.CoreV1().Nodes()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(ctx, b.opts.HostNode, metav1.GetOptions{})
		if err != nil {
			return err
		}
		fence := findFenceTaint(node)
		present := fence != nil
		taints := make([]corev1.Taint, 0, len(node.Spec.Taints)+1)
		switch {
		case want && !present:
			taints = append(append(taints, node.Spec.Taints...), corev1.Taint{
				Key: FenceTaintKey, Value: b.opts.VirtualNode, Effect: corev1.TaintEffectNoSchedule,
			})
		case !want && present && fence.Value == b.opts.VirtualNode:
			for _, t := range node.Spec.Taints {
				if t.Key != FenceTaintKey {
					taints = append(taints, t)
				}
			}
		default:
			return nil // already right, or another guest kubelet's fence (left alone)
		}
		// resourceVersion in a merge patch makes it conditional: a concurrent taint change
		// fails with Conflict and is retried on the fresh node.
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{"resourceVersion": node.ResourceVersion},
			"spec":     map[string]any{"taints": taints},
		})
		if err != nil {
			return err
		}
		if _, err := nodes.Patch(ctx, node.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return err
		}
		if want {
			log.G(ctx).WithField("node", node.Name).WithField("taint", FenceTaintKey).Warn("fence taint added")
		} else {
			log.G(ctx).WithField("node", node.Name).WithField("taint", FenceTaintKey).Info("fence taint removed")
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.fenceKnown, b.fenceState = true, want
	b.mu.Unlock()
	return nil
}
