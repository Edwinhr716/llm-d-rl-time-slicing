package mirror

import (
	"context"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// logReadopted runs once at startup with --mirror-identity=readopt. It changes nothing: the
// informer already holds every mirror, so the first GetPod for each guest finds its mirror and
// the library never creates a second one. It only records which mirrors this process took over.
func (b *Backend) logReadopted(ctx context.Context) error {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, m := range ms {
		if m.DeletionTimestamp != nil || m.Status.Phase == corev1.PodSucceeded || m.Status.Phase == corev1.PodFailed {
			continue
		}
		g, err := b.client.CoreV1().Pods(m.Namespace).Get(ctx, m.Annotations[AnnotationGuestName], metav1.GetOptions{})
		if err != nil || string(g.UID) != m.Labels[LabelMirrorOf] {
			continue // no live guest: orphanLoop reports it
		}
		log.G(ctx).WithField("guest", g.Namespace+"/"+g.Name).WithField("mirror", m.Name).
			WithField("mirrorUID", m.UID).WithField("jobID", m.Labels[LabelJobID]).
			WithField("podIP", m.Status.PodIP).WithField("cause", "vk-restart").
			Warn("re-adopted orphaned mirror")
	}
	return nil
}
