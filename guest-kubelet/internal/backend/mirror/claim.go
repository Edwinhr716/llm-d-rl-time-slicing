package mirror

import (
	"context"
	"fmt"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// reserveClaim does for the mirror what the scheduler's DRA plugin does for a pod it binds:
// add the pod to the claim's status.reservedFor. The kubelet only prepares a claim for pods
// listed there, and a mirror never passes through the scheduler. The claim must already be
// allocated (in the design, by the trainer that holds the GPU); the guest kubelet does not
// allocate devices. Optional: measured in M1, the resourceclaim controller in
// kube-controller-manager adds a pod that already has spec.nodeName to reservedFor by itself
// (about 1 s after the create), and removes the entry when the mirror is deleted.
func (b *Backend) reserveClaim(ctx context.Context, namespace, claimName string, m *corev1.Pod) error {
	claims := b.client.ResourceV1().ResourceClaims(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		c, err := claims.Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get claim %s/%s: %w", namespace, claimName, err)
		}
		if c.Status.Allocation == nil {
			return fmt.Errorf("claim %s/%s is not allocated; the GPU holder (trainer) must be scheduled first", namespace, claimName)
		}
		for _, r := range c.Status.ReservedFor {
			if r.Resource == "pods" && r.UID == m.UID {
				return nil
			}
		}
		c.Status.ReservedFor = append(c.Status.ReservedFor, resourcev1.ResourceClaimConsumerReference{
			Resource: "pods", Name: m.Name, UID: m.UID,
		})
		if _, err := claims.UpdateStatus(ctx, c, metav1.UpdateOptions{}); err != nil {
			return err
		}
		log.G(ctx).WithField("claim", namespace+"/"+claimName).WithField("mirror", m.Name).Info("mirror added to claim reservedFor")
		return nil
	})
}
