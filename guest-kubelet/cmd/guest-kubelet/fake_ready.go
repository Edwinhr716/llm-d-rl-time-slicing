package main

// D-VK-7 option "fake-ready": how a faked DaemonSet pod is removed after its delete.

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// deletePodNow deletes a pod with grace 0, only if it is still the same pod (UID precondition).
func deletePodNow(client kubernetes.Interface) provider.DeleteNowFunc {
	return func(ctx context.Context, ns, name string, uid types.UID) error {
		err := client.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{
			GracePeriodSeconds: new(int64),
			Preconditions:      &metav1.Preconditions{UID: &uid},
		})
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
}
