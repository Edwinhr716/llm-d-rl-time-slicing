// Package testutil holds test helpers shared by several packages.
package testutil

import (
	"errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var nodesGVR = corev1.SchemeGroupVersion.WithResource("nodes")

// NewClient returns a fake clientset whose Node deletes honour finalizers like the API server:
// deleting a Node with finalizers only sets its deletionTimestamp, and the update that clears
// the last finalizer of a Node being deleted removes it.
func NewClient(objs ...runtime.Object) *fake.Clientset {
	client := fake.NewSimpleClientset(objs...)
	tracker := client.Tracker()
	client.PrependReactor("delete", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		del, ok := action.(k8stesting.DeleteAction)
		if !ok {
			return false, nil, nil
		}
		obj, err := tracker.Get(nodesGVR, "", del.GetName())
		if err != nil {
			return true, nil, err
		}
		node, ok := obj.(*corev1.Node)
		if !ok {
			return true, nil, errors.New("tracker returned a non-Node")
		}
		if len(node.Finalizers) == 0 {
			return false, nil, nil // the default reactor removes it
		}
		if node.DeletionTimestamp != nil {
			return true, nil, nil
		}
		node = node.DeepCopy()
		now := metav1.Now()
		node.DeletionTimestamp = &now
		return true, nil, tracker.Update(nodesGVR, node, "")
	})
	client.PrependReactor("update", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		upd, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		node, ok := upd.GetObject().(*corev1.Node)
		if !ok || node.DeletionTimestamp == nil || len(node.Finalizers) > 0 {
			return false, nil, nil
		}
		return true, node, tracker.Delete(nodesGVR, "", node.Name)
	})
	return client
}
