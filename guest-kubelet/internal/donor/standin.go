// Package donor is a stand-in for one duty of the donor controller, which does not exist yet:
// deleting an orphaned virtual Node when its real host is truly gone. Deleting the Node lets the
// pod garbage collector force-delete the guests bound to it, so their controllers retry.
//
// It exists for D-VK-2 option d: the virtual Node is protected by a ValidatingAdmissionPolicy
// that denies DELETE to everyone except the guest-kubelet and donor-controller service accounts,
// so on host death (the guest-kubelet runs on the dead host) this is the only actor that can
// delete it. It touches only Node objects.
package donor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// VirtualNodeLabel must be "true" on the Node before the stand-in deletes it, so a wrong
// --vk-node can never delete a real node. Same value as provider.VirtualNodeLabel.
const VirtualNodeLabel = "timeslice.io/virtual-node"

// Standin watches one real host and releases one virtual Node once the host is gone.
type Standin struct {
	Client   kubernetes.Interface
	VKNode   string        // the virtual Node to release
	HostNode string        // the real Node it lives on
	Poll     time.Duration // time between polls
	// Confirm is how many polls in a row must find the host Node absent before the host counts
	// as truly gone. Any other error resets nothing and deletes nothing (fail closed).
	Confirm int
	Log     *slog.Logger

	misses int
}

// Step runs one poll. It returns true once the virtual Node is released (deleted by this
// call, or already absent after the host was confirmed gone).
func (s *Standin) Step(ctx context.Context) (bool, error) {
	_, err := s.Client.CoreV1().Nodes().Get(ctx, s.HostNode, metav1.GetOptions{})
	switch {
	case err == nil:
		if s.misses > 0 {
			s.Log.Info("host node back", "host", s.HostNode, "misses", s.misses)
		}
		s.misses = 0
		return false, nil
	case !apierrors.IsNotFound(err):
		return false, fmt.Errorf("get host node %s: %w", s.HostNode, err)
	}
	s.misses++
	if s.misses < s.Confirm {
		s.Log.Info("host node missing", "host", s.HostNode, "misses", s.misses, "confirm", s.Confirm)
		return false, nil
	}

	vk, err := s.Client.CoreV1().Nodes().Get(ctx, s.VKNode, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		s.Log.Info("virtual node released", "node", s.VKNode, "reason", "host-gone", "already_gone", true)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get virtual node %s: %w", s.VKNode, err)
	}
	if vk.Labels[VirtualNodeLabel] != "true" {
		return false, fmt.Errorf("refusing to delete node %s: label %s is not \"true\"", s.VKNode, VirtualNodeLabel)
	}
	// Precondition on the uid: never delete a newer Node that took the same name.
	uid := vk.UID
	err = s.Client.CoreV1().Nodes().Delete(ctx, s.VKNode, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete virtual node %s: %w", s.VKNode, err)
	}
	s.Log.Info("virtual node released", "node", s.VKNode, "reason", "host-gone", "host", s.HostNode, "uid", string(uid))
	return true, nil
}

// Run polls until the virtual Node is released or ctx ends, then idles until ctx ends.
func (s *Standin) Run(ctx context.Context) {
	s.Log.Info("donor stand-in started", "vk_node", s.VKNode, "host", s.HostNode, "poll", s.Poll.String(), "confirm", s.Confirm)
	ticker := time.NewTicker(s.Poll)
	defer ticker.Stop()
	for {
		done, err := s.Step(ctx)
		if err != nil {
			s.Log.Error("poll failed", "err", err)
		}
		if done {
			<-ctx.Done()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
