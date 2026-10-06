package main

// Clean up the virtual Node when the guest kubelet is stopped.
// See internal/provider/stop.go for the two outcomes.

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// stopCleanupTimeout bounds the stop cleanup; the kubelet's default grace is 30 s.
const stopCleanupTimeout = 10 * time.Second

// clearStoppedInterval is how often a serving VK re-checks for a stale stop cordon.
const clearStoppedInterval = 10 * time.Second

// roleRan is set once this process has guarded the virtual Node (it served it). Only such a
// process cleans up on stop: a standby or a VK that failed before registering leaves the Node
// to whoever serves it.
var roleRan atomic.Bool

// selfID tells this process apart from its predecessor even when the container restarts in
// place under the same pod name.
var selfID = fmt.Sprintf("%s@%d", os.Getenv("POD_NAME"), time.Now().UnixNano())

// startOwner is the controller of this pod, read once at start (logStart); nil if unknown.
var startOwner atomic.Pointer[metav1.OwnerReference]

type stopAction string

const (
	stopRestart   stopAction = "restart"   // cordon + NotReady, keep the Node and its guests
	stopUninstall stopAction = "uninstall" // delete the Node and remove the finalizer
)

// decideStop says whether the controller that runs this pod will start a replacement on this
// host. Uninstall when the pod is being deleted and its controller is gone, being deleted,
// scaled to zero, or (DaemonSet) no longer selects the host; restart otherwise. Any read
// error means restart: the cordon is safe and reversible, a delete is not.
//
// known is the controller this pod had at start (nil if unknown). A force delete
// (--grace-period=0 --force) removes the pod object before the signal arrives; the controller
// is then judged from known, so a DaemonSet pod that is force-deleted still counts as a restart.
func decideStop(ctx context.Context, client kubernetes.Interface, namespace, podName, hostNode string, known *metav1.OwnerReference) (stopAction, string) {
	if namespace == "" || podName == "" {
		return stopRestart, "own pod unknown (POD_NAMESPACE/POD_NAME unset)"
	}
	pod, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	var ref *metav1.OwnerReference
	switch {
	case apierrors.IsNotFound(err) && known == nil:
		return stopUninstall, "own pod already deleted; controller unknown"
	case apierrors.IsNotFound(err):
		ref = known
	case err != nil:
		return stopRestart, "own pod unreadable: " + err.Error()
	case pod.DeletionTimestamp == nil:
		return stopRestart, "own pod is not being deleted (container restart)"
	default:
		ref = metav1.GetControllerOf(pod)
	}
	if ref == nil {
		return stopUninstall, "bare pod deleted"
	}
	apps := client.AppsV1()
	switch ref.Kind {
	case "ReplicaSet":
		rs, err := apps.ReplicaSets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if gone, why := goneOrDeleting(err, rs, "ReplicaSet "+ref.Name); gone {
			return stopUninstall, why
		} else if err != nil {
			return stopRestart, why
		}
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
			return stopUninstall, "ReplicaSet " + rs.Name + " scaled to 0"
		}
		dref := metav1.GetControllerOf(rs)
		if dref == nil || dref.Kind != "Deployment" {
			return stopRestart, "ReplicaSet " + rs.Name + " still wants pods"
		}
		d, err := apps.Deployments(namespace).Get(ctx, dref.Name, metav1.GetOptions{})
		if gone, why := goneOrDeleting(err, d, "Deployment "+dref.Name); gone {
			return stopUninstall, why
		} else if err != nil {
			return stopRestart, why
		}
		if d.Spec.Replicas != nil && *d.Spec.Replicas == 0 {
			return stopUninstall, "Deployment " + d.Name + " scaled to 0"
		}
		return stopRestart, "Deployment " + d.Name + " still wants pods"
	case "DaemonSet":
		ds, err := apps.DaemonSets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if gone, why := goneOrDeleting(err, ds, "DaemonSet "+ref.Name); gone {
			return stopUninstall, why
		} else if err != nil {
			return stopRestart, why
		}
		host, err := client.CoreV1().Nodes().Get(ctx, hostNode, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return stopUninstall, "host node gone"
		}
		if err != nil {
			return stopRestart, "host node unreadable: " + err.Error()
		}
		if sel := ds.Spec.Template.Spec.NodeSelector; len(sel) > 0 && !labels.SelectorFromSet(sel).Matches(labels.Set(host.Labels)) {
			return stopUninstall, "DaemonSet " + ds.Name + " no longer selects host " + hostNode
		}
		return stopRestart, "DaemonSet " + ds.Name + " still selects host " + hostNode
	}
	return stopRestart, "controller kind " + ref.Kind + " not inspected"
}

// goneOrDeleting reports an object that is NotFound or has a deletionTimestamp. For any
// other error it returns false and the error text.
func goneOrDeleting(err error, obj metav1.Object, what string) (bool, string) {
	switch {
	case apierrors.IsNotFound(err):
		return true, what + " deleted"
	case err != nil:
		return false, what + " unreadable: " + err.Error()
	case obj.GetDeletionTimestamp() != nil:
		return true, what + " being deleted"
	}
	return false, ""
}

// stopCleanup runs on a signal, after the kubelet role ended, in this process only if it served
// the Node. It never runs on a crash or SIGKILL; see the stop cleanup notes in internal/provider/stop.go.
func stopCleanup(ctx context.Context, client kubernetes.Interface, o *options) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopCleanupTimeout)
	defer cancel()
	start := time.Now()
	action, why := decideStop(ctx, client, o.leaseNamespace, o.podName, o.hostNode, startOwner.Load())
	logger := log.G(ctx).WithField("node", o.nodeName).WithField("action", string(action)).WithField("why", why)
	var err error
	switch action {
	case stopUninstall:
		_, err = provider.ReleaseNode(ctx, client, o.nodeName, provider.ReasonUninstall)
	default:
		err = provider.MarkStopped(ctx, client.CoreV1().Nodes(), o.nodeName, selfID)
	}
	logger = logger.WithField("tookMs", time.Since(start).Milliseconds())
	if err != nil {
		logger.WithError(err).Error("stop cleanup failed; the virtual Node is left as it was")
		return
	}
	logger.Info("stop cleanup done")
}
