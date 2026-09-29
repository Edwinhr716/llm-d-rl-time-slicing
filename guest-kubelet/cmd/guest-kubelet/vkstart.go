package main

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Startup and shutdown log lines for pending lead decision D-NS-11 (option keep: Deployment;
// option ns-ds: DaemonSet). They only log; nothing here changes what the VK does.

// controllerLookupTimeout bounds the one Get of the VK's own pod at startup.
const controllerLookupTimeout = 5 * time.Second

// controllerKind names the workload that runs this pod, from its controller ownerReference.
// A ReplicaSet is reported as Deployment: the VK is never deployed as a bare ReplicaSet.
func controllerKind(pod *corev1.Pod) string {
	ref := metav1.GetControllerOf(pod)
	switch {
	case ref == nil:
		return "none"
	case ref.Kind == "ReplicaSet":
		return "Deployment"
	default:
		return ref.Kind
	}
}

// lookupController reads the VK's own pod (POD_NAMESPACE/POD_NAME) and returns controllerKind,
// or "unknown" when the pod cannot be read.
func lookupController(ctx context.Context, client kubernetes.Interface, namespace, name string) string {
	if namespace == "" || name == "" {
		return "unknown"
	}
	ctx, cancel := context.WithTimeout(ctx, controllerLookupTimeout)
	defer cancel()
	pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		log.G(ctx).WithError(err).Warn("could not read own pod for the controller kind")
		return "unknown"
	}
	return controllerKind(pod)
}

// started is the VK that logStart announced, for logStop. main and run share one goroutine.
var started struct{ host, node string }

// vkClient is run's API client, with the --kube-api-qps and --kube-api-burst rate limit. It
// also writes the "vk starting" line.
func vkClient(ctx context.Context, o *options) (*kubernetes.Clientset, error) {
	cfg, err := restConfig(o.kubeconfig)
	if err != nil {
		return nil, err
	}
	withRateLimit(cfg, o.kubeAPIQPS, o.kubeAPIBurst)
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	logStart(ctx, client, o)
	return client, nil
}

// logStart writes msg="vk starting" with the host, the virtual Node and the controller kind.
func logStart(ctx context.Context, client kubernetes.Interface, o *options) {
	started.host, started.node = o.hostNode, o.nodeName
	log.G(ctx).WithField("host", o.hostNode).WithField("node", o.nodeName).
		WithField("controller", lookupController(ctx, client, o.leaseNamespace, o.podName)).
		Info("vk starting")
}

// logStop writes msg="vk stopping" when a signal (SIGTERM from the kubelet, for example on a
// rollout, a pod delete or a DaemonSet unbind when the donor label goes) ends the VK. The VK
// does not deregister (delete) its Node on exit: that is --deregister, run separately.
func logStop(ctx context.Context) {
	if ctx.Err() == nil || started.node == "" {
		return
	}
	log.G(context.WithoutCancel(ctx)).WithField("host", started.host).WithField("node", started.node).
		WithField("deregister", false).Info("vk stopping; the virtual Node is left registered")
}

// withRateLimit sets the client-side rate limit; zero or less keeps client-go's default (5 QPS,
// burst 10). At the default, a Resume that creates mirrors and reads donor pods while the
// informers and status updates also call the API waits in the limiter until its context ends
// ("client rate limiter Wait returned an error").
func withRateLimit(cfg *rest.Config, qps float64, burst int) {
	if qps > 0 {
		cfg.QPS = float32(qps)
	}
	if burst > 0 {
		cfg.Burst = burst
	}
}
