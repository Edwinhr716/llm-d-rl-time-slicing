// Package prepull pulls a guest's images onto the host as soon as the guest is admitted.
//
// A guest's mirror pod is created on the host only when the donor lends the GPU. If the host
// does not have the guest's images yet, the first lend spends its window pulling them (a vLLM
// image is several GB). The prepuller closes that gap: when the guest kubelet admits a guest it
// creates a short-lived pod on the host with the guest's images, whose containers exit at once,
// waits until the kubelet has pulled every image, and deletes it. The pod asks for almost
// nothing, holds no GPU and carries no timeslice labels, so it never counts as a GPU holder, a
// mirror or a guest. It runs under the guest's service account and image pull secrets, in the
// guest's namespace, so it can pull exactly what the mirror will pull.
package prepull

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/virtual-kubelet/virtual-kubelet/log"
)

const (
	// Suffix is appended to the guest's name for its prepull pod.
	Suffix = "-prepull"
	// LabelFor is the prepull pod's label: the UID of the guest it pulls for.
	LabelFor = "timeslice.io/prepull-for"
	// DefaultTimeout bounds one guest's pull.
	DefaultTimeout = 30 * time.Minute
	defaultPoll    = 2 * time.Second
)

// pulling are the waiting reasons of a container whose image is not on the node yet.
var pulling = map[string]bool{
	"":                  true,
	"ContainerCreating": true,
	"PodInitializing":   true,
	"ErrImagePull":      true,
	"ImagePullBackOff":  true,
}

// Prepuller creates, watches and deletes the prepull pods of one host.
type Prepuller struct {
	client  kubernetes.Interface
	host    string
	base    context.Context //nolint:containedctx // the pulls outlive the CreatePod call that starts them
	Timeout time.Duration
	Poll    time.Duration

	mu       sync.Mutex
	inflight map[string]context.CancelFunc // guest UID -> cancel
}

// New returns a prepuller for host. base bounds every pull (the guest kubelet's lifetime).
func New(base context.Context, client kubernetes.Interface, host string) *Prepuller {
	return &Prepuller{client: client, host: host, base: base, Timeout: DefaultTimeout, Poll: defaultPoll,
		inflight: map[string]context.CancelFunc{}}
}

// Prepull starts the pull of guest's images in the background; a guest already being pulled for
// is skipped. It never blocks and never fails the guest: a failed prepull only means the mirror
// pulls the images itself, as it would without the prepuller.
func (p *Prepuller) Prepull(_ context.Context, guest *corev1.Pod) {
	uid := string(guest.UID)
	p.mu.Lock()
	if _, ok := p.inflight[uid]; ok {
		p.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(p.base, p.Timeout)
	p.inflight[uid] = cancel
	p.mu.Unlock()
	go func() {
		defer func() {
			cancel()
			p.mu.Lock()
			delete(p.inflight, uid)
			p.mu.Unlock()
		}()
		p.run(ctx, guest)
	}()
}

// Forget stops the pull for a deleted guest (its pod is deleted by run's cleanup).
func (p *Prepuller) Forget(guest *corev1.Pod) {
	p.mu.Lock()
	cancel := p.inflight[string(guest.UID)]
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (p *Prepuller) run(ctx context.Context, guest *corev1.Pod) {
	l := log.G(ctx).WithField("guest", guest.Namespace+"/"+guest.Name).WithField("host", p.host)
	pod := Pod(guest, p.host)
	pods := p.client.CoreV1().Pods(guest.Namespace)
	start := time.Now()
	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		l.WithError(err).Warn("prepull: cannot create the prepull pod; the mirror will pull the images itself")
		return
	}
	defer func() {
		// Cleanup outlives ctx (a cancelled pull still removes its pod).
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		zero := int64(0)
		err := pods.Delete(dctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		if err != nil && !apierrors.IsNotFound(err) {
			l.WithError(err).Warn("prepull: cannot delete the prepull pod")
		}
	}()
	tick := time.NewTicker(p.Poll)
	defer tick.Stop()
	for {
		got, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
		if err == nil && Pulled(got) {
			l.WithField("seconds", fmt.Sprintf("%.1f", time.Since(start).Seconds())).
				WithField("images", len(pod.Spec.Containers)).Info("prepull: guest images are on the host")
			return
		}
		select {
		case <-ctx.Done():
			l.WithError(ctx.Err()).Info("prepull: stopped before the images were pulled")
			return
		case <-tick.C:
		}
	}
}

// Pulled reports whether the kubelet is past pulling every image of the prepull pod: each
// container is running, terminated, or waiting for a reason other than a pull (a start error of
// the no-op command still means the image is there), or the pod has finished.
func Pulled(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	if len(pod.Status.ContainerStatuses) < len(pod.Spec.Containers) {
		return false
	}
	for i := range pod.Status.ContainerStatuses {
		s := pod.Status.ContainerStatuses[i].State
		if s.Waiting != nil && pulling[s.Waiting.Reason] {
			return false
		}
		if s.Waiting == nil && s.Running == nil && s.Terminated == nil {
			return false
		}
	}
	return true
}

// Pod is the prepull pod for guest on host: one container per distinct image of the guest
// (init and app containers), each running a no-op.
func Pod(guest *corev1.Pod, host string) *corev1.Pod {
	var images []string
	seen := map[string]bool{}
	for _, cs := range [][]corev1.Container{guest.Spec.InitContainers, guest.Spec.Containers} {
		for i := range cs {
			if img := cs[i].Image; img != "" && !seen[img] {
				seen[img] = true
				images = append(images, img)
			}
		}
	}
	no, never := false, int64(0)
	ctrs := make([]corev1.Container, 0, len(images))
	for i, img := range images {
		ctrs = append(ctrs, corev1.Container{
			Name:            fmt.Sprintf("pull-%d", i),
			Image:           img,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"sh", "-c", "exit 0"},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m"),
					corev1.ResourceMemory: resource.MustParse("8Mi")},
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")},
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &no,
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      guest.Name + Suffix,
			Namespace: guest.Namespace,
			Labels: map[string]string{
				LabelFor:                       string(guest.UID),
				"app.kubernetes.io/managed-by": "guest-kubelet",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      host,
			RestartPolicy:                 corev1.RestartPolicyNever,
			ServiceAccountName:            guest.Spec.ServiceAccountName,
			AutomountServiceAccountToken:  &no,
			EnableServiceLinks:            &no,
			ImagePullSecrets:              guest.Spec.ImagePullSecrets,
			TerminationGracePeriodSeconds: &never,
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			SecurityContext:               &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers:                    ctrs,
		},
	}
}
