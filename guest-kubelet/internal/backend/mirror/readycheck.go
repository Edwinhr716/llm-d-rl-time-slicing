package mirror

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// ReadyCheck decides, after a resume, that the engine really serves again. It returns nil when
// it does, or an error when ctx ends first.
type ReadyCheck func(ctx context.Context, guest, mirror *corev1.Pod) error

// ProbeOnce runs one readiness probe attempt of container c against podIP; nil means it passed.
// The guest kubelet passes the M2 prober's (probe.NetProber), so a resume and the prober agree on
// what "ready" means.
type ProbeOnce func(ctx context.Context, podIP string, c *corev1.Container) error

// ProbeUntilReady is the resume ReadyCheck: it runs the guest's readinessProbe (httpGet or
// tcpSocket) against the mirror's pod IP every interval, with the probe's timeout per attempt,
// until one attempt passes. A guest without such a probe passes at once, as it is Ready as soon
// as its container runs. It does not wait for the prober's success threshold: the thawed
// process served before the freeze, so one pass is enough to lift the suspend state, and the
// prober's verdict then decides Ready as usual.
func ProbeUntilReady(interval time.Duration, once ProbeOnce) ReadyCheck {
	return func(ctx context.Context, guest, m *corev1.Pod) error {
		c, p := readinessProbe(guest)
		if p == nil || (p.HTTPGet == nil && p.TCPSocket == nil) {
			return nil
		}
		ip := m.Status.PodIP
		if ip == "" {
			return fmt.Errorf("mirror %s/%s has no pod IP", m.Namespace, m.Name)
		}
		timeout := time.Second
		if p.TimeoutSeconds > 0 {
			timeout = time.Duration(p.TimeoutSeconds) * time.Second
		}
		var last error
		for {
			actx, cancel := context.WithTimeout(ctx, timeout)
			last = once(actx, ip, c)
			cancel()
			if last == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("guest %s/%s not ready: %w (last: %w)", guest.Namespace, guest.Name, ctx.Err(), last)
			case <-time.After(interval):
			}
		}
	}
}

func readinessProbe(guest *corev1.Pod) (*corev1.Container, *corev1.Probe) {
	for i := range guest.Spec.Containers {
		if p := guest.Spec.Containers[i].ReadinessProbe; p != nil {
			return &guest.Spec.Containers[i], p
		}
	}
	return nil, nil
}
