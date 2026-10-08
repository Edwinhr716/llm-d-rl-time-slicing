package probe

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// runStartup is the startup gate (lead decision D-VK-5 c). It runs the container's
// startupProbe from the container start, with the probe's own initial delay, period and
// timeout, until it passes once, then marks the container started. It returns false if the
// worker was stopped first.
//
// A startupProbe that keeps failing past its failureThreshold is not acted on: the kubelet
// would restart the container, but the mirror carries no probes and the guest kubelet does not
// restart containers, so the container just stays not ready (and the Unhealthy events say why).
func (m *Manager) runStartup(
	ctx context.Context, job *worker, key workerKey, guest *corev1.Pod, target Target, startedAt time.Time,
) bool {
	spec := target.Container.StartupProbe
	target.Probe = spec
	if !waitUntil(ctx, startedAt.Add(seconds(spec.InitialDelaySeconds, 0))) {
		return false
	}
	ticker := time.NewTicker(seconds(spec.PeriodSeconds, defaultPeriod))
	defer ticker.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, seconds(spec.TimeoutSeconds, defaultTimeout))
		err := m.opts.Prober.Probe(probeCtx, target)
		cancel()
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			// successThreshold of a startupProbe must be 1 (API validation).
			if m.setStarted(job, key) {
				m.opts.OnChange(guest.Namespace, guest.Name)
			}
			return true
		}
		m.eventf(guest, corev1.EventTypeWarning, ReasonUnhealthy, "Startup probe failed: %v", err)
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// setStarted marks a container started if job is still its current worker, and reports whether
// it did.
func (m *Manager) setStarted(job *worker, key workerKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workers[key] != job {
		return false
	}
	m.started[key] = true
	return true
}
