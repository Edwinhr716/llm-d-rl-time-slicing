package probe

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// runStartup is the startup gate. It runs the container's startupProbe from the container
// start, with the probe's own initial delay, period and timeout, until it passes once, then
// marks the container started. It returns false if the worker was stopped first.
//
// A startupProbe that keeps failing past its failureThreshold is not acted on: the kubelet
// would restart the container, but the mirror carries no probes and the guest kubelet does not
// restart containers, so the container just stays not ready (and the Unhealthy events say why).
func (m *Manager) runStartup(
	ctx context.Context, cur *worker, key workerKey, guest *corev1.Pod, target Target, startedAt time.Time,
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
			if m.setStarted(cur, key) {
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

// setStarted marks a container started if cur is still its current worker, and reports whether
// it did.
func (m *Manager) setStarted(cur *worker, key workerKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workers[key] != cur {
		return false
	}
	m.started[key] = true
	return true
}
