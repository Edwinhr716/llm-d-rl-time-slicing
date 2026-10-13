package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// checkProbesOptionB is lead decision D-VK-5 option b: a guest carries no probes at all. Any
// livenessProbe, readinessProbe or startupProbe, on any container (init containers included),
// is refused, and so are readinessGates, which only make sense with a readiness signal the
// guest kubelet would have to honour. Ready then rests on the guest kubelet's own checks
// (the container running, and later tasks' serve check and NotReady-before-suspend).
func checkProbesOptionB(pod *corev1.Pod) *Rejection {
	containers := allContainers(pod)
	for i := range containers {
		ctr := &containers[i]
		for _, pr := range []struct {
			field, rule string
			probe       *corev1.Probe
		}{
			{"livenessProbe", "liveness-probe", ctr.LivenessProbe},
			{"readinessProbe", "readiness-probe", ctr.ReadinessProbe},
			{"startupProbe", "startup-probe", ctr.StartupProbe},
		} {
			if pr.probe != nil {
				return &Rejection{pr.rule, fmt.Sprintf(
					"container %q has a %s; guests may not have probes (guest probe policy b)", ctr.Name, pr.field)}
			}
		}
	}
	if n := len(pod.Spec.ReadinessGates); n > 0 {
		return &Rejection{"readiness-gates", fmt.Sprintf(
			"pod has %d readinessGates (first %q); guests may not have readiness gates (guest probe policy b)",
			n, pod.Spec.ReadinessGates[0].ConditionType)}
	}
	return nil
}
