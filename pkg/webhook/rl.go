package webhook

import (
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// RL integration injection (--rl-integration-image). The RL team runs its own verl image; the
// webhook adds the timeslice Python packages (the timeslice client and the llm-d-timeslice-verl
// plugin that registers async_training.trainer_name=timeslice) at admission:
//
//   - an emptyDir volume RLIntegrationVolume;
//   - an init container RLIntegrationInit (the --rl-integration-image) that copies the image's
//     /opt/timeslice-rl/python into that volume;
//   - a read-only mount of the volume at --rl-integration-path in every container;
//   - PYTHONPATH=<path>/python in front of the container's own PYTHONPATH, so the injected
//     packages win over copies installed in the image's site-packages.
//
// Donor pods get it with the rest of the donor wiring. Ray pods of the same job that are not
// donors (the head, the rollout workers: the job driver and verl actors can run there and import
// the plugin) opt in with the label timeslice.io/rl-integration=true and get only this injection.
const (
	// LabelRLIntegration opts a non-donor pod into the RL integration injection.
	LabelRLIntegration = "timeslice.io/rl-integration"
	// RLIntegrationVolume is the emptyDir the init container fills.
	RLIntegrationVolume = "timeslice-rl"
	// RLIntegrationInit is the name of the injected init container.
	RLIntegrationInit = "timeslice-rl-integration"
	// DefaultRLIntegrationPath is the default --rl-integration-path.
	DefaultRLIntegrationPath = "/opt/timeslice-rl"
	// RLIntegrationSource is the package tree inside the init image.
	RLIntegrationSource = "/opt/timeslice-rl/python"
	// rlIntegrationTarget is where the init container mounts the volume.
	rlIntegrationTarget = "/timeslice-rl"
	// EnvPythonPath is the variable the injection prepends to.
	EnvPythonPath = "PYTHONPATH"
)

// injectRLIntegration adds the volume, the init container, the mounts and PYTHONPATH. Every step
// checks the pod first, so a reinvocation adds nothing.
func (p *patch) injectRLIntegration(pod *corev1.Pod, cfg *Config) {
	if cfg.RLIntegrationImage == "" {
		return
	}
	p.ensureVolume(pod, &corev1.Volume{
		Name:         RLIntegrationVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	p.ensureInitContainer(pod, rlInitContainer(cfg.RLIntegrationImage))
	p.ensureMount(pod, &corev1.VolumeMount{Name: RLIntegrationVolume, MountPath: cfg.RLIntegrationPath, ReadOnly: true})
	p.ensurePythonPath(pod, path.Join(cfg.RLIntegrationPath, "python"))
}

// rlInitContainer copies the packages into the volume. It satisfies the restricted Pod Security
// profile and asks for almost nothing (an init container's requests only count when they exceed
// the app containers').
func rlInitContainer(image string) *corev1.Container {
	yes, no, uid := true, false, int64(65532)
	return &corev1.Container{
		Name:            RLIntegrationInit,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"cp", "-R", RLIntegrationSource, rlIntegrationTarget + "/"},
		VolumeMounts:    []corev1.VolumeMount{{Name: RLIntegrationVolume, MountPath: rlIntegrationTarget}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot:             &yes,
			RunAsUser:                &uid,
			AllowPrivilegeEscalation: &no,
			ReadOnlyRootFilesystem:   &yes,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
}

// ensureInitContainer appends c unless the pod has an init container of that name.
func (p *patch) ensureInitContainer(pod *corev1.Pod, c *corev1.Container) {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == c.Name {
			return
		}
	}
	if len(pod.Spec.InitContainers) == 0 {
		p.add("/spec/initContainers", []corev1.Container{*c})
	} else {
		p.add("/spec/initContainers/-", c)
	}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, *c)
}

// ensurePythonPath puts dir first in every container's PYTHONPATH: it adds the variable, or
// prepends dir to a literal value. A PYTHONPATH from valueFrom is left alone (it cannot be
// prepended to); so is a PYTHONPATH the image sets in its own ENV, which the added variable
// replaces (the documented image assumption).
func (p *patch) ensurePythonPath(pod *corev1.Pod, dir string) {
	for i := range pod.Spec.Containers {
		ctr := &pod.Spec.Containers[i]
		j := envIndex(ctr.Env, EnvPythonPath)
		switch {
		case j < 0:
			ev := corev1.EnvVar{Name: EnvPythonPath, Value: dir}
			if len(ctr.Env) == 0 {
				p.add(containerPath(i)+"/env", []corev1.EnvVar{ev})
			} else {
				p.add(containerPath(i)+"/env/-", ev)
			}
			ctr.Env = append(ctr.Env, ev)
		case ctr.Env[j].ValueFrom != nil:
		case strings.Split(ctr.Env[j].Value, ":")[0] == dir:
		default:
			v := dir
			if ctr.Env[j].Value != "" {
				v = dir + ":" + ctr.Env[j].Value
			}
			p.replace(containerPath(i)+"/env/"+strconv.Itoa(j)+"/value", v)
			ctr.Env[j].Value = v
		}
	}
}

func envIndex(env []corev1.EnvVar, name string) int {
	for i := range env {
		if env[i].Name == name {
			return i
		}
	}
	return -1
}
