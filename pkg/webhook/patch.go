package webhook

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// patchOp is one RFC 6902 operation. The webhook adds, and replaces only the value of a
// container's PYTHONPATH (the RL integration injection prepends to it).
type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// patch collects the operations. Every helper checks the pod first and adds nothing when the field
// is already there, so a second admission of an admitted pod yields no operations (reinvocation).
// The env, volume and mount helpers also apply their change to the in-memory pod, so a later
// helper in the same admission sees it (and never re-adds a list another helper created).
type patch struct {
	ops []patchOp
	// base is the JSON pointer of the pod-shaped object the helpers edit: "" for a Pod, or the
	// path of a pod template inside another object (a RayCluster's group templates).
	base string
}

func (p *patch) add(path string, value any) {
	p.ops = append(p.ops, patchOp{Op: "add", Path: path, Value: value})
}

func (p *patch) replace(path string, value any) {
	p.ops = append(p.ops, patchOp{Op: "replace", Path: path, Value: value})
}

// escape encodes a JSON pointer token (RFC 6901).
func escape(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

func (p *patch) containerPath(i int) string {
	return p.base + "/spec/containers/" + strconv.Itoa(i)
}

func (p *patch) addLabel(pod *corev1.Pod, key, value string) {
	if len(pod.Labels) == 0 {
		p.add(p.base+"/metadata/labels", map[string]string{key: value})
		pod.Labels = map[string]string{key: value}
		return
	}
	p.add(p.base+"/metadata/labels/"+escape(key), value)
	pod.Labels[key] = value
}

func (p *patch) addAnnotation(pod *corev1.Pod, key, value string) {
	if len(pod.Annotations) == 0 {
		p.add(p.base+"/metadata/annotations", map[string]string{key: value})
		pod.Annotations = map[string]string{key: value}
		return
	}
	p.add(p.base+"/metadata/annotations/"+escape(key), value)
	pod.Annotations[key] = value
}

// ensureToleration adds toleration unless the pod already tolerates its key.
func (p *patch) ensureToleration(pod *corev1.Pod, tol *corev1.Toleration) {
	for i := range pod.Spec.Tolerations {
		if pod.Spec.Tolerations[i].Key == tol.Key {
			return
		}
	}
	if len(pod.Spec.Tolerations) == 0 {
		p.add(p.base+"/spec/tolerations", []corev1.Toleration{*tol})
		return
	}
	p.add(p.base+"/spec/tolerations/-", tol)
}

// ensureNodeSelector sets nodeSelector key=value unless it is already set to value.
func (p *patch) ensureNodeSelector(pod *corev1.Pod, key, value string) {
	if pod.Spec.NodeSelector[key] == value {
		return
	}
	if len(pod.Spec.NodeSelector) == 0 {
		p.add(p.base+"/spec/nodeSelector", map[string]string{key: value})
		return
	}
	p.add(p.base+"/spec/nodeSelector/"+escape(key), value)
}

// ensurePreferred adds a weight-100 preferred node affinity term key In [value], unless a preferred
// term already matches key with value.
func (p *patch) ensurePreferred(pod *corev1.Pod, key, value string) {
	term := corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{
			{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value}},
		}},
	}
	aff := pod.Spec.Affinity
	switch {
	case aff == nil:
		p.add(p.base+"/spec/affinity", corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{term},
		}})
	case aff.NodeAffinity == nil:
		p.add(p.base+"/spec/affinity/nodeAffinity", corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{term},
		})
	case hasPreferred(aff.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, key, value):
	case len(aff.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0:
		p.add(p.base+"/spec/affinity/nodeAffinity/preferredDuringSchedulingIgnoredDuringExecution",
			[]corev1.PreferredSchedulingTerm{term})
	default:
		p.add(p.base+"/spec/affinity/nodeAffinity/preferredDuringSchedulingIgnoredDuringExecution/-", term)
	}
}

func hasPreferred(terms []corev1.PreferredSchedulingTerm, key, value string) bool {
	for i := range terms {
		for _, req := range terms[i].Preference.MatchExpressions {
			if req.Key != key || req.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			for _, v := range req.Values {
				if v == value {
					return true
				}
			}
		}
	}
	return false
}

// ensureEnv adds each variable to every container that does not already define its name.
func (p *patch) ensureEnv(pod *corev1.Pod, vars []corev1.EnvVar) {
	for i := range pod.Spec.Containers {
		ctr := &pod.Spec.Containers[i]
		var missing []corev1.EnvVar
		for _, ev := range vars {
			if !hasEnv(ctr.Env, ev.Name) {
				missing = append(missing, ev)
			}
		}
		switch {
		case len(missing) == 0:
		case len(ctr.Env) == 0:
			p.add(p.containerPath(i)+"/env", missing)
		default:
			for _, ev := range missing {
				p.add(p.containerPath(i)+"/env/-", ev)
			}
		}
		ctr.Env = append(ctr.Env, missing...)
	}
}

func hasEnv(env []corev1.EnvVar, name string) bool {
	for i := range env {
		if env[i].Name == name {
			return true
		}
	}
	return false
}

// ensureVolume adds vol unless the pod has a volume of that name.
func (p *patch) ensureVolume(pod *corev1.Pod, vol *corev1.Volume) {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == vol.Name {
			return
		}
	}
	if len(pod.Spec.Volumes) == 0 {
		p.add(p.base+"/spec/volumes", []corev1.Volume{*vol})
	} else {
		p.add(p.base+"/spec/volumes/-", vol)
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, *vol)
}

// ensureMount adds mount to every container that has no mount of that volume.
func (p *patch) ensureMount(pod *corev1.Pod, mount *corev1.VolumeMount) {
	for i := range pod.Spec.Containers {
		mounts := pod.Spec.Containers[i].VolumeMounts
		found := false
		for j := range mounts {
			found = found || mounts[j].Name == mount.Name
		}
		switch {
		case found:
			continue
		case len(mounts) == 0:
			p.add(p.containerPath(i)+"/volumeMounts", []corev1.VolumeMount{*mount})
		default:
			p.add(p.containerPath(i)+"/volumeMounts/-", mount)
		}
		pod.Spec.Containers[i].VolumeMounts = append(mounts, *mount)
	}
}
