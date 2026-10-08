package webhook

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Donor memory raise (--donor-gpu-memory). While the trainer's GPU is lent, the snapshot agent
// checkpoints the trainer's GPU memory (cuda-checkpoint) into host memory charged to the donor
// container's memory cgroup. A limit sized for the trainer's host footprint alone would OOM-kill
// the trainer at its first lend, so the webhook adds one GPU's memory per GPU to each GPU
// container's memory limit. Requests are left alone (the scheduler packs on requests), and a
// container without a memory limit has nothing to raise.
const (
	// DonorGPUMemoryAuto picks the per-GPU memory from the pod's GPU model.
	DonorGPUMemoryAuto = "auto"
	// DefaultDonorGPUMemory is the auto fallback when the pod does not name a known GPU model:
	// the largest common size (A100 80GB, H100), so the limit is never too small for those.
	DefaultDonorGPUMemory = "80Gi"
	// AnnotationMemoryRaised records the raise on the pod, so a reinvocation does not add it twice.
	AnnotationMemoryRaised = "timeslice.io/donor-memory-raised"
	// GPUResourceName is the device plugin resource a donor holds.
	GPUResourceName corev1.ResourceName = "nvidia.com/gpu"
)

// GPUMemoryByModel is the device memory of one GPU, by the model value of the GKE accelerator
// label (cloud.google.com/gke-accelerator) or NVIDIA feature discovery (nvidia.com/gpu.product).
var GPUMemoryByModel = map[string]string{
	"nvidia-tesla-t4":       "16Gi",
	"nvidia-l4":             "24Gi",
	"nvidia-tesla-a100":     "40Gi",
	"nvidia-a100-80gb":      "80Gi",
	"nvidia-h100-80gb":      "80Gi",
	"nvidia-h100-mega-80gb": "80Gi",
	"nvidia-h200-141gb":     "141Gi",
	"nvidia-b200":           "192Gi",
	"NVIDIA-L4":             "24Gi",
	"NVIDIA-A100-SXM4-40GB": "40Gi",
	"NVIDIA-A100-SXM4-80GB": "80Gi",
	"NVIDIA-H100-80GB-HBM3": "80Gi",
}

// gpuModelLabels are the node labels that name a GPU model, most specific first.
var gpuModelLabels = []string{"cloud.google.com/gke-accelerator", "nvidia.com/gpu.product"}

// parseDonorGPUMemory returns (auto, fixed quantity, error). auto=false with a zero quantity is off.
func parseDonorGPUMemory(v string) (bool, resource.Quantity, error) {
	if v == DonorGPUMemoryAuto {
		return true, resource.MustParse(DefaultDonorGPUMemory), nil
	}
	q, err := resource.ParseQuantity(v)
	if err != nil {
		return false, resource.Quantity{}, fmt.Errorf("want auto, 0 or a quantity, got %q", v)
	}
	if q.Sign() < 0 {
		return false, resource.Quantity{}, fmt.Errorf("negative quantity %q", v)
	}
	return false, q, nil
}

// podGPUModel returns the GPU model the pod selects with a nodeSelector or a required node
// affinity term with a single In value, or "".
func podGPUModel(pod *corev1.Pod) string {
	for _, l := range gpuModelLabels {
		if v := pod.Spec.NodeSelector[l]; v != "" {
			return v
		}
	}
	aff := pod.Spec.Affinity
	if aff == nil || aff.NodeAffinity == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, req := range term.MatchExpressions {
			for _, l := range gpuModelLabels {
				if req.Key == l && req.Operator == corev1.NodeSelectorOpIn && len(req.Values) == 1 {
					return req.Values[0]
				}
			}
		}
	}
	return ""
}

// perGPUMemory is the raise per GPU for this pod, or zero when the raise is off.
func perGPUMemory(pod *corev1.Pod, setting string) resource.Quantity {
	auto, q, err := parseDonorGPUMemory(setting)
	if err != nil || !auto {
		return q
	}
	if v, ok := GPUMemoryByModel[podGPUModel(pod)]; ok {
		return resource.MustParse(v)
	}
	return q
}

// raiseDonorMemory adds per-GPU memory x GPUs to the memory limit of every container that holds
// nvidia.com/gpu and has a memory limit, once per pod (AnnotationMemoryRaised).
func (p *patch) raiseDonorMemory(pod *corev1.Pod, setting string) {
	if _, done := pod.Annotations[AnnotationMemoryRaised]; done {
		return
	}
	per := perGPUMemory(pod, setting)
	if per.Sign() <= 0 {
		return
	}
	raised := false
	for i := range pod.Spec.Containers {
		res := &pod.Spec.Containers[i].Resources
		gpus, okGPU := res.Limits[GPUResourceName]
		if !okGPU {
			gpus, okGPU = res.Requests[GPUResourceName]
		}
		limit, okMem := res.Limits[corev1.ResourceMemory]
		if !okGPU || !okMem || gpus.Value() <= 0 {
			continue
		}
		add := per.DeepCopy()
		add.Mul(gpus.Value())
		limit.Add(add)
		v := resource.NewQuantity(limit.Value(), resource.BinarySI)
		p.replace(p.containerPath(i)+"/resources/limits/memory", v.String())
		res.Limits[corev1.ResourceMemory] = *v
		raised = true
	}
	if raised {
		p.addAnnotation(pod, AnnotationMemoryRaised, per.String()+"/gpu")
	}
}
