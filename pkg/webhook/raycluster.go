package webhook

import (
	"encoding/json"
	"fmt"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

// RayCluster wiring. In verl's fully-async mode the timeslice trainer's driver is a CPU-only Ray
// actor that can be placed on any Ray pod of the job, and the job's entrypoint runs on a Ray
// pod too, so the timeslice Python packages and the TIMESLICE_* settings are needed in every pod
// of the Ray cluster, not only in the donor (trainer) pods. The RL team labels only the trainer
// worker group timeslice.io/donor=true; at RayCluster creation (KubeRay creates the RayCluster of
// a RayJob) this hook finds that group and, in every group template:
//
//   - labels the head and the non-donor worker groups timeslice.io/rl-integration=true, so their
//     pods get the RL integration injection (W13) when they are created;
//   - with --donor-client-wiring=env and exactly one donor group, sets TIMESLICE_FULLY_ASYNC=1
//     (the hooks' activation gate), TIMESLICE_JOB_ID, TIMESLICE_GROUP and TIMESLICE_ORCH_ADDR in
//     every container: the same job id and group the donor pods derive (job id = the RayCluster
//     name = ray.io/cluster, group = <ns>.<cluster>.<donor group>), or the donor template's
//     explicit timeslice.io/job-id and timeslice.io/group labels;
//   - in the same case sets NCCL_CUMEM_ENABLE=0 and NCCL_NVLS_ENABLE=0 in every container: NCCL
//     then allocates no cuMem/NVLS buffers, which cuda-checkpoint does not handle, in the trainer
//     and in the rollout side of the weight-sync group.
//
// A RayCluster without a donor group is not changed. Like the pod path it is a pure function of
// the request: no client, no RBAC. Variables the user already set are kept.
const (
	// EnvFullyAsync is the activation gate of the verl lifecycle hooks.
	EnvFullyAsync = "TIMESLICE_FULLY_ASYNC"
	// EnvNCCLCuMem and EnvNCCLNVLS turn off NCCL's cuMem and NVLS allocations (set to "0").
	EnvNCCLCuMem = "NCCL_CUMEM_ENABLE"
	EnvNCCLNVLS  = "NCCL_NVLS_ENABLE"
	// RuleRayCluster: the Ray cluster of a donor group was wired.
	RuleRayCluster = "W14"
	// KindRayCluster is a RayCluster admission.
	KindRayCluster Kind = "raycluster"
)

// rayTemplate is the part of a RayCluster group the hook reads.
type rayTemplate struct {
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type rayGroup struct {
	GroupName string                 `json:"groupName"`
	Template  corev1.PodTemplateSpec `json:"template"`
}

type rayClusterObj struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		HeadGroupSpec struct {
			Template corev1.PodTemplateSpec `json:"template"`
		} `json:"headGroupSpec"`
		WorkerGroupSpecs []rayGroup `json:"workerGroupSpecs"`
	} `json:"spec"`
}

// rayClusterRaw tells which group templates have a metadata object (JSON patch adds need the parent).
type rayClusterRaw struct {
	Spec struct {
		HeadGroupSpec struct {
			Template rayTemplate `json:"template"`
		} `json:"headGroupSpec"`
		WorkerGroupSpecs []struct {
			Template rayTemplate `json:"template"`
		} `json:"workerGroupSpecs"`
	} `json:"spec"`
}

//nolint:gocritic // hugeParam: Outcome is passed by value like the pod path's.
func reviewRayCluster(req *admissionv1.AdmissionRequest, cfg *Config, out Outcome) (*admissionv1.AdmissionResponse, Outcome) {
	out.Kind, out.Rule = KindRayCluster, RulePassThrough
	var edits patch
	if req.Operation != admissionv1.Create {
		return respond(req, out, &edits)
	}
	var rc rayClusterObj
	var raw rayClusterRaw
	if err := json.Unmarshal(req.Object.Raw, &rc); err != nil {
		out.Rule, out.Result = RuleDecode, ResultDenied
		out.Message = fmt.Sprintf("[decode] cannot decode the RayCluster: %v", err)
		return deny(req.UID, out.Message), out
	}
	if err := json.Unmarshal(req.Object.Raw, &raw); err != nil {
		out.Rule, out.Result = RuleDecode, ResultDenied
		out.Message = fmt.Sprintf("[decode] cannot decode the RayCluster: %v", err)
		return deny(req.UID, out.Message), out
	}
	out.Name = rc.Metadata.Name
	ns := req.Namespace
	if ns == "" {
		ns = rc.Metadata.Namespace
	}
	if out.Namespace == "" {
		out.Namespace = ns
	}
	var donors []int
	for i := range rc.Spec.WorkerGroupSpecs {
		if isTrue(rc.Spec.WorkerGroupSpecs[i].Template.Labels, LabelDonor) {
			donors = append(donors, i)
		}
	}
	if len(donors) == 0 {
		return respond(req, out, &edits)
	}
	var wiring []corev1.EnvVar
	if cfg.DonorClientWiring == WiringEnv && len(donors) == 1 {
		g := &rc.Spec.WorkerGroupSpecs[donors[0]]
		jobID, group := g.Template.Labels[LabelJobID], g.Template.Labels[LabelGroup]
		if jobID == "" {
			jobID = rc.Metadata.Name
		}
		if group == "" && rc.Metadata.Name != "" {
			group = GroupName(ns, rc.Metadata.Name, g.GroupName)
		}
		if jobID != "" && group != "" {
			wiring = []corev1.EnvVar{
				{Name: EnvFullyAsync, Value: "1"},
				{Name: EnvJobID, Value: jobID},
				{Name: EnvGroup, Value: group},
				{Name: EnvOrchAddr, Value: cfg.OrchestratorAddr},
				{Name: EnvNCCLCuMem, Value: "0"},
				{Name: EnvNCCLNVLS, Value: "0"},
			}
		}
	}
	wire := func(base string, tmpl *corev1.PodTemplateSpec, hasMeta, donor bool) {
		sub := patch{base: base}
		pod := &corev1.Pod{ObjectMeta: tmpl.ObjectMeta, Spec: tmpl.Spec}
		if !donor && cfg.RLIntegrationImage != "" && !isTrue(pod.Labels, LabelRLIntegration) {
			if hasMeta {
				sub.addLabel(pod, LabelRLIntegration, "true")
			} else {
				sub.add(base+"/metadata", map[string]any{"labels": map[string]string{LabelRLIntegration: "true"}})
			}
		}
		if len(wiring) > 0 {
			sub.ensureEnv(pod, wiring)
		}
		edits.ops = append(edits.ops, sub.ops...)
	}
	wire("/spec/headGroupSpec/template", &rc.Spec.HeadGroupSpec.Template, hasMetadata(raw.Spec.HeadGroupSpec.Template), false)
	for i := range rc.Spec.WorkerGroupSpecs {
		hasMeta := i < len(raw.Spec.WorkerGroupSpecs) && hasMetadata(raw.Spec.WorkerGroupSpecs[i].Template)
		donor := isTrue(rc.Spec.WorkerGroupSpecs[i].Template.Labels, LabelDonor)
		wire(fmt.Sprintf("/spec/workerGroupSpecs/%d/template", i), &rc.Spec.WorkerGroupSpecs[i].Template, hasMeta, donor)
	}
	out.Rule = RuleRayCluster
	return respond(req, out, &edits)
}

func hasMetadata(t rayTemplate) bool {
	m := strings.TrimSpace(string(t.Metadata))
	return m != "" && m != "null"
}
