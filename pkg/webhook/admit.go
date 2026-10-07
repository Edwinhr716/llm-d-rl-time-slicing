package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
)

// Labels, taint and names the webhook reads or writes.
const (
	LabelDonor      = "timeslice.io/donor"
	LabelGuest      = "timeslice.io/guest"
	LabelJobID      = "timeslice.io/job-id"
	LabelGroup      = "timeslice.io/group"
	LabelRole       = "timeslice.io/role"
	LabelRayCluster = "ray.io/cluster"
	LabelRayGroup   = "ray.io/group"
	// GuestTaintKey is the key of the virtual node's taint that guests tolerate.
	GuestTaintKey = "timeslice.io/guest"
	// PodinfoVolume is the downward-API volume injected with WiringDownward.
	PodinfoVolume = "timeslice-podinfo"
	EnvJobID      = "TIMESLICE_JOB_ID"
	EnvGroup      = "TIMESLICE_GROUP"
	EnvOrchAddr   = "TIMESLICE_ORCH_ADDR"
	// ShadowResource is the pooled GPU shadow resource the shadow device plugin advertises on a
	// donor's host: its devices are the GPUs the donor holds. Only the virtual kubelet's mirror
	// pods may request it (W11), or any pod could take a GPU another pod already holds.
	ShadowResource = "timeslice.io/gpu-shadow"
	// ShadowResourcePrefix starts the per-GPU shadow resources (timeslice.io/gpu-shadow-<minor>).
	ShadowResourcePrefix = ShadowResource + "-"
)

// Kind is how the webhook classified the pod, for the log line and metrics.
type Kind string

// Pod kinds.
const (
	KindDonor      Kind = "donor"
	KindGuest      Kind = "guest"
	KindBackground Kind = "background"
	// KindRLIntegration is a pod labelled only timeslice.io/rl-integration=true.
	KindRLIntegration Kind = "rl-integration"
	KindOther         Kind = "other"
)

// Result is what the webhook did with the request.
type Result string

// Results.
const (
	ResultPatched Result = "patched"
	ResultAllowed Result = "allowed"
	ResultDenied  Result = "denied"
)

// Rule ids. Denial messages start with the rule id in brackets.
const (
	RulePassThrough = "R1"  // not a timeslice pod: unchanged
	RuleDerived     = "R2"  // donor identity derived from the KubeRay labels
	RuleExplicit    = "R3"  // donor identity given explicitly
	RuleNoIdentity  = "R4"  // donor identity cannot be derived
	RuleIdempotent  = "R8"  // guest already carries toleration and steering
	RuleBothRoles   = "R19" // donor and guest at once
	RuleGuestRoute  = "W1"  // guest toleration and steering injected
	RuleNoClaims    = "W2"  // guests carry no resource claims
	RuleJobIDGroup  = "W6"  // a group label needs a job-id label
	RuleMirror      = "W7"  // the virtual kubelet's requests pass through
	RuleGuestLabels = "W9"  // guests carry no group, job-id or role label
	RuleRoleReserve = "W10" // timeslice.io/role is set only by the virtual kubelet
	RuleShadow      = "W11" // the GPU shadow resources are requested only by the virtual kubelet
	RulePrivileged  = "W12" // guests are not privileged
	RuleRLOnly      = "W13" // RL integration injected into a timeslice.io/rl-integration pod
	RuleNone        = "-"
	RuleDecode      = "decode"
)

// Outcome describes one admission decision for logging and metrics.
type Outcome struct {
	Kind      Kind
	Op        string
	Result    Result
	Rule      string
	PatchOps  int
	Namespace string
	Name      string
	Message   string
}

// Admit is the pure admission function: it decides on one AdmissionRequest for a pod and returns
// the AdmissionResponse, with a JSONPatch when the pod is mutated. It uses no client and does no I/O.
//
//nolint:gocritic // hugeParam: the signature (Config by value) is the hook the evaluation harness calls.
func Admit(ctx context.Context, req *admissionv1.AdmissionRequest, cfg Config) *admissionv1.AdmissionResponse {
	resp, _ := Review(ctx, req, &cfg)
	return resp
}

// Review is Admit plus the Outcome used for the log line and the metrics.
func Review(_ context.Context, req *admissionv1.AdmissionRequest, cfg *Config) (*admissionv1.AdmissionResponse, Outcome) {
	if req == nil {
		out := Outcome{Kind: KindOther, Rule: RuleDecode, Result: ResultDenied, Message: "[decode] empty admission request"}
		return deny("", out.Message), out
	}
	out := Outcome{Kind: KindOther, Op: string(req.Operation), Namespace: req.Namespace, Rule: RuleNone}
	if req.Resource.Resource == "rayclusters" && req.SubResource == "" {
		return reviewRayCluster(req, cfg, out)
	}
	if req.Resource.Resource != "pods" || req.SubResource != "" {
		out.Result = ResultAllowed
		return allow(req.UID), out
	}
	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		out.Rule, out.Result = RuleDecode, ResultDenied
		out.Message = fmt.Sprintf("[decode] cannot decode the pod: %v", err)
		return deny(req.UID, out.Message), out
	}
	out.Name = pod.Name
	if out.Namespace == "" {
		out.Namespace = pod.Namespace
	}
	out.Kind = classify(pod.Labels)

	var edits patch
	switch {
	case cfg.VKUsername != "" && req.UserInfo.Username == cfg.VKUsername:
		out.Rule = RuleMirror
	case req.Operation != admissionv1.Create:
		// Mutation happens once, at creation. Other operations are not registered; allow them.
	case requestsShadow(pod):
		out.Rule, out.Message = RuleShadow, "[W11] "+ShadowResource+" and "+ShadowResourcePrefix+
			"* are requested only by the virtual kubelet on mirror pods; request nvidia.com/gpu instead"
	case out.Kind == KindDonor && isTrue(pod.Labels, LabelGuest):
		out.Rule, out.Message = RuleBothRoles, "[R19] a pod is either a donor (timeslice.io/donor) "+
			"or a guest (timeslice.io/guest), not both"
	case out.Kind == KindGuest:
		out.Rule, out.Message = admitGuest(pod, cfg, &edits)
	case pod.Labels[LabelRole] != "":
		out.Rule, out.Message = RuleRoleReserve, "[W10] timeslice.io/role is set only by the virtual "+
			"kubelet on mirror pods; remove it"
	case out.Kind == KindDonor:
		out.Rule, out.Message = admitDonor(pod, out.Namespace, cfg, &edits)
	case pod.Labels[LabelGroup] != "" && pod.Labels[LabelJobID] == "":
		out.Rule, out.Message = RuleJobIDGroup, "[W6] a pod with timeslice.io/group needs timeslice.io/job-id; "+
			"label it timeslice.io/donor=true to have both derived, or set both"
	case out.Kind == KindRLIntegration && cfg.RLIntegrationImage != "":
		edits.injectRLIntegration(pod, cfg)
		out.Rule = RuleRLOnly
	default:
		out.Rule = RulePassThrough
	}
	return respond(req, out, &edits)
}

// respond turns the decision into the AdmissionResponse: a denial when out has a message, an
// allow with the JSONPatch when there are edits, a plain allow otherwise.
func respond(req *admissionv1.AdmissionRequest, out Outcome, edits *patch) (*admissionv1.AdmissionResponse, Outcome) {
	switch {
	case out.Message != "":
		out.Result = ResultDenied
		return deny(req.UID, out.Message), out
	case len(edits.ops) == 0:
		out.Result = ResultAllowed
		return allow(req.UID), out
	}
	body, err := json.Marshal(edits.ops)
	if err != nil {
		out.Rule, out.Result = RuleDecode, ResultDenied
		out.Message = fmt.Sprintf("[decode] cannot encode the patch: %v", err)
		return deny(req.UID, out.Message), out
	}
	out.Result, out.PatchOps = ResultPatched, len(edits.ops)
	resp := allow(req.UID)
	pt := admissionv1.PatchTypeJSONPatch
	resp.Patch, resp.PatchType = body, &pt
	return resp, out
}

// classify: guest and donor by their boolean label, background by the reserved role label.
func classify(labels map[string]string) Kind {
	switch {
	case isTrue(labels, LabelDonor):
		return KindDonor
	case isTrue(labels, LabelGuest):
		return KindGuest
	case labels[LabelRole] != "":
		return KindBackground
	case isTrue(labels, LabelRLIntegration):
		return KindRLIntegration
	}
	return KindOther
}

func isTrue(labels map[string]string, key string) bool {
	return labels[key] == "true"
}

// admitGuest checks W9 and W2, then injects the toleration and the steering (W1). It returns the
// rule and, for a denial, the message.
//
//nolint:gocritic // unnamedResult conflicts with nonamedreturns.
func admitGuest(pod *corev1.Pod, cfg *Config, edits *patch) (string, string) {
	for _, key := range []string{LabelGroup, LabelJobID, LabelRole} {
		if _, ok := pod.Labels[key]; ok {
			return RuleGuestLabels, fmt.Sprintf("[W9] a guest pod must not carry %s: group, job-id and role "+
				"are set by the virtual kubelet on the mirror pod", key)
		}
	}
	if c := privilegedContainer(pod); c != "" {
		return RulePrivileged, fmt.Sprintf("[W12] guest container %s is privileged: a guest shares a GPU "+
			"another pod holds and a privileged container sees every host device; remove privileged", c)
	}
	if hasClaims(pod) {
		return RuleNoClaims, "[W2] a guest pod must not request resource claims: the mirror pod runs " +
			"on the donor's GPU through the virtual kubelet"
	}
	edits.ensureToleration(pod, &corev1.Toleration{
		Key: GuestTaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
	})
	if cfg.GuestSteering == SteeringPreferred {
		edits.ensurePreferred(pod, cfg.VirtualNodeLabel, "true")
	} else {
		edits.ensureNodeSelector(pod, cfg.VirtualNodeLabel, "true")
	}
	if len(edits.ops) == 0 {
		return RuleIdempotent, ""
	}
	return RuleGuestRoute, ""
}

// IsShadowResource reports whether name is a GPU shadow resource, pooled or per-GPU.
func IsShadowResource(name corev1.ResourceName) bool {
	return name == ShadowResource || strings.HasPrefix(string(name), ShadowResourcePrefix)
}

// requestsShadow reports whether any container or init container requests or limits a GPU
// shadow resource.
func requestsShadow(pod *corev1.Pod) bool {
	for _, list := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range list {
			for _, rl := range []corev1.ResourceList{list[i].Resources.Requests, list[i].Resources.Limits} {
				for name := range rl {
					if IsShadowResource(name) {
						return true
					}
				}
			}
		}
	}
	return false
}

// privilegedContainer returns the name of the first privileged container or init container,
// or "" when there is none.
func privilegedContainer(pod *corev1.Pod) string {
	for _, list := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range list {
			if sc := list[i].SecurityContext; sc != nil && sc.Privileged != nil && *sc.Privileged {
				return list[i].Name
			}
		}
	}
	return ""
}

func hasClaims(pod *corev1.Pod) bool {
	if len(pod.Spec.ResourceClaims) > 0 {
		return true
	}
	for i := range pod.Spec.Containers {
		if len(pod.Spec.Containers[i].Resources.Claims) > 0 {
			return true
		}
	}
	return false
}

// admitDonor keeps explicit timeslice.io/job-id and timeslice.io/group labels and derives the
// missing ones: job-id = ray.io/cluster, group = <ns>.<ray.io/cluster>.<ray.io/group>. Then it
// injects the client wiring and the RL integration (--rl-integration-image). It returns the rule
// and, for a denial, the message.
//
//nolint:gocritic // unnamedResult conflicts with nonamedreturns.
func admitDonor(pod *corev1.Pod, namespace string, cfg *Config, edits *patch) (string, string) {
	rule := RuleExplicit
	cluster, rayGroup := pod.Labels[LabelRayCluster], pod.Labels[LabelRayGroup]
	jobID, group := pod.Labels[LabelJobID], pod.Labels[LabelGroup]
	if jobID == "" || group == "" {
		if cluster == "" || (group == "" && rayGroup == "") {
			return RuleNoIdentity, "[R4] donor pod has no identity: it needs the KubeRay labels ray.io/cluster " +
				"and ray.io/group, or explicit timeslice.io/job-id and timeslice.io/group labels"
		}
		rule = RuleDerived
	}
	if jobID == "" {
		jobID = cluster
		edits.addLabel(pod, LabelJobID, jobID)
	}
	if group == "" {
		group = GroupName(namespace, cluster, rayGroup)
		edits.addLabel(pod, LabelGroup, group)
	}
	switch cfg.DonorClientWiring {
	case WiringEnv:
		edits.ensureEnv(pod, []corev1.EnvVar{
			{Name: EnvJobID, Value: jobID},
			{Name: EnvGroup, Value: group},
			{Name: EnvOrchAddr, Value: cfg.OrchestratorAddr},
		})
	case WiringDownward:
		edits.ensureVolume(pod, &corev1.Volume{Name: PodinfoVolume, VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{
				{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}},
			}},
		}})
		edits.ensureMount(pod, &corev1.VolumeMount{Name: PodinfoVolume, MountPath: cfg.PodinfoPath, ReadOnly: true})
	}
	edits.raiseDonorMemory(pod, cfg.DonorGPUMemory)
	edits.injectRLIntegration(pod, cfg)
	return rule, ""
}

func allow(uid apitypes.UID) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{UID: uid, Allowed: true}
}

func deny(uid apitypes.UID, msg string) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{UID: uid, Allowed: false, Result: &metav1.Status{
		Status: metav1.StatusFailure, Message: msg, Reason: metav1.StatusReasonForbidden, Code: http.StatusForbidden,
	}}
}
