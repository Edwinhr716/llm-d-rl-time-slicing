{{/* Namespace of the workloads (donor, guests, serving path). */}}
{{- define "demo.app" -}}
{{- .Values.appNamespace | default .Release.Namespace }}
{{- end }}

{{/* Namespace of the control-plane side pods (tools, fault, GPU exporter). */}}
{{- define "demo.sys" -}}
{{- .Values.sysNamespace | default .Release.Namespace }}
{{- end }}

{{/* Virtual Node of a host: vk-<last dash-separated part of the host name>, or .vk when set. */}}
{{- define "demo.vk" -}}
{{- .vk | default (printf "vk-%s" (last (splitList "-" .node))) }}
{{- end }}

{{/* Slots of the group hosts, as a list. */}}
{{- define "demo.slots" -}}
{{- $s := list }}{{- range .Values.hosts }}{{- $s = append $s .slot }}{{- end }}{{- join "," $s }}
{{- end }}

{{/* nodeSelector pinning a pod to the control node. */}}
{{- define "demo.controlSelector" -}}
{{- if .Values.controlNode }}
nodeSelector: {kubernetes.io/hostname: {{ .Values.controlNode }}}
{{- else if .Values.controlNodeSelector }}
nodeSelector:
  {{- toYaml .Values.controlNodeSelector | nindent 2 }}
{{- end }}
tolerations: [{key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}]
{{- end }}

{{/* RL worker prep args (KubeRay appends its ray start). Context: donor.rayjob. */}}
{{- define "demo.rlWorkerArgs" }}
- >-
  set -eo pipefail;
  mkdir -p /workspace/results;
  export RESULTS_DIR=/workspace/results;
  source /opt/timeslice-demo/install_common.sh;
  {{- if .clientOverlayConfigMap }}
  mkdir -p /tmp/ovl && tar -xzf /opt/client-overlay/client.tgz -C /tmp/ovl &&
  pip install --no-deps --force-reinstall /tmp/ovl/client /tmp/ovl/verl &&
  echo CLIENT_OVERLAY_OK;
  {{- end }}
  cd /workspace;
  python3 /opt/timeslice-demo/data_prep.py --out_dir /workspace/data/eurus_code
  --tokenizer Qwen/Qwen2.5-0.5B-Instruct --train_size 512 --val_size 64
{{- end }}
{{/* RL worker env shared by trainers and samplers. Context: donor.rayjob. */}}
{{- define "demo.rlEnv" }}
- {name: VLLM_USE_V1, value: "1"}
- {name: HF_HOME, value: /workspace/hf}
- {name: NCCL_CUMEM_ENABLE, value: "0"}
- {name: NCCL_NVLS_ENABLE, value: "0"}
- {name: VERL_REPO, value: {{ .verlRepo | quote }}}
- {name: VERL_REF, value: {{ .verlRef | quote }}}
{{- end }}
