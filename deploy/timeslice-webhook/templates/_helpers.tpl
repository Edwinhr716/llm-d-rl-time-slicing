{{/*
Expand the name of the chart.
*/}}
{{- define "timeslice-webhook.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "timeslice-webhook.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "timeslice-webhook.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "timeslice-webhook.labels" -}}
helm.sh/chart: {{ include "timeslice-webhook.chart" . }}
{{ include "timeslice-webhook.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "timeslice-webhook.selectorLabels" -}}
app.kubernetes.io/name: {{ include "timeslice-webhook.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Namespace every namespaced resource of this chart is created in.
*/}}
{{- define "timeslice-webhook.namespace" -}}
{{- .Values.namespace | default "timeslice-system" }}
{{- end }}

{{/*
Command-line flags from a map of flag name to value, one "--name=value" list item each, in key
order. A key whose value is "" or null renders nothing, so the binary default applies.
*/}}
{{- define "timeslice-webhook.flags" -}}
{{- range $k, $v := . }}
{{- if and (not (kindIs "invalid" $v)) (ne (toString $v) "") }}
- {{ printf "--%s=%v" $k $v | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*
The guest kubelet's user name, exempt from the CEL policy (W7).
*/}}
{{- define "timeslice-webhook.vkUsername" -}}
{{- printf "system:serviceaccount:%s" (include "timeslice-webhook.vkServiceAccount" .) }}
{{- end }}

{{/*
flags.vk-service-account, else the umbrella chart's guest kubelet in this namespace: <namespace>:<its
default fullname>, which is the release name when it contains "guest-kubelet" and
<release>-guest-kubelet otherwise. Set the flag when the guest kubelet has another namespace or name.
*/}}
{{- define "timeslice-webhook.vkServiceAccount" -}}
{{- $sa := index .Values.flags "vk-service-account" | default "" }}
{{- if not $sa }}
{{- $full := printf "%s-guest-kubelet" .Release.Name }}
{{- if contains "guest-kubelet" .Release.Name }}{{ $full = .Release.Name }}{{ end }}
{{- $sa = printf "%s:%s" (include "timeslice-webhook.namespace" .) ($full | trunc 63 | trimSuffix "-") }}
{{- end }}
{{- $sa }}
{{- end }}

{{/*
flags.orchestrator-addr, else the umbrella chart's orchestrator Service in this namespace:
<its default fullname>.<namespace>.svc:50051.
*/}}
{{- define "timeslice-webhook.orchestratorAddr" -}}
{{- $a := index .Values.flags "orchestrator-addr" | default "" }}
{{- if not $a }}
{{- $full := printf "%s-timesliceorchestrator" .Release.Name }}
{{- if contains "timesliceorchestrator" .Release.Name }}{{ $full = .Release.Name }}{{ end }}
{{- $a = printf "%s.%s.svc:50051" ($full | trunc 63 | trimSuffix "-") (include "timeslice-webhook.namespace" .) }}
{{- end }}
{{- $a }}
{{- end }}

{{/*
The flags with the derived defaults filled in (vk-service-account, orchestrator-addr).
*/}}
{{- define "timeslice-webhook.effectiveFlags" -}}
{{- $f := deepCopy .Values.flags }}
{{- $_ := set $f "vk-service-account" (include "timeslice-webhook.vkServiceAccount" .) }}
{{- $_ := set $f "orchestrator-addr" (include "timeslice-webhook.orchestratorAddr" .) }}
{{- if .Values.rlIntegration.enabled }}
{{- $img := .Values.rlIntegration.image }}
{{- $_ := set $f "rl-integration-image" (printf "%s:%s" $img.repository ($img.tag | default .Chart.AppVersion)) }}
{{- with .Values.rlIntegration.path }}{{ $_ := set $f "rl-integration-path" . }}{{ end }}
{{- if .Values.rlIntegration.injectVerl }}{{ $_ := set $f "rl-integration-verl" "true" }}{{ end }}
{{- end }}
{{- toYaml $f }}
{{- end }}

{{/*
namespaceSelector of the webhook entries and the policy binding.
*/}}
{{- define "timeslice-webhook.namespaceSelector" -}}
{{- if .Values.namespaceSelector }}
{{- toYaml .Values.namespaceSelector }}
{{- else }}
matchExpressions:
  - key: kubernetes.io/metadata.name
    operator: NotIn
    values: ["kube-system", {{ include "timeslice-webhook.namespace" . | quote }}]
{{- end }}
{{- end }}
