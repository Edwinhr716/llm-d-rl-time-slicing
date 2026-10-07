{{/*
Expand the name of the chart.
*/}}
{{- define "guest-kubelet.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "guest-kubelet.fullname" -}}
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
{{- define "guest-kubelet.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "guest-kubelet.labels" -}}
helm.sh/chart: {{ include "guest-kubelet.chart" . }}
{{ include "guest-kubelet.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "guest-kubelet.selectorLabels" -}}
app.kubernetes.io/name: {{ include "guest-kubelet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "guest-kubelet.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "guest-kubelet.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace every namespaced resource of this chart is created in.
*/}}
{{- define "guest-kubelet.namespace" -}}
{{- .Values.namespace | default "timeslice-system" }}
{{- end }}

{{/*
Command-line flags from a map of flag name to value, one "--name=value" list item each, in key
order. A key whose value is "" or null renders nothing, so the binary default applies.
*/}}
{{- define "guest-kubelet.flags" -}}
{{- range $k, $v := . }}
{{- if and (not (kindIs "invalid" $v)) (ne (toString $v) "") }}
- {{ printf "--%s=%v" $k $v | quote }}
{{- end }}
{{- end }}
{{- end }}
