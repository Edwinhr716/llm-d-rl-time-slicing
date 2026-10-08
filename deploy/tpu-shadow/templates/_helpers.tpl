{{/*
Expand the name of the chart.
*/}}
{{- define "tpu-shadow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "tpu-shadow.fullname" -}}
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
{{- define "tpu-shadow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tpu-shadow.labels" -}}
helm.sh/chart: {{ include "tpu-shadow.chart" . }}
{{ include "tpu-shadow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tpu-shadow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tpu-shadow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Namespace every namespaced resource of this chart is created in.
*/}}
{{- define "tpu-shadow.namespace" -}}
{{- .Values.namespace | default "timeslice-system" }}
{{- end }}

{{/*
Command-line flags from a map of flag name to value, one "--name=value" list item each, in key
order. A key whose value is "" or null renders nothing, so the binary default applies.
*/}}
{{- define "tpu-shadow.flags" -}}
{{- range $k, $v := . }}
{{- if and (not (kindIs "invalid" $v)) (ne (toString $v) "") }}
- {{ printf "--%s=%v" $k $v | quote }}
{{- end }}
{{- end }}
{{- end }}
