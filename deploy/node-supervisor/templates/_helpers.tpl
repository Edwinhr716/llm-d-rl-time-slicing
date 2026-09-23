{{/*
Expand the name of the chart.
*/}}
{{- define "node-supervisor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "node-supervisor.fullname" -}}
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
{{- define "node-supervisor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "node-supervisor.labels" -}}
helm.sh/chart: {{ include "node-supervisor.chart" . }}
{{ include "node-supervisor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "node-supervisor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "node-supervisor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "node-supervisor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "node-supervisor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The namespace every object in this chart lands in.

Unlike snapshot-agent (hardcoded to timeslice-system) the supervisor lives in
the TENANT namespace: that is where the pods it supervises are, and where its
ServiceAccount has to be for the cluster-scoped binding to be meaningful in the
audit log.
*/}}
{{- define "node-supervisor.namespace" -}}
{{- default "default" .Values.tenantNamespace }}
{{- end }}
