{{/*
Expand the name of the chart.
*/}}
{{- define "timesliceorchestrator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "timesliceorchestrator.fullname" -}}
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
{{- define "timesliceorchestrator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "timesliceorchestrator.labels" -}}
helm.sh/chart: {{ include "timesliceorchestrator.chart" . }}
{{ include "timesliceorchestrator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "timesliceorchestrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "timesliceorchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "timesliceorchestrator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "timesliceorchestrator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace every namespaced resource of this chart is created in.
*/}}
{{- define "timesliceorchestrator.namespace" -}}
{{- .Values.namespace | default "timeslice-system" }}
{{- end }}

{{/*
Namespace of the lock ConfigMap: lock.namespace, else the chart namespace.
*/}}
{{- define "timesliceorchestrator.lockNamespace" -}}
{{- .Values.lock.namespace | default (include "timesliceorchestrator.namespace" .) }}
{{- end }}

{{/*
RBAC scope (rbac.scope, decision D-ORCH-7). "cluster" (default): one
ClusterRole for pods and nodes. "namespaced": pods are granted by one Role per
watched namespace (pods-roles.yaml) and the ClusterRole keeps nodes only. With
an empty scope.watchNamespaces the orchestrator watches every namespace, so
"namespaced" falls back to the "cluster" ClusterRole.
*/}}
{{- define "timesliceorchestrator.rbacScope" -}}
{{- $scope := .Values.rbac.scope | default "cluster" }}
{{- if not (has $scope (list "cluster" "namespaced")) }}
{{- fail (printf "rbac.scope must be \"cluster\" or \"namespaced\", got %q" $scope) }}
{{- end }}
{{- $scope }}
{{- end }}

{{/*
"true" when pods are granted per watched namespace instead of cluster-wide.
*/}}
{{- define "timesliceorchestrator.podsInRoles" -}}
{{- if and (eq (include "timesliceorchestrator.rbacScope" .) "namespaced") .Values.scope.watchNamespaces }}
{{- "true" }}
{{- end }}
{{- end }}

{{/*
rbac.scope=namespaced: a Role and a RoleBinding granting get/list/watch on
pods in each watched namespace, bound to the chart ServiceAccount. Names carry
the release fullname so two installs never collide.
*/}}
{{- define "timesliceorchestrator.podRoles" -}}
{{- if and .Values.rbac.create (eq (include "timesliceorchestrator.podsInRoles" .) "true") }}
{{- $fullname := include "timesliceorchestrator.fullname" . }}
{{- $labels := include "timesliceorchestrator.labels" . }}
{{- $sa := include "timesliceorchestrator.serviceAccountName" . }}
{{- $saNamespace := include "timesliceorchestrator.namespace" . }}
{{- range $ns := uniq .Values.scope.watchNamespaces }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ $fullname }}-pods
  namespace: {{ $ns }}
  labels:
    {{- $labels | nindent 4 }}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-pods
  namespace: {{ $ns }}
  labels:
    {{- $labels | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ $fullname }}-pods
subjects:
  - kind: ServiceAccount
    name: {{ $sa }}
    namespace: {{ $saNamespace }}
{{- end }}
{{- end }}
{{- end }}
