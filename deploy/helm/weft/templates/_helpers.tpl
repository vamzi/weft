{{/*
_helpers.tpl — naming, labels, image refs and security-relevant identities for the Weft chart.
*/}}

{{/* Base name (chart name unless overridden). */}}
{{- define "weft.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified release name, RFC1123-safe, <= 63 chars. */}}
{{- define "weft.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* chart label value, e.g. weft-0.0.0 */}}
{{- define "weft.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common metadata labels. */}}
{{- define "weft.labels" -}}
helm.sh/chart: {{ include "weft.chart" . }}
{{ include "weft.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: weft
{{- end -}}

{{/* Stable selector labels (must never change for a given workload). */}}
{{- define "weft.selectorLabels" -}}
app.kubernetes.io/name: {{ include "weft.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* ----------------------------------------------------------------------- */}}
{{/* Gateway                                                                  */}}
{{/* ----------------------------------------------------------------------- */}}

{{- define "weft.gateway.fullname" -}}
{{- printf "%s-gateway" (include "weft.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weft.gateway.labels" -}}
{{ include "weft.labels" . }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "weft.gateway.selectorLabels" -}}
{{ include "weft.selectorLabels" . }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "weft.gateway.serviceAccountName" -}}
{{- default (include "weft.gateway.fullname" .) .Values.gateway.serviceAccount.name -}}
{{- end -}}

{{/* Name of the Secret holding the JWT signing key (existingSecret, or a chart-managed fallback). */}}
{{- define "weft.gateway.jwtSecretName" -}}
{{- if .Values.gateway.jwt.existingSecret -}}
{{- .Values.gateway.jwt.existingSecret -}}
{{- else -}}
{{- printf "%s-jwt" (include "weft.gateway.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Name of the cluster-manage ClusterRole the gateway binds to itself, per namespace, at runtime. */}}
{{- define "weft.clusterManageRoleName" -}}
{{- default (printf "%s-cluster-manage" (include "weft.fullname" .)) .Values.rbac.clusterManageRoleName -}}
{{- end -}}

{{/* The gateway SA's API-server username — used by the admission policy matchCondition. */}}
{{- define "weft.gateway.saUsername" -}}
{{- printf "system:serviceaccount:%s:%s" .Release.Namespace (include "weft.gateway.serviceAccountName" .) -}}
{{- end -}}

{{/* ----------------------------------------------------------------------- */}}
{{/* Image refs                                                               */}}
{{/* ----------------------------------------------------------------------- */}}

{{/* weft.image — dict {registry, repository, tag, defaultTag} -> registry/repo:tag */}}
{{- define "weft.image" -}}
{{- $tag := default .defaultTag .tag -}}
{{- if .registry -}}
{{- printf "%s/%s:%s" .registry .repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "weft.gateway.image" -}}
{{- include "weft.image" (dict "registry" .Values.image.registry "repository" .Values.gateway.image.repository "tag" .Values.gateway.image.tag "defaultTag" .Chart.AppVersion) -}}
{{- end -}}

{{- define "weft.gateway.kubectlImage" -}}
{{- include "weft.image" (dict "registry" .Values.gateway.kubectl.image.registry "repository" .Values.gateway.kubectl.image.repository "tag" .Values.gateway.kubectl.image.tag "defaultTag" "latest") -}}
{{- end -}}

{{- define "weft.cluster.connectImage" -}}
{{- include "weft.image" (dict "registry" .Values.image.registry "repository" .Values.cluster.image.connectServer.repository "tag" .Values.cluster.image.connectServer.tag "defaultTag" .Chart.AppVersion) -}}
{{- end -}}

{{- define "weft.cluster.workerImage" -}}
{{- include "weft.image" (dict "registry" .Values.image.registry "repository" .Values.cluster.image.worker.repository "tag" .Values.cluster.image.worker.tag "defaultTag" .Chart.AppVersion) -}}
{{- end -}}
