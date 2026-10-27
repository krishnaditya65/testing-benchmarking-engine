{{/*
Chart name, allowing override.
*/}}
{{- define "pipeline-api-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "pipeline-api-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s" (include "pipeline-api-gateway.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "pipeline-api-gateway.labels" -}}
app.kubernetes.io/name: {{ include "pipeline-api-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: orchestration-control-plane
app.kubernetes.io/part-of: distributed-testing-benchmarking-tool
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "pipeline-api-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pipeline-api-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "pipeline-api-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pipeline-api-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
