{{- define "sortie.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sortie.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "sortie.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "sortie.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "sortie.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "sortie.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sortie.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sortie.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sortie.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The pod spec, shared by the Job and the CronJob so the two cannot drift.

The ConfigMap's checksum is an annotation so that changing the plan rolls a new
pod rather than leaving a CronJob running the previous one.
*/}}
{{/*
A short digest of everything that decides what this run does. It suffixes the
Job name so that changing the plan produces a new Job rather than an attempt to
patch an existing one's immutable spec.template -- which fails with
"field is immutable" and never runs the new plan.
*/}}
{{- define "sortie.runHash" -}}
{{- printf "%s|%s|%s" (toYaml .Values.plan) .Values.image.ref (toYaml .Values.args) | sha256sum | trunc 8 -}}
{{- end -}}

{{- define "sortie.podSpec" -}}
metadata:
  labels:
    {{- include "sortie.selectorLabels" . | nindent 4 }}
  annotations:
    checksum/plan: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  restartPolicy: Never
  serviceAccountName: {{ include "sortie.serviceAccountName" . }}
  # sortie makes outbound gRPC calls and never touches the Kubernetes API, so a
  # mounted bearer token is a credential a compromised load generator could use
  # and nothing else.
  automountServiceAccountToken: {{ .Values.automountServiceAccountToken }}
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  containers:
    - name: sortie
      image: {{ .Values.image.ref | quote }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      args:
        {{- toYaml .Values.args | nindent 8 }}
        - /etc/sortie/plan.yaml
      volumeMounts:
        - name: plan
          mountPath: /etc/sortie
          readOnly: true
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
  volumes:
    - name: plan
      configMap:
        name: {{ include "sortie.fullname" . }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end -}}
