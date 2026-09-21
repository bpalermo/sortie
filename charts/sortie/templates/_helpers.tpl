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

Changing the plan changes the name of the ConfigMap this mounts, so the pod
template changes with it and a CronJob does not keep running the previous plan.
That is why there is no checksum annotation: it would be a second mechanism for
the thing the volume name already does.
*/}}
{{/*
The ConfigMap holding the plan, named by its contents.

A stable name would be updated in place by `helm upgrade`, and a Job that the
CronJob controller created before the upgrade but has not started yet would then
mount the new plan while its pod template and its Job name still describe the
old one. A load test that silently measures a different plan than the one it
reports is worse than one that does not start, so each plan gets its own object
and a Job can only ever mount the plan it was created for.

Helm deletes the previous ConfigMap on upgrade, since it is no longer in the
manifest. A Job still pending against it then fails to mount and stays visibly
unstarted, rather than running the wrong plan.
*/}}
{{- define "sortie.configMapName" -}}
{{- printf "%s-%s" (include "sortie.fullname" . | trunc 54 | trimSuffix "-") (.Values.plan | sha256sum | trunc 8) -}}
{{- end -}}

{{/*
A short digest of the rendered pod template. It suffixes the Job name so that
changing the template produces a new Job rather than an attempt to patch an
existing one's immutable spec.template -- which fails with "field is immutable"
and never runs the new plan.

This hashes the rendered podSpec rather than the individual values that feed it.
Enumerating them means the hash silently stops covering any field added to
podSpec later; hashing the output covers every one of them, including the
resources, security contexts, annotations and scheduling fields, and keeps
covering them. It also stays narrower than hashing .Values: a Job's backoffLimit
and ttlSecondsAfterFinished are mutable, so changing one should patch the Job in
place rather than start a new run.
*/}}
{{- define "sortie.runHash" -}}
{{- include "sortie.podSpec" . | sha256sum | trunc 8 -}}
{{- end -}}

{{- define "sortie.podSpec" -}}
metadata:
  labels:
    {{- include "sortie.selectorLabels" . | nindent 4 }}
  {{- with .Values.podAnnotations }}
  annotations:
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
        name: {{ include "sortie.configMapName" . }}
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
