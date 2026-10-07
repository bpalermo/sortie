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

{{/*
The engine's Deployment and Service name. fullname may already be 63
characters, the DNS label limit a Service name has to meet, so the base is
cut to leave room for the suffix.
*/}}
{{- define "sortie.engineName" -}}
{{- printf "%s-engine" (include "sortie.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end -}}

{{/*
The engine's Service. A DaemonSet's is headless and has a name of its own:
spec.clusterIP is immutable, so turning the Deployment's ClusterIP Service
headless in place would fail the upgrade that switches engine.kind. With two
names Helm deletes one Service and creates the other.
*/}}
{{- define "sortie.engineServiceName" -}}
{{- if eq (include "sortie.engineKind" .) "DaemonSet" -}}
{{- printf "%s-engine-nodes" (include "sortie.fullname" . | trunc 50 | trimSuffix "-") -}}
{{- else -}}
{{- include "sortie.engineName" . -}}
{{- end -}}
{{- end -}}

{{/*
Labels a user may add to a pod, without the ones the selectors match on: a
podLabels entry for one of those would be a duplicate key that replaces the
fixed value and leaves the workload and the Service selecting nothing.
*/}}
{{- define "sortie.extraPodLabels" -}}
{{- with omit . "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
The engine's workload kind, checked: a misspelt kind would otherwise render
nothing at all and install a Service pointing at no pods.
*/}}
{{- define "sortie.engineKind" -}}
{{- $kind := .Values.engine.kind | default "Deployment" -}}
{{- if not (has $kind (list "Deployment" "DaemonSet")) -}}
{{- fail (printf "engine.kind must be Deployment or DaemonSet, got %q" $kind) -}}
{{- end -}}
{{- $kind -}}
{{- end -}}

{{/*
Where the engine's --backend-name comes from, checked: a misspelt source would
otherwise start the engine without a name, and a plan with `stats.backend:
name` would then be refused by every engine when the run starts.
*/}}
{{- define "sortie.engineBackendNameFrom" -}}
{{- $from := .Values.engine.backendNameFrom | default "none" -}}
{{- if not (has $from (list "none" "node" "pod")) -}}
{{- fail (printf "engine.backendNameFrom must be none, node or pod, got %q" $from) -}}
{{- end -}}
{{- $from -}}
{{- end -}}

{{/*
The engine's pod template, shared by the Deployment and the DaemonSet so the
two cannot drift. Which one owns it is engine.kind; the pod is the same.
*/}}
{{- define "sortie.enginePodTemplate" -}}
metadata:
  labels:
    {{- include "sortie.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/component: engine
    {{- with include "sortie.extraPodLabels" .Values.engine.podLabels }}
    {{- . | nindent 4 }}
    {{- end }}
  {{- with .Values.engine.podAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  serviceAccountName: {{ include "sortie.serviceAccountName" . }}
  automountServiceAccountToken: {{ .Values.automountServiceAccountToken }}
  {{- with .Values.engine.priorityClassName }}
  priorityClassName: {{ . | quote }}
  {{- end }}
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  containers:
    - name: engine
      image: {{ .Values.engine.image.ref | quote }}
      imagePullPolicy: {{ .Values.engine.image.pullPolicy }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      {{- $nameFrom := include "sortie.engineBackendNameFrom" . }}
      {{- if and (not .Values.engine.backendName) (ne $nameFrom "none") }}
      # The image has no shell to read the environment with, so the name
      # reaches the engine as an argument the kubelet expands from this.
      env:
        - name: SORTIE_BACKEND_NAME
          valueFrom:
            fieldRef:
              fieldPath: {{ eq $nameFrom "node" | ternary "spec.nodeName" "metadata.name" }}
      {{- end }}
      # The image's entrypoint is nighthawk_service.
      args:
        - --listen
        - 0.0.0.0:8443
        # A weighted scenario runs one execution per target at once on
        # every backend; this is how many the engine accepts.
        - --max-concurrent-executions
        - {{ .Values.engine.maxConcurrentExecutions | quote }}
        {{- /* Two arguments each: the engine's flag parser separates a flag
        from its value by a space and does not read --flag=value. */}}
        {{- if .Values.engine.backendName }}
        - --backend-name
        - {{ .Values.engine.backendName | quote }}
        {{- else if ne $nameFrom "none" }}
        - --backend-name
        - $(SORTIE_BACKEND_NAME)
        {{- end }}
      ports:
        - name: grpc
          containerPort: 8443
          protocol: TCP
      # nighthawk_service serves grpc.health.v1.Health, which is what the
      # kubelet's gRPC probe queries; the image has no shell or curl, so a
      # probe has to be one the kubelet performs itself.
      readinessProbe:
        grpc:
          port: 8443
      livenessProbe:
        grpc:
          port: 8443
        initialDelaySeconds: 10
      resources:
        {{- toYaml .Values.engine.resources | nindent 8 }}
  {{- with .Values.engine.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.engine.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.engine.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
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

{{/*
The format of the report file, checked.
*/}}
{{- define "sortie.reportFormat" -}}
{{- $format := .Values.report.format | default "json" -}}
{{- if not (has $format (list "json" "text")) -}}
{{- fail (printf "report.format must be json or text, got %q" $format) -}}
{{- end -}}
{{- if not .Values.report.volume -}}
{{- fail "report.volume is required with report.path: the root filesystem is read-only, and a report written to the pod itself is gone with the pod" -}}
{{- end -}}
{{- if not (isAbs .Values.report.path) -}}
{{- fail (printf "report.path must be an absolute file path, got %q" .Values.report.path) -}}
{{- end -}}
{{- /* The volume is mounted at the file's directory, so that directory must
not be one the image needs: / is the whole image, /sortie is the binary the
container runs, /etc holds the plan's mount, and /etc/sortie is the plan. */ -}}
{{- /* A file, not a directory: --output creates the path it is given, and a
trailing slash or a dot segment at the end names a directory. */ -}}
{{- $last := base .Values.report.path -}}
{{- if or (hasSuffix "/" .Values.report.path) (eq $last ".") (eq $last "..") -}}
{{- fail (printf "report.path must name a file, got the directory %q" .Values.report.path) -}}
{{- end -}}
{{- $path := clean .Values.report.path -}}
{{- $dir := dir $path -}}
{{- range $reserved := list "/sortie" "/etc/sortie" -}}
{{- if or (eq $path $reserved) (eq $dir $reserved) (hasPrefix (printf "%s/" $reserved) $dir) -}}
{{- fail (printf "report.path %q is at or under %s, which the container needs; put the file in a directory of its own, such as /var/run/sortie" $.Values.report.path $reserved) -}}
{{- end -}}
{{- end -}}
{{- if or (eq $dir "/") (eq $dir "/etc") -}}
{{- fail (printf "report.path %q would mount the report volume at %s, over the image; put the file in a directory of its own, such as /var/run/sortie" .Values.report.path $dir) -}}
{{- end -}}
{{- $format -}}
{{- end -}}

{{/*
The path of the results stream, checked: report.stream is a file name, and
the file is written beside the report, on the volume mounted there. A name
is all it can be -- the root filesystem is read-only, so anywhere off that
volume the stream could not be opened and the run would refuse to start.
*/}}
{{- define "sortie.reportStream" -}}
{{- $name := .Values.report.stream -}}
{{- if not .Values.report.path -}}
{{- fail "report.stream needs report.path: the stream is written beside the report, on report.volume" -}}
{{- end -}}
{{- if or (contains "/" $name) (eq $name ".") (eq $name "..") -}}
{{- fail (printf "report.stream must be a file name, written in the directory of report.path; got %q" $name) -}}
{{- end -}}
{{- if eq $name (base (clean .Values.report.path)) -}}
{{- fail (printf "report.stream and report.path name the same file, %q" $name) -}}
{{- end -}}
{{- printf "%s/%s" (dir (clean .Values.report.path)) $name -}}
{{- end -}}

{{- define "sortie.podSpec" -}}
metadata:
  labels:
    {{- include "sortie.selectorLabels" . | nindent 4 }}
    {{- with include "sortie.extraPodLabels" .Values.podLabels }}
    {{- . | nindent 4 }}
    {{- end }}
  {{- with .Values.podAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  restartPolicy: Never
  serviceAccountName: {{ include "sortie.serviceAccountName" . }}
  {{- with .Values.priorityClassName }}
  priorityClassName: {{ . | quote }}
  {{- end }}
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
        {{- if .Values.report.path }}
        {{- /* Stated either way: args may already carry --json, and the last
        one on the command line wins. */}}
        - --json={{ eq (include "sortie.reportFormat" .) "json" }}
        - --output
        - {{ clean .Values.report.path | quote }}
        {{- end }}
        {{- if .Values.report.stream }}
        - --results-stream
        - {{ include "sortie.reportStream" . | quote }}
        {{- end }}
        - /etc/sortie/plan.yaml
      volumeMounts:
        - name: plan
          mountPath: /etc/sortie
          readOnly: true
        {{- if .Values.report.path }}
        - name: report
          mountPath: {{ dir (clean .Values.report.path) | quote }}
        {{- end }}
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
  volumes:
    - name: plan
      configMap:
        name: {{ include "sortie.configMapName" . }}
    {{- if .Values.report.path }}
    - name: report
      {{- toYaml .Values.report.volume | nindent 6 }}
    {{- end }}
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
