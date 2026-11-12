{{/* Labels on every object. */}}
{{- define "oxchange.labels" -}}
app.kubernetes.io/part-of: oxchange
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/* The image holding every service binary. */}}
{{- define "oxchange.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "oxchange.webImage" -}}
{{ .Values.web.image.repository }}:{{ .Values.web.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/* Every broker, so clients can still find the cluster when one is down. */}}
{{- define "oxchange.kafkaBrokers" -}}
{{- $ns := .Release.Namespace -}}
{{- range $i := until (int .Values.redpanda.replicas) -}}
{{- if $i }},{{ end }}redpanda-{{ $i }}.redpanda.{{ $ns }}.svc.cluster.local:9092
{{- end -}}
{{- end }}

{{/* One address per partition, in partition order. */}}
{{- define "oxchange.matchingUrls" -}}
{{- range $i := until (int .Values.matching.partitions) -}}
{{- if $i }},{{ end }}http://matching-{{ $i }}.matching:50051
{{- end -}}
{{- end }}

{{- define "oxchange.jwtSecret" -}}
{{ .Values.jwt.existingSecret | default "oxchange-jwt" }}
{{- end }}

{{/*
DATABASE_URL for a service's own database, with the password taken from
the Secret. Call with (list "ENV_NAME" "role" "database").
*/}}
{{- define "oxchange.databaseEnv" -}}
{{- $name := index . 0 -}}{{- $role := index . 1 -}}{{- $db := index . 2 -}}
- name: {{ $name }}_PASSWORD
  valueFrom:
    secretKeyRef: { name: oxchange-db, key: {{ $role }} }
- name: {{ $name }}
  value: postgres://{{ $role }}:$({{ $name }}_PASSWORD)@postgres:5432/{{ $db }}
{{- end }}

{{/* Spec: non-root, read-only filesystem. */}}
{{- define "oxchange.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: ["ALL"] }
{{- end }}

{{- define "oxchange.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001
  seccompProfile: { type: RuntimeDefault }
{{- end }}

{{/*
Probes for a service whose /healthz and /readyz are on the named port
(spec: "Health and shutdown"). The startup probe gives a slow start two
minutes before the liveness probe can restart it.
*/}}
{{- define "oxchange.probes" -}}
startupProbe:
  httpGet: { path: /healthz, port: {{ . }} }
  periodSeconds: 2
  failureThreshold: 60
livenessProbe:
  httpGet: { path: /healthz, port: {{ . }} }
  periodSeconds: 10
  timeoutSeconds: 3
readinessProbe:
  httpGet: { path: /readyz, port: {{ . }} }
  periodSeconds: 3
  timeoutSeconds: 3
{{- end }}

{{/* Where a service sends traces and logs (nothing when observability is off). */}}
{{- define "oxchange.telemetryEnv" -}}
{{- if .Values.observability.enabled }}
- { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: "http://otel-collector:4318" }
- { name: TRACE_SAMPLE_RATIO, value: {{ .Values.observability.traceSampleRatio | quote }} }
{{- end }}
- { name: METRICS_ADDR, value: "0.0.0.0:9464" }
{{- end }}

{{/* Pod annotations that ask Prometheus to scrape it. Call with (list port path). */}}
{{- define "oxchange.scrape" -}}
prometheus.io/scrape: "true"
prometheus.io/port: {{ index . 0 | quote }}
prometheus.io/path: {{ index . 1 | default "/metrics" | quote }}
{{- end }}
