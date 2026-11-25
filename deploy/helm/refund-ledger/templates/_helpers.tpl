{{- define "ledger.image" -}}
{{- with .root.Values.image.registry }}{{ . }}/{{ end }}{{ .name }}:{{ .root.Values.image.tag }}
{{- end -}}

{{- define "ledger.env" -}}
- name: ConnectionStrings__Postgres
  valueFrom: { secretKeyRef: { name: {{ .Values.connectionSecret }}, key: postgres } }
- name: ConnectionStrings__Redis
  valueFrom: { secretKeyRef: { name: {{ .Values.connectionSecret }}, key: redis } }
- name: Kafka__BootstrapServers
  value: {{ .Values.kafka.bootstrapServers | quote }}
- name: Psp__BaseUrl
  value: {{ .Values.psp.baseUrl | quote }}
- name: Otlp__Traces
  value: {{ .Values.otlp.traces | quote }}
- name: Otlp__Metrics
  value: {{ .Values.otlp.metrics | quote }}
{{- end -}}

{{- define "ledger.security" -}}
runAsNonRoot: true
allowPrivilegeEscalation: false
capabilities: { drop: [ALL] }
{{- end -}}
