{{- define "model-serving.namespace" -}}
{{- default .Release.Namespace .Values.global.namespace -}}
{{- end -}}
