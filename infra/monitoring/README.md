# Monitoring

Artifacts for Arco observability:

- `dashboard.json`: Grafana dashboard for API, compactor, GC, and storage metrics.
- `alerts.yaml`: Prometheus alert rules (PromQL).
- `otel-collector.yaml`: OpenTelemetry Collector config to export Prometheus metrics
  to Cloud Monitoring, plus an OTLP receiver for push-based metrics.

## Prometheus scrape targets

Expose metrics from:

- `arco-api`: `GET /metrics`
- `arco-compactor`: `GET /metrics`

## Cloud Monitoring export (GCP)

Use the OpenTelemetry Collector to scrape Prometheus endpoints and export to
Cloud Monitoring. The collector also accepts OTLP metrics for push-based
pipelines (useful when `/metrics` is unavailable). Set `GCP_PROJECT_ID` (used as
`${env:GCP_PROJECT_ID}` in `otel-collector.yaml`) and update target addresses as
needed.

If the compactor `/metrics` shared-secret gate is enabled, also set
`ARCO_METRICS_SECRET` so the collector can scrape it.

```bash
otelcol --config infra/monitoring/otel-collector.yaml
```

## Notes

- Storage inventory gauges (`arco_storage_*`) are updated during GC runs.
- The compaction lag gauge is a proxy based on time since the last successful
  compaction cycle until true backlog measurement is implemented.
