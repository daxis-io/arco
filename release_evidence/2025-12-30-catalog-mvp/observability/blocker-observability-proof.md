# Observability Proof Blocker

Date: 2025-12-30
Status: DEFERRED (alpha/local-only; no deployment yet)

## What's Present (Config Artifacts)
- `/Users/ethanurbanski/arco/infra/monitoring/dashboard.json` — Grafana dashboard JSON definition
- `/Users/ethanurbanski/arco/infra/monitoring/alerts.yaml` — Prometheus/Alertmanager alert rules
- `/Users/ethanurbanski/arco/infra/monitoring/otel-collector.yaml` — OpenTelemetry Collector config
- `/Users/ethanurbanski/arco/docs/runbooks/grafana-dashboard.json` — Runbook-shipped dashboard
- `/Users/ethanurbanski/arco/docs/runbooks/grafana-dashboard-gate5.json` — Gate-5 dashboard variant
- `/Users/ethanurbanski/arco/docs/runbooks/prometheus-alerts.yaml` — Runbook-shipped alerts
- `/Users/ethanurbanski/arco/docs/runbooks/metrics-catalog.md` — Metrics catalog documentation

## What's Missing (Proof Artifacts)
- Dashboard screenshot or export showing live/healthy state (timestamped)
- Alert trigger proof (Alertmanager FIRING output, PagerDuty incident, or test alert delivery)
- SLO targets plus burn-rate visualization (error budget and multi-window burn rate view)
- Metrics endpoint scrape sample (Prometheus /metrics or Cloud Monitoring time series export)
- Alert routing/notification policy evidence (contact points/escalation chain)

## Why Blocked
- No deployment yet (alpha/local-only); observability stack not running
- No live dashboards, alerts, or metrics to capture
- SLO targets and error budget policies not defined

## Required Actions to Unblock (when deploying)
1. Deploy monitoring stack to dev/staging environment
2. Capture dashboard screenshot (PNG/PDF) showing healthy state (timestamped)
3. Define SLO targets plus error budgets and capture burn-rate view/export
4. Generate synthetic alert or capture real alert-firing proof (delivery log or incident)
5. Capture metrics scrape sample (Prometheus /metrics or Cloud Monitoring time series export)
6. Store artifacts in `release_evidence/2025-12-30-catalog-mvp/observability/`

## Acceptable Evidence (per industry standards)
- Dashboard screenshot with timestamp (Grafana or Cloud Monitoring)
- SLO definition (targets plus error budget) with burn-rate panel export
- `promtool check rules` output for alert rule validation
- Alertmanager webhook delivery log or incident ticket (test or real)
- Metrics scrape output (`/metrics`) or Cloud Monitoring time series export
