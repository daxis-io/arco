# Observability Evidence

Date: 2025-12-31
Status: Pending proof artifacts

## Contents

- `blocker-observability-proof.md` - Documents config artifacts present and proof artifacts missing

## Config Artifacts Present

| Artifact | Path | Status |
|----------|------|--------|
| Grafana Dashboard JSON | `infra/monitoring/dashboard.json` | Present |
| Prometheus/Alertmanager Rules | `infra/monitoring/alerts.yaml` | Present |
| OpenTelemetry Collector Config | `infra/monitoring/otel-collector.yaml` | Present |
| Runbook Dashboard | `docs/runbooks/grafana-dashboard.json` | Present |
| Gate-5 Dashboard | `docs/runbooks/grafana-dashboard-gate5.json` | Present |
| Runbook Alerts | `docs/runbooks/prometheus-alerts.yaml` | Present |
| Metrics Catalog | `docs/runbooks/metrics-catalog.md` | Present |

## Required Proof Artifacts

The following proof artifacts are required (per industry standards):

### 1. Dashboard Screenshot
- Timestamped screenshot of live Grafana/Cloud Monitoring dashboard
- Must show healthy state with real metrics
- Save as: `dashboard-screenshot-YYYY-MM-DD.png`

### 2. Alert Delivery Proof
- Alertmanager FIRING output, OR
- PagerDuty/Opsgenie incident ticket, OR
- Test alert webhook delivery log
- Save as: `alert-delivery-proof.txt` or `alert-incident-ticket.pdf`

### 3. SLO Targets and Burn Rate
- SLO definition document with specific targets (e.g., 99.9% availability)
- Error budget policy
- Burn rate visualization/export
- Save as: `slo-definition.md` and `burnrate-panel-export.png`

### 4. Metrics Scrape Sample
- Prometheus `/metrics` endpoint output, OR
- Cloud Monitoring time series export
- Save as: `metrics-scrape-sample.txt`

### 5. Alert Routing Evidence
- Notification policy configuration
- Contact points / escalation chain
- Save as: `alert-routing-config.yaml` or screenshot

## How to Complete

1. Deploy monitoring stack to dev/staging environment
2. Generate or capture each proof artifact above
3. Save artifacts to this directory
4. Update `blocker-observability-proof.md` to mark items complete
5. Update audit_plan.md observability evidence notes

## Industry Standards Reference

- Google SRE Workbook: https://sre.google/workbook/alerting-on-slos/
- Grafana Alerting Best Practices: https://grafana.com/docs/grafana/latest/alerting/best-practices/
- Prometheus Alerting Best Practices: https://prometheus.io/docs/practices/alerting
