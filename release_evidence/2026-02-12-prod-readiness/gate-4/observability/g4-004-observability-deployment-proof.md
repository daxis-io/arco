# G4-004 Observability Deployment Proof

Generated UTC: 2026-02-14T04:51:40Z
Status: PARTIAL (config + drill proven locally; live staging deployment visibility pending)

## Local Evidence (Complete)

- Dashboard config coverage:
  - `observability/observability_dashboard_config_proof.md`
- Scrape/export wiring proof:
  - `observability/observability_scrape_wiring_proof.md`
- Alert rules compile:
  - `observability/command-logs/promtool_check_g4_alerts.log`
- Controlled alert drill passes:
  - `observability/observability_gate4_alert_drill.test.yaml`
  - `observability/command-logs/promtool_test_g4_alert_drill.log`

## Remaining External Evidence Required

Owner: Observability Team

1. Deploy dashboard to staging Grafana/monitoring workspace.
   - Expected artifact:
     - Dashboard URL and screenshot including panel data for orchestration metrics.
   - Destination:
     - `observability/dashboard_staging_url.txt`
     - `observability/dashboard_staging_screenshot.png`

2. Verify staging scrape path is live.
   - Command (example):
     - `curl -f https://<staging-observability-endpoint>/api/v1/targets`
   - Expected artifact:
     - Target health JSON showing `arco-api` and `arco-compactor` as up.
   - Destination:
     - `observability/command-logs/staging_scrape_targets.json`

3. Execute staging alert drill against deployed system.
   - Expected artifact:
     - Alertmanager/Cloud Monitoring incident showing alert fired and resolved with timestamps.
   - Destination:
     - `observability/command-logs/staging_alert_drill_events.json`
     - `observability/staging_alert_drill_screenshot.png`
