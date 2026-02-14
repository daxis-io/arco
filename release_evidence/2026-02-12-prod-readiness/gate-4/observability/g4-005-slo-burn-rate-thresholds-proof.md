# G4-005 SLO + Burn-Rate Threshold Proof

Generated UTC: 2026-02-14T04:51:40Z
Status: PARTIAL

## Documented Thresholds

- Runbook matrix added:
  - `docs/runbooks/metrics-catalog.md` (section: `Gate 4 Staging SLO + Burn-Rate Threshold Checks (2026-02-14)`)
- Rule/doc alignment proof:
  - `observability/observability_alert_threshold_proof.md`

## Tested Threshold Behavior

- Rule syntax check:
  - `observability/command-logs/promtool_check_g4_alerts.log`
- Controlled deterministic drill:
  - `observability/observability_gate4_alert_drill.test.yaml`
  - `observability/command-logs/promtool_test_g4_alert_drill.log`

Validated firing behavior for:
- `ArcoApiErrorRateHigh`
- `ArcoCompactionLagHigh`
- `ArcoOrchSloObservedAboveTarget`
- `ArcoOrchSloBreachesIncreasing` (short-window burn-rate proxy)

## Remaining External Evidence Required

Owner: Observability Team

1. Run a live staged drill and capture alert lifecycle evidence.
   - Expected artifact:
     - Trigger timestamp, fire timestamp, acknowledge timestamp, resolve timestamp.
   - Destination:
     - `observability/staging_threshold_drill_timeline.md`

2. Reviewer confirmation that staged behavior matches documented thresholds.
   - Expected artifact:
     - Reviewer signoff (name + timestamp + approver role).
   - Destination:
     - `observability/staging_threshold_signoff.md`
