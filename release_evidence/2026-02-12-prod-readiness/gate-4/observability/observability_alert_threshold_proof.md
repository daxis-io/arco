# Alert Threshold Proof (G4-005)

Generated UTC: 2026-02-14T04:50:30Z
Sources: `infra/monitoring/alerts.yaml`, `docs/runbooks/metrics-catalog.md`

| Alert | Present In Alerts | Documented In Runbook |
|---|---|---|
| `ArcoApiErrorRateHigh` | YES | YES |
| `ArcoRateLimitHitsHigh` | YES | YES |
| `ArcoCompactionLagHigh` | YES | YES |
| `ArcoCasRetryRateHigh` | YES | YES |
| `ArcoOrchSloObservedAboveTarget` | YES | YES |
| `ArcoOrchSloBreachesIncreasing` | YES | YES |

## Result
- PASS: threshold definitions are documented and align with alert rules.

## Controlled Drill Coverage
- `promtool` drill validates deterministic firing for:
  - `ArcoApiErrorRateHigh`
  - `ArcoCompactionLagHigh`
  - `ArcoOrchSloObservedAboveTarget`
  - `ArcoOrchSloBreachesIncreasing`
