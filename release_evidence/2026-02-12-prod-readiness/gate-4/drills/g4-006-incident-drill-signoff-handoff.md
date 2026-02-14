# G4-006 Incident Drills + Reviewer Signoff Handoff

Prepared UTC: 2026-02-14T04:51:40Z
Status: BLOCKED-EXTERNAL (requires human drill execution and reviewer signoff)

## Local Prework Evidence

- Alert rules compile:
  - `release_evidence/2026-02-12-prod-readiness/gate-4/observability/command-logs/promtool_check_g4_alerts.log`
- Controlled drill pass (non-live):
  - `release_evidence/2026-02-12-prod-readiness/gate-4/observability/command-logs/promtool_test_g4_alert_drill.log`

## External Drill Transcript Template

Owner: SRE

| Field | Value |
|---|---|
| Drill Start UTC | `<fill>` |
| Drill End UTC | `<fill>` |
| Environment | `staging` |
| Scenario ID | `<fill>` |
| Trigger Method | `<fill>` |
| Alert Name(s) Fired | `<fill>` |
| TTD (time-to-detect) | `<fill>` |
| TTA (time-to-ack) | `<fill>` |
| TTR (time-to-resolve) | `<fill>` |
| Post-incident checks | `<fill>` |
| Action items | `<fill>` |

## Required Reviewer Signoff

| Role | Name | Decision | Timestamp (UTC) | Notes |
|---|---|---|---|---|
| SRE Reviewer | `<fill>` | `<APPROVE/REJECT>` | `<fill>` | `<fill>` |
| Platform Reviewer | `<fill>` | `<APPROVE/REJECT>` | `<fill>` | `<fill>` |
| Observability Reviewer | `<fill>` | `<APPROVE/REJECT>` | `<fill>` | `<fill>` |

## Artifact Destinations

- Drill transcript: `release_evidence/2026-02-12-prod-readiness/gate-4/drills/staging_incident_drill_transcript.md`
- Alert timeline export: `release_evidence/2026-02-12-prod-readiness/gate-4/drills/staging_incident_alert_timeline.json`
- Signoff record: `release_evidence/2026-02-12-prod-readiness/gate-4/drills/staging_incident_drill_signoff.md`
