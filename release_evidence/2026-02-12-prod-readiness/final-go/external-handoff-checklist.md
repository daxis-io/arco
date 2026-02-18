# Final-Go External Handoff Checklist

Generated UTC: 2026-02-15T19:29:45Z

| Signal | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| G7-001 | Release Engineering + SRE | `gcloud auth login`; run `gcloud run services update-traffic ...` for 5% -> 25% -> 100%; capture health gates between shifts | canary progression transcript + gate decisions | `final-go/` |
| G7-002 | Daxis Integrations | export production auth/context vars and run discovery/query/admin production checks with captured responses | production integration validation report | `final-go/` |
| G7-003 | Product + Release | publish messaging only after `jq -e '[.gates[] | select(.status != \"GO\")] | length == 0' docs/audits/2026-02-12-prod-readiness/gate-tracker.json` succeeds | messaging diff + publication record | `final-go/g7-003-messaging-update-gate.md` |
