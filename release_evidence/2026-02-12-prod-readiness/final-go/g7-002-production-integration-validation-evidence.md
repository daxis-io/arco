# G7-002 Production Integration Validation Evidence

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: Daxis Integrations

## Closure Requirement

Validate production integration contracts (discovery/query/admin mutation paths) against production systems.

## Fresh Evidence Captured (Local)

1. OpenAPI contract test passed locally:
   - `cargo test -p arco-api --test openapi_contract -- --nocapture`
2. API integration suite inventory listing succeeds locally.
3. Production execution prerequisites are missing in this workspace:
   - `PROD_API_BASE_URL`
   - `PROD_API_TOKEN`
   - `PROD_TENANT`
   - `PROD_WORKSPACE`

## Why This Signal Is External

Gate requires production contract validation with production identity, data, and integration dependencies.

## Evidence Artifacts

- `release_evidence/2026-02-12-prod-readiness/final-go/g7-002-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192833Z_openapi_contract_test_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192922Z_api_integration_test_listing_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192925Z_prod_integration_env_requirements_check.log`

## External Handoff Steps

| Step | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| Production auth setup | Daxis Integrations | export `PROD_API_BASE_URL`, `PROD_API_TOKEN`, `PROD_TENANT`, `PROD_WORKSPACE` in a secured shell | auth proof (redacted) | `final-go/prod_integration_auth_context.md` |
| Discovery contract checks | Daxis Integrations | `curl -fsS -H \"Authorization: Bearer $PROD_API_TOKEN\" \"$PROD_API_BASE_URL/api/v1/catalog/namespaces\"` | pass/fail report | `final-go/prod_discovery_checks.md` |
| Query contract checks | Daxis Integrations | `curl -fsS -H \"Authorization: Bearer $PROD_API_TOKEN\" -H \"Content-Type: application/json\" -d '{\"sql\":\"SELECT 1\"}' \"$PROD_API_BASE_URL/api/v1/query\"` | result log + latency/error summary | `final-go/prod_query_checks.log`, `final-go/prod_query_checks_summary.md` |
| Admin mutation checks | Daxis Integrations | execute approved safe-mutation suite with production credentials and capture responses | pass/fail matrix | `final-go/prod_admin_mutation_checks.md` |
| Integration signoff | Daxis Integrations + SRE | approve production contract status | signed validation note | `final-go/prod_integration_signoff.md` |
