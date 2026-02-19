# Signal Ledger — Delta-Primary Cutover

- Audit window: 2026-02-12 through 2026-02-18
- Scope: Single release-train cutover to Delta-primary defaults and Delta commit gating

## Signals

| Signal ID | Requirement | Evidence Path | Status |
|---|---|---|---|
| `delta-default-api` | New tables default to `delta` on namespace + schema routes | `release_evidence/2026-02-18-delta-primary-cutover/ci/delta-default-api.txt` | Pending |
| `delta-format-canonicalization` | `delta|iceberg|parquet` canonicalization + unknown format rejection | `release_evidence/2026-02-18-delta-primary-cutover/ci/delta-format-validation.txt` | Pending |
| `delta-commit-gating` | Delta commit endpoints enforce table existence + effective delta format | `release_evidence/2026-02-18-delta-primary-cutover/ci/delta-commit-gating.txt` | Pending |
| `delta-location-log-path` | `_delta_log` writes derive from table location root | `release_evidence/2026-02-18-delta-primary-cutover/ci/delta-location-paths.txt` | Pending |
| `uc-openapi-pinned` | Vendored Unity Catalog OpenAPI fixture is pinned and compliance checks enabled | `release_evidence/2026-02-18-delta-primary-cutover/uc/openapi-parity.txt` | Pending |
| `iceberg-secondary-compat` | Iceberg compatibility paths remain green | `release_evidence/2026-02-18-delta-primary-cutover/ci/iceberg-secondary.txt` | Pending |
| `sdk-delta-default` | Python SDK `IoConfig.format` default is `delta` | `release_evidence/2026-02-18-delta-primary-cutover/sdk/python-default-format.txt` | Pending |
| `policy-docs-updated` | Runbooks/ADRs/security scope updated for Delta-primary policy | `release_evidence/2026-02-18-delta-primary-cutover/docs/policy-updates.md` | Pending |
