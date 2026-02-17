# Gate 1 Scope Checklist (Locked)

- Gate: `1` (`Release discipline / provenance`)
- Scope lock timestamp (UTC): `2026-02-16T02:26:13Z`
- Authoritative sources:
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:44-56`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:192-209`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6-12`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:22-25`

## Gate-Level Closure Conditions

1. Gate 1 remains closed only when all required signals `G1-001..G1-004` are `GO`.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:48-53`
2. Gate status cannot be `GO` if any required signal is not `GO`.
   - Source: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6`, `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:9-12`
3. Evidence must be reproducible, with archived artifacts under Gate 1 evidence root.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:55`
   - Source: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:9`

## Signal Criteria and Pass/Fail Checks

| Signal | Ledger Criterion (Authoritative) | Pass Criteria (Executable) | Fail Criteria | Code/Workflow Paths | Required Artifact Paths |
|---|---|---|---|---|---|
| `G1-001` | Signed release tags and provenance pipeline. Exit: signed tag + provenance evidence generated for release tag. (`signal-ledger.md:22`) | A release-tag workflow validates tag signature material and produces provenance attestation evidence plus logs for a concrete `v*` tag. | Unsigned tag accepted, provenance step absent, or no per-tag proof artifact/log. | `RELEASE.md`, `.github/workflows/release-sbom.yml`, `tools/collect_release_evidence.sh` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-001-signed-tag-provenance-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-signed-tag-prepared.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-signed-tag-has-signature-material.log` |
| `G1-002` | Immutable release evidence collector script. Exit: deterministic evidence pack per tag. (`signal-ledger.md:23`) | Collector script accepts tag input, writes deterministic immutable path naming keyed by tag identity, emits manifest/checksums, and refuses mutable overwrite by default. | Non-deterministic naming, mutable/overwrite-prone output, or missing integrity manifest. | `tools/collect_release_evidence.sh`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-002-immutable-collector-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-collector-create-pack.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-collector-immutable-expected-fail.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-collector-manifest-verifies.log`, generated collector pack directory |
| `G1-003` | CI enforces changelog + release note completeness on release tags. Exit: release-tag CI fails when incomplete. (`signal-ledger.md:24`) | CI has release-tag gate that exits non-zero when changelog section or required release-note file content is missing/incomplete; evidence includes negative test command/log. | Tag CI allows missing/incomplete changelog or release notes; no failing proof. | `.github/workflows/ci.yml`, `CHANGELOG.md`, `RELEASE.md`, `tools/check-release-tag-discipline.sh` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-003-release-tag-ci-enforcement-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-release-tag-discipline-pass.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-release-tag-discipline-expected-fail.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-ci-release-tag-enforcement-present.log` |
| `G1-004` | SBOM publication tied to release artifact retention. Exit: published SBOM linked to release artifact + retained per policy. (`signal-ledger.md:25`) | SBOM workflow publishes SBOM assets for release tag, sets explicit retention policy for uploaded artifacts, and links SBOM outputs to release artifact set in evidence. | SBOM only ephemeral, no retention policy, or no linkage between release tag artifact set and SBOM outputs. | `.github/workflows/release-sbom.yml`, `RELEASE.md`, `tools/collect_release_evidence.sh` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-004-sbom-retention-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-sbom-retention-and-provenance-present.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-collector-manifest-verifies.log` |

## Scope Notes

- No scope conflict detected between tracker and ledger for Gate 1.
- Ambiguity resolved to executable checks by binding each signal to deterministic command-log proof and concrete artifact paths under `release_evidence/2026-02-12-prod-readiness/gate-1/`.
