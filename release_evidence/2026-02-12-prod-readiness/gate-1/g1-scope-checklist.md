# Gate 1 Scope Checklist (Locked)

- Gate: `1` (`Release discipline / provenance`)
- Scope lock timestamp (UTC): `2026-02-18T19:32:14Z`
- Authoritative sources:
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:44-63`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:199-216`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:22-25`

## Gate Closure Conditions

1. Gate 1 can be `GO` only when required signals `G1-001..G1-004` are all `GO`.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:48-53`, `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:199-216`
2. Gate 1 closure requires concrete evidence artifacts under Gate 1 evidence root.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:54-63`
3. Definition of done remains `ALL_GATES_GO`; Gate 1 cannot be treated as complete in isolation for production GO.
   - Source: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6`

## Signal Criteria and Pass/Fail Checks

| Signal | Authoritative Criterion (Source) | Pass Criteria | Fail Criteria | Code Paths | Evidence Paths |
|---|---|---|---|---|---|
| `G1-001` | Signed release tags and provenance pipeline (`signal-ledger.md:22`) | Signed `v0.1.4` tag verifies using repo-pinned signers; tag-triggered Release SBOM run succeeds; provenance attestation step succeeds; CI release-tag discipline dependency is satisfied. | Unsigned/unverifiable tag, no provenance attestation, or no successful tag-triggered SBOM run for the release commit. | `.github/workflows/release-sbom.yml`, `.github/release-signers.allowed`, `tools/collect_release_evidence.sh`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-001-signed-tag-provenance-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` |
| `G1-002` | Immutable release evidence collector script (`signal-ledger.md:23`) | Collector output path deterministically resolves from tag object + commit; manifest verification passes; `--allow-existing` succeeds and preserves immutability semantics. | Path derivation is non-deterministic, manifest integrity check fails, or overwrite is possible without `--allow-existing`. | `tools/collect_release_evidence.sh`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-002-immutable-collector-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` |
| `G1-003` | CI enforces changelog + release note completeness on release tags (`signal-ledger.md:24`) | Discipline check passes for complete `v0.1.4` inputs; expected-fail case returns non-zero for incomplete notes; CI `Release Tag Discipline` check-run is successful for the release commit. | Incomplete changelog/release notes are accepted, expected-fail case does not fail, or CI check-run missing/failing. | `.github/workflows/ci.yml`, `tools/check-release-tag-discipline.sh`, `CHANGELOG.md`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-003-release-tag-ci-enforcement-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` |
| `G1-004` | SBOM publication tied to release artifact retention (`signal-ledger.md:25`) | Release SBOM workflow succeeds for `v0.1.4`; release page includes required SBOM assets; retained workflow artifacts include non-expired retention metadata (`expires_at`). | Required SBOM release assets missing, retained artifacts missing, or retention metadata absent. | `.github/workflows/release-sbom.yml`, `RELEASE.md`, `tools/collect_release_evidence.sh` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-004-sbom-retention-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` |

## Canonical Remote URLs (v0.1.4)

- Release SBOM workflow run: <https://github.com/daxis-io/arco/actions/runs/22153366267>
- CI workflow run: <https://github.com/daxis-io/arco/actions/runs/22153366241>
- Release page: <https://github.com/daxis-io/arco/releases/tag/v0.1.4>
