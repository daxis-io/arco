# Gate 1 Scope Checklist (Locked)

- Gate: `1` (`Release discipline / provenance`)
- Scope lock timestamp (UTC): `2026-02-18T19:01:46Z`
- Authoritative sources:
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:44-63`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:199-216`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:22-25`

## Gate-Level Closure Conditions

1. Gate 1 is closed only when required signals `G1-001..G1-004` are all `GO`.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:48-53`, `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:199-216`
2. Definition of done remains `ALL_GATES_GO`; Gate 1 cannot be declared closed with partial signal state.
   - Source: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:6`
3. Closure evidence must be reproducible and archived under Gate 1 evidence root.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:54-63`

## Signal Criteria and Pass/Fail Checks

| Signal | Ledger Criterion (Authoritative) | Pass Criteria (Executable) | Fail Criteria | Code/Workflow Paths | Required Artifact Paths |
|---|---|---|---|---|---|
| `G1-001` | Signed release tags and provenance pipeline. Exit: signed tag + provenance evidence generated for release tag. (`signal-ledger.md:22`) | Tag-triggered SBOM workflow verifies signed tag material, confirms `Release Tag Discipline`, and succeeds through provenance attestation for a concrete release tag. | Signed tag verification not enforced, provenance step missing/failing, or no run-level proof. | `.github/workflows/release-sbom.yml`, `.github/release-signers.allowed`, `tools/collect_release_evidence.sh`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-001-signed-tag-provenance-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run.json`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run-jobs.json`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/check-runs.json` |
| `G1-002` | Immutable release evidence collector script. Exit: deterministic evidence pack per tag. (`signal-ledger.md:23`) | Collector creates deterministic immutable pack path for release tag, manifest verifies, and `--allow-existing` verify/read mode does not rewrite pack. | Non-deterministic pack path, mutable overwrite behavior, or missing/broken manifest. | `tools/collect_release_evidence.sh`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-002-immutable-collector-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/latest-pack-path.txt`, `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c/manifest.sha256` |
| `G1-003` | CI enforces changelog + release note completeness on release tags. Exit: release-tag CI fails when incomplete. (`signal-ledger.md:24`) | Discipline script passes complete `v0.1.4` artifacts, expected-fail harness proves fail-closed behavior for incomplete notes, and tag check-run reports `Release Tag Discipline=success`. | Incomplete changelog/release-notes accepted, expected-fail harness does not fail, or release-tag check-run missing/failing. | `.github/workflows/ci.yml`, `tools/check-release-tag-discipline.sh`, `CHANGELOG.md`, `RELEASE.md` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-003-release-tag-ci-enforcement-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185606Z-targeted_release_tag_discipline_pass_v0_1_4.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185607Z-targeted_release_tag_discipline_expected_fail_v0_1_4.log`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/check-runs.json` |
| `G1-004` | SBOM publication tied to release artifact retention. Exit: published SBOM linked to release artifact + retained per policy. (`signal-ledger.md:25`) | Successful tag SBOM run publishes SBOM assets to release, uploads retained SBOM/evidence artifacts with 90-day retention metadata, and downloaded artifact pack contains expected files. | Missing SBOM release assets, missing retention metadata, or missing retained artifacts/evidence pack linkage. | `.github/workflows/release-sbom.yml`, `RELEASE.md`, `tools/collect_release_evidence.sh` | `release_evidence/2026-02-12-prod-readiness/gate-1/g1-004-sbom-retention-evidence.md`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-view.json`, `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run-artifacts.json`, `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/` |

## Scope Notes

- Tracker and ledger are now aligned with Gate 1 and all required signals at `GO`.
- Historical failed attempts (`v0.1.1`..`v0.1.3`) are retained under `github-run/` for audit traceability but do not alter final closure state.
