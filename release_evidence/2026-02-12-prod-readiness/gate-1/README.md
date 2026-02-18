# Gate 1 Evidence

Gate 1 closure artifacts for release discipline and provenance.

## Scope Lock

- `g1-scope-checklist.md`

## Signal Evidence

- `g1-001-signed-tag-provenance-evidence.md`
- `g1-002-immutable-collector-evidence.md`
- `g1-003-release-tag-ci-enforcement-evidence.md`
- `g1-004-sbom-retention-evidence.md`

## Verification Matrix

- `command-matrix-status.tsv`

## Key Command Logs (v0.1.4 closure)

- `command-logs/2026-02-18T185606Z-targeted_shell_syntax_v0_1_4.log`
- `command-logs/2026-02-18T185606Z-targeted_release_tag_discipline_pass_v0_1_4.log`
- `command-logs/2026-02-18T185607Z-targeted_release_tag_discipline_expected_fail_v0_1_4.log`
- `command-logs/2026-02-18T185607Z-full_release_gate1_hardening_suite_v0_1_4.log`
- `command-logs/2026-02-18T185625Z-release_commit_v0_1_4.log`
- `command-logs/2026-02-18T185626Z-release_push_tag_v0_1_4.log`
- `command-logs/2026-02-18T185649Z-external_wait_v0_1_4_sbom_and_discipline.log`
- `command-logs/2026-02-18T185746Z-external_fetch_sbom_run_log_v0_1_4.log`
- `command-logs/2026-02-18T185835Z-targeted_collector_create_pack_v0_1_4.log`
- `command-logs/2026-02-18T185835Z-full_collector_manifest_verifies_v0_1_4.log`

## GitHub Run Evidence

- Successful closure run: `github-run/v0.1.4/`
- Prior failed attempts retained for audit traceability:
  - `github-run/v0.1.1/`
  - `github-run/v0.1.2/`
  - `github-run/v0.1.3/`

## Release Assets and Collector Packs

- Downloaded retained workflow artifacts: `release-assets/v0.1.4/`
- Latest immutable collector path pointer: `latest-pack-path.txt`
- Deterministic collector packs:
  - `collector-packs/v0.1.0/`
  - `collector-packs/v0.1.4/`
