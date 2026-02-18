# G1-002 Evidence - Immutable Release Evidence Collector Script

- Signal: `G1-002`
- Ledger criterion: `Script produces deterministic evidence pack per tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md`)
- Status: `GO`

## Implementation Evidence

1. Collector path is deterministic: `<tag-object-sha>-<commit-sha>` under `collector-packs/<tag>/`.
   - `tools/collect_release_evidence.sh`
2. Collector blocks mutable overwrite unless `--allow-existing` is used.
   - `tools/collect_release_evidence.sh`
3. Collector writes a manifest and metadata for integrity verification.
   - `tools/collect_release_evidence.sh`

## Verification Evidence (`v0.1.4`)

1. Collector generated the deterministic path matching tag object + commit identity.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`collector_create_pack`, `collector_expected_path`)
2. Manifest verification succeeded (`sha256sum -c manifest.sha256`).
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`collector_manifest_verify`)
3. `--allow-existing` verification mode succeeded without mutable overwrite.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`collector_allow_existing`)
4. Release workflow also archived a retained release-evidence artifact for this tag.
   - <https://github.com/daxis-io/arco/actions/runs/22153366267>
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_sbom_artifacts`)
