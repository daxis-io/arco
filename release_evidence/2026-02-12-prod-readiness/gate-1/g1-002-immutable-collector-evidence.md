# G1-002 Evidence - Immutable Release Evidence Collector Script

- Signal: `G1-002`
- Ledger criterion: `Script produces deterministic evidence pack per tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:23`)
- Status in this batch: `GO`

## Implementation Evidence

1. Collector output path is deterministic and keyed by immutable tag object + commit identity.
   - `tools/collect_release_evidence.sh:111-112`
2. Collector refuses mutable overwrite unless explicitly invoked in verify/read mode (`--allow-existing`).
   - `tools/collect_release_evidence.sh:113-131`
3. Collector emits checksum manifest and package snapshots (including signer trust roots).
   - `tools/collect_release_evidence.sh:167-170`
   - `tools/collect_release_evidence.sh:181-187`

## Verification Evidence (v0.1.4)

1. Deterministic collector pack creation succeeded for release tag `v0.1.4`:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185835Z-targeted_collector_create_pack_v0_1_4.log`
2. Deterministic path assertion passed (`collector-packs/v0.1.4/<tag-object-sha>-<commit-sha>`):
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185835Z-full_collector_path_determinism_v0_1_4.log`
3. Manifest integrity verified for every file in the immutable pack:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185835Z-full_collector_manifest_verifies_v0_1_4.log`
4. `--allow-existing` verify/read flow completed without rewriting the pack:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185835Z-targeted_collector_allow_existing_v0_1_4.log`
5. Immutable pack path and manifest:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/latest-pack-path.txt`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c/manifest.sha256`
