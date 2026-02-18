# G1-002 Evidence - Immutable Release Evidence Collector Script

- Signal: `G1-002`
- Ledger criterion: `Script produces deterministic evidence pack per tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:23`)
- Status in this batch: `GO`

## Implementation Evidence

1. Collector script added with deterministic path derivation and immutable default behavior.
   - `tools/collect_release_evidence.sh:6-16`
   - `tools/collect_release_evidence.sh:67-70`
   - `tools/collect_release_evidence.sh:87-93`
2. Collector writes a deterministic manifest over collected evidence files.
   - `tools/collect_release_evidence.sh:139-147`
3. Release process documents deterministic output naming and overwrite refusal.
   - `RELEASE.md:49-63`

## Verification Evidence

1. Collector generated a deterministic pack path for `v0.1.0`:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-collector-create-pack.log`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-collector-path-determinism.log`
2. Immutable overwrite protection is enforced:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-collector-immutable-expected-fail.log`
3. Manifest integrity validates all collected files:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-collector-manifest-verifies.log`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.0/352cba80c843fd9cf54b1ab169bd0ac081c51bde-dc3072977aef3b0572c58498bea6ceddab26fd22/manifest.sha256`

## Deterministic Artifact

- Collector pack root:
  - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.0/352cba80c843fd9cf54b1ab169bd0ac081c51bde-dc3072977aef3b0572c58498bea6ceddab26fd22/`
