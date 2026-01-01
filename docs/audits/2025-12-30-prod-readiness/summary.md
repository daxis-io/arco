# Production Readiness Audit — Arco

- Audit date: 2025-12-30
- Scope: Catalog MVP / Productization
- Baseline commit: a906816291347849f7c59118c0f0b235ff1efcdc
- Owner: Ethan Urbanski

## Gate Status (GO/NO-GO)

| Gate | Area | Status | P0 Blockers | Evidence Folder | Notes |
|---:|---|---|---|---|---|
| 0 | Plan coherence & architecture | NO-GO | P0: Snippet audit not performed; DataFusion server-read path not productized | `evidence/gate-0/` | See `release_evidence/2025-12-30-catalog-mvp/gate-0/` |
| 1 | Engineering system & repo hygiene | NO-GO | P0: Release tag evidence missing | `evidence/gate-1/` | See `release_evidence/2025-12-30-catalog-mvp/gate-1/` |
| 2 | Storage, manifests & correctness | NO-GO | P0: Notification consumer limits Catalog/Lineage fast-path; anti-entropy search is derived (no listing); determinism tests missing | `evidence/gate-2/` | See `findings/gate-2-findings.md`; evidence index: `evidence/gate-2/00-evidence-index.md` |
| 3 | Product surface completeness | GO | None | `evidence/gate-3/` | See `release_evidence/2025-12-30-catalog-mvp/gate-3/` |
| 4 | Deployment, security & ops readiness | NO-GO | P0: Deployment + observability proof missing (no env) | `evidence/gate-4/` | See `release_evidence/2025-12-30-catalog-mvp/deploy/` and `release_evidence/2025-12-30-catalog-mvp/observability/` |
| 5 | Release readiness & lifecycle | NO-GO | P0: Upstream gates not green; rollback drill + release notes missing; perf/soak not run | `evidence/gate-5/` | See `release_evidence/2025-12-30-catalog-mvp/gate-5/` |

Status values:
- `TBD`: not yet assessed
- `GO`: meets gate criteria with evidence
- `NO-GO`: has at least one P0 blocker

## Evidence Pack Conventions

- Primary evidence pack lives at `release_evidence/2025-12-30-audit/` with `gate-0/` through `gate-5/` subfolders.
- `docs/audits/2025-12-30-prod-readiness/evidence/` remains a working area for audit notes and scratch evidence.
- Store logs, screenshots, and links under the relevant `gate-N/` folder.
- For every claim in findings, include at least one evidence reference:
  - Code reference: `path/to/file.rs:123`
  - Command + output (either pasted or saved as a file under `gate-N/`)
  - CI run link

## Findings Docs

- Findings live under `findings/`.
- Gate 2 findings: `findings/gate-2-findings.md`.
