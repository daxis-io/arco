# Historical worktree publication inventory — September 12, 2026

This inventory covers all registered worktrees of `daxis-io/arco`, refreshed against
`origin/main` at `745ed92e25ff7b4ec85aa0be57b02450642fda59` and the complete GitHub
PR inventory. The user authorized publication of completed, valid local work.
No worktree, archive, dirty code, or raw evidence was deleted. Private `arco-ops`
repositories also stored under `.worktrees` belong to another repository and are
not published into this public source repository.

A clean Git status is not validation. Commit ancestry, exact PR heads, merged
replacement series, source contents, and retained qualification records were checked.
An older squash-merged lineage can appear ahead without being unpublished work.
Where final-source proof was not found, the disposition is explicitly outstanding;
this does not assert the code is defective or that the underlying issue is solved.

## Publication results

- PR #429 contains the complete measured state-store series through the bounded directory; PR #430 contains runtime-cache reuse. Their exact source snapshots and all six phase branches were pushed earlier in this publication task.
- Fast-forwarded existing PRs #376, #377, #394, #395, and #396 with their previously local integration/lint commits. All five passed fresh workspace formatting and diff checks; the CI certification contract script and four affected worker Python tests also passed. These updates retain their original commits. Full Rust CI on the updated heads is pending, not claimed passing.
- Publish the completed historical remediation-ledger update and this inventory through existing documentation PR #374. Historical findings keep their original checkpoint date and are not current issue-closure claims.
- Raw agent-run logs, `.closeout`, and Gate 5 qualification artifacts stay in their retained evidence locations. They are not blindly staged as source.
- No new provider testing, infrastructure deployment, or resource creation is part of this publication.

## Dispositions

Disposition | Worktrees
|---|---:|
| Already merged | 2 |
| Already published | 53 |
| Already published; local evidence retained | 1 |
| Local evidence retained | 1 |
| Publish documentation | 1 |
| Superseded | 11 |
| Unavailable | 2 |
| Updated existing PR | 5 |
| Validation/integration outstanding | 21 |

## Complete inventory

| Worktree | Head | Disposition | Evidence / remaining work |
|---|---|---|---|
| `arco` | `8acff327` | Superseded | Diverged July main and original audit copy; corrected audit is already on main. Keep raw agent-run evidence local. |
| `arco-control-state-pr-stack` | `f21b50b9` | Unavailable | Prunable/missing Git worktree metadata; preserved without mutation. |
| `arco-pr-landing-sim-20260803` | `71b29a39` | Unavailable | Prunable/missing Git worktree metadata; preserved without mutation. |
| `audit-fixes` | `c3c0867c` | Validation/integration outstanding | Large mixed dirty patch quarry; no final current-source validation closure found. |
| `audit-remediation-advisory-exceptions` | `2f7d8ec5` | Already published | PR #402 is merged. Exact commit matches the PR head. |
| `audit-remediation-all-target-clippy` | `123d7675` | Already published | PR #400 is open. Exact commit matches the PR head. |
| `audit-remediation-auth-hygiene` | `a1f14c71` | Updated existing PR | Fast-forwarded #377 to a1f14c71; fresh format/diff checks passed. Full remote CI remains pending. |
| `audit-remediation-bounded-ledger-pager` | `e8c95e23` | Already published | PR #398 is merged. Exact commit matches the PR head. |
| `audit-remediation-browser-artifact-policy` | `c3c0867c` | Validation/integration outstanding | Ledger records Rust tests never executed. Current main already denies commits.parquet with its own regression; earlier four-file allowlist is not independently qualified. |
| `audit-remediation-catalog-fencing` | `f839a11d` | Already published | PR #379 is merged. Exact commit matches the PR head. |
| `audit-remediation-catalog-publication` | `243b25fb` | Already published | PR #378 is merged. Exact commit matches the PR head. |
| `audit-remediation-ci-certification` | `d5bc439e` | Updated existing PR | Fast-forwarded #376 to d5bc439e; fresh format/diff checks passed. Full remote CI remains pending. |
| `audit-remediation-delta-root-authority` | `cd7bf997` | Validation/integration outstanding | Dirty pre-final root-authority implementation; no exact final-source validation closure found. Overlaps phase9 and later authority changes. |
| `audit-remediation-emergency-posture` | `4bc02b7a` | Already published | PR #375 is open. Exact commit matches the PR head. |
| `audit-remediation-internal-auth-defaults` | `6f4b3a08` | Updated existing PR | Fast-forwarded #394 to 6f4b3a08; fresh format/diff checks passed. Full remote CI remains pending. |
| `audit-remediation-legacy-entity-idempotency` | `c3c0867c` | Validation/integration outstanding | Ledger explicitly records unexecuted Rust regressions; not a validated final candidate. |
| `audit-remediation-paginated-run-logs` | `318cfc45` | Already published | PR #401 is merged. Exact commit matches the PR head. |
| `audit-remediation-python-audit` | `7005c6d0` | Already published | PR #399 is merged. Exact commit matches the PR head. |
| `audit-remediation-repair-body-identity` | `a6bf008c` | Validation/integration outstanding | Committed patch, but ledger explicitly records formatting/metadata checks only and no Rust execution; needs current-source validation. |
| `audit-remediation-repair-retention-age` | `d88fe219` | Already published | PR #393 is merged. Exact commit matches the PR head. |
| `audit-remediation-retention-repair` | `4543fe9e` | Already published | PR #380 is closed. Exact commit matches the PR head. |
| `audit-remediation-retry-bootstrap` | `e910d1a0` | Already published | PR #405 is merged. Exact commit matches the PR head. |
| `audit-remediation-root-identity-scope` | `c3c0867c` | Validation/integration outstanding | Ledger explicitly records unexecuted Rust regressions; needs a fresh compatible source and validation. |
| `audit-remediation-signed-url-budgets` | `e2f1ecc8` | Updated existing PR | Fast-forwarded #396 to e2f1ecc8; fresh format/diff checks passed. Full remote CI remains pending. |
| `audit-remediation-snapshot-ref-validation` | `cd7bf997` | Validation/integration outstanding | Dirty broader validation draft; PR #406 already landed the unknown-snapshot-ref rejection. Additional changes need separate proof. |
| `audit-remediation-stable-repair-task-id` | `c3c0867c` | Validation/integration outstanding | Ledger explicitly records unexecuted Rust regression. Current sweeper has additional ULID uses; old patch needs current-source review. |
| `audit-remediation-stale-takeover-reconciliation` | `cd7bf997` | Validation/integration outstanding | Dirty commit/recovery draft; no exact final-source validation closure found. |
| `audit-remediation-uat-operator-boundaries` | `427463f5` | Already published | PR #397 is merged. Exact commit matches the PR head. |
| `audit-remediation-worker-dedup` | `8e50c0cd` | Already published | PR #403 is merged. Exact commit matches the PR head. |
| `audit-remediation-worker-heartbeats` | `4500de57` | Already published | PR #404 is merged. Exact commit matches the PR head. |
| `audit-remediation-worker-observability` | `cee98654` | Updated existing PR | Fast-forwarded #395 to cee98654; fresh format/diff checks passed. Full remote CI remains pending. |
| `audit-remediation-zombie-reaper` | `d4e8210e` | Already merged | Exact commit is an ancestor of current origin/main. |
| `audit-remediation-zombie-reaper-followup` | `1261228f` | Validation/integration outstanding | Dirty accepted-frontier work after an older runtime base; prior ledger records a remaining reaper correctness blocker. No final-source closure found. |
| `batch-5-iceberg-uc-compat` | `6d362d05` | Already published | PR #305 is merged. Exact commit matches the PR head. |
| `batch4-live-uat` | `55a2337e` | Validation/integration outstanding | Dirty historical UAT quarry; later revised UAT implementation landed in PR #304. Extra old changes lack final-source closure. |
| `batch4-live-uat-20260625` | `508638de` | Already published | PR #304 is merged. Exact commit matches the PR head. |
| `batch6-public-api-contracts` | `3b82107e` | Already published | PR #306 is merged. Exact commit matches the PR head. |
| `batch7-performance-architecture-docs` | `9b56ca9c` | Already published | PR #307 is merged. Exact commit matches the PR head. |
| `complete-batches-0-3` | `e4877ea3` | Already published | PR #303 is merged. Exact commit matches the PR head. |
| `control-plane-readiness` | `77b432af` | Already published | PR #373 is closed. Exact commit matches the PR head. |
| `control-plane-split-stack` | `657ff669` | Already published | PR #392 is merged. Exact commit matches the PR head. |
| `control-store-idempotency-grants-ddl-pilots` | `22da9f78` | Validation/integration outstanding | Clean but large historical pilot branch; target program requires exact-SHA review and provider/promotion gates. No final task/program closure found; authority implementation has since changed. |
| `control-v1-first-metastore-hard-cut` | `59228f20` | Already published | PR #418 is merged. Exact commit matches the PR head. |
| `dagster-planner-runtime` | `b2abe97f` | Validation/integration outstanding | Earlier planner alternative plus an explicitly Draft reliability design; no current combined-source closure found. |
| `event-listener-rustsec-2026-0221` | `a1ca04af` | Already published | PR #381 is merged. Exact commit matches the PR head. |
| `fold-retention` | `eba7a297` | Validation/integration outstanding | PR #389 merged a revised branch; this older divergent base still has additional dirty retention/ledger changes without final-source closure. |
| `hydra-runtime-read-reuse-20260910-01` | `0da54a1c` | Already published | PR #430 is open. Exact commit matches the PR head. |
| `issue-132-catalog-run-index` | `36223fc4` | Validation/integration outstanding | PR #200 already merged; dirty ledger/compaction changes are additional work with no final-source closure found. |
| `lineage-l0` | `3c4f24f0` | Already published | PR #392 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `open-issue-independent-audit-20260731` | `d999435c` | Publish documentation | Retained historical ledger update is documentation-only; publish with dated context and this inventory in existing PR #374. |
| `phase-0-1-roadmap-seams` | `d92f0e43` | Already published | PR #383 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `phase2-contract-conformance-slice` | `13ba98bc` | Already published | PR #310 is merged. Exact commit matches the PR head. |
| `phase3-control-store` | `7321e7bb` | Already published | PR #390 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `phase3-state-store-prototype-gates` | `cbd76af9` | Already published | PR #316 is merged. Exact commit matches the PR head. |
| `phase3a-deterministic-state-model-slice` | `bbcc068d` | Superseded | Earlier model variant; consolidated implementation published in merged PR #316. |
| `phase3b-object-store-control-mvp-slice` | `7548dcaa` | Superseded | Earlier object-store MVP variant; consolidated implementation published in merged PR #316. |
| `phase3c-prototype-gate` | `b2db39d6` | Superseded | Earlier prototype variant; consolidated implementation published in merged PR #316. |
| `phase4-shadow-replay-comparison-reads` | `07d07a72` | Already published | PR #317 is merged. Exact commit matches the PR head. |
| `phase4a-shadow-replay` | `48822d02` | Already published | Commit is contained by PR head(s) #317. |
| `phase4b-internal-read-comparison` | `381ebb9e` | Already published | Commit is contained by PR head(s) #317. |
| `phase5-low-risk-writable-domains` | `0d31a28e` | Already published | PR #319 is merged. Exact commit matches the PR head. |
| `phase5a-projection-outbox-acks` | `0b562589` | Superseded | Earlier ack slice; revised combined implementation published in merged PR #319. |
| `phase5b-low-risk-writable-domain-hardening` | `8ce83f5d` | Superseded | Earlier ack hardening slice; revised combined implementation published in merged PR #319. |
| `phase6-completion` | `fffbb48d` | Already published | PR #391 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `phase6-storage-governance-metadata-completion` | `8c9ae995` | Already published | PR #320 is merged. Exact commit matches the PR head. |
| `phase6a-path-governance-metadata` | `c56a66a2` | Already published | PR #318 is merged. Exact commit matches the PR head. |
| `phase6b-external-location-metadata-without-vending` | `3bf8cf84` | Superseded | Earlier metadata slice; revised combined implementation published in merged PR #320. |
| `phase6c-storage-governance-metadata-closure` | `fc31e9fc` | Superseded | Earlier metadata closure; revised combined implementation published in merged PR #320. |
| `phase7-deployed-posture` | `ab12c665` | Already published | PR #387 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `phase7-snapshot-export-restore-handles` | `02621623` | Already published | PR #322 is merged. Exact commit matches the PR head. |
| `phase9-blockers` | `42e35afd` | Validation/integration outstanding | Clean six-commit safety/release stack with embedded regressions, but no retained exact-head test/audit closure found. Requires validation and integration review before selection. |
| `planner-runtime-handoff-seam-slice` | `7cd7de00` | Validation/integration outstanding | Earlier planner seam alternative; source-prerequisite note remains explicit. No current combined-source validation closure found. |
| `pr-315-refresh` | `166e0a1b` | Already published | PR #315 is merged. Exact commit matches the PR head. |
| `pr-369-refresh` | `f0a13ca3` | Already published | PR #369 is merged. Exact commit matches the PR head. |
| `pr-370-refresh` | `a3b0cc5d` | Already published | PR #370 is closed. Exact commit matches the PR head. |
| `pr-371-refresh` | `d901e669` | Already published | PR #371 is merged. Exact commit matches the PR head. |
| `pr-372-refresh` | `fabd6b4f` | Already published | PR #372 is merged. Exact commit matches the PR head. |
| `pr-408-review` | `3c1e6609` | Already published | PR #408 is merged. Exact commit matches the PR head. |
| `pr-419-review` | `f22a5436` | Already published | PR #419 is merged. Exact commit matches the PR head. |
| `pr-review-landing-20260803` | `8fa67fdb` | Superseded | Historical execution plan targets the August 3 PR cohort, most already merged; no current implementation to republish. |
| `reconcile-ci` | `db71500c` | Already published | PR #386 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `reconcile-docs` | `2437e553` | Already published | PR #384 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `reconcile-platform` | `feedd7c6` | Already published | PR #385 is merged. Local head differs from the published revised head; do not republish the old lineage. |
| `release-v0.2.1-prep` | `10c72b8a` | Already merged | Exact commit is an ancestor of current origin/main. |
| `runtime-convergence` | `8cca0201` | Validation/integration outstanding | PR #388 merged a revised branch; this older divergent base has additional dirty storage/docs/tests without final-source closure. |
| `s3-lambda-state-kernel-v1` | `7f18d74e` | Already published | PR #412 is merged. Exact commit matches the PR head. |
| `state-store-directory-20260912-01` | `fd419cb9` | Already published | PR #429 is open. Exact commit matches the PR head. |
| `state-store-vnext` | `409e4944` | Superseded | Original Gate 2/3/4 series was split/revised and landed through PRs #421-423. |
| `state-store-vnext-gate5` | `745ed92e` | Superseded | Superseded dirty maintenance candidate; corrected measured maintenance commit is included in PR #429. |
| `state-store-vnext-gate5-20260908-01` | `d246e952` | Already published; local evidence retained | Commit d246e952 is in PR #429 and pushed; 394 untracked evidence entries are raw qualification artifacts, not new source. |
| `state-store-vnext-gate6-20260909-01` | `92fd19f1` | Already published | Commit is contained by PR head(s) #430, #429. |
| `state-store-vnext-gate7-20260910-01` | `6ec0a8e6` | Already published | Commit is contained by PR head(s) #429. |
| `state-store-vnext-gate7-capacity-20260912-01` | `c1d1a729` | Already published | Commit is contained by PR head(s) #429. |
| `tier1-durable-identity-root-authority-schema-v2` | `357d9ccf` | Validation/integration outstanding | Staged v2 kernel plus dirty follow-up, explicitly not cut over; no final-source validation closure found and later control/v1 authority supersedes its base. |
| `topdown-issue-closeout` | `51c0f257` | Local evidence retained | Detached merged baseline; .closeout directory is historical operational evidence, not a source candidate. |
| `uc-route-authz-openapi-hardening` | `ab86f478` | Validation/integration outstanding | Old divergent source plus a mixed dirty API/Delta/governance patch; no current-source validation closure found. |
| `user-acceptance-pipeline-uat` | `ad596cf6` | Validation/integration outstanding | Historical dirty UAT implementation overlaps later merged UAT branches; no separate final-source closure for remaining patch. |

## Follow-up boundary

The validation/integration-outstanding rows were not selected as completed valid
candidates. Their source remains preserved. A follow-up can port and qualify those
changes against current main one issue at a time; this inventory does not authorize
claiming the old implementations are tested or closing their issues. Prior local
40 GiB build-admission notes describe their historical runs; this sweep did not
replace missing Rust evidence with formatting success or start broad old-graph
rebuilds with the current low disk headroom.

Machine-readable inventory, original dirty statuses, PR metadata, exact publication
checks, and source manifests are retained outside the checkout at
`/private/tmp/arco-publish-local-20260912-01/`.
