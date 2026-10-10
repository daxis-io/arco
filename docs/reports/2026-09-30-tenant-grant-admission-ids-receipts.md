# Test-only tenant grant admission ID and sequence receipts

Base: refreshed `origin/main` at `f4f827b7cf920d2ad645203d689eb013dea43a87`
(PR #449 squash merge). Worktree:
`.worktrees/tenant-grant-admission-ids-20260930`. Checks used its dedicated,
ignored `target/`, cloned copy-on-write from the preceding identity target.
The original target and other worktrees were preserved. Core trybuild output
created under `crates/arco-core/target/` was moved intact to the ignored
`target/trybuild-arco-core-admission/` after the suite.

The test-only preparation path now rejects an event ID found in native replay
and a sequence at or below the ledger's reservation high-water mark before it
persists a pending grant. This prevents a repeated request from wedging the
metastore with an intent whose native event cannot land. Two independent probe
instances admit only one pending grant in the memory-backend test.

## Source SHA-256

| File | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/authz/tenant_probe.rs` | `c28762fcb222fb71cd1eec4d6b34f67c5564ef83c0507fa548472492dd2bac82` |
| `crates/arco-catalog/tests/tenant_catalog_paths.rs` | `8930f9e71e9a0dc419b52317b930331a42b4297d10c5ee27de684cadfa4f9b9f` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `7decd5af0f493c91f0b7b7e4720d1b0b3b730483b2ed637c544e3ff95244ce4d` |
| [Admission plan](../plans/2026-09-30-tenant-grant-admission-ids.md) | `57492abc18d1e8a06403e3349d2cb2d9e0c6088479fd86e2f869b3dd55df1b02` |

## Checks

| Check | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check`, `git diff --check` | passed | empty output |
| Focused baseline `tenant_catalog_paths` | passed: 17 | `target/admission_baseline.log`: `e3a256c083f9a21969335205e507b69765cd28190d53ce33c7ed07f475384757` |
| Focused final `tenant_catalog_paths` | passed: 20 | `target/admission_green_first.log`: `1a303cbec84742d1d72f08c6c6b20cf6f279fb47a8410dbbd6a518a239ba9ef5` |
| Core/catalog suite with catalog `test-utils` | passed; updated integration file: 20 | `target/core_catalog.log`: `9130c6cfe95ce53f8d775be8b4ca97b4733943a9a3612cde458504a84452453f` |
| Explicit catalog rustdoc | passed: 3 ordinary, 11 compile-fail, 5 ignored | `target/rustdoc.log`: `08f357062caaee143f8d75a77aa110f059faae7d29defaba151726d9a671107d` |
| Core/catalog all-target Clippy, `-D warnings` | passed | `target/clippy.log`: `186ed743a150e64081b9b9147bf67767ecd3203dcb4ca671eaee9e02b9a6695d` |
| Default catalog check without `test-utils` | passed | `target/default_check.log`: `6f77dceb8c7781d1bad435859e6d1fa47bf47812654a59e07c5ee4b46c3c152d` |

## Retained failure and limit

- Test-first reuse cases failed before the source change: both a committed
  event ID and an aborted event ID could be prepared again
  (`target/admission_red.log`, SHA-256
  `9b4062f00df7fcccb1a462ea2455e98dfaf12bfa7d13a884db1b0d043bf65bb8`).
- This memory-backend preflight is not a production multiwriter admission
  protocol. A direct native writer can race the check; the immutable ledger
  event and sequence paths then reject conflicts, while the pending intent
  remains fail-closed. Production routing, provider fencing, revocation
  freshness, credential revocation, and principal purge remain separate gates.
