# Tenant identity native grant and catalog probe: local receipts

Base: refreshed `origin/main` at `5314d42e87398f498752c47acfa31dce07d216b1`
(PR #446 merge). Worktree: `.worktrees/tenant-identity-real-paths-20260929`.
Toolchain: `rustc 1.88.0`, `cargo 1.88.0`. Checks used this worktree's
dedicated ignored `target/`, `CARGO_PROFILE_TEST_DEBUG=0`,
`CARGO_PROFILE_DEV_DEBUG=0` for Clippy/check, and `CARGO_INCREMENTAL=0`.
After shared Cargo registry source unpacking was denied by the filesystem
sandbox, offline checks used the isolated `target/cargo-home/` with a local
registry index and a read-only link to the existing crate archive cache.
Neither shared Cargo files nor other worktree targets were changed.

## Tested source SHA-256

| File | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/authz/tenant_probe.rs` | `7b66f6a51dc7e80fad58ff3d1fc9fb0ba964217f2b77fd7cbf8bac4b37922e03` |
| `crates/arco-catalog/src/metastore/events.rs` | `1834e4e5fe837d0b1f4c0ccb42410c4097cc938a152549b7896595d0927be0d2` |
| `crates/arco-catalog/src/reader.rs` | `ffc9a1a716f6ace0ef4005a2a3007319cd5c11968bf9ab742a5117004793d18d` |
| `crates/arco-catalog/src/state_store/shadow_replay.rs` | `523727df19c6aa51e597119edfbe866b296a00955a59ab48b38ee1d5f90a95c7` |
| `crates/arco-catalog/tests/metastore_replay_publication.rs` | `342f9c49ad205ce36f8dac2d5e51e11b728ef169e8ead1f7240255fc7c371291` |
| `crates/arco-catalog/tests/permission_compilation.rs` | `37b4586e15b6fb33639438b5da5f2b9d4012e8f6bd49a8e5ecf5842c70cc267a` |
| `crates/arco-catalog/tests/tenant_revocation_probe.rs` | `a31ebddc3e46e50c9d912c8b905a9080299e1100855af6eec49729bcec843b30` |
| `crates/arco-catalog/tests/tenant_catalog_paths.rs` | `c0a9a949e8dac4fe22036bb9fbee4405b59e1ff5ce79c51fd95198d35c53f1cd` |
| `crates/arco-api/tests/unity_catalog_production_wiring.rs` | `5b57acb87bb7606c5dc0b121f2ad59df1c8fa92e3d56027522c42fda9eafbfa7` |
| `crates/arco-integration-tests/tests/delta_engine_smoke.rs` | `c5154689c506f2a3bc96abbb7fc0af8bb89a8d23bd94c0e4fb6b68f6de9b48c2` |
| `crates/arco-uc/src/permissions.rs` | `0ea47349f321a2eff8a73c24ad74e00bb0d11e2fb4bc76d2b6bb6a05fb5a42c7` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `060b476a6c6a2e8b233b17db39a4470e577c493f0e20aaf2b2ecaaad4bc898ba` |

## Checks

| Check | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check` | passed | empty output |
| Focused `identity_principals`, `tenant_catalog_paths`, `tenant_revocation_probe` | passed: 3 + 9 + 4 | `target/receipts/focused_source_final.log`: `e51415b787ff173660c02eb6f6b81f04dcd430d6edd8629998b7b8d9eacd7991` |
| `cargo test -p arco-core -p arco-catalog --features arco-catalog/test-utils --locked --offline --quiet` | passed, including 1,115 catalog unit tests, integration tests, core tests, and doctests | `target/receipts/core_catalog_source_final.log`: `e74bb8e97772d1955b7732eb393eb37f58d4c25afc16302266836cbdbb467dda` |
| `cargo test -p arco-catalog --features test-utils --doc --locked --offline` | passed: 3 ordinary, 11 compile-fail, 5 ignored | `target/receipts/rustdoc_final.log`: `e4034863b6f629eebf96af09287e94fb1eb7db99707d34cc385f9a9273a0cdc1` |
| `cargo clippy -p arco-core -p arco-catalog --all-targets --features arco-catalog/test-utils --locked --offline -- -D warnings` | passed | `target/receipts/clippy_source_final.log`: `c7c53e123050d21030da2532f43cf75a9fc51b423425f10a31f8d7ae438d9382` |
| `cargo check -p arco-catalog --locked --offline` without `test-utils` | passed | `target/receipts/default_check_final.log`: `243507c778f357958904af620bc1e3ecac8f4c3116a292b769127c51f12d3e17` |
| `cargo check --workspace --all-targets --features arco-catalog/test-utils --locked --offline` | passed | `target/receipts/workspace_check_final.log`: `1ed532ecd06bdbf8f35c968fee020b73cbde7476276704ec6de32988c2e52d85` |
| `cargo test -p arco-uc --lib --locked --offline --quiet` | passed: 27 | `target/receipts/uc_lib_final.log`: `d5626a832f2af09905c1e16e0b8d952ab5cb4e963d05344384dcb7f55933d799` |
| `cargo xtask repo-hygiene-check` on staged files | passed | `target/receipts/hygiene_pass.log`: `a1ec4f5e12cada7c502f4ee773c95d506f611d956f7f7cc5b6f344c5f0b50504` |

The new nine-case integration file uses actual `MetastoreLedger` events and
`CatalogReader` snapshots on two metastore roots. It checks persisted grant
identity-cut evidence, metastore state-token/watermark binding, current and
historical table reads, cache reuse and stale cuts, concurrent disable and
grant, restart, membership changes, foreign roots/events, and an unadmitted
owner field. Historical root transaction records are seeded test fixtures;
their catalog reader path is real. The native ledger append and metastore
state-token witness are separate commits.

## Retained failures and limits

- Test-first compile failed with `E0432` for the missing `TenantCatalogProbe`
  and `E0609`/`E0560` for the missing grant identity-cut field. Historical
  tests failed with `E0599` before the reader and probe methods existed.
- With equal ledger event IDs and sequences across two metastores, the foreign
  cut incorrectly returned `Allow`; the scope-binding assertion failed, then
  passed after binding `ControlPlaneScope` to the cut.
- A disabled grant was initially admitted. A foreign-scoped persisted event
  initially compiled. An owner update initially authorized a table read
  without grant admission. Each behavioral assertion failed before its fix.
- The first Clippy run found the optional field mistakenly added to principal
  and external-location test records. A later Clippy run found the long
  `compile_current` method. Both were corrected. Shared-registry Cargo unpack
  failed with `Operation not permitted`; the isolated offline Cargo home
  passed. An intermediate concurrency test assumed a metastore token existed
  even when disable won before grant admission; it now checks both valid
  outcomes and requires any readable path to deny.
- The first repository hygiene run rejected a literal path in this receipt
  report (`target/receipts/hygiene_final.log`, SHA-256
  `04e819e3b933b4468b7cd5189a23226fb554ef9c61efabd349ca9153f261d412`).
  The path is now a relative link; the source and tests were unchanged.
- A whole-repository constructor search after PR creation found four
  `GrantRecord` initializers in API, integration, and UC test code omitted by
  the focused package checks. They now set the optional evidence to `None`;
  the workspace all-targets check and UC library tests passed on that change.
- This is a `test-utils` probe. Production grant writers and catalog readers
  are not routed through it. Direct native ledger writes remain possible in
  the compatibility path; before routing, they must be unable to bypass
  identity admission. The new optional grant evidence field is wire-compatible
  with old records via serde default, but its Rust struct initializer is a
  source change for callers constructing `GrantRecord`.
- Owner-derived permissions are deliberately excluded until owner mutations
  gain tenant identity admission. A native ledger event may land without its
  metastore state-token witness if the second commit fails; production needs
  reconciliation and an explicit ambiguity receipt. The probe makes such
  unwitnessed state fail closed. No provider timing bound, credential TTL or
  provider revocation proof, production freshness policy, routing, tombstone,
  or principal purge is claimed.
