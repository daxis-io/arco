# Test-only tenant grant witness recovery: local receipts

Base: refreshed `origin/main` at `70b82f2d87639eefbb9931d130e4c5fcf5c01707`
(PR #447 squash merge). Worktree:
`.worktrees/tenant-identity-witness-recovery-20260929`. Checks used its
dedicated ignored `target/` with offline Cargo cache, debug information off,
and incremental compilation off. Other worktrees, targets, and evidence were
preserved.

The test-only path now stores one prepared grant event and its prior ledger
watermark in the metastore authority before native ledger append. After an
interrupted append, restart can witness the event only if its contents, scope,
sequence, prior witness, and uninterrupted ledger tail match. Pending, absent,
changed, or superseded evidence denies compilation. Recovery after a principal
disable records the grant but current identity still denies access. A later
admitted grant cannot witness an intervening direct ledger grant.

## Source SHA-256

| File | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/authz/tenant_probe.rs` | `1d157c6ef76462cf8dfa7b6c93f5571a9af45fc2c4ba0dbe0c6d1310426190c3` |
| `crates/arco-catalog/tests/tenant_catalog_paths.rs` | `36e6aaaf493838adcc513dcbecd87f977b2fe2deb77856b494c9a9ff84f2ecdb` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `153860011b5001bf02794d2763fa9dcb801e470f71f4ddb2dcd372b985bc8dda` |

## Checks

| Check | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check`, `git diff --check` | passed | empty output |
| Focused `tenant_catalog_paths` baseline | passed: 9 | `target/receipts/baseline.log`: `f5f7ac913dc233192efe3a1f7be021998bc1256f12afaba32e7d75190791b6f7` |
| Focused final `tenant_catalog_paths` | passed: 12 | `target/receipts/recovery_green_final.log`: `7c8ada12c523590794874509363aed0e48adc1937a5720f9dcdea04c86e72c49` |
| Core/catalog suite with catalog `test-utils` | passed | `target/receipts/core_catalog_final.log`: `e4ae10177d4aaf49b382274a7b3216de2a54eccf0d6ac095744b86af6172efe5` |
| Catalog rustdoc | passed: 3 ordinary, 11 compile-fail, 5 ignored | `target/receipts/rustdoc.log`: `124170d5065b5e91067198b720b930af9bafbe139a824b3905bdf123e4bae45d` |
| Core/catalog all-target Clippy with catalog `test-utils`, `-D warnings` | passed | `target/receipts/clippy_final.log`: `3c8b2dc2ac106b399e5a3d56f9dc312a7236e3b17e8e861c4ff778b9a1c27784` |
| Default catalog check without `test-utils` | passed | `target/receipts/default_check.log`: `eba45ce2c96e3168cf97c3a6332ba8d0dd059902eb1b942c88d7c7bc3ed67661` |
| Staged repository hygiene | passed | `target/receipts/hygiene.log`: `3e09c33159181f73f4c6acda784cc9d043a968beaeb24f4e9670c802489892bc` |

## Retained failures and limits

- Test-first recovery calls failed to compile with `E0599` because
  `prepare_grant` and `reconcile_prepared_grant` did not exist
  (`target/receipts/recovery_red.log`, SHA-256
  `962ca5d287bd45a7a6699961b083397e5a81d700511a1ee9c44d0eea16e7db9c`).
- A copied identity cut on a direct native grant was initially witnessed by a
  later legitimate grant. The focused assertion failed before the prior
  watermark guard (`target/receipts/direct_grant_red.log`, SHA-256
  `06502586a8ca3fedf6758b1c57e1c02ff64afcba64c788fe962e448b9b3fc04c`).
- The first full core/catalog run reached the new integration tests and failed
  eight of twelve because a seeded object gave the ledger a prior watermark
  without a metastore witness. A single-case rerun reproduced
  `prepared native grant has a changed prior witness`. Recording whether a
  prior witness existed fixed that distinction (`target/receipts/core_catalog.log`,
  SHA-256 `05733dcafed4621ae9218abde2c9acc8f8c927e432d213ad42124b962ef0a47a`;
  `target/receipts/prior_witness_red.log`, SHA-256
  `2f884a539b2ac30604c2c3971c66d7f221d024ea1c42909f0045241b0fc1c0c6`).
- A prepared event that never appears is retained and blocks further grant
  admission in that metastore. Production needs a fenced abort or disposition
  contract for late writers and a multiwriter admission protocol. Direct native
  grant writers remain available outside this test-only probe. No production
  routing, principal purge, provider credential revocation, or production
  freshness budget is qualified here.
