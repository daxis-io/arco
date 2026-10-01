# Test-only prepared tenant grant disposition: local receipts

Base: refreshed `origin/main` at `b8f8d8883686cf3f062ef75a39cea843bd156f4c`
(PR #448 squash merge). Worktree:
`.worktrees/tenant-grant-disposition-20260930`. Checks used its dedicated,
ignored `target/`, cloned copy-on-write from the preceding identity target.
The original target and other worktrees were preserved. Core trybuild output
created under `crates/arco-core/target/` was moved intact to the ignored
`target/trybuild-arco-core/` after the suite.

The test-only abort occupies the exact native event ID and sequence of a
prepared grant. The immutable ledger event path makes the original grant and
the abort mutually exclusive. A partial sequence reservation and pending
watermark without an event can be completed by the abort. The metastore
authority witnesses the winning event before clearing the pending intent.
Changed or superseded evidence remains pending and fails closed.

## Source SHA-256

| File | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/authz/tenant_probe.rs` | `9466162c57bdc404b78306d4a1306937c6a3da6ab7dbe2467a0f07934a6badcd` |
| `crates/arco-catalog/src/metastore/events.rs` | `d44355c89ad3dcbd04d5cf2c4b780d22ba4aa3433c8327ecc73e1dae98d1da8d` |
| `crates/arco-catalog/src/metastore/replay.rs` | `ac8de1bc12b46d468fe030e5606275603d36842a66a47fe0b21fb5a558aea277` |
| `crates/arco-catalog/tests/tenant_catalog_paths.rs` | `410eea243ed5338cb9da8a83ff47846dadf4b9aa0e32b8c54fef876917c9ff07` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `ece16d19a7329ba8e1a94e23cead41d14a2a72387b9dd4dc401461de03c0dcfe` |
| [Disposition plan](../plans/2026-09-30-tenant-grant-disposition.md) | `5ab1b5df18cfc88e072ef747872d48a95c1c899447167bd314d47d6f45878e06` |

## Checks

| Check | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check`, `git diff --check` | passed | empty output |
| Focused `tenant_catalog_paths` before change | passed: 12 | baseline observed before test edits |
| Focused disposition after partial-append fix | passed: 16 | `target/disposition_green_second.log`: `0f5f57d594d6f8fc0fef34f171ba62e609b630c5386a20cdf7307bac0ba8f3c0` |
| Concurrent grant/abort test | passed: 1 | `target/race_first.log`: `a5b3804866cae2990ea93e206ef49facf0f7a323be28775c2d6f7a059be65fe2` |
| Core/catalog suite with catalog `test-utils` | passed; updated integration file: 17 | `target/core_catalog.log`: `e25e7f2476d3b8dc176dff88555ff24c722048079ce6a297402a838ffc78d52c` |
| Explicit catalog rustdoc | passed: 3 ordinary, 11 compile-fail, 5 ignored | `target/rustdoc.log`: `adc8e26554a23d95d1a23f481cea873672f3a3999b8269ca07c27d7a33d043f3` |
| Core/catalog all-target Clippy, `-D warnings` | passed | `target/clippy.log`: `ef7302e09d81d8bb3f05c5b978e72c5e845408fbe8fe919b513bea0e4e8f7b97` |
| Default catalog check without `test-utils` | passed | `target/default_check.log`: `418be60f51aa617a8ccc4f319b9eda173184dbc3391a605d6ca57767e6f3d5d1` |

## Retained failures and limits

- Test-first disposition calls failed compilation with `E0599`: missing
  `abort_prepared_grant` and `GrantAdmissionAborted` (`target/disposition_red.log`,
  SHA-256 `483ac7b6ed6b77bae28ec1361124d58242341e31982037509143f2541b928fbe`).
- The first partial-append test failed because `latest_watermark` refuses an
  in-flight watermark whose event is absent. Checking the persisted event tail
  before attempting the abort fixed this case (`target/partial_red.log`,
  SHA-256 `74d0e0bb8a43311cc517ca261202a160bf09b2138608e3a36ab494fb5b571dd1`).
- This is a memory-backend, `test-utils` probe. Direct native writers remain
  available; no production multiwriter admission, provider fencing, tenant-wide
  revocation freshness, credential revocation, routing, or principal purge is
  qualified. The principal purge retention period remains undecided by policy.
