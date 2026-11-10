# Arrow run-end validation backport

Hosted CI for authority-8 GC PR #463 found RUSTSEC-2026-0442 on November 9,
2026. The affected dependencies are arrow-data 54.3.1 (workspace Arrow/Parquet)
and 55.2.0 (Iceberg 0.6). RustSec issued the advisory on November 5; neither
locked major line has a patched registry release.

## Decision

Backport Apache Arrow's bounds validation repair in the two existing dependency
versions using the repository's existing vendoring pattern. This avoids a broad
Arrow/Parquet/Iceberg migration during GC qualification. No dependency edge,
public API, package version, advisory exception or policy relaxation is added.
Keep this repair in a separate commit from the qualified GC implementation.

The defect checks the run-end child's physical length and offset against its
last value. Validation must instead compare the parent's logical length plus
offset, except that an empty parent is valid beyond the last run. The backport
passes those parent values into the existing validator and checks their sum
for overflow before comparison.

The source-pinned repair is
[apache/arrow-rs a9455d4a269d1edce4891c0927fed1c2bf24fb72](https://github.com/apache/arrow-rs/commit/a9455d4a269d1edce4891c0927fed1c2bf24fb72),
published October 19, 2026. The only compatibility adaptations are older module
imports/field names and an inline checked addition because these releases lack
the newer checked-length helper. Their public validation signatures stay intact.
The upstream empty-parent behavior is preserved.

## Scope and provenance

The vendor directories contain the existing published source and license/notice
files, plus the fix and regression tests in src/data.rs. Registry-generated
Cargo.lock and local .cargo-ok are omitted. Cargo.toml and Cargo.toml.orig retain
their published content. Cargo resolves both existing versions through explicit
patch aliases; the lockfile removes only their registry source/checksum entries.

| Published archive | SHA-256 |
| --- | --- |
| arrow-data-54.3.1.crate | 61cfdd7d99b4ff618f167e548b2411e5dd2c98c0ddebedd7df433d34c20a4429 |
| arrow-data-55.2.0.crate | 8de1ce212d803199684b658fc4ba55fb2d7e87b213de5af415308d2fee3619c2 |

The registry archive hashes, pristine per-file hashes, test-first failures,
upstream patch and local/hosted gate receipts are retained with the publication
evidence. Local path packages are not registry advisory matches: a green
cargo-deny result alone does not prove these backports. Review the actual
validation delta and require the behavioral tests for both versions in CI.

## Verification and removal

Before the repair, the upstream regression tests reject the implementation for
both an overlong parent and a parent slice ending past the last run. Empty and
valid slice controls pass. After the repair, all four tests and each vendored
crate's existing unit tests must pass through the actual workspace resolution:

```sh
cargo shared test -p arrow-data@54.3.1 --lib
cargo shared test -p arrow-data@55.2.0 --lib
```

Qualify the resulting dependency graph with strict workspace Clippy, relevant
GC/catalog/storage regressions, complete hosted CI and the existing GC
qualification boundaries. The previous 49-file GC performance measurements
remain bound to their original exact source; do not label old executable
results as a measurement of a new build.

The local cargo-deny 0.20.2 default admits transitive unsound advisories, unlike
CI's pinned 0.18.9. Use the CI version or an evidence-only configuration with
unsound = "all" when comparing local advisory gates. Do not weaken CI to make
the results agree.

Remove these two patches when upstream publishes compatible fixed releases,
or as part of a separately qualified Arrow/Iceberg upgrade. Remove only after
both resolved versions have the fix and all regression tests remain covered.

References:
[RustSec advisory](https://rustsec.org/advisories/RUSTSEC-2026-0442.html),
[upstream reproducer](https://github.com/apache/arrow-rs/issues/11587).
