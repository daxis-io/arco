# Proto Hard-Cut Follow-Up Addendum: Slice 5

## Why this batch now

The public proto surface is now close enough to the intended hard-cut shape that the highest-value
remaining step is to freeze it explicitly:

- `arco.common.v1` is reduced to cross-domain value objects (`Partition*`, `ScalarValue`,
  `NullValue`).
- `proto/arco/orchestration/v1/orchestration.proto` has a visible conceptual event boundary, but a
  physical split is still mostly file-organization churn because the event envelope shares several
  supporting public types with the rest of the package.
- CI still checks `buf breaking` against `main`, which does not by itself preserve a named
  post-hard-cut baseline inside the repository.

## Scope

1. Commit a frozen Buf image for the intended post-hard-cut public surface.
2. Add a repo-local verification command that runs `buf breaking` against that frozen image.
3. Run the frozen-baseline check in CI in addition to the existing branch-vs-`main` comparison.
4. Update proto docs to point authors at the explicit post-cut baseline command/path.

## Explicit deferrals after this batch

- Treat `arco.common.v1` as complete unless a later slice finds a concrete non-shared value object
  reintroduced there.
- Defer physically splitting `proto/arco/orchestration/v1/orchestration.proto` until the split is
  clearly mechanical enough to avoid large generated churn for mostly organizational gain.
