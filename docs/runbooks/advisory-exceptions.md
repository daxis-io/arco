# Advisory Exception Runbook

## Status

Active from 2026-08-02.

## Policy

Entries in `[advisories].ignore` make `cargo deny check advisories` succeed while
the workspace still contains the vulnerability. Exceptions are therefore
granted, never permanent. Every entry must be a table whose `reason` records:

- `tracking #<issue>` for the removal work;
- `granted <YYYY-MM-DD>`;
- `review-by <YYYY-MM-DD>`;
- the Arco-specific reachability assessment; and
- the concrete dependency condition that permits removal.

An exception gets its own issue and review. It must not be hidden inside an
unrelated feature change. The normal CI gate rejects missing metadata. The
scheduled Security Audit additionally rejects expired review dates and exposes
the result through its read-only `Security Audit Status` check and job summary.

## Granting and reviewing

1. Prove that a lockfile-only upgrade cannot reach the patched dependency.
2. Record the upstream constraint and reachability assessment on the tracking
   issue.
3. Add a dated exception with a review window no longer than 90 days.
4. Run `python3 .github/scripts/check_advisory_exceptions.py --check-expiry`
   and `cargo deny check advisories`.
5. At review, repeat the upgrade probe. Remove the exception as soon as the
   patched dependency resolves; otherwise record fresh evidence before changing
   the review date.

## Current quick-xml exception

RUSTSEC-2026-0194 and RUSTSEC-2026-0195 affect quick-xml before 0.41.0. The
frozen lock contains quick-xml 0.37.5 through object_store 0.11.2 and quick-xml
0.38.4 through opendal 0.54.1 and iceberg 0.6.0. These paths parse XML returned
by authenticated S3/Azure-compatible storage endpoints. The 0.3.0 integration
profile includes R2, so the assessment cannot rely on a GCS-only deployment.
Storage endpoints and credentials are operator-configured; API callers cannot
submit raw XML to these parsers. A hostile or compromised provider can still
return XML that consumes excessive CPU or memory. Provider qualification does
not remove that risk.

Tracking issue: [#327](https://github.com/daxis-io/arco/issues/327).
The original grant expired on 2026-09-30. Reviewed and re-granted on 2026-10-21;
review by 2026-11-20. This is a 30-day exception, not a fixed vulnerability.

Both lockfile-only probes failed on `main@274a3877` on 2026-10-21:

```text
cargo update -p quick-xml@0.37.5 --precise 0.41.0
  blocked by quick-xml ^0.37.0 in vendored object_store 0.11.2
cargo update -p quick-xml@0.38.4 --precise 0.41.0
  blocked by quick-xml ^0.38 in opendal 0.54.1 through iceberg 0.6.0
```

The probes exit 101 without changing the lockfile. Upgrading or backporting those
dependency chains requires its own compatibility and security review; extending
the exception does not qualify those changes. Review this grant explicitly in the
0.3.0 security follow-up rather than treating a green audit as absence of advisories.

At review time, try upgrading each locked quick-xml line to 0.41 or later and
inspect the constraint Cargo reports. Retire this exception when the relevant
object_store/DataFusion and opendal/Iceberg dependency chains accept a patched
quick-xml line.
