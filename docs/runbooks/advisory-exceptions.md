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
by authenticated S3/Azure-compatible storage endpoints; Arco's frozen deployed
configuration uses the GCS JSON API. The direct end-user reachability is
therefore low, while a hostile or compromised storage endpoint remains the
residual risk.

Tracking issue: #327. Granted: 2026-07-05. Review by: 2026-09-30.

At review time, try upgrading each locked quick-xml line to 0.41 or later and
inspect the constraint Cargo reports. Retire this exception when the relevant
object_store/DataFusion and opendal/Iceberg dependency chains accept a patched
quick-xml line.
