#!/usr/bin/env python3
"""Keep deny.toml advisory suppressions accountable.

cargo-deny reports success for advisories listed in [advisories].ignore. This
checker supplies the lifecycle metadata and expiry control that cargo-deny does
not provide natively:

* every PR requires a tracking issue, granted date, and review-by date;
* the scheduled audit additionally fails after the review-by date.

The scheduled workflow surfaces expiry through its read-only consolidated
Security Audit Status check; this script never changes repository issues.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DENY_TOML = REPO_ROOT / "deny.toml"
RUNBOOK = "docs/runbooks/advisory-exceptions.md"

TRACKING_RE = re.compile(r"tracking #(\d+)")
GRANTED_RE = re.compile(r"granted (\d{4}-\d{2}-\d{2})")
REVIEW_BY_RE = re.compile(r"review-by (\d{4}-\d{2}-\d{2})")


def _parse_date(raw: str) -> dt.date | None:
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        return None


def check_exceptions(
    deny_toml: str, *, today: dt.date, check_expiry: bool
) -> list[str]:
    """Return one problem per malformed or, when requested, expired exception."""
    config = tomllib.loads(deny_toml)
    ignored = config.get("advisories", {}).get("ignore", [])

    problems: list[str] = []
    for position, entry in enumerate(ignored):
        if not isinstance(entry, dict):
            problems.append(
                f"[advisories].ignore[{position}] ({entry!r}) is a bare id: use "
                f"{{ id = ..., reason = ... }} and document it per {RUNBOOK}"
            )
            continue

        advisory_id = entry.get("id", f"<entry {position}>")
        reason = entry.get("reason", "")
        tracking = TRACKING_RE.search(reason)
        granted = GRANTED_RE.search(reason)
        review_by = REVIEW_BY_RE.search(reason)

        if tracking is None:
            problems.append(
                f"{advisory_id}: reason must name the tracking issue as "
                "'tracking #<number>'"
            )
        granted_date = None if granted is None else _parse_date(granted.group(1))
        if granted_date is None:
            problems.append(
                f"{advisory_id}: reason must record 'granted <YYYY-MM-DD>'"
            )
        deadline = None if review_by is None else _parse_date(review_by.group(1))
        if deadline is None:
            problems.append(
                f"{advisory_id}: reason must record 'review-by <YYYY-MM-DD>'"
            )
            continue

        if granted_date is not None and deadline < granted_date:
            problems.append(
                f"{advisory_id}: review-by date must not precede the granted date"
            )
        elif granted_date is not None and (deadline - granted_date).days > 90:
            problems.append(
                f"{advisory_id}: exception window exceeds 90 days"
            )

        if check_expiry and deadline < today:
            problems.append(
                f"{advisory_id}: exception lapsed on {deadline.isoformat()}. "
                "Re-check the upstream constraint, then drop the entry or "
                f"re-grant it with fresh evidence ({RUNBOOK})"
            )

    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "deny_toml",
        nargs="?",
        type=Path,
        default=DEFAULT_DENY_TOML,
        help="path to deny.toml (default: repository root)",
    )
    parser.add_argument(
        "--check-expiry",
        action="store_true",
        help="also fail when an exception is past its review-by date",
    )
    args = parser.parse_args(argv)

    problems = check_exceptions(
        args.deny_toml.read_text(encoding="utf-8"),
        today=dt.date.today(),
        check_expiry=args.check_expiry,
    )
    if problems:
        print(f"{args.deny_toml}: advisory exception policy violated", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    status = (
        "tracked and unexpired"
        if args.check_expiry
        else "carry the required lifecycle metadata"
    )
    print(f"{args.deny_toml}: advisory exceptions {status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
