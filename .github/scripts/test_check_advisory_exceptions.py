"""Regression tests for the deny.toml advisory-exception policy checker."""

from __future__ import annotations

import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_advisory_exceptions import (  # noqa: E402
    DEFAULT_DENY_TOML,
    check_exceptions,
)

TODAY = dt.date(2026, 8, 2)

ANNOTATED = """
[advisories]
ignore = [
    { id = "RUSTSEC-2026-0194", reason = "blocked upstream; tracking #327; granted 2026-07-05; review-by 2026-09-30" },
]
"""

BARE_ID = """
[advisories]
ignore = ["RUSTSEC-2026-0194"]
"""

NO_TRACKING = """
[advisories]
ignore = [
    { id = "RUSTSEC-2026-0194", reason = "blocked upstream; granted 2026-07-05; review-by 2026-09-30" },
]
"""

LAPSED = """
[advisories]
ignore = [
    { id = "RUSTSEC-2026-0194", reason = "blocked upstream; tracking #327; granted 2026-02-01; review-by 2026-04-30" },
]
"""

OVERLONG = """
[advisories]
ignore = [
    { id = "RUSTSEC-2026-0194", reason = "blocked upstream; tracking #327; granted 2026-07-05; review-by 2026-10-31" },
]
"""


class CheckAdvisoryExceptionsTest(unittest.TestCase):
    def test_annotated_exception_passes(self) -> None:
        self.assertEqual(
            check_exceptions(ANNOTATED, today=TODAY, check_expiry=True), []
        )

    def test_bare_advisory_id_is_rejected(self) -> None:
        problems = check_exceptions(BARE_ID, today=TODAY, check_expiry=False)
        self.assertEqual(len(problems), 1)
        self.assertIn("bare id", problems[0])

    def test_missing_tracking_issue_is_rejected(self) -> None:
        problems = check_exceptions(NO_TRACKING, today=TODAY, check_expiry=False)
        self.assertEqual(len(problems), 1)
        self.assertIn("tracking #<number>", problems[0])

    def test_lapsed_review_date_fails_only_expiry_check(self) -> None:
        self.assertEqual(check_exceptions(LAPSED, today=TODAY, check_expiry=False), [])
        problems = check_exceptions(LAPSED, today=TODAY, check_expiry=True)
        self.assertEqual(len(problems), 1)
        self.assertIn("lapsed on 2026-04-30", problems[0])

    def test_exception_window_cannot_exceed_ninety_days(self) -> None:
        problems = check_exceptions(OVERLONG, today=TODAY, check_expiry=False)
        self.assertEqual(len(problems), 1)
        self.assertIn("exceeds 90 days", problems[0])

    def test_no_ignore_section_is_allowed(self) -> None:
        self.assertEqual(
            check_exceptions("[advisories]\n", today=TODAY, check_expiry=True), []
        )

    def test_committed_deny_toml_satisfies_policy(self) -> None:
        problems = check_exceptions(
            DEFAULT_DENY_TOML.read_text(encoding="utf-8"),
            today=TODAY,
            check_expiry=True,
        )
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
