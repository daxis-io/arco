# Governance

Arco uses an Apache-inspired governance model focused on open participation, consensus-building, and transparent technical decisions.

This project is independent and is not affiliated with or endorsed by the Apache Software Foundation.

## Roles

- Users: consume releases, report issues, and provide feedback.
- Contributors: submit code, docs, tests, and design proposals.
- Maintainers: review and merge changes, steward releases, and uphold project quality.

Current maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

## Decision-Making

### Default: Lazy Consensus

Most changes proceed through public pull request discussion. If no substantive objections are raised in review, maintainers may merge.

### Escalated Decisions

The following require explicit maintainer consensus and recorded rationale (usually ADR-linked):

- Breaking API or data-contract changes.
- Security posture changes.
- Major architecture pivots.
- Governance or release policy changes.

When consensus cannot be reached, a majority vote of maintainers decides.

## Maintainer Responsibilities

Maintainers are expected to:

- Keep reviews technical, respectful, and timely.
- Enforce repository policies and CI gates consistently.
- Prefer reversible, well-tested changes.
- Ensure release notes and decision records are accurate.

## Becoming a Maintainer

A contributor may be nominated by any maintainer based on sustained, high-quality contributions across code review, implementation, and incident response. Promotion requires agreement from current maintainers.

## Inactivity and Emeritus Status

Maintainers who are inactive for an extended period may be moved to emeritus status by maintainer consensus. Emeritus maintainers may return through the same consensus process.

## Conflict Resolution

When technical disagreement persists:

1. Clarify requirements and constraints in writing.
2. Compare alternatives with explicit tradeoffs.
3. Prefer small experiments and measurable outcomes.
4. Record final decisions via PR and ADR when applicable.

## Governance Changes

Changes to this document must be made by pull request and require explicit maintainer approval.
