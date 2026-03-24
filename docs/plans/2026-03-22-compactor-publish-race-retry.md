# Compactor Publish Race Retry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden orchestration compaction against concurrent manifest publish races without relying on fragile storage error strings.

**Architecture:** Keep the existing manifest snapshot plus pointer publish flow, but classify publish race outcomes at the `publish_manifest` boundary with a typed internal error. Retry `compact_events_with_epoch` from a fresh manifest/state load with bounded attempts and epoch validation on each pass so the retry observes the winner's state instead of republishing stale state.

**Tech Stack:** Rust, `cargo test`, `arco-flow` orchestration compactor, `arco-core` storage backends, `metrics` crate.

---

## Scope

- Add deterministic tests for snapshot-conflict retry recovery and stale-epoch validation after a retry-triggering race.
- Replace string-matched publish conflict detection with a typed internal publish error.
- Add bounded retry logging and a compactor-specific retry counter.
- Verify the existing pointer-CAS conflict behavior with the new typed boundary and document the compactor-path assumption in code.

## Verification Matrix

- `cargo test -p arco-flow compact_events_retries_after_concurrent_manifest_snapshot_conflict -- --nocapture`
- `cargo test -p arco-flow compact_events_retry_revalidates_expected_epoch -- --nocapture`
- `cargo test -p arco-flow publish_manifest_uses_cas -- --nocapture`
- `cargo test -p arco-flow publish_manifest_reports_persisted_not_visible_on_cas_loss -- --nocapture`
- `cargo test -p arco-flow 'orchestration::compactor::service::tests::' -- --nocapture`

## Acceptance Criteria

- `compact_events_with_epoch` retries boundedly after a concurrent immutable snapshot conflict.
- Retry classification is based on typed publish errors emitted from `publish_manifest`, not storage message strings.
- Each retry re-reads manifest state and re-validates `expected_epoch` against the current pointer epoch.
- Retry attempts emit tracing and increment a dedicated compactor retry metric.
