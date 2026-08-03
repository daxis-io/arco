//! Helpers for Cloud Run flow controller services.
//!
//! Flow controller services (dispatcher, sweeper, automation) operate by:
//! 1. Reading Parquet projections (via [`crate::orchestration::compactor::MicroCompactor::load_state`])
//! 2. Reconciling via stateless controllers
//! 3. Appending orchestration events to the ledger
//! 4. Triggering micro-compaction so Parquet projections stay fresh
//!
//! Without step (4), services will repeatedly make decisions from stale projections
//! and spam the ledger with duplicate acknowledgement events (e.g., `DispatchEnqueued`).

use arco_core::FlowPaths;
use arco_core::ScopedStorage;
use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use chrono::{DateTime, Days, Utc};
use ulid::Ulid;

use crate::compaction_client::compact_orchestration_events_fenced;
use crate::error::Result;
use crate::orchestration::LedgerWriter;
use crate::orchestration::compactor::MicroCompactor;
use crate::orchestration::compactor::fold::event_id_from_ledger_path;
use crate::orchestration::compactor::manifest::Watermarks;
use crate::orchestration::controllers::LedgerFreshness;
use crate::orchestration::events::OrchestrationEvent;
use crate::orchestration_compaction_lock_path;

/// Appends orchestration events and triggers compaction (when configured).
///
/// In production, flow services should always compact the exact event paths they append
/// so subsequent reconciliations observe updated Parquet projections.
///
/// # Errors
///
/// Returns an error if appending events to the ledger fails or if remote compaction fails.
pub async fn append_events_and_compact(
    ledger: &LedgerWriter,
    orch_compactor_url: Option<&str>,
    events: Vec<OrchestrationEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let storage = ledger.storage();
    let lock_path = orchestration_compaction_lock_path();
    let lock = DistributedLock::new(storage.backend().clone(), lock_path);
    let guard = lock.acquire(DEFAULT_LOCK_TTL, 10).await?;
    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;

    if let Some(url) = orch_compactor_url {
        let response = compact_orchestration_events_fenced(
            url,
            event_paths,
            guard.fencing_token().sequence(),
            lock_path,
            None,
        )
        .await?;
        if response.visibility_status != arco_core::VisibilityStatus::Visible {
            return Err(crate::error::Error::dispatch(format!(
                "orchestration compaction did not become visible: {}",
                response.visibility_status.as_str()
            )));
        }
    } else {
        let compactor = MicroCompactor::new(storage.clone());
        let result = compactor
            .compact_events_fenced(event_paths, guard.fencing_token().sequence(), lock_path)
            .await?;
        if result.visibility_status.as_str() != arco_core::VisibilityStatus::Visible.as_str() {
            return Err(crate::error::Error::dispatch(format!(
                "orchestration compaction did not become visible: {}",
                result.visibility_status.as_str()
            )));
        }
    }

    if let Err(error) = guard.release().await {
        tracing::warn!(
            error = %error,
            "failed to release orchestration compaction lock after successful compaction; relying on TTL cleanup"
        );
    }
    Ok(())
}

/// Maximum number of ledger date prefixes inspected when proving freshness.
///
/// A watermark more than this many days behind `now` means the workspace has
/// unprocessed history far beyond normal operation; the check fails safe to
/// [`LedgerFreshness::Stale`] instead of listing unbounded prefixes.
const LEDGER_FRESHNESS_MAX_DATE_PREFIXES: u64 = 32;

/// Determines whether the orchestration ledger has any event the compactor
/// has not folded into the Parquet projections.
///
/// Ledger objects live under `ledger/orchestration/{date}/{event_id}.json`,
/// where `date` is derived from the event id's ULID timestamp, so any event
/// newer than the fold watermark lives in a date prefix at or after the
/// watermark's date. The function lists those prefixes (bounded by
/// `LEDGER_FRESHNESS_MAX_DATE_PREFIXES`, plus one day of forward clock
/// slack) and reports [`LedgerFreshness::Current`] only when every listed
/// event id is at or below the watermark.
///
/// Every failure mode (missing watermark data, list errors, unrecognized
/// objects, out-of-range dates) degrades to [`LedgerFreshness::Stale`], so
/// callers fall back to the wall-clock compaction-lag guard. This is the
/// proof that lets the anti-entropy zombie reaper run in an idle workspace
/// (issue #338) without weakening the guard when compaction is genuinely
/// behind.
///
/// # This is not a contiguous-frontier proof
///
/// The check compares ids against `events_processed_through`. It says nothing
/// about events **at or below** that watermark. A straggler can exist: writer
/// A appends `E1`, writer B appends `E2 > E1` and folds through `E2`, then A
/// crashes before its fold — `E1` is durable, unfolded, and invisible here
/// because its id does not exceed the watermark. If `E1` carries a terminal
/// completion or a protective heartbeat, the projection is wrong in exactly
/// the direction that matters.
///
/// [`LedgerFreshness::Current`] therefore means "no ledger id exceeds the fold
/// watermark", not "every durable event is included". Callers must not treat
/// it as authorisation to destroy evidence; the anti-entropy sweeper accepts
/// it only for non-destructive repairs and still applies the wall-clock guard
/// before force-failing a RUNNING task (see
/// [`crate::orchestration::controllers::AntiEntropySweeper::scan_with_ledger_freshness`]).
/// Upgrading this to a real proof needs an accepted-event manifest or per-fold
/// publication witnesses covering the whole replayed prefix, not a maximum-id
/// comparison.
///
/// # Horizon limitation
///
/// The scan is bounded to `LEDGER_FRESHNESS_MAX_DATE_PREFIXES` (32) days of
/// date prefixes. A watermark older than that horizon yields
/// [`LedgerFreshness::Stale`], deliberately restoring the conservative
/// wall-clock compaction-lag path instead of listing unbounded prefixes.
///
/// Forward, the scan covers one day past `now`. That slack is sufficient
/// precisely because
/// [`MAX_EVENT_ID_FUTURE_SKEW`](crate::orchestration::ledger::MAX_EVENT_ID_FUTURE_SKEW)
/// rejects out-of-policy ids at append: without it an event whose ULID
/// timestamp is weeks ahead would be durable in a prefix this scan never
/// lists, and the function would report `Current` while that event sat
/// unfolded.
pub async fn orchestration_ledger_freshness(
    storage: &ScopedStorage,
    watermarks: &Watermarks,
    now: DateTime<Utc>,
) -> LedgerFreshness {
    if watermarks.last_committed_event_id != watermarks.last_visible_event_id {
        return LedgerFreshness::Stale;
    }

    let Some(processed_through) = watermarks.events_processed_through.clone() else {
        // Nothing folded yet: the projections are exact only if the ledger is
        // completely empty.
        return match storage.list(FlowPaths::ORCHESTRATION_LEDGER_PREFIX).await {
            Ok(objects) if objects.is_empty() => LedgerFreshness::Current,
            _ => LedgerFreshness::Stale,
        };
    };

    let Ok(watermark_ulid) = Ulid::from_string(&processed_through) else {
        return LedgerFreshness::Stale;
    };
    let Ok(watermark_ms) = i64::try_from(watermark_ulid.timestamp_ms()) else {
        return LedgerFreshness::Stale;
    };
    let Some(watermark_time) = DateTime::from_timestamp_millis(watermark_ms) else {
        return LedgerFreshness::Stale;
    };

    let start_date = watermark_time.date_naive();
    // One extra day absorbs forward producer-clock slack around midnight.
    let Some(end_date) = now.date_naive().checked_add_days(Days::new(1)) else {
        return LedgerFreshness::Stale;
    };
    if end_date < start_date {
        return LedgerFreshness::Stale;
    }
    let span_days = u64::try_from((end_date - start_date).num_days()).unwrap_or(u64::MAX);
    if span_days >= LEDGER_FRESHNESS_MAX_DATE_PREFIXES {
        return LedgerFreshness::Stale;
    }

    let mut date = start_date;
    while date <= end_date {
        let prefix = format!(
            "{}/{}",
            FlowPaths::ORCHESTRATION_LEDGER_PREFIX,
            date.format("%Y-%m-%d")
        );
        let Ok(objects) = storage.list(&prefix).await else {
            return LedgerFreshness::Stale;
        };
        for object in objects {
            let Some(event_id) = event_id_from_ledger_path(object.as_str()) else {
                return LedgerFreshness::Stale;
            };
            if event_id > processed_through.as_str() {
                return LedgerFreshness::Stale;
            }
        }
        let Some(next) = date.succ_opt() else {
            return LedgerFreshness::Stale;
        };
        date = next;
    }

    LedgerFreshness::Current
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;

    use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::orchestration::compactor::MicroCompactor;
    use crate::orchestration::events::{OrchestrationEventData, TriggerInfo};

    #[derive(Debug)]
    struct LockReleaseFailureBackend {
        inner: MemoryBackend,
    }

    impl LockReleaseFailureBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for LockReleaseFailureBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path == orchestration_compaction_lock_path()
                && matches!(precondition, WritePrecondition::MatchesVersion(_))
            {
                return Err(arco_core::Error::storage(
                    "injected lock release failure for test",
                ));
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn append_events_and_compact_makes_inline_writes_visible_without_remote_compactor() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage);

        append_events_and_compact(
            &ledger,
            None,
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_inline_visible".to_string(),
                    plan_id: "plan_inline_visible".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
        )
        .await
        .expect("append and compact");

        let (_manifest, state) = compactor.load_state().await.expect("load state");
        assert!(
            state.runs.contains_key("run_inline_visible"),
            "inline append+compact must leave the emitted event visible to readers"
        );
    }

    #[tokio::test]
    async fn append_events_and_compact_succeeds_when_lock_release_fails_after_visible_compaction() {
        let backend = Arc::new(LockReleaseFailureBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage);

        let result = append_events_and_compact(
            &ledger,
            None,
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_release_failure".to_string(),
                    plan_id: "plan_release_failure".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
        )
        .await;

        assert!(
            result.is_ok(),
            "post-commit lock release failures should not fail the caller"
        );

        let (_manifest, state) = compactor.load_state().await.expect("load state");
        assert!(state.runs.contains_key("run_release_failure"));
    }

    fn sample_event(run_id: &str) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::RunTriggered {
                run_id: run_id.to_string(),
                plan_id: format!("plan_{run_id}"),
                trigger: TriggerInfo::Manual {
                    user_id: "tester".to_string(),
                },
                root_assets: vec![],
                run_key: None,
                labels: std::collections::HashMap::new(),
                code_version: None,
            },
        )
    }

    fn watermarks_processed_through(event_id: Option<&str>) -> Watermarks {
        Watermarks {
            last_committed_event_id: event_id.map(ToString::to_string),
            last_visible_event_id: event_id.map(ToString::to_string),
            events_processed_through: event_id.map(ToString::to_string),
            last_processed_file: None,
            last_processed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn ledger_freshness_is_current_for_empty_ledger_without_watermark() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let watermarks = watermarks_processed_through(None);

        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Current);
    }

    #[tokio::test]
    async fn ledger_freshness_is_current_when_all_events_are_folded() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());

        let first = sample_event("run_a");
        let second = sample_event("run_b");
        let max_id = first.event_id.clone().max(second.event_id.clone());
        ledger.append(first).await.expect("append first");
        ledger.append(second).await.expect("append second");

        let watermarks = watermarks_processed_through(Some(&max_id));
        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Current);
    }

    #[tokio::test]
    async fn ledger_freshness_is_unknown_when_unprocessed_events_exist() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());

        let first = sample_event("run_a");
        let second = sample_event("run_b");
        let min_id = first.event_id.clone().min(second.event_id.clone());
        ledger.append(first).await.expect("append first");
        ledger.append(second).await.expect("append second");

        // Watermark stops at the earlier event: the later one is unprocessed.
        let watermarks = watermarks_processed_through(Some(&min_id));
        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Stale);
    }

    #[tokio::test]
    async fn ledger_freshness_is_unknown_for_empty_watermark_with_pending_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());
        ledger.append(sample_event("run_a")).await.expect("append");

        let watermarks = watermarks_processed_through(None);
        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Stale);
    }

    #[tokio::test]
    async fn ledger_freshness_is_unknown_when_committed_and_visible_diverge() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");

        let mut watermarks = watermarks_processed_through(Some("01HQ123EVT"));
        watermarks.last_committed_event_id = Some("01HQ999EVT".to_string());

        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Stale);
    }

    #[tokio::test]
    async fn ledger_freshness_fails_safe_for_unparseable_watermark() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");

        let watermarks = watermarks_processed_through(Some("not-a-ulid"));
        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(freshness, LedgerFreshness::Stale);
    }

    /// H4: this check cannot see a durable event at or below the watermark.
    ///
    /// Pinning the limitation in a test keeps [`LedgerFreshness::Current`]
    /// honest: the previous cases only ever placed pending events *above* the
    /// watermark, which made the result look like a completeness proof. It is
    /// not one, which is why the sweeper refuses to force-fail on it.
    #[tokio::test]
    async fn ledger_freshness_reports_current_despite_a_pending_event_below_the_watermark() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());

        // A straggler appended by a writer that crashed before folding it, and
        // a later event that a second writer appended and folded through.
        let straggler = sample_event("run_straggler");
        let folded = sample_event("run_folded");
        assert!(straggler.event_id < folded.event_id);
        let watermark = folded.event_id.clone();

        ledger.append(straggler).await.expect("append straggler");
        ledger.append(folded).await.expect("append folded");

        let watermarks = watermarks_processed_through(Some(&watermark));
        let freshness = orchestration_ledger_freshness(&storage, &watermarks, Utc::now()).await;

        assert_eq!(
            freshness,
            LedgerFreshness::Current,
            "a maximum-id scan cannot exclude an unfolded event at or below the \
             watermark; callers must not read Current as a completeness proof"
        );
    }

    /// The append horizon is what makes the scan's one-day forward slack
    /// sufficient: an id further ahead would live in a prefix the scan never
    /// lists, so it is refused at append instead.
    #[tokio::test]
    async fn ledger_append_rejects_event_ids_beyond_the_future_horizon() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let ledger = LedgerWriter::new(storage.clone());

        let mut far_future = sample_event("run_future");
        let future_ms = u64::try_from((Utc::now() + chrono::Duration::days(30)).timestamp_millis())
            .expect("a future timestamp fits a ULID millisecond");
        far_future.event_id = Ulid::from_parts(future_ms, 1).to_string();

        let error = ledger
            .append(far_future)
            .await
            .expect_err("an out-of-policy future event id must be rejected at append");
        assert!(
            error.to_string().contains("append horizon"),
            "the failure must name the horizon policy: {error}"
        );

        let objects = storage
            .list(FlowPaths::ORCHESTRATION_LEDGER_PREFIX)
            .await
            .expect("list ledger");
        assert!(
            objects.is_empty(),
            "a rejected event must not become durable"
        );
    }
}
