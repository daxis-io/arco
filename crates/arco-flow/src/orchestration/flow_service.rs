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

use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};

use crate::compaction_client::compact_orchestration_events_fenced;
use crate::error::Result;
use crate::orchestration::LedgerWriter;
use crate::orchestration::compactor::MicroCompactor;
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
}
