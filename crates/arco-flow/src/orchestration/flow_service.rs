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
    }

    guard.release().await?;
    Ok(())
}
