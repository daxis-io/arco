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

use crate::compaction_client::compact_orchestration_events;
use crate::error::Result;
use crate::orchestration::LedgerWriter;
use crate::orchestration::events::OrchestrationEvent;

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

    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;

    if let Some(url) = orch_compactor_url {
        compact_orchestration_events(url, event_paths).await?;
    }

    Ok(())
}
