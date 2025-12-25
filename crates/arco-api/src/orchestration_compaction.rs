//! Orchestration compaction helpers (sync or remote).

use serde::Serialize;

use arco_core::ScopedStorage;
use arco_flow::orchestration::OrchestrationLedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::OrchestrationEvent;
use arco_flow::orchestration::ledger::LedgerWriter;

use crate::config::Config;
use crate::error::ApiError;

#[derive(Debug, Serialize)]
struct CompactRequest {
    event_paths: Vec<String>,
}

/// Compacts orchestration events using a remote compactor or inline micro-compactor.
pub async fn compact_orchestration_events(
    config: &Config,
    storage: ScopedStorage,
    event_paths: Vec<String>,
) -> Result<(), ApiError> {
    if event_paths.is_empty() {
        return Ok(());
    }

    if let Some(url) = config.orchestration_compactor_url.as_ref() {
        let client = reqwest::Client::new();
        let endpoint = format!("{}/compact", url.trim_end_matches('/'));
        let response = client
            .post(endpoint)
            .json(&CompactRequest { event_paths })
            .send()
            .await
            .map_err(|e| {
                ApiError::internal(format!("orchestration compaction request failed: {e}"))
            })?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| ApiError::internal(format!("compaction error body read failed: {e}")))?;
        let message = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());

        return Err(ApiError::internal(format!(
            "orchestration compaction failed ({status}): {message}"
        )));
    }

    if config.debug {
        let compactor = MicroCompactor::new(storage);
        compactor
            .compact_events(event_paths)
            .await
            .map_err(|e| ApiError::internal(format!("orchestration compaction failed: {e}")))?;
    }

    Ok(())
}

/// Ledger writer that triggers orchestration compaction after each event append.
pub struct CompactingLedgerWriter {
    ledger: LedgerWriter,
    storage: ScopedStorage,
    config: Config,
}

impl CompactingLedgerWriter {
    /// Creates a new compacting ledger writer.
    #[must_use]
    pub fn new(storage: ScopedStorage, config: Config) -> Self {
        Self {
            ledger: LedgerWriter::new(storage.clone()),
            storage,
            config,
        }
    }
}

impl OrchestrationLedgerWriter for CompactingLedgerWriter {
    async fn write_event(&self, event: &OrchestrationEvent) -> Result<(), String> {
        let path = LedgerWriter::event_path(event);
        self.ledger
            .append(event.clone())
            .await
            .map_err(|e| format!("{e}"))?;
        compact_orchestration_events(&self.config, self.storage.clone(), vec![path])
            .await
            .map_err(|e| format!("{e:?}"))
    }
}
