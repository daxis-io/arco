//! Orchestration compaction helpers (sync or remote).

use std::time::Duration;

use serde::Serialize;

use arco_core::{DistributedLock, ScopedStorage};
use arco_flow::error::Error as FlowError;
use arco_flow::orchestration::OrchestrationLedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::OrchestrationEvent;
use arco_flow::orchestration::ledger::LedgerWriter;
use arco_flow::orchestration_compaction_lock_path;

use crate::config::Config;
use crate::error::ApiError;

#[derive(Debug, Serialize)]
struct CompactRequest {
    event_paths: Vec<String>,
    fencing_token: u64,
    lock_path: String,
}

const COMPACTION_LOCK_TTL: Duration = Duration::from_secs(30);
const COMPACTION_LOCK_MAX_RETRIES: u32 = 8;

/// Compacts orchestration events using a remote compactor or inline micro-compactor.
pub async fn compact_orchestration_events(
    config: &Config,
    storage: ScopedStorage,
    event_paths: Vec<String>,
) -> Result<(), ApiError> {
    if event_paths.is_empty() {
        return Ok(());
    }
    if config.orchestration_compactor_url.is_none() && !config.debug {
        return Ok(());
    }

    let lock_path = orchestration_compaction_lock_path().to_string();
    let lock = DistributedLock::new(storage.backend().clone(), lock_path.clone());
    let guard = lock
        .acquire(COMPACTION_LOCK_TTL, COMPACTION_LOCK_MAX_RETRIES)
        .await
        .map_err(|e| match e {
            arco_core::Error::PreconditionFailed { message } => ApiError::conflict(format!(
                "orchestration compaction lock unavailable: {message}"
            )),
            other => ApiError::from(other),
        })?;
    let fencing_token = guard.fencing_token().sequence();

    if let Some(url) = config.orchestration_compactor_url.as_ref() {
        let client = reqwest::Client::new();
        let endpoint = format!("{}/compact", url.trim_end_matches('/'));
        let response = client
            .post(endpoint)
            .json(&CompactRequest {
                event_paths,
                fencing_token,
                lock_path,
            })
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

        return Err(map_remote_compaction_error(status, &message));
    }

    if config.debug {
        let compactor = MicroCompactor::new(storage);
        compactor
            .compact_events_fenced(event_paths, fencing_token, &lock_path)
            .await
            .map_err(|e| map_inline_compaction_error(&e))?;
    }

    Ok(())
}

fn map_remote_compaction_error(status: reqwest::StatusCode, message: &str) -> ApiError {
    let detail = format!("orchestration compaction failed ({status}): {message}");
    if status == reqwest::StatusCode::CONFLICT || status == reqwest::StatusCode::PRECONDITION_FAILED
    {
        ApiError::conflict(detail)
    } else {
        ApiError::internal(detail)
    }
}

fn map_inline_compaction_error(error: &FlowError) -> ApiError {
    let detail = format!("orchestration compaction failed: {error}");
    match error {
        FlowError::StaleFencingToken { .. } | FlowError::FencingLockUnavailable { .. } => {
            ApiError::conflict(detail)
        }
        FlowError::Core(arco_core::Error::PreconditionFailed { message })
            if message.contains("fencing") || message.contains("stale fencing token") =>
        {
            ApiError::conflict(detail)
        }
        _ => ApiError::internal(detail),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arco_flow::error::Error as FlowError;
    use reqwest::StatusCode;

    #[test]
    fn maps_remote_compaction_conflict_to_http_409() {
        let error = map_remote_compaction_error(StatusCode::CONFLICT, "stale fencing token");
        assert_eq!(error.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn maps_remote_compaction_non_conflict_to_http_500() {
        let error = map_remote_compaction_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "upstream internal error",
        );
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn maps_inline_fencing_precondition_to_http_409() {
        let error =
            map_inline_compaction_error(&FlowError::Core(arco_core::Error::PreconditionFailed {
                message: "stale fencing token".to_string(),
            }));
        assert_eq!(error.status(), StatusCode::CONFLICT);
    }
}
