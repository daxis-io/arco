//! Canonical Tier-2 event envelope (ADR-004).
//!
//! Tier-2 producers write events to an append-only ledger. The ledger must remain
//! evolvable and debuggable in production, which requires a stable envelope with:
//! - Version gating (`event_version`)
//! - Deduplication (`idempotency_key`)
//! - Traceability (`source`, optional `trace_id`)
//! - Optional monotonic ordering (`sequence_position`)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::error::{Error, Result};

/// Canonical envelope for all Tier-2 ledger events (ADR-004).
///
/// The envelope is intentionally metadata-heavy to support:
/// - Schema evolution (via `event_version`)
/// - Deduplication (via `idempotency_key`)
/// - Operational debugging (via `source` and `trace_id`)
/// - Clock-skew resistant ordering when ingest assigns `sequence_position`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEvent<T> {
    /// Event type discriminator (e.g., `"materialization.completed"`).
    pub event_type: String,

    /// Schema version for this event type (enables evolution).
    pub event_version: u32,

    /// Idempotency key for deduplication (client-provided or generated).
    pub idempotency_key: String,

    /// When the event occurred (source timestamp).
    pub occurred_at: DateTime<Utc>,

    /// Service/component that produced the event.
    pub source: String,

    /// Correlation ID for request tracing.
    pub trace_id: Option<String>,

    /// Optional monotonic position for ordering (assigned at ingest).
    pub sequence_position: Option<u64>,

    /// The actual event payload.
    pub payload: T,
}

/// Trait for strongly-typed event payloads with stable envelope metadata.
///
/// Implement this for any payload type that can be written to the Tier-2 ledger.
/// It allows writers to populate `event_type` and `event_version` without relying
/// on ad-hoc strings at call sites.
pub trait CatalogEventPayload {
    /// Event type discriminator (stable across producers).
    const EVENT_TYPE: &'static str;

    /// Event schema version (starts at `1`).
    const EVENT_VERSION: u32;
}

impl<T> CatalogEvent<T> {
    /// Validates required envelope fields.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or invalid.
    pub fn validate(&self) -> Result<()> {
        if self.event_type.trim().is_empty() {
            return Err(Error::InvalidInput("event_type must be non-empty".into()));
        }
        if self.event_version == 0 {
            return Err(Error::InvalidInput("event_version must be >= 1".into()));
        }
        if self.idempotency_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "idempotency_key must be non-empty".into(),
            ));
        }
        if self.source.trim().is_empty() {
            return Err(Error::InvalidInput("source must be non-empty".into()));
        }
        Ok(())
    }

    /// Generates a deterministic idempotency key for a payload (ADR-004).
    ///
    /// The key is derived from a canonical JSON representation of:
    /// `{ event_type, event_version, payload }`.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn generate_idempotency_key(
        event_type: &str,
        event_version: u32,
        payload: &impl Serialize,
    ) -> Result<String> {
        #[derive(Serialize)]
        struct IdempotencyKeyInput<'a, P: ?Sized> {
            event_type: &'a str,
            event_version: u32,
            payload: &'a P,
        }

        let input = IdempotencyKeyInput {
            event_type,
            event_version,
            payload,
        };

        let bytes = canonical_json_bytes(&input)?;
        let hash = sha2::Sha256::digest(&bytes);
        let hash_bytes = hash.as_slice();
        let prefix = hash_bytes.get(..16).unwrap_or(hash_bytes);
        Ok(format!("auto:{}", hex::encode(prefix)))
    }
}

fn canonical_json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut json = serde_json::to_value(value).map_err(|e| Error::Serialization {
        message: format!("failed to convert value to JSON: {e}"),
    })?;
    canonicalize_json_value(&mut json);
    serde_json::to_vec(&json).map_err(|e| Error::Serialization {
        message: format!("failed to serialize canonical JSON: {e}"),
    })
}

fn canonicalize_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // Ensure deterministic key ordering even if serde_json enables preserve_order.
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();

            let mut new_map = serde_json::Map::new();
            for key in keys {
                if let Some(mut child) = map.remove(&key) {
                    canonicalize_json_value(&mut child);
                    new_map.insert(key, child);
                }
            }
            *map = new_map;
        }
        serde_json::Value::Array(values) => {
            for child in values {
                canonicalize_json_value(child);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_is_deterministic() {
        #[derive(Serialize)]
        struct Payload {
            b: u32,
            a: u32,
        }

        let payload = Payload { b: 2, a: 1 };
        let key1 =
            CatalogEvent::<()>::generate_idempotency_key("test.event", 1, &payload).expect("key");
        let key2 =
            CatalogEvent::<()>::generate_idempotency_key("test.event", 1, &payload).expect("key");
        assert_eq!(key1, key2);
        assert!(key1.starts_with("auto:"));
    }
}
