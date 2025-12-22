//! Identifier helpers for orchestration.
//!
//! Centralizes Cloud Tasks ID generation and internal ID formats.

use base32::Alphabet;
use sha2::{Digest, Sha256};

/// Generates a Cloud Tasks-compliant task ID from an internal ID.
///
/// Uses base32 encoding of the SHA-256 hash and truncates to 26 characters,
/// yielding 130 bits of entropy in a compact, API-compliant ID.
///
/// Format: `{prefix}_{base32(sha256(internal_id))[0..26]}`
#[must_use]
pub fn cloud_task_id(prefix: &str, internal_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(internal_id.as_bytes());
    let hash = hasher.finalize();

    // 32-byte hash -> 52 base32 chars; use 26 chars (130 bits) for compact IDs.
    let encoded = base32::encode(Alphabet::Rfc4648 { padding: false }, &hash);
    let short = encoded.get(..26).unwrap_or(&encoded).to_lowercase();

    format!("{prefix}_{short}")
}

/// Generates the internal dispatch ID for a task attempt.
///
/// Format: `dispatch:{run_id}:{task_key}:{attempt}`
#[must_use]
pub fn dispatch_internal_id(run_id: &str, task_key: &str, attempt: u32) -> String {
    format!("dispatch:{run_id}:{task_key}:{attempt}")
}

/// Generates the internal timer ID for a retry timer.
///
/// Format: `timer:retry:{run_id}:{task_key}:{attempt}:{fire_epoch}`
#[must_use]
pub fn retry_timer_internal_id(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    fire_epoch: i64,
) -> String {
    format!("timer:retry:{run_id}:{task_key}:{attempt}:{fire_epoch}")
}

/// Generates the internal timer ID for a heartbeat check timer.
///
/// Format: `timer:heartbeat:{run_id}:{task_key}:{attempt}:{check_epoch}`
#[must_use]
pub fn heartbeat_timer_internal_id(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    check_epoch: i64,
) -> String {
    format!("timer:heartbeat:{run_id}:{task_key}:{attempt}:{check_epoch}")
}

/// Generates the internal timer ID for a cron timer.
///
/// Format: `timer:cron:{schedule_id}:{fire_epoch}`
#[must_use]
pub fn cron_timer_internal_id(schedule_id: &str, fire_epoch: i64) -> String {
    format!("timer:cron:{schedule_id}:{fire_epoch}")
}

/// Generates a deterministic `attempt_id` from a `dispatch_id`.
///
/// This ensures controller idempotency: the same dispatch always produces
/// the same `attempt_id`, preventing duplicate `DispatchRequested` events from
/// creating multiple `attempt_id`s for the same logical dispatch.
///
/// Format: `att_{hex(sha256(dispatch_id))[0..24]}`
#[must_use]
pub fn deterministic_attempt_id(dispatch_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"attempt:");
    hasher.update(dispatch_id.as_bytes());
    let hash = hasher.finalize();

    // Take first 12 bytes = 24 hex chars = 96 bits of entropy.
    let hex_encoded = hex::encode(hash.get(..12).unwrap_or(&hash));

    format!("att_{hex_encoded}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_attempt_id_is_stable() {
        let dispatch_id = "dispatch:run1:extract:1";
        let id1 = deterministic_attempt_id(dispatch_id);
        let id2 = deterministic_attempt_id(dispatch_id);
        assert_eq!(id1, id2, "same dispatch_id must produce same attempt_id");
    }

    #[test]
    fn test_deterministic_attempt_id_differs_for_different_dispatches() {
        let id1 = deterministic_attempt_id("dispatch:run1:extract:1");
        let id2 = deterministic_attempt_id("dispatch:run1:extract:2");
        assert_ne!(id1, id2, "different dispatch_ids must produce different attempt_ids");
    }

    #[test]
    fn test_cloud_task_id_format() {
        let id = cloud_task_id("d", "dispatch:run1:extract:1");
        assert!(id.starts_with("d_"));
        assert_eq!(id.len(), 28); // prefix + underscore + 26 base32 chars
    }
}
