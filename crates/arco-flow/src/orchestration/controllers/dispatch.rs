//! Dispatch utilities for Cloud Tasks integration.
//!
//! Provides deterministic, Cloud Tasks-compliant task IDs per ADR-021.
//!
//! ## Dual-Identifier Pattern
//!
//! We maintain two identifiers for each dispatch/timer:
//!
//! 1. **Internal ID** (human-readable): Used in Parquet PKs, idempotency keys, and logs
//!    - Example: `dispatch:run123:extract:1`
//!
//! 2. **Cloud Tasks ID** (API-compliant): Hash-based, non-sequential
//!    - Example: `d_7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069`
//!
//! This allows human-readable debugging while ensuring Cloud Tasks compliance.

use sha2::{Digest, Sha256};

/// Generates a Cloud Tasks-compliant task ID from an internal ID.
///
/// The result is deterministic: same internal_id always produces the same
/// Cloud Tasks ID. This enables idempotent dispatch operations.
///
/// # Format
///
/// `{prefix}_{hex(sha256(internal_id))[0..40]}`
///
/// - `prefix`: Single character indicating type (`d` for dispatch, `t` for timer)
/// - `hex`: 40 hex characters = 160 bits of entropy (plenty for uniqueness)
///
/// # Properties
///
/// - **Deterministic**: Same input always produces same output
/// - **Unique**: SHA-256 collision is practically impossible
/// - **Non-sequential**: Hash prefix avoids Cloud Tasks latency issues
/// - **Compliant**: Only uses `[A-Za-z0-9_]` characters
///
/// # Example
///
/// ```
/// use arco_flow::orchestration::controllers::dispatch::cloud_task_id;
///
/// let id1 = cloud_task_id("d", "dispatch:run123:extract:1");
/// let id2 = cloud_task_id("d", "dispatch:run123:extract:1");
/// assert_eq!(id1, id2);  // Deterministic
///
/// let id3 = cloud_task_id("d", "dispatch:run123:extract:2");
/// assert_ne!(id1, id3);  // Different input = different output
/// ```
#[must_use]
pub fn cloud_task_id(prefix: &str, internal_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(internal_id.as_bytes());
    let hash = hasher.finalize();

    // Take first 20 bytes = 40 hex chars = 160 bits of entropy
    // This is plenty for uniqueness (birthday problem: 2^80 operations for 50% collision)
    let hex_encoded = hex::encode(&hash[..20]);

    format!("{}_{}", prefix, hex_encoded)
}

/// Generates the internal dispatch ID for a task attempt.
///
/// Format: `dispatch:{run_id}:{task_key}:{attempt}`
#[must_use]
pub fn dispatch_internal_id(run_id: &str, task_key: &str, attempt: u32) -> String {
    format!("dispatch:{}:{}:{}", run_id, task_key, attempt)
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
    format!(
        "timer:retry:{}:{}:{}:{}",
        run_id, task_key, attempt, fire_epoch
    )
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
    format!(
        "timer:heartbeat:{}:{}:{}:{}",
        run_id, task_key, attempt, check_epoch
    )
}

/// Generates the internal timer ID for a cron timer.
///
/// Format: `timer:cron:{schedule_id}:{fire_epoch}`
#[must_use]
pub fn cron_timer_internal_id(schedule_id: &str, fire_epoch: i64) -> String {
    format!("timer:cron:{}:{}", schedule_id, fire_epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_task_id_is_deterministic() {
        let internal_id = "dispatch:run123:extract:1";

        let id1 = cloud_task_id("d", internal_id);
        let id2 = cloud_task_id("d", internal_id);

        assert_eq!(id1, id2); // Same input = same output
    }

    #[test]
    fn test_cloud_task_id_is_unique() {
        let id1 = cloud_task_id("d", "dispatch:run123:extract:1");
        let id2 = cloud_task_id("d", "dispatch:run123:extract:2");

        assert_ne!(id1, id2); // Different input = different output
    }

    #[test]
    fn test_cloud_task_id_format() {
        let id = cloud_task_id("d", "dispatch:run123:extract:1");

        assert!(id.starts_with("d_")); // Correct prefix
        assert_eq!(id.len(), 42); // "d_" + 40 hex chars
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    }

    #[test]
    fn test_cloud_task_id_different_prefixes() {
        let dispatch_id = cloud_task_id("d", "dispatch:run123:extract:1");
        let timer_id = cloud_task_id("t", "timer:retry:run123:extract:1:1705340400");

        assert!(dispatch_id.starts_with("d_"));
        assert!(timer_id.starts_with("t_"));
    }

    #[test]
    fn test_dispatch_internal_id() {
        let id = dispatch_internal_id("run123", "extract", 1);
        assert_eq!(id, "dispatch:run123:extract:1");

        let id2 = dispatch_internal_id("run123", "extract", 2);
        assert_eq!(id2, "dispatch:run123:extract:2");
        assert_ne!(id, id2);
    }

    #[test]
    fn test_retry_timer_internal_id() {
        let id = retry_timer_internal_id("run123", "extract", 1, 1705340400);
        assert_eq!(id, "timer:retry:run123:extract:1:1705340400");
    }

    #[test]
    fn test_heartbeat_timer_internal_id() {
        let id = heartbeat_timer_internal_id("run123", "extract", 1, 1705340400);
        assert_eq!(id, "timer:heartbeat:run123:extract:1:1705340400");
    }

    #[test]
    fn test_cron_timer_internal_id() {
        let id = cron_timer_internal_id("daily-etl", 1705340400);
        assert_eq!(id, "timer:cron:daily-etl:1705340400");
    }

    #[test]
    fn test_cloud_task_id_compliant_characters() {
        // Test that even with special characters in input, output is compliant
        let internal_id = "dispatch:run:123:task/with/slashes:1";
        let id = cloud_task_id("d", internal_id);

        // Cloud Tasks allows: [A-Za-z0-9_-]
        // Our IDs use only: [a-z0-9_] (lowercase hex + underscore)
        assert!(id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
    }

    #[test]
    fn test_end_to_end_dispatch_id_generation() {
        // Simulate the full flow: internal ID -> Cloud Tasks ID
        let run_id = "01HQXYZ123RUN";
        let task_key = "extract";
        let attempt = 1;

        let internal = dispatch_internal_id(run_id, task_key, attempt);
        let cloud_id = cloud_task_id("d", &internal);

        // Internal ID is human-readable
        assert!(internal.contains(run_id));
        assert!(internal.contains(task_key));

        // Cloud Tasks ID is opaque but deterministic
        assert!(cloud_id.starts_with("d_"));
        let cloud_id2 = cloud_task_id("d", &internal);
        assert_eq!(cloud_id, cloud_id2);
    }
}
