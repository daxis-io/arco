//! Identifier helpers for orchestration.
//!
//! Centralizes Cloud Tasks ID generation and internal ID formats.
//!
//! Per P0-4: Run IDs are namespaced by tenant/workspace and use HMAC to prevent
//! enumeration attacks while maintaining determinism for replay safety.

use base32::Alphabet;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

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

/// Generate deterministic `run_id` from `run_key` (replay-safe, tenant-scoped).
///
/// Uses HMAC with `tenant_secret` to prevent enumeration attacks while
/// maintaining determinism - the same inputs always produce the same `run_id`.
///
/// # Arguments
/// - `tenant_id`: Tenant identifier for namespacing
/// - `workspace_id`: Workspace identifier for namespacing
/// - `run_key`: The run key (e.g., `"sched:daily-etl:1736935200"`)
/// - `tenant_secret`: Per-tenant stable secret for HMAC
///
/// # Returns
/// A deterministic `run_id` in the format `run_<base32_hash>` (30 chars total)
///
/// # Panics
/// This function never panics. HMAC-SHA256 accepts keys of any length.
#[must_use]
pub fn run_id_from_run_key(
    tenant_id: &str,
    workspace_id: &str,
    run_key: &str,
    tenant_secret: &[u8],
) -> String {
    // Namespace the input to prevent cross-tenant collisions
    let input = format!("{tenant_id}:{workspace_id}:{run_key}");

    // HMAC instead of raw hash to prevent enumeration attacks
    // Safety: HMAC-SHA256 accepts keys of any length, so this never fails
    #[allow(clippy::expect_used)]
    let mut mac =
        HmacSha256::new_from_slice(tenant_secret).expect("HMAC-SHA256 accepts any key length");
    mac.update(input.as_bytes());
    let result = mac.finalize();
    let hash = result.into_bytes();

    // Base32 encode for URL-safe ID (use first 16 bytes = 128 bits)
    // SHA-256 always produces 32 bytes, so .get() always succeeds
    let hash_prefix = hash.get(..16).unwrap_or(&hash);
    let encoded = base32::encode(Alphabet::Rfc4648 { padding: false }, hash_prefix);

    // Return "run_" + first 26 chars of base32 encoding (all lowercase)
    // 16 bytes = 128 bits = 26 base32 chars, so encoded is always >= 26 chars
    let short = encoded.get(..26).unwrap_or(&encoded).to_lowercase();
    format!("run_{short}")
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
        assert_ne!(
            id1, id2,
            "different dispatch_ids must produce different attempt_ids"
        );
    }

    #[test]
    fn test_cloud_task_id_format() {
        let id = cloud_task_id("d", "dispatch:run1:extract:1");
        assert!(id.starts_with("d_"));
        assert_eq!(id.len(), 28); // prefix + underscore + 26 base32 chars
    }

    // ========================================================================
    // run_id_from_run_key tests (P0-4)
    // ========================================================================

    #[test]
    fn test_run_id_is_deterministic() {
        let run_key = "sched:daily-etl:1736935200";
        let secret = b"test-secret";

        let run_id_1 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, secret);
        let run_id_2 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, secret);

        assert_eq!(run_id_1, run_id_2, "Same inputs should produce same run_id");
    }

    #[test]
    fn test_run_id_format() {
        let run_id = run_id_from_run_key(
            "tenant-abc",
            "workspace-prod",
            "sched:daily-etl:1736935200",
            b"test-secret",
        );

        assert!(run_id.starts_with("run_"), "run_id should start with 'run_'");
        assert_eq!(run_id.len(), 30, "run_id should be 30 chars: 'run_' + 26");
        // Base32 uses lowercase a-z and digits 2-7
        assert!(
            run_id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "run_id should be lowercase alphanumeric"
        );
    }

    #[test]
    fn test_different_run_keys_produce_different_ids() {
        let secret = b"test-secret";
        let id1 = run_id_from_run_key(
            "tenant-abc",
            "workspace-prod",
            "sched:daily-etl:1736935200",
            secret,
        );
        let id2 = run_id_from_run_key(
            "tenant-abc",
            "workspace-prod",
            "sched:daily-etl:1736935201",
            secret,
        );

        assert_ne!(id1, id2, "Different run_keys should produce different run_ids");
    }

    #[test]
    fn test_different_tenants_produce_different_ids() {
        let run_key = "sched:daily-etl:1736935200";
        let secret = b"test-secret";

        let id1 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, secret);
        let id2 = run_id_from_run_key("tenant-xyz", "workspace-prod", run_key, secret);

        assert_ne!(id1, id2, "Different tenants should produce different run_ids");
    }

    #[test]
    fn test_different_workspaces_produce_different_ids() {
        let run_key = "sched:daily-etl:1736935200";
        let secret = b"test-secret";

        let id1 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, secret);
        let id2 = run_id_from_run_key("tenant-abc", "workspace-dev", run_key, secret);

        assert_ne!(id1, id2, "Different workspaces should produce different run_ids");
    }

    #[test]
    fn test_different_secrets_produce_different_ids() {
        let run_key = "sched:daily-etl:1736935200";

        let id1 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, b"secret-1");
        let id2 = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, b"secret-2");

        assert_ne!(id1, id2, "Different secrets should produce different run_ids");
    }
}
