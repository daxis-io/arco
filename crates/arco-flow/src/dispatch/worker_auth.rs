//! Worker dispatch authentication helpers.

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Header carrying the shared worker dispatch secret.
///
/// Cloud Tasks owns the `Authorization` header when OIDC is enabled, so worker
/// dispatch uses a separate header for the shared dispatch secret.
pub const WORKER_DISPATCH_SECRET_HEADER: &str = "X-Arco-Dispatch-Secret";

/// Builds headers required by Python workers for authenticated dispatch.
///
/// # Errors
///
/// Returns an error when the secret is empty or whitespace.
pub fn worker_dispatch_headers(secret: &str) -> Result<HashMap<String, String>> {
    let secret = secret.trim();
    if secret.is_empty() {
        return Err(Error::configuration(
            "ARCO_FLOW_WORKER_DISPATCH_SECRET must not be empty",
        ));
    }

    Ok(HashMap::from([(
        WORKER_DISPATCH_SECRET_HEADER.to_string(),
        secret.to_string(),
    )]))
}
