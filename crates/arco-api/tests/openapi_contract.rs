//! OpenAPI contract tests.
//!
//! Ensures the checked-in OpenAPI spec matches the implementation.

use anyhow::{Context, Result};

/// If this test fails, regenerate `crates/arco-api/openapi.json` with:
/// `cargo run -p arco-api --bin gen_openapi > crates/arco-api/openapi.json`
#[test]
fn contract_openapi_matches_implementation() -> Result<()> {
    let generated = arco_api::openapi::openapi_json().context("generate OpenAPI JSON")?;
    let generated: serde_json::Value =
        serde_json::from_str(&generated).context("parse generated OpenAPI JSON")?;

    let checked_in: serde_json::Value = serde_json::from_str(include_str!("../openapi.json"))
        .context("parse checked-in OpenAPI JSON")?;

    anyhow::ensure!(
        generated == checked_in,
        "OpenAPI spec mismatch: regenerate `crates/arco-api/openapi.json`"
    );

    Ok(())
}
