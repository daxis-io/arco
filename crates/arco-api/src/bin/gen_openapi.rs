//! Generates the `OpenAPI` spec for `arco-api` as JSON on stdout.

use anyhow::Context as _;
use std::io::{self, Write};

fn main() -> anyhow::Result<()> {
    let json = arco_api::openapi::openapi_json().context("generate OpenAPI JSON")?;
    io::stdout()
        .write_all(json.as_bytes())
        .context("write OpenAPI JSON to stdout")?;
    Ok(())
}
