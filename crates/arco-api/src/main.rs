//! `arco-api` binary entrypoint (Cloud Run).
//!
//! Loads configuration from environment variables and starts the HTTP server.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;

use anyhow::Result;

use arco_api::config::Config;
use arco_api::server::Server;
use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{MemoryBackend, ObjectStoreBackend, StorageBackend};

fn choose_log_format(config: &Config) -> LogFormat {
    if config.debug {
        LogFormat::Pretty
    } else {
        LogFormat::Json
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;

    if !config.debug && config.compactor_url.is_none() {
        anyhow::bail!("ARCO_COMPACTOR_URL is required when ARCO_DEBUG=false");
    }

    init_logging(choose_log_format(&config));

    let storage: Arc<dyn StorageBackend> = if let Some(bucket) = config.storage.bucket.as_deref() {
        let backend = if bucket.trim().starts_with("s3://") || bucket.trim().starts_with("s3a://") {
            "S3"
        } else {
            "GCS"
        };
        tracing::info!(bucket = %bucket, backend = backend, "Using object storage backend");
        Arc::new(ObjectStoreBackend::from_bucket(bucket)?)
    } else {
        if !config.debug {
            anyhow::bail!("ARCO_STORAGE_BUCKET is required when ARCO_DEBUG=false");
        }
        tracing::warn!("ARCO_STORAGE_BUCKET not set; using in-memory storage backend (debug only)");
        Arc::new(MemoryBackend::new())
    };

    let server = Server::with_storage_backend(config, storage);
    server.serve().await?;
    Ok(())
}
