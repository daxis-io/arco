//! Sync compaction client abstraction for Tier-1 DDL operations (ADR-018).

use async_trait::async_trait;

use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

use crate::error::{CatalogError, Result};
use crate::tier1_compactor::Tier1Compactor;

/// Trait for invoking synchronous compaction.
#[async_trait]
pub trait SyncCompactor: Send + Sync {
    /// Runs sync compaction for a set of explicit event paths.
    async fn sync_compact(&self, request: SyncCompactRequest) -> Result<SyncCompactResponse>;
}

#[async_trait]
impl SyncCompactor for Tier1Compactor {
    async fn sync_compact(&self, request: SyncCompactRequest) -> Result<SyncCompactResponse> {
        let result = self
            .sync_compact_request(request)
            .await
            .map_err(CatalogError::from)?;

        Ok(SyncCompactResponse {
            manifest_version: result.manifest_version,
            commit_ulid: result.commit_ulid,
            events_processed: result.events_processed,
            snapshot_version: result.snapshot_version,
            visibility_status: result.visibility_status,
            repair_pending: result.repair_pending,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use arco_core::{CatalogDomain, ScopedStorage};
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone)]
    struct SharedWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn new(buffer: Arc<Mutex<Vec<u8>>>) -> Self {
            Self { buffer }
        }
    }

    struct BufferWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedWriter {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn scoped_storage() -> ScopedStorage {
        let backend = Arc::new(arco_core::storage::MemoryBackend::new());
        ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
    }

    fn sync_request(
        domain: CatalogDomain,
        lock_path: Option<String>,
        request_id: Option<&str>,
    ) -> SyncCompactRequest {
        SyncCompactRequest {
            domain: domain.as_str().to_string(),
            event_paths: vec![format!("ledger/{}/evt-01.json", domain.as_str())],
            fencing_token: 1,
            lock_path,
            request_id: request_id.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn sync_compact_rejects_noncanonical_lock_path() {
        let storage = scoped_storage();
        let compactor = Tier1Compactor::new(storage.clone());
        let request = sync_request(
            CatalogDomain::Catalog,
            Some(storage.lock(CatalogDomain::Lineage)),
            None,
        );

        let error = <Tier1Compactor as SyncCompactor>::sync_compact(&compactor, request)
            .await
            .expect_err("noncanonical lock path must be rejected");

        assert!(
            matches!(error, CatalogError::Validation { ref message } if message.contains("invalid lock_path")),
            "expected invalid lock_path validation error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn sync_compact_honors_canonical_lock_path_and_reaches_stale_fencing_validation() {
        let storage = scoped_storage();
        let compactor = Tier1Compactor::new(storage.clone());
        let request = sync_request(
            CatalogDomain::Search,
            Some(storage.lock(CatalogDomain::Search)),
            None,
        );

        let error = <Tier1Compactor as SyncCompactor>::sync_compact(&compactor, request)
            .await
            .expect_err("missing lock holder should still fail");

        assert!(
            matches!(error, CatalogError::PreconditionFailed { ref message } if message.contains("stale fencing token")),
            "expected stale fencing token after canonical lock path validation, got {error:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_compact_logs_request_id() {
        let storage = scoped_storage();
        let compactor = Tier1Compactor::new(storage.clone());
        let request = sync_request(
            CatalogDomain::Catalog,
            Some(storage.lock(CatalogDomain::Catalog)),
            Some("req-catalog-01"),
        );
        let logs = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(SharedWriter::new(Arc::clone(&logs)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let _ = <Tier1Compactor as SyncCompactor>::sync_compact(&compactor, request).await;

        let rendered =
            String::from_utf8(logs.lock().expect("logs lock").clone()).expect("utf8 logs");
        assert!(
            rendered.contains("req-catalog-01"),
            "expected request_id in tracing output, got {rendered}"
        );
    }
}
