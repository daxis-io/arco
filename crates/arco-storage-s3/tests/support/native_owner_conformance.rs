//! Native owner recovery conformance for the opt-in storage qualification.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_catalog::{
    ArcoStateAdmin as _, ArcoStateReader as _, ArcoStateTxn as _, CheckpointOptions,
    ControlMvpPaths, ControlMvpReadCacheConfig, ControlMvpStateStore,
    PersistedAuthorityAdapter as _, ScanRequest, StateScope, TxnOptions,
};
use arco_core::storage::{ListPage, ObjectMeta};
use arco_core::{
    Error, MemoryBackend, ScopedStorage, StorageBackend, WritePrecondition, WriteResult,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use sha2::{Digest, Sha256};

const MAX_LOGICAL_CALLS: u64 = 512;
const MAX_SUBMITTED_BYTES: u64 = 32 * 1024 * 1024;

type ScenarioResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct Budget {
    calls: AtomicU64,
    submitted_bytes: AtomicU64,
}

impl Budget {
    fn admit(&self, path: &str, submitted_bytes: u64) -> arco_core::Result<()> {
        let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if calls > MAX_LOGICAL_CALLS {
            return Err(Error::storage(
                "native owner qualification exceeded logical-call budget",
            ));
        }
        let bytes = self
            .submitted_bytes
            .fetch_add(submitted_bytes, Ordering::SeqCst)
            .saturating_add(submitted_bytes);
        if bytes > MAX_SUBMITTED_BYTES {
            return Err(Error::storage(
                "native owner qualification exceeded submitted-byte budget",
            ));
        }
        if path.is_empty()
            || path.contains(['\\', '%', '\0'])
            || path.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(Error::InvalidInput(
                "native owner qualification path is invalid".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct AcceptedPointer {
    sha256: String,
    version: String,
}

/// Limits the scenario to one scope and loses one response only after R2 accepted it.
struct ScopedFaultBackend {
    inner: Arc<dyn StorageBackend>,
    scope_prefix: String,
    pointer_path: String,
    budget: Arc<Budget>,
    armed: AtomicBool,
    fired: AtomicBool,
    accepted: Mutex<Option<AcceptedPointer>>,
}

impl ScopedFaultBackend {
    fn new(
        inner: Arc<dyn StorageBackend>,
        scope_prefix: String,
        pointer_path: String,
        budget: Arc<Budget>,
    ) -> Self {
        Self {
            inner,
            scope_prefix,
            pointer_path,
            budget,
            armed: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            accepted: Mutex::new(None),
        }
    }

    fn arm(&self) {
        self.fired.store(false, Ordering::SeqCst);
        *self.accepted.lock().expect("accepted pointer lock") = None;
        self.armed.store(true, Ordering::SeqCst);
    }

    fn ensure_owned(&self, path: &str, submitted_bytes: u64) -> arco_core::Result<()> {
        if !path.starts_with(&self.scope_prefix) {
            return Err(Error::TenantIsolation {
                message: "native owner qualification scope escape".to_string(),
            });
        }
        self.budget.admit(path, submitted_bytes)
    }

    fn accepted(&self) -> AcceptedPointer {
        self.accepted
            .lock()
            .expect("accepted pointer lock")
            .clone()
            .expect("armed pointer CAS was not accepted")
    }
}

#[async_trait]
impl StorageBackend for ScopedFaultBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.ensure_owned(path, 0)?;
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.ensure_owned(path, 0)?;
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.ensure_owned(path, data.len() as u64)?;
        let is_conditional = !matches!(precondition, WritePrecondition::None);
        let sha256 = format!("{:x}", Sha256::digest(&data));
        let result = self.inner.put(path, data, precondition).await;
        if self.armed.load(Ordering::SeqCst)
            && is_conditional
            && path == self.pointer_path
            && let Ok(WriteResult::Success { version }) = &result
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            *self.accepted.lock().expect("accepted pointer lock") = Some(AcceptedPointer {
                sha256,
                version: version.clone(),
            });
            self.armed.store(false, Ordering::SeqCst);
            return Err(Error::storage(
                "injected client-boundary response loss after accepted pointer CAS",
            ));
        }
        result
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.ensure_owned(path, 0)?;
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.ensure_owned(prefix, 0)?;
        self.inner.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        self.ensure_owned(prefix, 0)?;
        if start_after.is_some_and(|path| !path.starts_with(&self.scope_prefix)) {
            return Err(Error::TenantIsolation {
                message: "native owner qualification list cursor escaped scope".to_string(),
            });
        }
        self.inner.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.ensure_owned(path, 0)?;
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, _expiry: Duration) -> arco_core::Result<String> {
        self.ensure_owned(path, 0)?;
        Err(Error::InvalidInput(
            "native owner qualification forbids signed URL escape".to_string(),
        ))
    }
}

struct OwnerRecoveryReport {
    tenant: String,
    workspace: String,
    checkpoint_id: String,
    checkpoint_sequence: u64,
    first_sequence: u64,
    second_sequence: u64,
    first_token: String,
    second_token: String,
    pointer_sha256: String,
    pointer_version: String,
    cache_modes: Vec<CacheModeReceipt>,
    logical_calls: u64,
    submitted_bytes: u64,
}

struct CacheModeReceipt {
    mode: &'static str,
    initial_demands: u64,
    initial_loads: u64,
}

/// Exercises normal control-store reconciliation after one accepted pointer CAS loses its response.
#[allow(
    clippy::redundant_pub_crate,
    reason = "the parent integration-test module invokes this support helper"
)]
pub(super) async fn assert_owner_recovery(
    backend: Arc<dyn StorageBackend>,
    restart_backend: Arc<dyn StorageBackend>,
) {
    let report = owner_recovery(backend, restart_backend)
        .await
        .expect("native owner recovery conformance");
    emit_report(&report, true);
}

#[allow(
    clippy::print_stdout,
    reason = "the qualification contract retains a sanitized JSON receipt in the test log"
)]
fn emit_report(report: &OwnerRecoveryReport, provider: bool) {
    println!(
        "{}",
        json!({
            "kind": "native-owner-recovery",
            "provider": provider,
            "fixture": {"tenant": &report.tenant, "workspace": &report.workspace, "domain": "catalog"},
            "fault_location": "client_boundary_after_accepted_pointer_cas",
            "wire_loss": false,
            "cases": {"accepted_write_reconciled": "passed", "restart_current_read": "passed", "retained_checkpoint_read": "passed", "scoped_budget": "passed"},
            "pointer": {"sha256": &report.pointer_sha256, "version": &report.pointer_version},
            "checkpoint": {"id": &report.checkpoint_id, "sequence": report.checkpoint_sequence},
            "authority": {"sequence_before": report.first_sequence, "sequence_after": report.second_sequence,
                "token_before": &report.first_token, "token_after": &report.second_token},
            "cache_modes": report.cache_modes.iter().map(|mode| json!({
                "mode": mode.mode, "initial_demands": mode.initial_demands, "initial_loads": mode.initial_loads
            })).collect::<Vec<_>>(),
            "limits": {"logical_calls": MAX_LOGICAL_CALLS, "submitted_bytes": MAX_SUBMITTED_BYTES},
            "observed": {"logical_calls": report.logical_calls, "submitted_bytes": report.submitted_bytes},
        })
    );
}

#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "the ordered recovery trace deliberately keeps its assertions together"
)]
async fn owner_recovery(
    backend: Arc<dyn StorageBackend>,
    restart_backend: Arc<dyn StorageBackend>,
) -> ScenarioResult<OwnerRecoveryReport> {
    let run = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let tenant = format!("native-owner-{run}");
    let workspace = "qualification".to_string();
    let scope_prefix = format!("tenant={tenant}/workspace={workspace}/");
    let pointer_relative = ControlMvpPaths::new("catalog").current_pointer();
    let pointer_path = format!("{scope_prefix}{pointer_relative}");
    let budget = Arc::new(Budget::default());
    let fault = Arc::new(ScopedFaultBackend::new(
        backend,
        scope_prefix.clone(),
        pointer_path.clone(),
        budget.clone(),
    ));
    let restart_guard = Arc::new(ScopedFaultBackend::new(
        restart_backend,
        scope_prefix,
        pointer_path.clone(),
        budget.clone(),
    ));
    let storage = ScopedStorage::new(fault.clone(), &tenant, &workspace)?;
    let scope = StateScope::new(&tenant, &workspace, "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone())?;

    let first = commit_value(&store, b"owner-key", b"v1").await?;
    let checkpoint = store.checkpoint(CheckpointOptions::default()).await?;
    store
        .persist_checkpoint_reference(&checkpoint, chrono::Utc::now() + chrono::Duration::days(30))
        .await?;
    let retained = store.read_checkpoint(checkpoint.clone()).await?;
    assert_eq!(
        retained.get(b"owner-key").await?,
        Some(Bytes::from_static(b"v1"))
    );

    fault.arm();
    let second = commit_value(&store, b"owner-key", b"v2").await?;
    assert!(
        fault.fired.load(Ordering::SeqCst),
        "lost response did not fire"
    );
    assert_eq!(
        second.logical_sequence(),
        first.logical_sequence() + 1,
        "accepted retry must not advance the sequence twice"
    );
    let accepted = fault.accepted();
    let pointer = fault.get(&pointer_path).await?;
    let meta = fault
        .head(&pointer_path)
        .await?
        .expect("accepted pointer must exist");
    assert_eq!(format!("{:x}", Sha256::digest(&pointer)), accepted.sha256);
    assert_eq!(meta.version, accepted.version);
    assert_eq!(
        store.get(b"owner-key").await?,
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        retained.get(b"owner-key").await?,
        Some(Bytes::from_static(b"v1"))
    );
    drop(retained);
    drop(store);

    let mut cache_modes = Vec::new();
    for (mode, cache) in [
        (
            "disabled",
            ControlMvpReadCacheConfig {
                metadata_bytes: 0,
                decoded_bytes: 0,
            },
        ),
        ("default", ControlMvpReadCacheConfig::default()),
        (
            "pressure",
            ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            },
        ),
    ] {
        let restart_storage = ScopedStorage::new(restart_guard.clone(), &tenant, &workspace)?;
        let fresh = ControlMvpStateStore::new(restart_storage, scope.clone())?
            .with_read_cache_config(cache)?;
        let (initial_demands, initial_loads) = fresh.read_cache().map_or((0, 0), |cache| {
            let initial = cache.statistics();
            assert_eq!(initial.demands, 0, "{mode} cache already had demands");
            assert_eq!(initial.loads, 0, "{mode} cache already had loads");
            (initial.demands, initial.loads)
        });
        cache_modes.push(CacheModeReceipt {
            mode,
            initial_demands,
            initial_loads,
        });
        assert_eq!(
            fresh.get(b"owner-key").await?,
            Some(Bytes::from_static(b"v2"))
        );
        let page = fresh
            .scan(ScanRequest::new(b"owner-").with_limits(16, 64 * 1024, 8))
            .await?;
        assert_eq!(page.entries().len(), 1, "{mode} range read");
        assert_eq!(
            page.entries()
                .first()
                .expect("range read entry")
                .value()
                .bytes(),
            &Bytes::from_static(b"v2")
        );
        assert!(page.continuation().is_none());
        assert_eq!(fresh.current_state_token().await?, second);
        let retained_after_restart = fresh.read_checkpoint(checkpoint.clone()).await?;
        assert_eq!(
            retained_after_restart.get(b"owner-key").await?,
            Some(Bytes::from_static(b"v1")),
            "{mode} retained checkpoint read"
        );
    }

    Ok(OwnerRecoveryReport {
        tenant,
        workspace,
        checkpoint_id: checkpoint.checkpoint_id().to_string(),
        checkpoint_sequence: first.logical_sequence(),
        first_sequence: first.logical_sequence(),
        second_sequence: second.logical_sequence(),
        first_token: first.authority_manifest_id().to_string(),
        second_token: second.authority_manifest_id().to_string(),
        pointer_sha256: accepted.sha256,
        pointer_version: accepted.version,
        cache_modes,
        logical_calls: budget.calls.load(Ordering::SeqCst),
        submitted_bytes: budget.submitted_bytes.load(Ordering::SeqCst),
    })
}

async fn commit_value(
    store: &ControlMvpStateStore,
    key: &[u8],
    value: &[u8],
) -> Result<arco_catalog::StateToken, arco_catalog::CatalogError> {
    let mut transaction = store.begin_control_txn(TxnOptions::default()).await?;
    transaction.put(key, Bytes::copy_from_slice(value)).await?;
    Ok(transaction.commit().await?.state_token().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_owner_recovery_reconciles_and_retains() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let report = owner_recovery(backend.clone(), backend).await.unwrap();
        emit_report(&report, false);
        assert!(report.logical_calls <= MAX_LOGICAL_CALLS);
        assert!(report.submitted_bytes <= MAX_SUBMITTED_BYTES);
        assert_ne!(report.first_token, report.second_token);
    }

    #[tokio::test]
    async fn scoped_fault_refuses_escape_and_budget_before_io() {
        let budget = Arc::new(Budget::default());
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let fault = ScopedFaultBackend::new(
            backend,
            "tenant=native-owner/workspace=qualification/".to_string(),
            "tenant=native-owner/workspace=qualification/control/v1/domains/catalog/head/current.json"
                .to_string(),
            budget,
        );
        assert!(
            fault
                .get("tenant=other/workspace=qualification/value")
                .await
                .is_err()
        );
        assert!(
            fault
                .signed_url(
                    "tenant=native-owner/workspace=qualification/value",
                    Duration::from_secs(60)
                )
                .await
                .is_err()
        );
        assert!(
            fault
                .ensure_owned(
                    "tenant=native-owner/workspace=qualification/value",
                    MAX_SUBMITTED_BYTES + 1
                )
                .is_err()
        );
    }
}
