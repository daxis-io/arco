//! Compactor for Tier 2 event consolidation.
//!
//! INVARIANT 2: Compactor is the SOLE writer of Parquet state files.
//! INVARIANT 3: Compaction is idempotent (dedupe by `idempotency_key`, primary key upsert).
//! INVARIANT 4: Publish is atomic (manifest CAS).
//!
//! It reads from `ledger/`, writes to `state/`, and updates the watermark atomically via CAS.
//!
//! NOTE: This MVP compactor writes full Parquet snapshots (no incremental/delta files). That
//! makes each compaction `O(total_records)` I/O and CPU. This is acceptable for early workloads,
//! but should evolve toward incremental compaction as catalog size grows.

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::Array;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;
use arco_core::{CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths};

use crate::error::{CatalogError, Result};
use crate::manifest::{ExecutionsManifest, RootManifest};

/// Result of a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of events processed from ledger.
    pub events_processed: usize,
    /// Number of Parquet files written.
    pub parquet_files_written: usize,
    /// New watermark position (event ID).
    pub new_watermark: String,
}

#[derive(Debug, Clone)]
struct RecordWithSequence {
    record: MaterializationRecord,
    sequence_position: Option<u64>,
}

struct CompactionWork {
    root: RootManifest,
    manifest: ExecutionsManifest,
    current_version: String,
    events_to_process: Vec<String>,
    late_events: Vec<String>,
    new_watermark_event_id: Option<String>,
}

/// Policy for handling unknown or unsupported event envelopes during compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownEventPolicy {
    /// Fail the compaction when an unknown event is encountered.
    Reject,
    /// Copy the event into quarantine and continue.
    Quarantine,
}

/// Policy for handling late/out-of-order events (events at or before the watermark).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateEventPolicy {
    /// Skip late events (no logging).
    Skip,
    /// Skip late events, but log a warning (recommended default until sequencing exists).
    Log,
    /// Process late events (anti-entropy replays, missed notifications).
    Process,
    /// Copy late events into quarantine and continue.
    Quarantine,
}

fn cas_backoff(base: Duration, max: Duration, attempt: u32) -> Duration {
    // Exponential backoff with small random jitter to avoid thundering herds.
    // Clamp the exponent to avoid overflow and unbounded waits.
    let exp = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
    let backoff = base.saturating_mul(exp);
    let jitter = Duration::from_millis(rand_jitter_ms(50));
    backoff.saturating_add(jitter).min(max)
}

fn rand_jitter_ms(max_exclusive: u64) -> u64 {
    if max_exclusive == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    u64::from(nanos) % max_exclusive
}

fn should_replace_sequence(existing: Option<u64>, incoming: Option<u64>) -> bool {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) => incoming >= existing,
        (Some(_), None) => false,
        (None, Some(_) | None) => true,
    }
}

/// Compacts Tier 2 ledger events into Parquet snapshots.
///
/// INVARIANT: Compactor is the SOLE writer of `state/` Parquet files.
pub struct Compactor {
    storage: ScopedStorage,
    cas_max_retries: u32,
    cas_backoff_base: Duration,
    cas_backoff_max: Duration,
    unknown_event_policy: UnknownEventPolicy,
    late_event_policy: LateEventPolicy,
}

impl Compactor {
    /// Creates a new compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            cas_max_retries: 10,
            cas_backoff_base: Duration::from_millis(50),
            cas_backoff_max: Duration::from_secs(2),
            unknown_event_policy: UnknownEventPolicy::Reject,
            late_event_policy: LateEventPolicy::Log,
        }
    }

    /// Sets the maximum CAS retries for the manifest update publish.
    #[must_use]
    pub const fn with_cas_retries(mut self, max_retries: u32) -> Self {
        self.cas_max_retries = if max_retries == 0 { 1 } else { max_retries };
        self
    }

    /// Sets the CAS retry backoff window.
    #[must_use]
    pub const fn with_cas_backoff(mut self, base: Duration, max: Duration) -> Self {
        self.cas_backoff_base = base;
        self.cas_backoff_max = max;
        self
    }

    /// Sets the policy for unknown or unsupported events.
    #[must_use]
    pub const fn with_unknown_event_policy(mut self, policy: UnknownEventPolicy) -> Self {
        self.unknown_event_policy = policy;
        self
    }

    /// Sets the policy for late/out-of-order events.
    #[must_use]
    pub const fn with_late_event_policy(mut self, policy: LateEventPolicy) -> Self {
        self.late_event_policy = policy;
        self
    }

    /// Compacts a domain, reading events since watermark and writing Parquet.
    ///
    /// ARCHITECTURE: Uses CAS for atomic manifest publish. The sequence is:
    /// 1. Read manifest + version (head)
    /// 2. Write Parquet snapshot
    /// 3. CAS-update manifest with `MatchesVersion`
    /// 4. Only if CAS succeeds: compaction is visible
    ///
    /// # Errors
    ///
    /// Returns an error if reading ledger or writing state fails.
    /// Returns `CatalogError::CasFailed` only if the manifest publish loses the CAS race after
    /// exhausting retries.
    #[allow(clippy::too_many_lines)]
    pub async fn compact_domain(&self, domain: CatalogDomain) -> Result<CompactionResult> {
        if domain != CatalogDomain::Executions {
            return Err(CatalogError::InvariantViolation {
                message: format!("compaction not implemented for domain: {domain}"),
            });
        }

        for attempt in 1..=self.cas_max_retries {
            match self.compact_domain_once(domain).await {
                Ok(result) => return Ok(result),
                Err(CatalogError::CasFailed { .. }) if attempt < self.cas_max_retries => {
                    let backoff = cas_backoff(self.cas_backoff_base, self.cas_backoff_max, attempt);
                    let backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
                    tracing::info!(attempt, backoff_ms, "compactor lost CAS race, retrying");
                    crate::metrics::record_cas_retry("compaction_manifest");
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }

        Err(CatalogError::CasFailed {
            message: format!(
                "manifest update lost CAS race after {} retries",
                self.cas_max_retries
            ),
        })
    }

    /// Compacts a domain using explicit event paths (no listing).
    #[allow(clippy::too_many_lines, clippy::missing_errors_doc)]
    pub async fn compact_domain_explicit(
        &self,
        domain: CatalogDomain,
        event_paths: Vec<String>,
    ) -> Result<CompactionResult> {
        if domain != CatalogDomain::Executions {
            return Err(CatalogError::InvariantViolation {
                message: format!("compaction not implemented for domain: {domain}"),
            });
        }

        let event_paths = validate_event_paths(domain, event_paths)?;

        for attempt in 1..=self.cas_max_retries {
            match self
                .compact_domain_explicit_once(domain, &event_paths)
                .await
            {
                Ok(result) => return Ok(result),
                Err(CatalogError::CasFailed { .. }) if attempt < self.cas_max_retries => {
                    let backoff = cas_backoff(self.cas_backoff_base, self.cas_backoff_max, attempt);
                    let backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
                    tracing::info!(attempt, backoff_ms, "compactor lost CAS race, retrying");
                    crate::metrics::record_cas_retry("compaction_manifest");
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }

        Err(CatalogError::CasFailed {
            message: format!(
                "manifest update lost CAS race after {} retries",
                self.cas_max_retries
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn compact_domain_once(&self, domain: CatalogDomain) -> Result<CompactionResult> {
        // 1. Read current manifest + its version for CAS
        let (root, manifest, current_version) = self.read_manifest_with_version().await?;

        let watermark_event_id = manifest.watermark_event_id.clone();
        let last_compaction_at = manifest.last_compaction_at;

        // 2. List ledger events (with metadata for late-event detection)
        let prefix = CatalogPaths::ledger_dir(domain);
        let mut metas =
            self.storage
                .list_meta(&prefix)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to list events: {e}"),
                })?;

        metas.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));

        // 3. Partition events into:
        // - events after watermark (eligible for compaction)
        // - late events (<= watermark but newly written since last_compaction_at)
        let mut events_to_process: Vec<String> = Vec::new();
        let mut late_events: Vec<String> = Vec::new();

        for meta in metas {
            let path = meta.path.as_str();
            let filename = path.rsplit('/').next().unwrap_or("");

            let is_after_watermark = watermark_event_id
                .as_deref()
                .is_none_or(|watermark| filename > watermark);

            if is_after_watermark {
                events_to_process.push(path.to_string());
                continue;
            }

            // Late arrival detection: object was modified after last compaction.
            if let (Some(last_compaction_at), Some(last_modified)) =
                (last_compaction_at, meta.last_modified)
            {
                if last_modified > last_compaction_at {
                    late_events.push(path.to_string());
                }
            }
        }

        let last_event_id = events_to_process
            .last()
            .and_then(|p| p.rsplit('/').next())
            .map(str::to_string);

        let work = CompactionWork {
            root,
            manifest,
            current_version,
            events_to_process,
            late_events,
            new_watermark_event_id: last_event_id,
        };

        self.compact_domain_with_events(domain, work).await
    }

    #[allow(clippy::too_many_lines)]
    async fn compact_domain_explicit_once(
        &self,
        domain: CatalogDomain,
        event_paths: &[String],
    ) -> Result<CompactionResult> {
        let (root, manifest, current_version) = self.read_manifest_with_version().await?;
        let watermark_event_id = manifest.watermark_event_id.clone();

        let mut events_to_process: Vec<String> = Vec::new();
        let mut late_events: Vec<String> = Vec::new();

        for path in event_paths {
            let filename = path.rsplit('/').next().unwrap_or("");
            let is_after_watermark = watermark_event_id
                .as_deref()
                .is_none_or(|watermark| filename > watermark);

            if is_after_watermark {
                events_to_process.push(path.clone());
                continue;
            }

            late_events.push(path.clone());
            if matches!(self.late_event_policy, LateEventPolicy::Process) {
                events_to_process.push(path.clone());
            }
        }

        events_to_process.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        let last_event_id = events_to_process
            .last()
            .and_then(|p| p.rsplit('/').next())
            .map(str::to_string);

        let new_watermark_event_id = match (&watermark_event_id, &last_event_id) {
            (Some(current), Some(candidate)) => Some(if candidate > current {
                candidate.clone()
            } else {
                current.clone()
            }),
            (Some(current), None) => Some(current.clone()),
            (None, Some(candidate)) => Some(candidate.clone()),
            (None, None) => None,
        };

        let work = CompactionWork {
            root,
            manifest,
            current_version,
            events_to_process,
            late_events,
            new_watermark_event_id,
        };

        self.compact_domain_with_events(domain, work).await
    }

    #[allow(clippy::too_many_lines)]
    async fn compact_domain_with_events(
        &self,
        domain: CatalogDomain,
        work: CompactionWork,
    ) -> Result<CompactionResult> {
        let CompactionWork {
            root,
            manifest,
            current_version,
            events_to_process,
            late_events,
            new_watermark_event_id,
        } = work;

        self.handle_late_events(domain, &late_events).await?;

        if events_to_process.is_empty() {
            // No new events to compact. If we saw late events, advance `last_compaction_at`
            // to avoid repeatedly re-quarantining/re-logging the same files.
            if !late_events.is_empty() {
                let now = Utc::now();
                let mut new_manifest = manifest.clone();
                new_manifest.last_compaction_at = Some(now);
                new_manifest.updated_at = now;
                self.publish_manifest(
                    &root.executions_manifest_path,
                    &new_manifest,
                    &current_version,
                )
                .await?;
            }

            return Ok(CompactionResult {
                events_processed: 0,
                parquet_files_written: 0,
                new_watermark: manifest.watermark_event_id.unwrap_or_default(),
            });
        }

        let snapshot_id = events_to_process
            .last()
            .and_then(|p| p.rsplit('/').next())
            .unwrap_or("");
        let snapshot_id = snapshot_id
            .strip_suffix(".json")
            .unwrap_or(snapshot_id)
            .to_string();

        // 4. Read, parse, and dedupe events by idempotency_key; upsert by primary key.
        let mut records_by_key: HashMap<String, RecordWithSequence> = HashMap::new();
        if let Some(snapshot_path) = manifest.snapshot_path.as_deref() {
            let existing = self.read_snapshot_records(snapshot_path).await?;
            for record in existing {
                records_by_key.insert(
                    record.materialization_id.clone(),
                    RecordWithSequence {
                        record,
                        sequence_position: None,
                    },
                );
            }
        }

        let mut seen_idempotency_keys: HashSet<String> = HashSet::new();
        let mut events_applied: usize = 0;
        let mut events_deduped: usize = 0;
        let mut events_quarantined: usize = 0;

        let expected_type = <MaterializationRecord as CatalogEventPayload>::EVENT_TYPE;
        let expected_version = <MaterializationRecord as CatalogEventPayload>::EVENT_VERSION;

        for file_path in &events_to_process {
            let filename = file_path.rsplit('/').next().unwrap_or("");

            let data =
                self.storage
                    .get_raw(file_path)
                    .await
                    .map_err(|e| CatalogError::Storage {
                        message: format!("failed to read event: {e}"),
                    })?;

            // Prefer the enveloped format (ADR-004), but accept legacy raw payloads for migration.
            let parsed_envelope = serde_json::from_slice::<CatalogEvent<serde_json::Value>>(&data);

            let (event_type, event_version, idempotency_key, record, sequence_position): (
                String,
                u32,
                String,
                MaterializationRecord,
                Option<u64>,
            ) = if let Ok(envelope) = parsed_envelope {
                if let Err(e) = envelope.validate() {
                    match self.unknown_event_policy {
                        UnknownEventPolicy::Reject => {
                            return Err(CatalogError::InvariantViolation {
                                message: format!("invalid event envelope in '{file_path}': {e}"),
                            });
                        }
                        UnknownEventPolicy::Quarantine => {
                            self.quarantine_event_bytes(
                                domain,
                                filename,
                                data.clone(),
                                "invalid_envelope",
                            )
                            .await?;
                            events_quarantined += 1;
                            continue;
                        }
                    }
                }

                let record: MaterializationRecord = match serde_json::from_value(envelope.payload) {
                    Ok(record) => record,
                    Err(e) => match self.unknown_event_policy {
                        UnknownEventPolicy::Reject => {
                            return Err(CatalogError::Serialization {
                                message: format!(
                                    "failed to parse event payload '{file_path}': {e}"
                                ),
                            });
                        }
                        UnknownEventPolicy::Quarantine => {
                            self.quarantine_event_bytes(
                                domain,
                                filename,
                                data.clone(),
                                "invalid_payload",
                            )
                            .await?;
                            events_quarantined += 1;
                            continue;
                        }
                    },
                };
                (
                    envelope.event_type,
                    envelope.event_version,
                    envelope.idempotency_key,
                    record,
                    envelope.sequence_position,
                )
            } else {
                // Legacy payload format (raw MaterializationRecord JSON).
                let record: MaterializationRecord = match serde_json::from_slice(&data) {
                    Ok(record) => record,
                    Err(e) => match self.unknown_event_policy {
                        UnknownEventPolicy::Reject => {
                            return Err(CatalogError::Serialization {
                                message: format!("failed to parse legacy event '{file_path}': {e}"),
                            });
                        }
                        UnknownEventPolicy::Quarantine => {
                            self.quarantine_event_bytes(
                                domain,
                                filename,
                                data.clone(),
                                "unparseable_legacy_event",
                            )
                            .await?;
                            events_quarantined += 1;
                            continue;
                        }
                    },
                };

                let idempotency_key = CatalogEvent::<()>::generate_idempotency_key(
                    expected_type,
                    expected_version,
                    &record,
                )
                .map_err(|e| CatalogError::Serialization {
                    message: format!("failed to generate idempotency key for '{file_path}': {e}"),
                })?;

                (
                    expected_type.to_string(),
                    expected_version,
                    idempotency_key,
                    record,
                    None,
                )
            };

            if event_type != expected_type || event_version != expected_version {
                match self.unknown_event_policy {
                    UnknownEventPolicy::Reject => {
                        return Err(CatalogError::InvariantViolation {
                            message: format!(
                                "unsupported event in '{file_path}': type='{event_type}' version={event_version}"
                            ),
                        });
                    }
                    UnknownEventPolicy::Quarantine => {
                        self.quarantine_event_bytes(
                            domain,
                            filename,
                            data.clone(),
                            "unsupported_event",
                        )
                        .await?;
                        events_quarantined += 1;
                        continue;
                    }
                }
            }

            // DEDUPE: ignore duplicate deliveries by idempotency_key within this fold.
            if !seen_idempotency_keys.insert(idempotency_key) {
                events_deduped += 1;
                continue;
            }

            // UPSERT: materialization_id is the primary key; sequence position wins when present.
            match records_by_key.entry(record.materialization_id.clone()) {
                Entry::Occupied(mut entry) => {
                    if should_replace_sequence(entry.get().sequence_position, sequence_position) {
                        entry.insert(RecordWithSequence {
                            record,
                            sequence_position,
                        });
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(RecordWithSequence {
                        record,
                        sequence_position,
                    });
                }
            }
            events_applied += 1;
        }

        // 5. Write Parquet snapshot BEFORE manifest update (atomic publish).
        let (parquet_files_written, new_snapshot_path, new_snapshot_version) = if events_applied > 0
        {
            let mut records: Vec<_> = records_by_key
                .into_values()
                .map(|entry| entry.record)
                .collect();
            records.sort_by(|a, b| a.materialization_id.cmp(&b.materialization_id));

            let snapshot_version = manifest.snapshot_version + 1;
            let path = CatalogPaths::state_snapshot(domain, snapshot_version, &snapshot_id);

            let wrote_new = self.write_parquet(&path, &records).await?;
            (usize::from(wrote_new), Some(path), snapshot_version)
        } else {
            (0, manifest.snapshot_path.clone(), manifest.snapshot_version)
        };

        let now = Utc::now();
        let mut new_manifest = manifest.clone();
        let new_watermark_event_id =
            new_watermark_event_id.or_else(|| manifest.watermark_event_id.clone());
        new_manifest.watermark_version = manifest.watermark_version + 1;
        new_manifest
            .watermark_event_id
            .clone_from(&new_watermark_event_id);
        new_manifest.snapshot_path = new_snapshot_path;
        new_manifest.snapshot_version = new_snapshot_version;
        new_manifest.last_compaction_at = Some(now);
        new_manifest.compaction.total_events_compacted += events_to_process.len() as u64;
        new_manifest.compaction.total_files_written += parquet_files_written as u64;
        new_manifest.updated_at = now;

        self.publish_manifest(
            &root.executions_manifest_path,
            &new_manifest,
            &current_version,
        )
        .await?;

        tracing::info!(
            events_processed = events_to_process.len(),
            events_applied,
            events_deduped,
            events_quarantined,
            late_events = late_events.len(),
            "compaction completed"
        );

        Ok(CompactionResult {
            events_processed: events_to_process.len(),
            parquet_files_written,
            new_watermark: new_watermark_event_id.unwrap_or_default(),
        })
    }

    async fn publish_manifest(
        &self,
        path: &str,
        manifest: &ExecutionsManifest,
        current_version: &str,
    ) -> Result<()> {
        let execution_json =
            serde_json::to_vec_pretty(manifest).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        let result = self
            .storage
            .put_raw(
                path,
                Bytes::from(execution_json),
                WritePrecondition::MatchesVersion(current_version.to_string()),
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write manifest: {e}"),
            })?;

        match result {
            arco_core::storage::WriteResult::Success { .. } => Ok(()),
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                Err(CatalogError::CasFailed {
                    message: "manifest updated by another compactor".to_string(),
                })
            }
        }
    }

    async fn handle_late_events(&self, domain: CatalogDomain, paths: &[String]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        match self.late_event_policy {
            LateEventPolicy::Skip => Ok(()),
            LateEventPolicy::Log => {
                tracing::warn!(
                    late_events = paths.len(),
                    metric = "arco_compactor_late_events_total",
                    "late events detected (arrived at or before watermark); skipping per policy"
                );
                Ok(())
            }
            LateEventPolicy::Process => {
                tracing::warn!(
                    late_events = paths.len(),
                    metric = "arco_compactor_late_events_total",
                    "late events detected (arrived at or before watermark); processing per policy"
                );
                Ok(())
            }
            LateEventPolicy::Quarantine => {
                for path in paths {
                    let filename = path.rsplit('/').next().unwrap_or("");
                    let data =
                        self.storage
                            .get_raw(path)
                            .await
                            .map_err(|e| CatalogError::Storage {
                                message: format!("failed to read late event: {e}"),
                            })?;
                    self.quarantine_event_bytes(domain, filename, data, "late_event")
                        .await?;
                }
                tracing::warn!(
                    late_events = paths.len(),
                    metric = "arco_compactor_late_events_total",
                    "late events quarantined"
                );
                Ok(())
            }
        }
    }

    async fn quarantine_event_bytes(
        &self,
        domain: CatalogDomain,
        filename: &str,
        data: Bytes,
        reason: &str,
    ) -> Result<()> {
        let path = CatalogPaths::quarantine_event(domain, filename);

        let result = self
            .storage
            .put_raw(&path, data, WritePrecondition::DoesNotExist)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to quarantine event ({reason}): {e}"),
            })?;

        match result {
            arco_core::storage::WriteResult::Success { .. }
            | arco_core::storage::WriteResult::PreconditionFailed { .. } => Ok(()),
        }
    }

    /// Reads all records from an existing Parquet snapshot.
    async fn read_snapshot_records(&self, path: &str) -> Result<Vec<MaterializationRecord>> {
        let parquet_data = self
            .storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read snapshot '{path}': {e}"),
            })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to open Parquet snapshot '{path}': {e}"),
            })?
            .build()
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to build Parquet reader for '{path}': {e}"),
            })?;

        let mut records = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| CatalogError::Serialization {
                message: format!("failed to read Parquet batch from '{path}': {e}"),
            })?;

            let schema = batch.schema();
            let materialization_id_col_idx =
                schema
                    .index_of("materialization_id")
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("snapshot '{path}' is missing 'materialization_id': {e}"),
                    })?;
            let asset_id_col_idx =
                schema
                    .index_of("asset_id")
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("snapshot '{path}' is missing 'asset_id': {e}"),
                    })?;
            let row_count_col_idx =
                schema
                    .index_of("row_count")
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("snapshot '{path}' is missing 'row_count': {e}"),
                    })?;
            let byte_size_col_idx =
                schema
                    .index_of("byte_size")
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("snapshot '{path}' is missing 'byte_size': {e}"),
                    })?;

            let materialization_ids = batch
                .column(materialization_id_col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CatalogError::Serialization {
                    message: format!(
                        "snapshot '{path}' column 'materialization_id' has unexpected type"
                    ),
                })?;
            let asset_ids = batch
                .column(asset_id_col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CatalogError::Serialization {
                    message: format!("snapshot '{path}' column 'asset_id' has unexpected type"),
                })?;
            let row_counts = batch
                .column(row_count_col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CatalogError::Serialization {
                    message: format!("snapshot '{path}' column 'row_count' has unexpected type"),
                })?;
            let byte_sizes = batch
                .column(byte_size_col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CatalogError::Serialization {
                    message: format!("snapshot '{path}' column 'byte_size' has unexpected type"),
                })?;

            for row in 0..batch.num_rows() {
                records.push(MaterializationRecord {
                    materialization_id: materialization_ids.value(row).to_string(),
                    asset_id: asset_ids.value(row).to_string(),
                    row_count: row_counts.value(row),
                    byte_size: byte_sizes.value(row),
                });
            }
        }

        Ok(records)
    }

    /// Writes records to a Parquet snapshot file.
    ///
    /// Returns `true` if a new file was written, or `false` if the file already existed.
    async fn write_parquet(&self, path: &str, records: &[MaterializationRecord]) -> Result<bool> {
        let materialization_ids: Vec<_> = records
            .iter()
            .map(|r| r.materialization_id.as_str())
            .collect();
        let asset_ids: Vec<_> = records.iter().map(|r| r.asset_id.as_str()).collect();
        let row_counts: Vec<_> = records.iter().map(|r| r.row_count).collect();
        let byte_sizes: Vec<_> = records.iter().map(|r| r.byte_size).collect();

        let schema = Schema::new(vec![
            Field::new("materialization_id", DataType::Utf8, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
            Field::new("byte_size", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(materialization_ids)),
                Arc::new(StringArray::from(asset_ids)),
                Arc::new(Int64Array::from(row_counts)),
                Arc::new(Int64Array::from(byte_sizes)),
            ],
        )
        .map_err(|e| CatalogError::Serialization {
            message: format!("failed to create record batch: {e}"),
        })?;

        let mut buffer = Cursor::new(Vec::new());
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(props)).map_err(|e| {
                CatalogError::Serialization {
                    message: format!("failed to create Parquet writer: {e}"),
                }
            })?;

        writer
            .write(&batch)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to write batch: {e}"),
            })?;
        writer.close().map_err(|e| CatalogError::Serialization {
            message: format!("failed to close writer: {e}"),
        })?;

        let result = self
            .storage
            .put_raw(
                path,
                Bytes::from(buffer.into_inner()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write Parquet: {e}"),
            })?;

        match result {
            arco_core::storage::WriteResult::Success { .. } => Ok(true),
            arco_core::storage::WriteResult::PreconditionFailed { .. } => Ok(false),
        }
    }

    /// Reads the execution manifest with its current version for CAS.
    async fn read_manifest_with_version(
        &self,
    ) -> Result<(RootManifest, ExecutionsManifest, String)> {
        let mut root: RootManifest = self
            .storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read root manifest: {e}"),
            })
            .and_then(|data| {
                serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                    message: format!("failed to parse root manifest: {e}"),
                })
            })?;
        root.normalize_paths();

        let manifest_path = root.executions_manifest_path.clone();

        // First, get metadata to retrieve version
        let meta = self
            .storage
            .head_raw(&manifest_path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read manifest metadata: {e}"),
            })?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "manifest".to_string(),
                name: manifest_path.clone(),
            })?;

        let version = meta.version.clone();

        // Read the content
        let data =
            self.storage
                .get_raw(&manifest_path)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to read manifest: {e}"),
                })?;

        let manifest: ExecutionsManifest =
            serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse manifest: {e}"),
            })?;

        Ok((root, manifest, version))
    }
}

fn validate_event_paths(domain: CatalogDomain, event_paths: Vec<String>) -> Result<Vec<String>> {
    let prefix = format!("ledger/{}/", domain.as_str());
    let mut unique = HashSet::new();
    let mut filtered = Vec::new();

    for path in event_paths {
        if !path.starts_with(&prefix) {
            return Err(CatalogError::Validation {
                message: format!("event path '{path}' is outside {prefix}"),
            });
        }
        if !std::path::Path::new(&path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            return Err(CatalogError::Validation {
                message: format!("event path '{path}' must end with .json"),
            });
        }
        if unique.insert(path.clone()) {
            filtered.push(path);
        }
    }

    Ok(filtered)
}

/// Record structure for materialization events (matches `EventWriter` format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationRecord {
    /// Unique materialization ID.
    pub materialization_id: String,
    /// Asset ID this materialization belongs to.
    pub asset_id: String,
    /// Number of rows produced.
    #[serde(default)]
    pub row_count: i64,
    /// Size in bytes.
    #[serde(default)]
    pub byte_size: i64,
}

impl CatalogEventPayload for MaterializationRecord {
    const EVENT_TYPE: &'static str = "materialization.completed";
    const EVENT_VERSION: u32 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventWriter;
    use crate::Tier1Writer;
    use arco_core::EventId;
    use arco_core::storage::{
        MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use std::ops::Range;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn compact_empty_ledger_is_noop() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("init");

        let compactor = Compactor::new(storage);
        let result = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact");

        assert_eq!(result.events_processed, 0);
        assert_eq!(result.parquet_files_written, 0);
    }

    #[tokio::test]
    async fn compact_processes_ledger_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        for i in 0..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append(CatalogDomain::Executions, &event)
                .await
                .expect("append");
        }

        let compactor = Compactor::new(storage);
        let result = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact");

        assert_eq!(result.events_processed, 5);
        assert!(result.parquet_files_written > 0);
    }

    #[tokio::test]
    async fn parquet_write_is_deterministic() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let compactor = Compactor::new(storage.clone());
        let records = vec![
            MaterializationRecord {
                materialization_id: "mat_001".to_string(),
                asset_id: "asset_001".to_string(),
                row_count: 10,
                byte_size: 100,
            },
            MaterializationRecord {
                materialization_id: "mat_002".to_string(),
                asset_id: "asset_002".to_string(),
                row_count: 20,
                byte_size: 200,
            },
        ];

        let path_one =
            CatalogPaths::state_snapshot(CatalogDomain::Executions, 1, "deterministic_a");
        let path_two =
            CatalogPaths::state_snapshot(CatalogDomain::Executions, 1, "deterministic_b");

        let wrote_one = compactor
            .write_parquet(&path_one, &records)
            .await
            .expect("write one");
        let wrote_two = compactor
            .write_parquet(&path_two, &records)
            .await
            .expect("write two");

        assert!(wrote_one);
        assert!(wrote_two);

        let bytes_one = storage.get_raw(&path_one).await.expect("read one");
        let bytes_two = storage.get_raw(&path_two).await.expect("read two");

        assert_eq!(bytes_one, bytes_two);
    }

    #[tokio::test]
    async fn second_compaction_only_processes_new_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        let compactor = Compactor::new(storage.clone());

        // Write 3 events and compact
        for i in 0..3 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append(CatalogDomain::Executions, &event)
                .await
                .expect("append");
        }
        let result1 = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact1");
        assert_eq!(result1.events_processed, 3);

        // Ensure new events have distinct ULID timestamps (ULID uses millisecond precision)
        tokio::time::sleep(Duration::from_millis(2)).await;

        // Write 2 more events and compact again
        for i in 3..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append(CatalogDomain::Executions, &event)
                .await
                .expect("append");
        }
        let result2 = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact2");

        assert_eq!(
            result2.events_processed, 2,
            "only new events since watermark"
        );
    }

    #[tokio::test]
    async fn unknown_event_version_is_rejected_by_default() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let ledger_path = CatalogPaths::ledger_event(CatalogDomain::Executions, event_id);

        let envelope = CatalogEvent {
            event_type: <MaterializationRecord as CatalogEventPayload>::EVENT_TYPE.to_string(),
            event_version: 999,
            idempotency_key: "test:key".to_string(),
            occurred_at: Utc::now(),
            source: "test".to_string(),
            trace_id: None,
            sequence_position: None,
            payload: serde_json::json!({
                "materialization_id": "mat_001",
                "asset_id": "asset_abc",
                "row_count": 1,
                "byte_size": 1
            }),
        };

        storage
            .put_raw(
                &ledger_path,
                Bytes::from(serde_json::to_vec(&envelope).expect("serialize")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write");

        let compactor = Compactor::new(storage);
        let result = compactor.compact_domain(CatalogDomain::Executions).await;
        assert!(
            matches!(result, Err(CatalogError::InvariantViolation { .. })),
            "unknown event_version should be rejected by default"
        );
    }

    #[tokio::test]
    async fn unknown_event_version_can_be_quarantined() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let ledger_path = CatalogPaths::ledger_event(CatalogDomain::Executions, event_id);

        let envelope = CatalogEvent {
            event_type: <MaterializationRecord as CatalogEventPayload>::EVENT_TYPE.to_string(),
            event_version: 999,
            idempotency_key: "test:key".to_string(),
            occurred_at: Utc::now(),
            source: "test".to_string(),
            trace_id: None,
            sequence_position: None,
            payload: serde_json::json!({
                "materialization_id": "mat_001",
                "asset_id": "asset_abc",
                "row_count": 1,
                "byte_size": 1
            }),
        };

        storage
            .put_raw(
                &ledger_path,
                Bytes::from(serde_json::to_vec(&envelope).expect("serialize")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write");

        let compactor = Compactor::new(storage.clone())
            .with_unknown_event_policy(UnknownEventPolicy::Quarantine);
        let result = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact");
        assert_eq!(result.events_processed, 1);
        assert_eq!(result.parquet_files_written, 0);

        let quarantined = storage
            .list(&CatalogPaths::quarantine_dir(CatalogDomain::Executions))
            .await
            .expect("list quarantine");
        assert_eq!(quarantined.len(), 1);
        assert!(
            quarantined[0]
                .as_str()
                .ends_with(&format!("{event_id}.json")),
            "quarantine should preserve original filename"
        );
    }

    #[tokio::test]
    async fn late_event_is_quarantined() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        let id_high: EventId = "01ARZ3NDEKTSV4RRFFQ69G5FAZ".parse().expect("event id");
        let id_mid: EventId = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().expect("event id");

        // First compaction establishes a watermark at `id_high`.
        event_writer
            .append_with_id(
                CatalogDomain::Executions,
                &MaterializationRecord {
                    materialization_id: "mat_001".into(),
                    asset_id: "asset_abc".into(),
                    row_count: 300,
                    byte_size: 3000,
                },
                &id_high,
            )
            .await
            .expect("append high");

        let compactor =
            Compactor::new(storage.clone()).with_late_event_policy(LateEventPolicy::Quarantine);
        compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact");

        // Late arrival: a new event whose filename sorts at/before the watermark.
        event_writer
            .append_with_id(
                CatalogDomain::Executions,
                &MaterializationRecord {
                    materialization_id: "mat_002".into(),
                    asset_id: "asset_xyz".into(),
                    row_count: 100,
                    byte_size: 1000,
                },
                &id_mid,
            )
            .await
            .expect("append mid (late)");

        let result = compactor
            .compact_domain(CatalogDomain::Executions)
            .await
            .expect("compact late");
        assert_eq!(result.events_processed, 0);
        assert_eq!(result.parquet_files_written, 0);
        assert_eq!(result.new_watermark, format!("{id_high}.json"));

        let quarantined = storage
            .list(&CatalogPaths::quarantine_dir(CatalogDomain::Executions))
            .await
            .expect("list quarantine");
        assert_eq!(quarantined.len(), 1);
        assert!(quarantined[0].as_str().ends_with(&format!("{id_mid}.json")));
    }

    #[derive(Debug)]
    struct BarrierBackend {
        inner: Arc<MemoryBackend>,
        cas_barrier: Arc<Barrier>,
    }

    impl BarrierBackend {
        fn new(parties: usize) -> Self {
            Self {
                inner: Arc::new(MemoryBackend::new()),
                cas_barrier: Arc::new(Barrier::new(parties)),
            }
        }

        fn is_executions_manifest_path(path: &str) -> bool {
            path.ends_with("/manifests/executions.manifest.json")
        }
    }

    #[async_trait::async_trait]
    impl StorageBackend for BarrierBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if Self::is_executions_manifest_path(path)
                && matches!(precondition, WritePrecondition::MatchesVersion(_))
            {
                self.cas_barrier.wait().await;
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn concurrent_compactors_retry_on_cas_conflict() {
        let backend: Arc<dyn StorageBackend> = Arc::new(BarrierBackend::new(2));
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        let event_id: EventId = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().expect("event id");
        event_writer
            .append_with_id(
                CatalogDomain::Executions,
                &MaterializationRecord {
                    materialization_id: "mat_001".into(),
                    asset_id: "asset_abc".into(),
                    row_count: 100,
                    byte_size: 5000,
                },
                &event_id,
            )
            .await
            .expect("append");

        let compactor_a = Compactor::new(storage.clone())
            .with_cas_retries(5)
            .with_cas_backoff(Duration::from_millis(1), Duration::from_millis(1));
        let compactor_b = Compactor::new(storage.clone())
            .with_cas_retries(5)
            .with_cas_backoff(Duration::from_millis(1), Duration::from_millis(1));

        let (r1, r2) = tokio::join!(
            compactor_a.compact_domain(CatalogDomain::Executions),
            compactor_b.compact_domain(CatalogDomain::Executions)
        );
        let r1 = r1.expect("compactor A");
        let r2 = r2.expect("compactor B");

        assert_eq!(
            r1.events_processed + r2.events_processed,
            1,
            "exactly one compactor should publish the snapshot; the other should retry and no-op"
        );
    }
}
