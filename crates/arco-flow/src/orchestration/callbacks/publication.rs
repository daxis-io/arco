//! Storage-owner verification for worker publication claims.

use std::future::Future;
use std::pin::Pin;

use chrono::Utc;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use sha2::{Digest, Sha256};

use arco_core::ScopedStorage;
use arco_worker_contract::{PublicationDescriptor, PublicationOwnerEvidence};

const PUBLICATION_DESCRIPTOR_VERSION: u32 = 1;
const HASH_READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const INITIAL_PARQUET_FOOTER_BYTES: u64 = 64 * 1024;
const MAX_PARQUET_FOOTER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PUBLICATION_BYTES: u64 = 256 * 1024 * 1024;

/// Verifies a worker publication claim using owner-controlled authority.
pub trait PublicationVerifier: Send + Sync {
    /// Returns storage-owner evidence only after exact identity and integrity checks.
    fn verify<'a>(
        &'a self,
        descriptor: &'a PublicationDescriptor,
    ) -> Pin<Box<dyn Future<Output = Result<PublicationOwnerEvidence, String>> + Send + 'a>>;
}

/// Publication verifier rooted at the authenticated workspace storage scope.
#[derive(Clone)]
pub struct ScopedStoragePublicationVerifier {
    storage: ScopedStorage,
}

impl ScopedStoragePublicationVerifier {
    /// Creates a verifier for one authenticated workspace scope.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }
}

impl PublicationVerifier for ScopedStoragePublicationVerifier {
    fn verify<'a>(
        &'a self,
        descriptor: &'a PublicationDescriptor,
    ) -> Pin<Box<dyn Future<Output = Result<PublicationOwnerEvidence, String>> + Send + 'a>> {
        Box::pin(async move {
            validate_descriptor(descriptor)?;

            let before = self
                .storage
                .head_raw(&descriptor.object_path)
                .await
                .map_err(|error| format!("publication metadata unavailable: {error}"))?
                .ok_or_else(|| "publication object is missing".to_string())?;
            if before.version != descriptor.object_version {
                return Err("publication object version mismatch".to_string());
            }
            if before.size != descriptor.byte_size {
                return Err("publication object size mismatch".to_string());
            }

            let mut hasher = Sha256::new();
            let mut offset = 0_u64;
            while offset < descriptor.byte_size {
                let end = offset
                    .saturating_add(HASH_READ_CHUNK_BYTES)
                    .min(descriptor.byte_size);
                let chunk = self
                    .storage
                    .get_range_with_ownership(&descriptor.object_path, offset..end)
                    .await
                    .map_err(|error| format!("publication bytes unavailable: {error}"))?;
                if chunk.bytes.len() as u64 != end - offset {
                    return Err("publication ranged read length mismatch".to_string());
                }
                if offset == 0 && !chunk.bytes.starts_with(b"PAR1") {
                    return Err("publication Parquet header is invalid".to_string());
                }
                hasher.update(&chunk.bytes);
                offset = end;
            }
            let actual_checksum = hex::encode(hasher.finalize());
            if actual_checksum != descriptor.checksum_sha256 {
                return Err("publication checksum mismatch".to_string());
            }
            verify_parquet_schema(&self.storage, descriptor).await?;

            let after = self
                .storage
                .head_raw(&descriptor.object_path)
                .await
                .map_err(|error| format!("publication metadata unavailable after read: {error}"))?
                .ok_or_else(|| "publication object disappeared during verification".to_string())?;
            if after.version != before.version || after.size != before.size {
                return Err("publication object changed during verification".to_string());
            }

            Ok(PublicationOwnerEvidence {
                verified_at: Utc::now(),
                object_version: after.version,
                etag: after.etag,
            })
        })
    }
}

async fn verify_parquet_schema(
    storage: &ScopedStorage,
    descriptor: &PublicationDescriptor,
) -> Result<(), String> {
    let file_size = usize::try_from(descriptor.byte_size)
        .map_err(|_| "publication size is unsupported on this platform".to_string())?;
    let mut footer_bytes = descriptor.byte_size.min(INITIAL_PARQUET_FOOTER_BYTES);
    loop {
        let tail = storage
            .get_range_with_ownership(
                &descriptor.object_path,
                descriptor.byte_size - footer_bytes..descriptor.byte_size,
            )
            .await
            .map_err(|error| format!("publication footer unavailable: {error}"))?;
        let mut reader = ParquetMetaDataReader::new();
        match reader.try_parse_sized(&tail.bytes, file_size) {
            Ok(()) => {
                let metadata = reader
                    .finish()
                    .map_err(|error| format!("publication Parquet metadata is invalid: {error}"))?;
                if metadata.file_metadata().schema_descr().num_columns() == 0 {
                    return Err("publication Parquet schema is empty".to_string());
                }
                return Ok(());
            }
            Err(ParquetError::NeedMoreData(needed)) => {
                let needed = u64::try_from(needed)
                    .map_err(|_| "publication Parquet footer is too large".to_string())?;
                if needed <= footer_bytes
                    || needed > descriptor.byte_size
                    || needed > MAX_PARQUET_FOOTER_BYTES
                {
                    return Err("publication Parquet footer exceeds verification limit".to_string());
                }
                footer_bytes = needed;
            }
            Err(error) => {
                return Err(format!("publication Parquet metadata is invalid: {error}"));
            }
        }
    }
}

fn validate_descriptor(descriptor: &PublicationDescriptor) -> Result<(), String> {
    if descriptor.version != PUBLICATION_DESCRIPTOR_VERSION {
        return Err("unsupported publication descriptor version".to_string());
    }
    if descriptor.byte_size > MAX_PUBLICATION_BYTES {
        return Err("publication exceeds verification size limit".to_string());
    }
    if descriptor.object_version.is_empty() {
        return Err("publication identity, format, and schema reference are required".to_string());
    }
    if descriptor.checksum_sha256.len() != 64
        || !descriptor
            .checksum_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("publication checksum must be lowercase SHA-256 hex".to_string());
    }
    if descriptor.manifest_id != format!("sha256:{}", descriptor.checksum_sha256) {
        return Err("publication manifest identity does not match checksum".to_string());
    }
    if descriptor.format != "parquet" {
        return Err("unsupported publication format".to_string());
    }
    if descriptor.schema_ref != format!("{}#parquet-schema", descriptor.manifest_id) {
        return Err("publication schema reference mismatch".to_string());
    }
    ScopedStorage::validate_path(&descriptor.object_path).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::{MemoryBackend, StorageBackend, WritePrecondition};
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn parquet_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("batch");
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.into_inner().expect("finish")
    }

    fn descriptor(bytes: &[u8]) -> PublicationDescriptor {
        let checksum = hex::encode(Sha256::digest(bytes));
        PublicationDescriptor {
            version: 1,
            manifest_id: format!("sha256:{checksum}"),
            object_path: "outputs/run-1/result.parquet".to_string(),
            object_version: "1".to_string(),
            checksum_sha256: checksum.clone(),
            byte_size: bytes.len() as u64,
            format: "parquet".to_string(),
            schema_ref: format!("sha256:{checksum}#parquet-schema"),
            owner_evidence: None,
        }
    }

    async fn verifier_with(bytes: &[u8]) -> ScopedStoragePublicationVerifier {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(Arc::clone(&backend), "tenant-1", "workspace-1").expect("scope");
        storage
            .put_raw(
                "outputs/run-1/result.parquet",
                Bytes::copy_from_slice(bytes),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("put");
        ScopedStoragePublicationVerifier::new(storage)
    }

    #[tokio::test]
    async fn verifies_exact_owner_version_size_and_checksum() {
        let parquet = parquet_bytes();
        let verifier = verifier_with(&parquet).await;
        let evidence = verifier
            .verify(&descriptor(&parquet))
            .await
            .expect("verified");
        assert_eq!(evidence.object_version, "1");
    }

    #[tokio::test]
    async fn rejects_missing_corrupt_and_mismatched_publications() {
        let parquet = parquet_bytes();
        let verifier = verifier_with(&parquet).await;

        let mut changed = parquet.clone();
        changed[4] ^= 1;
        let mut corrupt = descriptor(&changed);
        corrupt.byte_size = parquet.len() as u64;
        assert_eq!(
            verifier.verify(&corrupt).await.unwrap_err(),
            "publication checksum mismatch"
        );

        let mut wrong_version = descriptor(&parquet);
        wrong_version.object_version = "2".to_string();
        assert_eq!(
            verifier.verify(&wrong_version).await.unwrap_err(),
            "publication object version mismatch"
        );

        let missing_storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant-1", "workspace-1")
                .expect("scope");
        assert_eq!(
            ScopedStoragePublicationVerifier::new(missing_storage)
                .verify(&descriptor(&parquet))
                .await
                .unwrap_err(),
            "publication object is missing"
        );

        let mut wrong_schema = descriptor(&parquet);
        wrong_schema.schema_ref = "schema://analytics.daily/v2".to_string();
        assert_eq!(
            verifier.verify(&wrong_schema).await.unwrap_err(),
            "publication schema reference mismatch"
        );

        let invalid_footer = b"PAR1dataPAR1";
        let invalid_verifier = verifier_with(invalid_footer).await;
        assert!(
            invalid_verifier
                .verify(&descriptor(invalid_footer))
                .await
                .unwrap_err()
                .starts_with("publication Parquet metadata is invalid")
        );

        let mut invalid_header = parquet.clone();
        invalid_header[0] = b'X';
        let invalid_header_verifier = verifier_with(&invalid_header).await;
        assert_eq!(
            invalid_header_verifier
                .verify(&descriptor(&invalid_header))
                .await
                .unwrap_err(),
            "publication Parquet header is invalid"
        );

        let mut oversized = descriptor(&parquet);
        oversized.byte_size = MAX_PUBLICATION_BYTES + 1;
        assert_eq!(
            verifier.verify(&oversized).await.unwrap_err(),
            "publication exceeds verification size limit"
        );
    }
}
