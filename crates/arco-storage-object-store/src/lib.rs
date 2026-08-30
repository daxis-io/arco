//! Provider-neutral adapter from `object_store` to Arco storage contracts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use http::Method;
use object_store::path::Path as ObjectStorePath;
use object_store::signer::Signer as ObjectStoreSigner;
use object_store::{DynObjectStore, PutMode, PutOptions, UpdateVersion};

/// Provider-neutral adapter from an [`object_store::ObjectStore`] to Arco's
/// storage interface.
///
/// Cloud-specific crates construct the underlying store and signer. This
/// adapter owns only protocol translation: opaque version tokens, conditional
/// writes, range validation, bounded listing, metadata, and signed URLs.
#[derive(Debug, Clone)]
pub struct ObjectStoreBackend {
    store: Arc<DynObjectStore>,
    signer: Option<Arc<dyn ObjectStoreSigner>>,
    ordered_listing: bool,
}

impl ObjectStoreBackend {
    /// Creates an adapter that fails closed for bounded ordered listing.
    #[must_use]
    pub fn new(store: Arc<DynObjectStore>, signer: Option<Arc<dyn ObjectStoreSigner>>) -> Self {
        Self {
            store,
            signer,
            ordered_listing: false,
        }
    }

    /// Creates an adapter for a provider that guarantees lexicographically
    /// ordered listing with an exclusive offset.
    #[must_use]
    pub fn new_with_ordered_listing(
        store: Arc<DynObjectStore>,
        signer: Option<Arc<dyn ObjectStoreSigner>>,
    ) -> Self {
        Self {
            store,
            signer,
            ordered_listing: true,
        }
    }

    /// Returns whether bounded ordered listing is enabled for this adapter.
    #[must_use]
    pub const fn ordered_listing_enabled(&self) -> bool {
        self.ordered_listing
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct VersionToken {
    e_tag: Option<String>,
    version: Option<String>,
}

impl VersionToken {
    fn from_parts(e_tag: Option<String>, version: Option<String>) -> Self {
        Self { e_tag, version }
    }

    fn to_update_version(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }

    fn encode(&self) -> String {
        match serde_json::to_string(self) {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to serialize version token; falling back to raw version"
                );
                self.version.clone().unwrap_or_default()
            }
        }
    }

    fn decode(token: &str) -> Self {
        let token = token.trim();
        if token.is_empty() {
            return Self::from_parts(None, None);
        }
        serde_json::from_str::<Self>(token)
            .unwrap_or_else(|_| Self::from_parts(None, Some(token.to_string())))
    }
}

fn map_object_store_error(error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { path, .. } => Error::NotFound(path),
        object_store::Error::InvalidPath { source } => {
            Error::InvalidInput(format!("invalid object store path: {source}"))
        }
        error @ object_store::Error::PermissionDenied { .. } => {
            Error::storage_with_source("permission denied".to_string(), error)
        }
        error @ object_store::Error::Unauthenticated { .. } => {
            Error::storage_with_source("unauthenticated".to_string(), error)
        }
        error => Error::storage_with_source("object store error".to_string(), error),
    }
}

fn is_write_precondition_failure(
    precondition: &WritePrecondition,
    error: &object_store::Error,
) -> bool {
    match precondition {
        WritePrecondition::None => false,
        WritePrecondition::DoesNotExist => matches!(
            error,
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
        ),
        WritePrecondition::MatchesVersion(_) => match error {
            object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. } => true,
            object_store::Error::Generic { source, .. } => {
                let message = source.to_string();
                message.contains("ETag required for conditional update")
                    || message.contains("MissingETag")
            }
            _ => false,
        },
    }
}

fn object_store_meta_to_meta(meta: object_store::ObjectMeta) -> ObjectMeta {
    let version = VersionToken::from_parts(meta.e_tag.clone(), meta.version.clone()).encode();
    ObjectMeta {
        path: meta.location.to_string(),
        size: u64::try_from(meta.size).unwrap_or(u64::MAX),
        version,
        last_modified: Some(meta.last_modified),
        etag: meta.e_tag,
    }
}

fn validate_page_cursor(prefix: &str, start_after: Option<&str>) -> Result<()> {
    let Some(start_after) = start_after else {
        return Ok(());
    };
    let boundary = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    };
    if !prefix.is_empty() && start_after != prefix && !start_after.starts_with(&boundary) {
        return Err(Error::InvalidInput(format!(
            "list cursor '{start_after}' is outside prefix '{prefix}'"
        )));
    }
    Ok(())
}

fn validate_ordered_page(
    prefix: &str,
    start_after: Option<&str>,
    limit: usize,
    objects: &[ObjectMeta],
) -> Result<()> {
    if objects.len() > limit {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' returned {} objects for limit {limit}",
            objects.len()
        )));
    }
    if let (Some(start_after), Some(first)) = (start_after, objects.first())
        && first.path.as_str() <= start_after
    {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' returned non-exclusive path '{}' after '{start_after}'",
            first.path
        )));
    }
    if let Some((left, right)) = objects.windows(2).find_map(|window| match window {
        [left, right] if left.path >= right.path => Some((left, right)),
        _ => None,
    }) {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' was not strictly ordered: '{}' then '{}'",
            left.path, right.path
        )));
    }
    Ok(())
}

fn next_start_after(objects: &[ObjectMeta], limit: usize) -> Option<String> {
    if objects.len() == limit {
        objects.last().map(|meta| meta.path.clone())
    } else {
        None
    }
}

#[async_trait]
impl StorageBackend for ObjectStoreBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        let location = ObjectStorePath::from(path);
        let result = self
            .store
            .get(&location)
            .await
            .map_err(map_object_store_error)?;
        result.bytes().await.map_err(map_object_store_error)
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let meta = self
            .head(path)
            .await?
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))?;
        let size = usize::try_from(meta.size).map_err(|_| {
            Error::InvalidInput(format!(
                "object size too large for range requests: {}",
                meta.size
            ))
        })?;
        let start = usize::try_from(range.start)
            .map_err(|_| Error::InvalidInput(format!("range start too large: {}", range.start)))?;
        if start > size {
            return Err(Error::InvalidInput(format!(
                "range start {start} exceeds object length {size}"
            )));
        }
        let end = usize::try_from(range.end).unwrap_or(usize::MAX).min(size);
        if end < start {
            return Err(Error::InvalidInput(format!(
                "range end {end} is before start {start}"
            )));
        }
        self.store
            .get_range(&ObjectStorePath::from(path), start..end)
            .await
            .map_err(map_object_store_error)
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let location = ObjectStorePath::from(path);
        let options = match &precondition {
            WritePrecondition::DoesNotExist => PutOptions::from(PutMode::Create),
            WritePrecondition::MatchesVersion(token) => PutOptions::from(PutMode::Update(
                VersionToken::decode(token).to_update_version(),
            )),
            WritePrecondition::None => PutOptions::default(),
        };
        match self.store.put_opts(&location, data.into(), options).await {
            Ok(result) => Ok(WriteResult::Success {
                version: VersionToken::from_parts(result.e_tag, result.version).encode(),
            }),
            Err(error) if is_write_precondition_failure(&precondition, &error) => {
                let current_version = self
                    .head(path)
                    .await?
                    .map_or_else(String::new, |meta| meta.version);
                Ok(WriteResult::PreconditionFailed { current_version })
            }
            Err(error) => Err(map_object_store_error(error)),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        match self.store.delete(&ObjectStorePath::from(path)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(map_object_store_error(error)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.store
            .list(Some(&ObjectStorePath::from(prefix)))
            .try_collect::<Vec<_>>()
            .await
            .map(|metas| metas.into_iter().map(object_store_meta_to_meta).collect())
            .map_err(map_object_store_error)
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        validate_page_cursor(prefix, start_after)?;
        if limit == 0 {
            return Ok(ListPage {
                objects: Vec::new(),
                next_start_after: None,
            });
        }
        if !self.ordered_listing {
            return Err(Error::storage(
                "bounded ordered listing is unsupported for this object-store adapter",
            ));
        }

        let prefix_path = ObjectStorePath::from(prefix);
        let stream = start_after.map_or_else(
            || self.store.list(Some(&prefix_path)),
            |start_after| {
                self.store
                    .list_with_offset(Some(&prefix_path), &ObjectStorePath::from(start_after))
            },
        );
        let objects = stream
            .take(limit)
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_object_store_error)?
            .into_iter()
            .map(object_store_meta_to_meta)
            .collect::<Vec<_>>();
        validate_ordered_page(prefix, start_after, limit, &objects)?;
        let next_start_after = next_start_after(&objects, limit);
        Ok(ListPage {
            objects,
            next_start_after,
        })
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        match self.store.head(&ObjectStorePath::from(path)).await {
            Ok(meta) => Ok(Some(object_store_meta_to_meta(meta))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(map_object_store_error(error)),
        }
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        let signer = self.signer.as_ref().ok_or_else(|| {
            Error::storage("signed URLs are not supported by this storage backend")
        })?;
        signer
            .signed_url(Method::GET, &ObjectStorePath::from(path), expiry)
            .await
            .map(|url| url.to_string())
            .map_err(map_object_store_error)
    }
}
