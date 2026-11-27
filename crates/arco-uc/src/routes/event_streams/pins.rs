//! Durable input identities for authorized Event Stream reads.

use super::{
    CredentialOperation, DeltaRoot, Extension, Json, JsonRejection, Path, Privilege, Response,
    ScopedStorage, State, StreamBinding, StreamError, StreamErrorBody, UnityCatalogError,
    UnityCatalogRequestContext, UnityCatalogResult, UnityCatalogState, authorize, bad_request,
    binding_for, json_response, published_head, revalidate_observed_watermark, scoped_storage,
    unavailable,
};
use arco_core::canonical_json::to_canonical_bytes;
use arco_core::storage::{StorageBackend as _, WritePrecondition, WriteResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreatePin {
    published_version: Option<i64>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct StreamPin {
    pin_id: String,
    published_version: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PinRecord {
    stream_sha256: String,
    published_version: i64,
}

const MAX_PIN_BYTES: u64 = 256;

#[utoipa::path(
    post, path = "/api/v1/event-streams/{streamId}/pins", tag = "event-streams",
    params(("streamId" = String, Path, description = "Approved Workspace stream ID")),
    request_body = CreatePin,
    responses(
        (status = 200, description = "Durable pin for one finalized stream version", body = StreamPin),
        (status = 400, description = "Requested version is invalid or unpublished", body = StreamErrorBody),
        (status = 401, description = "JWT authentication required", body = StreamErrorBody),
        (status = 403, description = "Missing stream binding or table SELECT grant", body = StreamErrorBody),
        (status = 409, description = "Stream or pin identity conflicts", body = StreamErrorBody),
        (status = 429, description = "Tenant rate limit exceeded", body = StreamErrorBody),
        (status = 503, description = "Event stream is unavailable", body = StreamErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub(super) async fn create(
    Path(stream_id): Path<String>,
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    request: Result<Json<CreatePin>, JsonRejection>,
) -> Result<Response, StreamError> {
    let Json(request) = request.map_err(|_| bad_request("event_pin_invalid"))?;
    let binding = binding_for(&state, &ctx, &stream_id)?;
    let (root, observed) = authorize(
        &state,
        &ctx,
        binding,
        Privilege::Select,
        CredentialOperation::Read,
    )
    .await?;
    let storage = scoped_storage(&state, &ctx)?;
    let (_, head) = published_head(&storage, &root.paths, binding).await?;
    let published_version = request.published_version.unwrap_or(head);
    if !(0..=head).contains(&published_version) {
        return Err(bad_request("event_pin_invalid").into());
    }
    let record = PinRecord {
        stream_sha256: stream_digest(binding, &root)?,
        published_version,
    };
    let bytes = to_canonical_bytes(&record).map_err(|_| unavailable())?;
    let pin_id = digest(&bytes);
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    let result = storage
        .put_raw(
            &pin_path(&pin_id),
            Bytes::from(bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|_| unavailable())?;
    if !matches!(result, WriteResult::Success { .. }) {
        load_pin(&storage, binding, &root, &pin_id).await?;
    }
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    json_response(&StreamPin {
        pin_id,
        published_version,
    })
    .map_err(Into::into)
}

pub(in crate::routes) async fn load_pin(
    storage: &ScopedStorage,
    binding: &StreamBinding,
    root: &DeltaRoot,
    pin_id: &str,
) -> UnityCatalogResult<i64> {
    if pin_id.len() != 64
        || !pin_id
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    {
        return Err(bad_request("event_pin_invalid"));
    }
    let path = pin_path(pin_id);
    let meta = storage
        .head_raw(&path)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: "event_pin_not_found".to_string(),
        })?;
    if !(1..=MAX_PIN_BYTES).contains(&meta.size) {
        return Err(pin_conflict());
    }
    let bytes = storage
        .get_range(&path, 0..meta.size)
        .await
        .map_err(|_| unavailable())?;
    if digest(&bytes) != pin_id {
        return Err(pin_conflict());
    }
    let record: PinRecord = serde_json::from_slice(&bytes).map_err(|_| pin_conflict())?;
    if record.stream_sha256 != stream_digest(binding, root)? || record.published_version < 0 {
        return Err(pin_conflict());
    }
    Ok(record.published_version)
}

fn pin_path(pin_id: &str) -> String {
    format!("event-streams/v1/pins/{pin_id}.json")
}

fn stream_digest(binding: &StreamBinding, root: &DeltaRoot) -> UnityCatalogResult<String> {
    Ok(digest(
        &to_canonical_bytes(&(binding, &root.uri)).map_err(|_| unavailable())?,
    ))
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn pin_conflict() -> UnityCatalogError {
    UnityCatalogError::Conflict {
        message: "event_pin_identity_mismatch".to_string(),
    }
}
