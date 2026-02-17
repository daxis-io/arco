use arco_core::ApiPaths;

pub const MANIFEST_PREFIX: &str = ApiPaths::MANIFEST_PREFIX;
pub const MANIFEST_IDEMPOTENCY_PREFIX: &str = ApiPaths::MANIFEST_IDEMPOTENCY_PREFIX;
pub const MANIFEST_LATEST_INDEX_PATH: &str = ApiPaths::MANIFEST_LATEST_INDEX_PATH;

pub fn manifest_path(manifest_id: &str) -> String {
    ApiPaths::manifest_path(manifest_id)
}

pub fn manifest_idempotency_path(idempotency_key: &str) -> String {
    ApiPaths::manifest_idempotency_path(idempotency_key)
}

pub fn backfill_idempotency_path(idempotency_key: &str) -> String {
    ApiPaths::backfill_idempotency_path(idempotency_key)
}
