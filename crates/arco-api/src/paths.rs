use base64::Engine;

pub const MANIFEST_PREFIX: &str = "manifests/";
pub const MANIFEST_IDEMPOTENCY_PREFIX: &str = "manifests/idempotency/";

pub fn manifest_path(manifest_id: &str) -> String {
    format!("{MANIFEST_PREFIX}{manifest_id}.json")
}

pub fn manifest_idempotency_path(idempotency_key: &str) -> String {
    let encoded =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(idempotency_key.as_bytes());
    format!("{MANIFEST_IDEMPOTENCY_PREFIX}{encoded}.json")
}
