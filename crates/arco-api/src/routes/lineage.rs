//! Lineage API routes.
//!
//! Provides operations for tracking data lineage (edges between tables).
//!
//! ## Routes
//!
//! - `POST /lineage/edges` - Add lineage edge(s)
//! - `GET  /lineage/{table_id}` - Get lineage for a table
//!
//! ## Idempotency
//!
//! Edge ids are derived deterministically from edge content
//! (`source_id`, `target_id`, `edge_type`, `run_id`), and the lineage fold
//! dedupes by id with first-write-wins, so duplicate POSTs of the same edge
//! converge to a single projected row (ADR-042 rule 10;
//! `docs/plans/2026-07-30-lineage-l0-schema-plan.md`). `created_at` records
//! the first accepted observation.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::error::ApiErrorBody;
use crate::server::AppState;
use arco_catalog::Tier1Compactor;

/// Request to add lineage edge(s).
#[derive(Debug, Deserialize, ToSchema)]
pub struct AddEdgesRequest {
    /// List of edges to add.
    #[schema(min_items = 1, max_items = 1000)]
    pub edges: Vec<EdgeDefinition>,
}

/// Edge definition for lineage.
///
/// The `maxLength` bounds published here are the same bounds
/// `validate_edges` enforces server-side; they are advertised so clients see
/// the contract rather than discovering it through a 400.
#[derive(Debug, Deserialize, ToSchema)]
pub struct EdgeDefinition {
    /// Source table ID (upstream).
    #[schema(max_length = 256)]
    pub source_id: String,
    /// Target table ID (downstream).
    #[schema(max_length = 256)]
    pub target_id: String,
    /// Edge type (e.g., `derives_from`, `copies`, `transforms`).
    #[serde(default = "default_edge_type")]
    #[schema(max_length = 64)]
    pub edge_type: String,
    /// Optional run ID that created this edge.
    #[schema(max_length = 256)]
    pub run_id: Option<String>,
}

// `utoipa`'s schema attributes only accept literals, so keep the advertised
// contract and the enforced bounds from drifting apart at compile time.
const _: () = {
    assert!(MAX_EDGES_PER_REQUEST == 1_000);
    assert!(MAX_ENTITY_ID_LEN == 256);
    assert!(MAX_EDGE_TYPE_LEN == 64);
    assert!(MAX_RUN_ID_LEN == 256);
};

fn default_edge_type() -> String {
    "derives_from".to_string()
}

/// Maximum number of edges accepted in a single request.
const MAX_EDGES_PER_REQUEST: usize = 1_000;
/// Maximum length for source/target entity ids.
const MAX_ENTITY_ID_LEN: usize = 256;
/// Maximum length for an edge type.
const MAX_EDGE_TYPE_LEN: usize = 64;
/// Maximum length for a run id.
const MAX_RUN_ID_LEN: usize = 256;

/// Validates an add-edges request before any storage work.
fn validate_edges(edges: &[EdgeDefinition]) -> Result<(), ApiError> {
    if edges.is_empty() {
        return Err(ApiError::bad_request("edges must not be empty"));
    }
    if edges.len() > MAX_EDGES_PER_REQUEST {
        return Err(ApiError::bad_request(format!(
            "edge count {} exceeds max {MAX_EDGES_PER_REQUEST}",
            edges.len()
        )));
    }
    for (index, edge) in edges.iter().enumerate() {
        validate_edge_field(index, "source_id", &edge.source_id, MAX_ENTITY_ID_LEN)?;
        validate_edge_field(index, "target_id", &edge.target_id, MAX_ENTITY_ID_LEN)?;
        validate_edge_field(index, "edge_type", &edge.edge_type, MAX_EDGE_TYPE_LEN)?;
        if let Some(run_id) = edge.run_id.as_deref() {
            validate_edge_field(index, "run_id", run_id, MAX_RUN_ID_LEN)?;
        }
    }
    Ok(())
}

fn validate_edge_field(
    index: usize,
    field: &str,
    value: &str,
    max_len: usize,
) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(format!(
            "edges[{index}].{field} must not be empty"
        )));
    }
    if value.len() > max_len {
        return Err(ApiError::bad_request(format!(
            "edges[{index}].{field} exceeds max length ({max_len} bytes)"
        )));
    }
    // Control characters (NUL in particular) have no legitimate place in an
    // entity id, edge type, or run id, and admitting them would let a client
    // smuggle the edge-id encoding's separators into field content. The id
    // derivation is length-prefixed and therefore unambiguous on its own; this
    // is a second, independent barrier, not the thing the identity relies on.
    if let Some(bad) = value.chars().find(|c| c.is_control()) {
        return Err(ApiError::bad_request(format!(
            "edges[{index}].{field} must not contain control characters (found U+{:04X})",
            bad as u32
        )));
    }
    Ok(())
}

/// Derives a deterministic content-derived edge id so duplicate POSTs
/// converge at fold time (first-write-wins dedup by id).
///
/// The encoding is injective: every field is absorbed as its byte length (a
/// big-endian `u64`) followed by its bytes, so no field content can shift the
/// boundary between two fields. A separator-only scheme would not be — with a
/// NUL separator, `("a", "b\0c")` and `("a\0b", "c")` hash the same byte
/// string. `run_id` additionally carries a presence tag so `None` and
/// `Some("")` stay distinct.
fn deterministic_edge_id(edge: &EdgeDefinition) -> String {
    use sha2::{Digest, Sha256};

    /// Absorbs one field as `len(u64 big-endian) || bytes`.
    fn absorb(hasher: &mut Sha256, part: &str) {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }

    let mut hasher = Sha256::new();
    hasher.update(b"arco-lineage-edge-v1");
    absorb(&mut hasher, &edge.source_id);
    absorb(&mut hasher, &edge.target_id);
    absorb(&mut hasher, &edge.edge_type);
    match edge.run_id.as_deref() {
        Some(run_id) => {
            hasher.update([1u8]);
            absorb(&mut hasher, run_id);
        }
        None => hasher.update([0u8]),
    }
    hex::encode(hasher.finalize())
}

/// Lineage edge response.
#[derive(Debug, Serialize, ToSchema)]
pub struct EdgeResponse {
    /// Edge ID.
    pub id: String,
    /// Source table ID.
    pub source_id: String,
    /// Target table ID.
    pub target_id: String,
    /// Edge type.
    pub edge_type: String,
    /// Optional run ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
}

/// Add edges response.
#[derive(Debug, Serialize, ToSchema)]
pub struct AddEdgesResponse {
    /// Number of unique edges accepted (duplicates within the request and
    /// across retries converge on the same content-derived edge id).
    pub added: usize,
}

/// Get lineage response.
#[derive(Debug, Serialize, ToSchema)]
pub struct LineageResponse {
    /// Table ID queried.
    pub table_id: String,
    /// Upstream edges (sources that feed into this table).
    pub upstream: Vec<EdgeResponse>,
    /// Downstream edges (tables that depend on this table).
    pub downstream: Vec<EdgeResponse>,
}

/// Creates lineage routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/lineage/edges", post(add_edges))
        .route("/lineage/:table_id", get(get_lineage))
}

/// Add lineage edge(s).
///
/// POST /api/v1/lineage/edges
#[utoipa::path(
    post,
    path = "/api/v1/lineage/edges",
    tag = "lineage",
    request_body = AddEdgesRequest,
    responses(
        (status = 201, description = "Edges added", body = AddEdgesResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn add_edges(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddEdgesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        edge_count = req.edges.len(),
        "Adding lineage edges"
    );

    validate_edges(&req.edges)?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage).with_sync_compactor(compactor);

    // Ensure initialized
    writer
        .initialize()
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    // Convert edges: ids are content-derived so duplicate submissions
    // (within one request or across retries) converge on one edge.
    let now = chrono::Utc::now().timestamp_millis();
    let mut seen_ids = std::collections::HashSet::new();
    let mut edges: Vec<arco_catalog::LineageEdge> = Vec::with_capacity(req.edges.len());
    for e in req.edges {
        let id = deterministic_edge_id(&e);
        if seen_ids.insert(id.clone()) {
            edges.push(arco_catalog::LineageEdge {
                id,
                source_id: e.source_id,
                target_id: e.target_id,
                edge_type: e.edge_type,
                run_id: e.run_id,
                created_at: now,
            });
        }
    }

    let count = edges.len();

    writer
        .add_lineage_edges(edges, options)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok((StatusCode::CREATED, Json(AddEdgesResponse { added: count })))
}

/// Get lineage for a table.
///
/// `GET /api/v1/lineage/{table_id}`
#[utoipa::path(
    get,
    path = "/api/v1/lineage/{table_id}",
    tag = "lineage",
    params(
        ("table_id" = String, Path, description = "Table ID")
    ),
    responses(
        (status = 200, description = "Lineage graph", body = LineageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_lineage(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        table_id = %table_id,
        "Getting lineage"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    // Get lineage graph for the table
    let graph = reader
        .get_lineage(&table_id)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Convert upstream edges
    let upstream = graph
        .upstream
        .into_iter()
        .map(|e| EdgeResponse {
            id: e.id,
            source_id: e.source_id,
            target_id: e.target_id,
            edge_type: e.edge_type,
            run_id: e.run_id,
            created_at: format_timestamp(e.created_at),
        })
        .collect();

    // Convert downstream edges
    let downstream = graph
        .downstream
        .into_iter()
        .map(|e| EdgeResponse {
            id: e.id,
            source_id: e.source_id,
            target_id: e.target_id,
            edge_type: e.edge_type,
            run_id: e.run_id,
            created_at: format_timestamp(e.created_at),
        })
        .collect();

    Ok(Json(LineageResponse {
        table_id,
        upstream,
        downstream,
    }))
}

/// Format a millisecond timestamp as ISO 8601.
fn format_timestamp(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map_or_else(|| millis.to_string(), |dt| dt.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(
        source_id: &str,
        target_id: &str,
        edge_type: &str,
        run_id: Option<&str>,
    ) -> EdgeDefinition {
        EdgeDefinition {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            edge_type: edge_type.to_string(),
            run_id: run_id.map(str::to_string),
        }
    }

    /// The exact separator-injection collision the length-prefixed encoding
    /// exists to defeat: under a NUL-separated scheme both inputs absorb the
    /// byte string `00 61 00 62 00 63 00 <type> 00`, so the fold's
    /// first-write-wins dedup would silently drop the second distinct edge.
    #[test]
    fn deterministic_edge_id_resists_nul_separator_injection() {
        let split_target = edge("a", "b\0c", "derives_from", None);
        let split_source = edge("a\0b", "c", "derives_from", None);
        assert_ne!(
            deterministic_edge_id(&split_target),
            deterministic_edge_id(&split_source)
        );
    }

    /// Validation is the second barrier: the same two edges never reach the
    /// hash, because control characters are rejected up front.
    #[test]
    fn validation_rejects_control_characters_in_every_field() {
        for candidate in [
            edge("a", "b\0c", "derives_from", None),
            edge("a\0b", "c", "derives_from", None),
            edge("s", "t", "deri\0ves", None),
            edge("s", "t", "derives_from", Some("run\u{1}")),
            edge("s", "t", "derives_from", Some("run\u{7f}1")),
            edge("s\n", "t", "derives_from", None),
        ] {
            assert!(
                validate_edges(std::slice::from_ref(&candidate)).is_err(),
                "expected rejection for {candidate:?}"
            );
        }
    }

    /// Property-style: over a corpus of adversarial field contents (embedded
    /// separators, length-prefix look-alikes, empty strings, unicode), every
    /// distinct edge tuple must map to a distinct id.
    #[test]
    fn deterministic_edge_id_is_injective_over_adversarial_fields() {
        let values = [
            "",
            "a",
            "b",
            "a\0b",
            "\0a",
            "a\0",
            "\0",
            "\0\0",
            "ab",
            "a\u{1}b",
            "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{1}",
            "aa\0",
            "\0aa",
            "é",
            "e\u{301}",
        ];

        let mut seen: std::collections::HashMap<String, EdgeDefinition> =
            std::collections::HashMap::new();
        for source in values {
            for target in values {
                for run_id in [None, Some(""), Some("a"), Some("a\0b")] {
                    let candidate = edge(source, target, "derives_from", run_id);
                    let id = deterministic_edge_id(&candidate);
                    if let Some(previous) = seen.insert(id.clone(), candidate) {
                        let clash = &seen[&id];
                        panic!("edge id collision between {previous:?} and {clash:?}");
                    }
                }
            }
        }
    }

    /// An absent `run_id` and an empty one are different observations.
    #[test]
    fn deterministic_edge_id_distinguishes_absent_and_empty_run_id() {
        assert_ne!(
            deterministic_edge_id(&edge("s", "t", "derives_from", None)),
            deterministic_edge_id(&edge("s", "t", "derives_from", Some("")))
        );
    }

    /// Identity is stable across calls for identical content — the property
    /// the fold's first-write-wins dedup depends on.
    #[test]
    fn deterministic_edge_id_is_stable_for_identical_content() {
        let a = edge("src", "tgt", "copies", Some("run-1"));
        let b = edge("src", "tgt", "copies", Some("run-1"));
        assert_eq!(deterministic_edge_id(&a), deterministic_edge_id(&b));
    }
}
