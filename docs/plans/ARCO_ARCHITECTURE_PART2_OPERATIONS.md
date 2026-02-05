# Arco Architecture Design Document
## Part 2: Operations & Integration

**Version:** 1.0
**Status:** Draft
**Last Updated:** January 2025

---

## Table of Contents

1. [Deployment Architecture](#1-deployment-architecture)
2. [Cloud Function: Compactor](#2-cloud-function-compactor)
3. [REST API Design](#3-rest-api-design)
4. [SDK Design](#4-sdk-design)
5. [Servo Integration](#5-servo-integration)
6. [Error Handling](#6-error-handling)
7. [Observability](#7-observability)
8. [Performance Optimization](#8-performance-optimization)
9. [Testing Strategy](#9-testing-strategy)
10. [Migration & Upgrades](#10-migration--upgrades)

---

## 1. Deployment Architecture

### 1.1 GCP Deployment Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GOOGLE CLOUD PLATFORM                            │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        CLOUD RUN                                    │ │
│  │                                                                     │ │
│  │   ┌─────────────────┐      ┌─────────────────┐                     │ │
│  │   │   Arco API      │      │   Daxis API     │                     │ │
│  │   │   Service       │      │   Service       │                     │ │
│  │   │                 │      │                 │                     │ │
│  │   │ • REST endpoints│      │ • User-facing   │                     │ │
│  │   │ • Tier 1 writes │      │ • Uses Arco SDK │                     │ │
│  │   │ • DataFusion    │      │                 │                     │ │
│  │   └────────┬────────┘      └────────┬────────┘                     │ │
│  │            │                        │                               │ │
│  └────────────┼────────────────────────┼───────────────────────────────┘ │
│               │                        │                                  │
│  ┌────────────┼────────────────────────┼───────────────────────────────┐ │
│  │            │    CLOUD FUNCTIONS     │                               │ │
│  │            │                        │                               │ │
│  │   ┌────────┴────────┐      ┌───────┴────────┐                      │ │
│  │   │   Compactor     │      │  Event Handler │                      │ │
│  │   │   Function      │      │  Function      │                      │ │
│  │   │                 │      │                │                      │ │
│  │   │ • Triggered by  │      │ • Webhook      │                      │ │
│  │   │   Pub/Sub       │      │   receivers    │                      │ │
│  │   │ • Processes     │      │                │                      │ │
│  │   │   event log     │      │                │                      │ │
│  │   └────────┬────────┘      └────────────────┘                      │ │
│  │            │                                                        │ │
│  └────────────┼────────────────────────────────────────────────────────┘ │
│               │                                                          │
│  ┌────────────┼────────────────────────────────────────────────────────┐ │
│  │            │              PUB/SUB                                   │ │
│  │            │                                                        │ │
│  │   ┌────────┴────────┐      ┌─────────────────┐                     │ │
│  │   │  catalog-events │◄─────│  GCS Triggers   │                     │ │
│  │   │  topic          │      │  (new events)   │                     │ │
│  │   └─────────────────┘      └─────────────────┘                     │ │
│  │                                                                     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                     CLOUD STORAGE                                   │ │
│  │                                                                     │ │
│  │   gs://arco-catalog-{env}/                                         │ │
│  │   ├── tenant=acme/                                                 │ │
│  │   │   └── _catalog/                                                │ │
│  │   │       ├── core/                                                │ │
│  │   │       ├── events/                                              │ │
│  │   │       └── operational/                                         │ │
│  │   ├── tenant=globex/                                               │ │
│  │   │   └── _catalog/                                                │ │
│  │   └── ...                                                          │ │
│  │                                                                     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Environment Configuration

```yaml
# terraform/environments/prod.tfvars

project_id       = "daxis-prod"
region           = "us-central1"
environment      = "prod"

# Storage
catalog_bucket   = "arco-catalog-prod"
storage_class    = "STANDARD"

# Cloud Run - Arco API
arco_api_config = {
  min_instances    = 1
  max_instances    = 10
  cpu              = "2"
  memory           = "4Gi"
  timeout_seconds  = 300
  concurrency      = 80
}

# Cloud Functions - Compactor
compactor_config = {
  min_instances    = 0
  max_instances    = 5
  memory           = "1Gi"
  timeout_seconds  = 540
  trigger_type     = "pubsub"  # or "scheduler"
}

# Pub/Sub
pubsub_config = {
  ack_deadline_seconds  = 600
  retention_duration    = "604800s"  # 7 days
}
```

### 1.3 Terraform Resources

```hcl
# terraform/modules/arco/main.tf

# Cloud Storage Bucket
resource "google_storage_bucket" "catalog" {
  name          = var.catalog_bucket
  location      = var.region
  storage_class = var.storage_class

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
      matches_prefix = ["tenant=*/catalog/events/"]
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 5
      matches_prefix = ["tenant=*/catalog/core/snapshots/"]
    }
    action {
      type = "Delete"
    }
  }
}

# GCS Notification for new events
resource "google_storage_notification" "events" {
  bucket         = google_storage_bucket.catalog.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.catalog_events.id
  event_types    = ["OBJECT_FINALIZE"]

  object_name_prefix = "tenant="

  custom_attributes = {
    path_filter = "*/catalog/events/*"
  }

  depends_on = [google_pubsub_topic_iam_member.gcs_publisher]
}

# Pub/Sub Topic
resource "google_pubsub_topic" "catalog_events" {
  name = "arco-catalog-events-${var.environment}"
}

resource "google_pubsub_subscription" "compactor" {
  name  = "arco-compactor-${var.environment}"
  topic = google_pubsub_topic.catalog_events.id

  ack_deadline_seconds = var.pubsub_config.ack_deadline_seconds

  message_retention_duration = var.pubsub_config.retention_duration

  push_config {
    push_endpoint = google_cloudfunctions2_function.compactor.service_config[0].uri
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Cloud Run - Arco API
resource "google_cloud_run_v2_service" "arco_api" {
  name     = "arco-api-${var.environment}"
  location = var.region

  template {
    scaling {
      min_instance_count = var.arco_api_config.min_instances
      max_instance_count = var.arco_api_config.max_instances
    }

    containers {
      image = var.arco_api_image

      resources {
        limits = {
          cpu    = var.arco_api_config.cpu
          memory = var.arco_api_config.memory
        }
      }

      env {
        name  = "CATALOG_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
    }

    service_account = google_service_account.arco_api.email
  }
}

# Cloud Function - Compactor
resource "google_cloudfunctions2_function" "compactor" {
  name     = "arco-compactor-${var.environment}"
  location = var.region

  build_config {
    runtime     = "provided"  # Custom Rust runtime
    entry_point = "compact"

    source {
      storage_source {
        bucket = var.deploy_bucket
        object = "arco-compactor-${var.version}.zip"
      }
    }
  }

  service_config {
    min_instance_count    = var.compactor_config.min_instances
    max_instance_count    = var.compactor_config.max_instances
    available_memory      = var.compactor_config.memory
    timeout_seconds       = var.compactor_config.timeout_seconds
    service_account_email = google_service_account.compactor.email

    environment_variables = {
      CATALOG_BUCKET = google_storage_bucket.catalog.name
      RUST_LOG       = "info"
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.catalog_events.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

# Service Accounts
resource "google_service_account" "arco_api" {
  account_id   = "arco-api-${var.environment}"
  display_name = "Arco API Service Account"
}

resource "google_service_account" "compactor" {
  account_id   = "arco-compactor-${var.environment}"
  display_name = "Arco Compactor Service Account"
}

# IAM
resource "google_storage_bucket_iam_member" "arco_api" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.arco_api.email}"
}

resource "google_storage_bucket_iam_member" "compactor" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.compactor.email}"
}
```

### 1.4 Container Configuration

```dockerfile
# Dockerfile.arco-api

FROM rust:1.75-slim as builder

WORKDIR /app

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build release
RUN cargo build --release --package arco-api

# Runtime image
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/arco-api /usr/local/bin/

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/arco-api"]
```

```dockerfile
# Dockerfile.compactor

FROM rust:1.75-slim as builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN cargo build --release --package arco-compactor

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/arco-compactor /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/arco-compactor"]
```

---

## 2. Cloud Function: Compactor

### 2.1 Entry Point

```rust
// arco-compactor/src/main.rs

use arco_catalog::compactor::EventCompactor;
use arco_core::storage::GcsBackend;
use cloud_functions_framework::{serve, Function};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct PubSubMessage {
    message: PubSubMessageData,
}

#[derive(Debug, Deserialize)]
struct PubSubMessageData {
    data: String,  // Base64 encoded
    attributes: PubSubAttributes,
}

#[derive(Debug, Deserialize)]
struct PubSubAttributes {
    #[serde(rename = "bucketId")]
    bucket_id: String,
    #[serde(rename = "objectId")]
    object_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .json()
        .init();

    let bucket = std::env::var("CATALOG_BUCKET")?;

    serve(CompactorFunction { bucket }).await?;
    Ok(())
}

struct CompactorFunction {
    bucket: String,
}

#[async_trait::async_trait]
impl Function for CompactorFunction {
    async fn call(&self, request: http::Request<Vec<u8>>) -> http::Response<Vec<u8>> {
        let result = self.handle(request).await;

        match result {
            Ok(_) => http::Response::builder()
                .status(200)
                .body(b"OK".to_vec())
                .unwrap(),
            Err(e) => {
                tracing::error!("Compaction failed: {}", e);
                http::Response::builder()
                    .status(500)
                    .body(format!("Error: {}", e).into_bytes())
                    .unwrap()
            }
        }
    }
}

impl CompactorFunction {
    async fn handle(&self, request: http::Request<Vec<u8>>) -> Result<(), CompactorError> {
        // Parse Pub/Sub message
        let body = request.body();
        let message: PubSubMessage = serde_json::from_slice(body)?;

        let object_id = &message.message.attributes.object_id;

        // Extract tenant from path: tenant=acme/_catalog/events/...
        let tenant_id = self.extract_tenant(object_id)?;

        tracing::info!(
            tenant = %tenant_id,
            object = %object_id,
            "Processing event notification"
        );

        // Check if this is an event file
        if !object_id.contains("/_catalog/events/") {
            tracing::debug!("Ignoring non-event file: {}", object_id);
            return Ok(());
        }

        // Debounce: only process if enough events accumulated or time elapsed
        if !self.should_compact(&tenant_id).await? {
            tracing::debug!("Skipping compaction, debounce active");
            return Ok(());
        }

        // Run compaction
        let backend = Arc::new(GcsBackend::new(&self.bucket, "").await?);
        let storage = Arc::new(TenantStorage::new(backend, &tenant_id));
        let compactor = EventCompactor::new(storage);

        let result = compactor.compact().await?;

        tracing::info!(
            tenant = %tenant_id,
            events_processed = result.events_processed,
            new_version = result.new_snapshot_version,
            "Compaction complete"
        );

        Ok(())
    }

    fn extract_tenant(&self, object_id: &str) -> Result<String, CompactorError> {
        // Parse: tenant=acme/_catalog/...
        let prefix = "tenant=";
        if let Some(start) = object_id.find(prefix) {
            let rest = &object_id[start + prefix.len()..];
            if let Some(end) = rest.find('/') {
                return Ok(rest[..end].to_string());
            }
        }
        Err(CompactorError::InvalidPath(object_id.to_string()))
    }

    async fn should_compact(&self, tenant_id: &str) -> Result<bool, CompactorError> {
        // Simple debounce: use a distributed lock with short TTL
        // If lock exists, skip (another invocation is handling or just handled)
        // This prevents duplicate compactions from multiple event triggers

        // For now, always compact (proper implementation would use Redis or similar)
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error)]
enum CompactorError {
    #[error("Invalid object path: {0}")]
    InvalidPath(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] arco_core::storage::StorageError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] arco_catalog::compactor::CompactionError),
}
```

### 2.2 Debouncing Strategy

```rust
// arco-compactor/src/debounce.rs

use std::time::Duration;
use redis::AsyncCommands;

const DEBOUNCE_KEY_PREFIX: &str = "arco:compactor:debounce:";
const DEBOUNCE_DURATION: Duration = Duration::from_secs(5);
const MIN_EVENTS_FOR_IMMEDIATE: usize = 100;

pub struct CompactionDebouncer {
    redis: redis::Client,
}

impl CompactionDebouncer {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let redis = redis::Client::open(redis_url)?;
        Ok(Self { redis })
    }

    /// Returns true if compaction should proceed
    pub async fn should_compact(&self, tenant_id: &str, event_count: usize) -> Result<bool, redis::RedisError> {
        let key = format!("{}{}", DEBOUNCE_KEY_PREFIX, tenant_id);

        let mut conn = self.redis.get_async_connection().await?;

        // If many events pending, skip debounce
        if event_count >= MIN_EVENTS_FOR_IMMEDIATE {
            // Clear debounce and proceed
            let _: () = conn.del(&key).await?;
            return Ok(true);
        }

        // Try to set debounce key (NX = only if not exists)
        let set: bool = conn
            .set_ex_nx(&key, "1", DEBOUNCE_DURATION.as_secs())
            .await?;

        if set {
            // We set the key, so we're the first - wait for debounce then compact
            tokio::time::sleep(DEBOUNCE_DURATION).await;
            Ok(true)
        } else {
            // Key exists, another invocation will handle it
            Ok(false)
        }
    }
}
```

### 2.3 Scheduled Compaction (Alternative)

```rust
// Alternative: Timer-based compaction instead of event-driven

// Cloud Scheduler job (every 30 seconds)
// POST /compact-all

async fn compact_all(storage_bucket: &str) -> Result<(), CompactorError> {
    let backend = Arc::new(GcsBackend::new(storage_bucket, "").await?);

    // List all tenants
    let tenants = list_tenants(&backend).await?;

    // Compact each tenant (could parallelize)
    for tenant_id in tenants {
        let storage = Arc::new(TenantStorage::new(backend.clone(), &tenant_id));
        let compactor = EventCompactor::new(storage);

        match compactor.compact().await {
            Ok(result) => {
                if result.events_processed > 0 {
                    tracing::info!(
                        tenant = %tenant_id,
                        events = result.events_processed,
                        "Compacted tenant"
                    );
                }
            }
            Err(e) => {
                tracing::error!(tenant = %tenant_id, error = %e, "Compaction failed");
            }
        }
    }

    Ok(())
}

async fn list_tenants(backend: &GcsBackend) -> Result<Vec<String>, CompactorError> {
    let objects = backend.list("tenant=").await?;

    let tenants: HashSet<String> = objects
        .iter()
        .filter_map(|obj| {
            let path = &obj.path;
            if let Some(start) = path.find("tenant=") {
                let rest = &path[start + 7..];
                if let Some(end) = rest.find('/') {
                    return Some(rest[..end].to_string());
                }
            }
            None
        })
        .collect();

    Ok(tenants.into_iter().collect())
}
```

---

## 2.5 Search Indexer

Arco uses a **progressive search architecture**:

### Phase 1: Token-Based Search (MVP)

```rust
// arco-catalog/src/search/indexer.rs

/// Search posting entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchPosting {
    pub token: String,
    pub token_norm: String,  // lowercase, normalized
    pub doc_type: String,    // "table" | "column" | "namespace"
    pub doc_id: String,
    pub field: String,       // "name" | "description" | "tag"
    pub score: f32,          // precomputed relevance weight
}

/// Build search postings from catalog snapshot
pub async fn build_search_index(
    tables: &[Table],
    columns: &[Column],
) -> Vec<SearchPosting> {
    let mut postings = Vec::new();

    for table in tables {
        // Tokenize table name
        for token in tokenize(&table.name) {
            postings.push(SearchPosting {
                token: token.clone(),
                token_norm: token.to_lowercase(),
                doc_type: "table".to_string(),
                doc_id: table.id.0.to_string(),
                field: "name".to_string(),
                score: 1.0,  // name matches weighted highest
            });
        }

        // Tokenize description
        if let Some(desc) = &table.description {
            for token in tokenize(desc) {
                postings.push(SearchPosting {
                    token: token.clone(),
                    token_norm: token.to_lowercase(),
                    doc_type: "table".to_string(),
                    doc_id: table.id.0.to_string(),
                    field: "description".to_string(),
                    score: 0.5,
                });
            }
        }

        // Index tags
        for tag in &table.tags {
            postings.push(SearchPosting {
                token: tag.clone(),
                token_norm: tag.to_lowercase(),
                doc_type: "table".to_string(),
                doc_id: table.id.0.to_string(),
                field: "tag".to_string(),
                score: 0.8,
            });
        }
    }

    postings
}

/// Tokenize text for indexing
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty() && s.len() >= 2)
        .map(|s| s.to_string())
        .collect()
}
```

### Phase 2: Trigram Index (Fuzzy Matching)

```rust
/// Trigram entry for fuzzy matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchTrigram {
    pub trigram: String,   // e.g., "cus", "ust", "sto"
    pub token: String,
    pub token_norm: String,
}

/// Build trigram index
pub fn build_trigram_index(tokens: &[String]) -> Vec<SearchTrigram> {
    let mut trigrams = Vec::new();

    for token in tokens {
        let padded = format!("  {}  ", token.to_lowercase());
        for i in 0..padded.len().saturating_sub(2) {
            let trigram = &padded[i..i+3];
            trigrams.push(SearchTrigram {
                trigram: trigram.to_string(),
                token: token.clone(),
                token_norm: token.to_lowercase(),
            });
        }
    }

    trigrams
}

/// Fuzzy search query
pub fn fuzzy_search(query: &str, trigram_index: &[SearchTrigram]) -> Vec<(String, f32)> {
    let query_trigrams: HashSet<_> = {
        let padded = format!("  {}  ", query.to_lowercase());
        (0..padded.len().saturating_sub(2))
            .map(|i| padded[i..i+3].to_string())
            .collect()
    };

    // Count trigram matches per token
    let mut scores: HashMap<String, u32> = HashMap::new();
    for t in trigram_index {
        if query_trigrams.contains(&t.trigram) {
            *scores.entry(t.token_norm.clone()).or_default() += 1;
        }
    }

    // Score by Jaccard similarity
    let mut results: Vec<_> = scores
        .into_iter()
        .map(|(token, matches)| {
            let token_trigram_count = (token.len() + 4).saturating_sub(2) as f32;
            let jaccard = matches as f32 / (query_trigrams.len() as f32 + token_trigram_count - matches as f32);
            (token, jaccard)
        })
        .filter(|(_, score)| *score > 0.3)  // threshold
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    results
}
```

### Partitioning Strategy

Search postings are partitioned by `token_bucket` for efficient pruning:

```rust
/// Compute token bucket for partitioning
pub fn token_bucket(token: &str, num_buckets: u32) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    token.to_lowercase().hash(&mut hasher);
    (hasher.finish() % num_buckets as u64) as u32
}

// Search query only needs to read buckets for query tokens
// e.g., searching "customer" reads bucket 42, not all 64 buckets
```

---

## 3. REST API Design

### 3.1 API Structure

```
/api/v1
├── /namespaces
│   ├── GET     /                    # List namespaces
│   ├── POST    /                    # Create namespace
│   ├── GET     /{namespace}         # Get namespace
│   └── DELETE  /{namespace}         # Delete namespace
│
├── /tables
│   ├── GET     /                    # List all tables
│   ├── POST    /                    # Register table
│   ├── GET     /{namespace}/{name}  # Get table
│   ├── PATCH   /{namespace}/{name}  # Update table
│   ├── DELETE  /{namespace}/{name}  # Drop table
│   └── GET     /{namespace}/{name}/columns  # Get columns
│
├── /lineage
│   ├── GET     /upstream/{table_id}   # Get upstream lineage
│   ├── GET     /downstream/{table_id} # Get downstream lineage
│   └── POST    /edges                  # Add lineage edge
│
├── /partitions
│   ├── GET     /table/{table_id}      # List partitions
│   └── GET     /{partition_id}        # Get partition
│
├── /executions
│   ├── GET     /                      # List executions
│   ├── GET     /{execution_id}        # Get execution
│   └── GET     /{execution_id}/tasks  # Get tasks
│
├── /quality
│   └── GET     /table/{table_id}      # Get quality results
│
├── /search
│   └── GET     /tables?q={query}      # Search tables
│
└── /browser
    └── GET     /urls                  # Get signed URLs for DuckDB-WASM
```

### 3.2 Axum Router Implementation

```rust
// arco-api/src/routes/mod.rs

use axum::{
    routing::{get, post, patch, delete},
    Router,
    middleware,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Namespaces
        .route("/api/v1/namespaces", get(handlers::list_namespaces))
        .route("/api/v1/namespaces", post(handlers::create_namespace))
        .route("/api/v1/namespaces/:namespace", get(handlers::get_namespace))
        .route("/api/v1/namespaces/:namespace", delete(handlers::delete_namespace))

        // Tables
        .route("/api/v1/tables", get(handlers::list_tables))
        .route("/api/v1/tables", post(handlers::register_table))
        .route("/api/v1/tables/:namespace/:name", get(handlers::get_table))
        .route("/api/v1/tables/:namespace/:name", patch(handlers::update_table))
        .route("/api/v1/tables/:namespace/:name", delete(handlers::drop_table))
        .route("/api/v1/tables/:namespace/:name/columns", get(handlers::get_columns))

        // Lineage
        .route("/api/v1/lineage/upstream/:table_id", get(handlers::get_upstream_lineage))
        .route("/api/v1/lineage/downstream/:table_id", get(handlers::get_downstream_lineage))
        .route("/api/v1/lineage/edges", post(handlers::add_lineage_edge))

        // Partitions
        .route("/api/v1/partitions/table/:table_id", get(handlers::list_partitions))
        .route("/api/v1/partitions/:partition_id", get(handlers::get_partition))

        // Executions
        .route("/api/v1/executions", get(handlers::list_executions))
        .route("/api/v1/executions/:execution_id", get(handlers::get_execution))
        .route("/api/v1/executions/:execution_id/tasks", get(handlers::get_execution_tasks))

        // Quality
        .route("/api/v1/quality/table/:table_id", get(handlers::get_quality_results))

        // Search
        .route("/api/v1/search/tables", get(handlers::search_tables))

        // Browser access
        .route("/api/v1/browser/urls", get(handlers::get_browser_urls))

        // Middleware
        .layer(middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::authenticate,
        ))
        .layer(middleware::from_fn(middleware::logging::log_request))

        .with_state(state)
}
```

### 3.3 Request/Response Types

```rust
// arco-api/src/handlers/tables.rs

use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct RegisterTableRequest {
    pub namespace: String,
    pub name: String,
    pub location: String,
    pub format: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub columns: Vec<ColumnDef>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub description: Option<String>,
    pub pii_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TableResponse {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub location: String,
    pub format: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub tags: Vec<String>,
    pub pii_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesQuery {
    pub namespace: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableResponse>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

pub async fn list_tables(
    State(state): State<AppState>,
    Query(query): Query<ListTablesQuery>,
) -> Result<Json<ListTablesResponse>, ApiError> {
    let reader = state.catalog_reader();

    let tables = match &query.namespace {
        Some(ns) => reader.list_tables_in_namespace(ns).await?,
        None => reader.list_tables().await?,
    };

    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let total = tables.len() as u64;
    let paged: Vec<_> = tables
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .map(TableResponse::from)
        .collect();

    Ok(Json(ListTablesResponse {
        tables: paged,
        total,
        limit,
        offset,
    }))
}

pub async fn register_table(
    State(state): State<AppState>,
    Json(request): Json<RegisterTableRequest>,
) -> Result<Json<TableResponse>, ApiError> {
    let registry = state.table_registry();

    let format = request.format.parse()
        .map_err(|_| ApiError::BadRequest("Invalid format".to_string()))?;

    let columns: Vec<_> = request.columns.into_iter()
        .map(|c| arco_core::types::ColumnDef {
            name: c.name,
            data_type: c.data_type,
            is_nullable: c.is_nullable,
            description: c.description,
            pii_type: c.pii_type.and_then(|s| s.parse().ok()),
            sensitivity: None,
        })
        .collect();

    let table = registry.register_table(
        &request.namespace,
        &request.name,
        &request.location,
        format,
        columns,
    ).await?;

    Ok(Json(TableResponse::from(table)))
}

pub async fn get_table(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<TableResponse>, ApiError> {
    let reader = state.catalog_reader();

    let table = reader.get_table(&namespace, &name).await?
        .ok_or_else(|| ApiError::NotFound(format!("{}.{}", namespace, name)))?;

    Ok(Json(TableResponse::from(table)))
}
```

### 3.4 Error Handling

```rust
// arco-api/src/error.rs

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Unauthorized,
    Forbidden,
    Internal(String),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: String,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "NOT_FOUND", msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "BAD_REQUEST", msg),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, "CONFLICT", msg),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "Unauthorized".to_string()),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "FORBIDDEN", "Forbidden".to_string()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg),
        };

        let body = ErrorResponse {
            error: ErrorDetail {
                code: code.to_string(),
                message,
            },
        };

        (status, Json(body)).into_response()
    }
}

impl From<arco_catalog::WriteError> for ApiError {
    fn from(err: arco_catalog::WriteError) -> Self {
        match err {
            arco_catalog::WriteError::TableNotFound(name) => ApiError::NotFound(name),
            arco_catalog::WriteError::TableExists(name) => ApiError::Conflict(name),
            arco_catalog::WriteError::Validation(msg) => ApiError::BadRequest(msg),
            _ => ApiError::Internal(err.to_string()),
        }
    }
}
```

### 3.5 Authentication Middleware

```rust
// arco-api/src/middleware/auth.rs

use axum::{
    extract::{Request, State},
    http::header::AUTHORIZATION,
    middleware::Next,
    response::Response,
};

#[derive(Clone)]
pub struct AuthContext {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: Vec<String>,
}

pub async fn authenticate(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Extract token from header
    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(ApiError::Unauthorized)?;

    // Validate JWT and extract claims
    let claims = state.auth_service()
        .validate_token(token)
        .await
        .map_err(|_| ApiError::Unauthorized)?;

    // Build auth context
    let auth_ctx = AuthContext {
        tenant_id: claims.tenant_id,
        user_id: claims.sub,
        roles: claims.roles,
    };

    // Attach to request extensions
    request.extensions_mut().insert(auth_ctx);

    Ok(next.run(request).await)
}

// Extract auth context in handlers
pub async fn some_handler(
    auth: Extension<AuthContext>,
    // ...
) {
    let tenant_id = &auth.tenant_id;
    // Use tenant_id to scope storage operations
}
```

---

## 4. SDK Design

### 4.1 Rust SDK

```rust
// arco-client/src/lib.rs

pub struct ArcoClient {
    base_url: String,
    token: String,
    http: reqwest::Client,
}

impl ArcoClient {
    pub fn new(base_url: &str, token: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            token: token.to_string(),
            http: reqwest::Client::new(),
        }
    }

    // Tables
    pub async fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<Table>, ClientError> {
        let mut url = format!("{}/api/v1/tables", self.base_url);
        if let Some(ns) = namespace {
            url = format!("{}?namespace={}", url, ns);
        }

        let response = self.http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        self.handle_response(response).await
    }

    pub async fn register_table(&self, request: RegisterTableRequest) -> Result<Table, ClientError> {
        let url = format!("{}/api/v1/tables", self.base_url);

        let response = self.http
            .post(&url)
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await?;

        self.handle_response(response).await
    }

    pub async fn get_table(&self, namespace: &str, name: &str) -> Result<Table, ClientError> {
        let url = format!("{}/api/v1/tables/{}/{}", self.base_url, namespace, name);

        let response = self.http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        self.handle_response(response).await
    }

    // Lineage
    pub async fn get_upstream_lineage(&self, table_id: &str) -> Result<Vec<LineageEdge>, ClientError> {
        let url = format!("{}/api/v1/lineage/upstream/{}", self.base_url, table_id);

        let response = self.http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        self.handle_response(response).await
    }

    pub async fn add_lineage_edge(&self, edge: AddLineageRequest) -> Result<LineageEdge, ClientError> {
        let url = format!("{}/api/v1/lineage/edges", self.base_url);

        let response = self.http
            .post(&url)
            .bearer_auth(&self.token)
            .json(&edge)
            .send()
            .await?;

        self.handle_response(response).await
    }

    // Browser URLs
    pub async fn get_browser_urls(&self) -> Result<CatalogUrls, ClientError> {
        let url = format!("{}/api/v1/browser/urls", self.base_url);

        let response = self.http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        self.handle_response(response).await
    }

    async fn handle_response<T: DeserializeOwned>(&self, response: reqwest::Response) -> Result<T, ClientError> {
        let status = response.status();

        if status.is_success() {
            Ok(response.json().await?)
        } else {
            let error: ErrorResponse = response.json().await?;
            Err(ClientError::Api {
                status: status.as_u16(),
                code: error.error.code,
                message: error.error.message,
            })
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error ({status}): [{code}] {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
}
```

### 4.2 TypeScript SDK (for Browser)

```typescript
// packages/arco-client/src/index.ts

export interface ArcoClientConfig {
  baseUrl: string;
  token: string;
}

export interface Table {
  id: string;
  namespace: string;
  name: string;
  location: string;
  format: string;
  description?: string;
  owner?: string;
  createdAt: string;
  updatedAt: string;
  rowCount?: number;
  sizeBytes?: number;
  tags: string[];
  piiColumns: string[];
}

export interface CatalogUrls {
  manifestVersion: number;
  coreSnapshotVersion: number;
  operationalSnapshotVersion: number;
  expiresAt: string;
  urls: {
    tables: string;
    columns: string;
    lineageEdges: string;
    partitions: string;
    qualityResults: string;
    executions: string;
  };
}

export class ArcoClient {
  private baseUrl: string;
  private token: string;

  constructor(config: ArcoClientConfig) {
    this.baseUrl = config.baseUrl;
    this.token = config.token;
  }

  private async fetch<T>(path: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers: {
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!response.ok) {
      const error = await response.json();
      throw new ArcoError(response.status, error.error.code, error.error.message);
    }

    return response.json();
  }

  // Tables
  async listTables(namespace?: string): Promise<Table[]> {
    const query = namespace ? `?namespace=${namespace}` : '';
    const response = await this.fetch<{ tables: Table[] }>(`/api/v1/tables${query}`);
    return response.tables;
  }

  async getTable(namespace: string, name: string): Promise<Table> {
    return this.fetch<Table>(`/api/v1/tables/${namespace}/${name}`);
  }

  // Browser URLs for DuckDB-WASM
  async getCatalogUrls(): Promise<CatalogUrls> {
    return this.fetch<CatalogUrls>('/api/v1/browser/urls');
  }
}

export class ArcoError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string
  ) {
    super(message);
    this.name = 'ArcoError';
  }
}
```

### 4.3 DuckDB-WASM Integration

```typescript
// packages/arco-client/src/duckdb.ts

import * as duckdb from '@duckdb/duckdb-wasm';

export class ArcoDuckDB {
  private db: duckdb.AsyncDuckDB | null = null;
  private conn: duckdb.AsyncDuckDBConnection | null = null;
  private catalogUrls: CatalogUrls | null = null;

  async initialize(catalogUrls: CatalogUrls): Promise<void> {
    this.catalogUrls = catalogUrls;

    // Initialize DuckDB-WASM
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker = new Worker(bundle.mainWorker!);
    const logger = new duckdb.ConsoleLogger();
    this.db = new duckdb.AsyncDuckDB(logger, worker);
    await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    this.conn = await this.db.connect();

    // Install and load httpfs for reading from GCS
    await this.conn.query(`INSTALL httpfs`);
    await this.conn.query(`LOAD httpfs`);
  }

  async listTables(namespace?: string): Promise<Table[]> {
    if (!this.conn || !this.catalogUrls) {
      throw new Error('DuckDB not initialized');
    }

    let query = `
      SELECT * FROM read_parquet('${this.catalogUrls.urls.tables}')
    `;

    if (namespace) {
      query += ` WHERE namespace = '${namespace}'`;
    }

    const result = await this.conn.query(query);
    return this.resultToTables(result);
  }

  async searchTables(searchQuery: string): Promise<Table[]> {
    if (!this.conn || !this.catalogUrls) {
      throw new Error('DuckDB not initialized');
    }

    const pattern = `%${searchQuery.toLowerCase()}%`;

    const query = `
      SELECT * FROM read_parquet('${this.catalogUrls.urls.tables}')
      WHERE LOWER(name) LIKE '${pattern}'
         OR LOWER(description) LIKE '${pattern}'
    `;

    const result = await this.conn.query(query);
    return this.resultToTables(result);
  }

  async getUpstreamLineage(tableId: string): Promise<LineageEdge[]> {
    if (!this.conn || !this.catalogUrls) {
      throw new Error('DuckDB not initialized');
    }

    const query = `
      SELECT * FROM read_parquet('${this.catalogUrls.urls.lineageEdges}')
      WHERE target_table_id = '${tableId}'
    `;

    const result = await this.conn.query(query);
    return this.resultToLineageEdges(result);
  }

  async getPartitions(tableId: string): Promise<Partition[]> {
    if (!this.conn || !this.catalogUrls) {
      throw new Error('DuckDB not initialized');
    }

    const query = `
      SELECT * FROM read_parquet('${this.catalogUrls.urls.partitions}')
      WHERE table_id = '${tableId}'
      ORDER BY created_at DESC
    `;

    const result = await this.conn.query(query);
    return this.resultToPartitions(result);
  }

  async runCustomQuery(sql: string): Promise<any[]> {
    if (!this.conn) {
      throw new Error('DuckDB not initialized');
    }

    const result = await this.conn.query(sql);
    return result.toArray();
  }

  async close(): Promise<void> {
    if (this.conn) {
      await this.conn.close();
    }
    if (this.db) {
      await this.db.terminate();
    }
  }

  private resultToTables(result: duckdb.Table): Table[] {
    // Convert Arrow table to Table objects
    return result.toArray().map((row: any) => ({
      id: row.table_id,
      namespace: row.namespace,
      name: row.name,
      location: row.location,
      format: row.format,
      description: row.description,
      owner: row.owner,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      rowCount: row.row_count,
      sizeBytes: row.size_bytes,
      tags: row.tags || [],
      piiColumns: row.pii_columns || [],
    }));
  }

  private resultToLineageEdges(result: duckdb.Table): LineageEdge[] {
    // Similar conversion
    return result.toArray().map((row: any) => ({
      id: row.edge_id,
      sourceTableId: row.source_table_id,
      sourceColumnId: row.source_column_id,
      targetTableId: row.target_table_id,
      targetColumnId: row.target_column_id,
      transformationType: row.transformation_type,
      createdAt: row.created_at,
    }));
  }

  private resultToPartitions(result: duckdb.Table): Partition[] {
    return result.toArray().map((row: any) => ({
      id: row.partition_id,
      tableId: row.table_id,
      partitionValues: row.partition_values,
      status: row.status,
      rowCount: row.row_count,
      sizeBytes: row.size_bytes,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  }
}
```

---

## 5. Servo Integration

### 5.1 Servo Catalog Client

```rust
// servo/src/catalog/client.rs

use arco_catalog::{EventWriter, CatalogReader};
use arco_core::types::{EventSource, CatalogEvent};

pub struct ServoCatalogClient {
    reader: CatalogReader,
    event_writer: EventWriter,
    pipeline_id: String,
}

impl ServoCatalogClient {
    pub fn new(
        storage: Arc<TenantStorage>,
        pipeline_id: &str,
    ) -> Self {
        let source = EventSource {
            source_type: "servo".to_string(),
            execution_id: None,
            task_name: None,
        };

        Self {
            reader: CatalogReader::new(storage.clone()),
            event_writer: EventWriter::new(storage, source),
            pipeline_id: pipeline_id.to_string(),
        }
    }

    /// Called at start of pipeline execution
    pub async fn start_execution(
        &mut self,
        execution_id: &str,
        trigger_type: &str,
    ) -> Result<(), CatalogError> {
        // Update source with execution context
        self.event_writer = EventWriter::new(
            self.event_writer.storage.clone(),
            EventSource {
                source_type: "servo".to_string(),
                execution_id: Some(execution_id.to_string()),
                task_name: None,
            },
        );

        self.event_writer.emit(CatalogEvent::ExecutionStarted {
            execution_id: execution_id.to_string(),
            pipeline_id: self.pipeline_id.clone(),
            pipeline_name: self.pipeline_id.clone(), // Could be different
            trigger_type: trigger_type.to_string(),
            triggered_by: None,
        }).await?;

        Ok(())
    }

    /// Called when execution completes
    pub async fn complete_execution(
        &self,
        execution_id: &str,
        status: &str,
        duration_ms: i64,
        error: Option<String>,
    ) -> Result<(), CatalogError> {
        self.event_writer.emit(CatalogEvent::ExecutionCompleted {
            execution_id: execution_id.to_string(),
            status: status.to_string(),
            duration_ms,
            error_message: error,
        }).await?;

        Ok(())
    }

    /// Called when a task starts
    pub async fn start_task(
        &self,
        execution_id: &str,
        task_id: &str,
        task_name: &str,
    ) -> Result<(), CatalogError> {
        self.event_writer.emit(CatalogEvent::TaskStarted {
            execution_id: execution_id.to_string(),
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
        }).await?;

        Ok(())
    }

    /// Called when a task completes with lineage
    pub async fn complete_task(
        &self,
        execution_id: &str,
        task_id: &str,
        task_name: &str,
        status: &str,
        duration_ms: i64,
        lineage: TaskLineage,
    ) -> Result<(), CatalogError> {
        self.event_writer.emit(CatalogEvent::TaskCompleted {
            execution_id: execution_id.to_string(),
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            status: status.to_string(),
            duration_ms,
            rows_read: lineage.rows_read,
            rows_written: lineage.rows_written,
            input_tables: lineage.input_tables,
            output_tables: lineage.output_tables,
        }).await?;

        Ok(())
    }

    /// Mark a partition as complete
    pub async fn partition_completed(
        &self,
        table_id: &str,
        partition_id: &str,
        metrics: PartitionMetrics,
    ) -> Result<(), CatalogError> {
        self.event_writer.emit(CatalogEvent::PartitionCompleted {
            partition_id: partition_id.to_string(),
            status: "COMPLETE".to_string(),
            row_count: metrics.row_count,
            size_bytes: metrics.size_bytes,
            duration_ms: metrics.duration_ms,
        }).await?;

        Ok(())
    }

    /// Record a quality check result
    pub async fn quality_check_result(
        &self,
        table_id: &str,
        check_name: &str,
        check_type: &str,
        passed: bool,
        details: QualityDetails,
    ) -> Result<(), CatalogError> {
        let event = if passed {
            CatalogEvent::QualityCheckPassed {
                table_id: table_id.to_string(),
                partition_id: details.partition_id,
                check_name: check_name.to_string(),
                check_type: check_type.to_string(),
                expected_value: details.expected_value,
                actual_value: details.actual_value,
            }
        } else {
            CatalogEvent::QualityCheckFailed {
                table_id: table_id.to_string(),
                partition_id: details.partition_id,
                check_name: check_name.to_string(),
                check_type: check_type.to_string(),
                expected_value: details.expected_value,
                actual_value: details.actual_value,
                message: details.message.unwrap_or_default(),
            }
        };

        self.event_writer.emit(event).await?;

        Ok(())
    }

    /// Get partitions that need processing
    pub async fn get_pending_partitions(&self, table_id: &str) -> Result<Vec<Partition>, CatalogError> {
        // Use fresh reader to get latest partition state
        let fresh_reader = FreshReader::new(self.reader.clone(), self.event_writer.storage.clone());
        let partitions = fresh_reader.get_partitions_fresh(table_id).await?;

        Ok(partitions.into_iter()
            .filter(|p| p.status == PartitionStatus::Pending || p.status == PartitionStatus::Stale)
            .collect())
    }

    /// Resolve table location by name
    pub async fn resolve_table(&self, namespace: &str, name: &str) -> Result<Option<Table>, CatalogError> {
        self.reader.get_table(namespace, name).await
    }
}

#[derive(Debug)]
pub struct TaskLineage {
    pub input_tables: Vec<String>,
    pub output_tables: Vec<String>,
    pub rows_read: Option<i64>,
    pub rows_written: Option<i64>,
}

#[derive(Debug)]
pub struct PartitionMetrics {
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug)]
pub struct QualityDetails {
    pub partition_id: Option<String>,
    pub expected_value: Option<String>,
    pub actual_value: Option<String>,
    pub message: Option<String>,
}
```

### 5.2 Servo Task Wrapper

```rust
// servo/src/executor/catalog_wrapper.rs

/// Wraps task execution to automatically record lineage
pub struct CatalogAwareExecutor {
    catalog: ServoCatalogClient,
    execution_id: String,
}

impl CatalogAwareExecutor {
    pub fn new(catalog: ServoCatalogClient, execution_id: &str) -> Self {
        Self {
            catalog,
            execution_id: execution_id.to_string(),
        }
    }

    /// Execute a task with automatic lineage recording
    pub async fn execute_task<F, T>(
        &self,
        task_name: &str,
        input_tables: Vec<String>,
        output_tables: Vec<String>,
        f: F,
    ) -> Result<T, TaskError>
    where
        F: Future<Output = Result<(T, TaskMetrics), TaskError>>,
    {
        let task_id = Uuid::new_v4().to_string();

        // Record task start
        self.catalog.start_task(&self.execution_id, &task_id, task_name).await?;

        let start = std::time::Instant::now();
        let result = f.await;
        let duration_ms = start.elapsed().as_millis() as i64;

        match result {
            Ok((value, metrics)) => {
                // Record task completion with lineage
                self.catalog.complete_task(
                    &self.execution_id,
                    &task_id,
                    task_name,
                    "SUCCESS",
                    duration_ms,
                    TaskLineage {
                        input_tables,
                        output_tables,
                        rows_read: metrics.rows_read,
                        rows_written: metrics.rows_written,
                    },
                ).await?;

                Ok(value)
            }
            Err(e) => {
                // Record task failure
                self.catalog.complete_task(
                    &self.execution_id,
                    &task_id,
                    task_name,
                    "FAILED",
                    duration_ms,
                    TaskLineage {
                        input_tables,
                        output_tables,
                        rows_read: None,
                        rows_written: None,
                    },
                ).await?;

                Err(e)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct TaskMetrics {
    pub rows_read: Option<i64>,
    pub rows_written: Option<i64>,
}
```

---

## 6. Error Handling

### 6.1 Error Categories

```rust
// arco-core/src/error.rs

#[derive(Debug, thiserror::Error)]
pub enum ArcoError {
    // Storage errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    // Concurrency errors
    #[error("Lock acquisition failed: {0}")]
    LockFailed(String),

    #[error("Concurrent modification detected")]
    ConcurrentModification,

    // Data errors
    #[error("Entity not found: {entity_type} '{id}'")]
    NotFound { entity_type: String, id: String },

    #[error("Entity already exists: {entity_type} '{id}'")]
    AlreadyExists { entity_type: String, id: String },

    #[error("Invalid data: {0}")]
    InvalidData(String),

    // Serialization
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    // Query errors
    #[error("Query error: {0}")]
    Query(#[from] datafusion::error::DataFusionError),

    // Internal
    #[error("Internal error: {0}")]
    Internal(String),
}

impl ArcoError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ArcoError::Storage(StorageError::Internal(_)) |
            ArcoError::LockFailed(_) |
            ArcoError::ConcurrentModification
        )
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            ArcoError::Storage(_) => "STORAGE_ERROR",
            ArcoError::LockFailed(_) => "LOCK_FAILED",
            ArcoError::ConcurrentModification => "CONCURRENT_MODIFICATION",
            ArcoError::NotFound { .. } => "NOT_FOUND",
            ArcoError::AlreadyExists { .. } => "ALREADY_EXISTS",
            ArcoError::InvalidData(_) => "INVALID_DATA",
            ArcoError::Serialization(_) => "SERIALIZATION_ERROR",
            ArcoError::Parquet(_) => "PARQUET_ERROR",
            ArcoError::Query(_) => "QUERY_ERROR",
            ArcoError::Internal(_) => "INTERNAL_ERROR",
        }
    }
}
```

### 6.2 Retry Logic

```rust
// arco-core/src/retry.rs

use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

pub async fn with_retry<F, Fut, T, E>(
    config: &RetryConfig,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = config.initial_delay;

    for attempt in 1..=config.max_attempts {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if attempt == config.max_attempts {
                    tracing::error!(
                        attempt = attempt,
                        error = %e,
                        "All retry attempts exhausted"
                    );
                    return Err(e);
                }

                tracing::warn!(
                    attempt = attempt,
                    error = %e,
                    delay_ms = delay.as_millis(),
                    "Retrying after error"
                );

                sleep(delay).await;

                delay = std::cmp::min(
                    Duration::from_secs_f64(delay.as_secs_f64() * config.multiplier),
                    config.max_delay,
                );
            }
        }
    }

    unreachable!()
}

// Usage with retryable check
pub async fn with_retry_if<F, Fut, T, E>(
    config: &RetryConfig,
    f: F,
    is_retryable: impl Fn(&E) -> bool,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = config.initial_delay;

    for attempt in 1..=config.max_attempts {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) if is_retryable(&e) && attempt < config.max_attempts => {
                tracing::warn!(
                    attempt = attempt,
                    error = %e,
                    "Retrying retryable error"
                );
                sleep(delay).await;
                delay = std::cmp::min(
                    Duration::from_secs_f64(delay.as_secs_f64() * config.multiplier),
                    config.max_delay,
                );
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!()
}
```

---

## 6.5 Governance Postures

Arco supports three explicit security postures. Choose based on your metadata confidentiality requirements:

### Posture A: Broad Metadata Visibility (MVP Default)

All authenticated tenant users can see all metadata. Access control is for UI hints only.

```rust
// No special implementation needed - default behavior
// compiled_grants table drives UI (edit buttons, action menus)
// but doesn't restrict read access to catalog Parquet files
```

**When to use**: Most organizations where knowing a table exists isn't sensitive.

### Posture B: Existence Privacy

Users can't see tables they don't have access to. Requires partitioned snapshots.

```
Storage layout for Posture B:
_catalog/
  snapshots/
    catalog/
      visibility=public/          # Everyone can read
        tables.parquet
      visibility=finance/         # Only finance IAM can read
        tables.parquet
      visibility=engineering/     # Only engineering IAM can read
        tables.parquet
```

```rust
// IAM policy (Terraform)
resource "google_storage_bucket_iam_binding" "finance_visibility" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"

  members = [
    "group:finance@company.com",
  ]

  condition {
    title       = "finance_visibility_only"
    description = "Access only finance visibility partition"
    expression  = "resource.name.startsWith('projects/_/buckets/${bucket}/objects/tenant=acme/_catalog/snapshots/catalog/visibility=finance/')"
  }
}
```

**When to use**: Finance, HR, or compliance data where existence itself is sensitive.

### Posture C: Encryption + Key Distribution

Objects encrypted at rest with customer-managed keys. Key service handles auth.

```rust
// Out of scope for MVP - document for future reference
// Would require:
// 1. CMEK (Customer Managed Encryption Keys) on GCS
// 2. Key service that validates auth before providing decryption keys
// 3. Client-side decryption in DuckDB-WASM (complex)
```

**When to use**: Highly regulated industries (healthcare, government).

### MVP Recommendation

Start with **Posture A**. Design storage layout to support **Posture B** later:

```rust
// Even in Posture A, store visibility partition key (unused initially)
pub struct Table {
    // ... existing fields ...

    /// Visibility domain for Posture B (default: "public")
    pub visibility_domain: String,
}
```

---

## 7. Observability

### 7.1 Structured Logging

```rust
// arco-core/src/observability/logging.rs

use tracing::{info, warn, error, instrument, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

// Span helpers for consistent logging
pub fn catalog_span(tenant_id: &str, operation: &str) -> Span {
    tracing::info_span!(
        "catalog_operation",
        tenant_id = %tenant_id,
        operation = %operation,
        otel.name = %format!("catalog.{}", operation),
    )
}

pub fn write_span(tenant_id: &str, entity_type: &str) -> Span {
    tracing::info_span!(
        "catalog_write",
        tenant_id = %tenant_id,
        entity_type = %entity_type,
        otel.name = %format!("catalog.write.{}", entity_type),
    )
}

// Example usage with instrument macro
#[instrument(
    skip(self),
    fields(
        tenant_id = %self.tenant_id,
        namespace = %namespace,
        table_name = %name,
    )
)]
pub async fn register_table(
    &self,
    namespace: &str,
    name: &str,
    // ...
) -> Result<Table, ArcoError> {
    info!("Registering table");

    // ... implementation

    info!(table_id = %table.id, "Table registered successfully");
    Ok(table)
}
```

### 7.2 Metrics

```rust
// arco-core/src/observability/metrics.rs

use prometheus::{
    Counter, CounterVec, Histogram, HistogramVec, IntGauge, IntGaugeVec,
    Opts, Registry,
};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Write metrics
    pub static ref WRITES_TOTAL: CounterVec = CounterVec::new(
        Opts::new("arco_writes_total", "Total write operations"),
        &["tenant_id", "operation", "tier", "status"]
    ).unwrap();

    pub static ref WRITE_LATENCY: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "arco_write_latency_seconds",
            "Write operation latency"
        ).buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["tenant_id", "operation", "tier"]
    ).unwrap();

    // Read metrics
    pub static ref READS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("arco_reads_total", "Total read operations"),
        &["tenant_id", "operation", "status"]
    ).unwrap();

    pub static ref READ_LATENCY: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "arco_read_latency_seconds",
            "Read operation latency"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["tenant_id", "operation"]
    ).unwrap();

    // Lock metrics
    pub static ref LOCK_ACQUISITIONS: CounterVec = CounterVec::new(
        Opts::new("arco_lock_acquisitions_total", "Lock acquisition attempts"),
        &["tenant_id", "status"]
    ).unwrap();

    pub static ref LOCK_WAIT_TIME: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "arco_lock_wait_seconds",
            "Time spent waiting for lock"
        ).buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        &["tenant_id"]
    ).unwrap();

    // Compaction metrics
    pub static ref COMPACTION_RUNS: CounterVec = CounterVec::new(
        Opts::new("arco_compaction_runs_total", "Compaction runs"),
        &["tenant_id", "status"]
    ).unwrap();

    pub static ref EVENTS_COMPACTED: CounterVec = CounterVec::new(
        Opts::new("arco_events_compacted_total", "Events compacted"),
        &["tenant_id"]
    ).unwrap();

    pub static ref COMPACTION_LAG: IntGaugeVec = IntGaugeVec::new(
        Opts::new("arco_compaction_lag_events", "Number of uncompacted events"),
        &["tenant_id"]
    ).unwrap();

    // Snapshot metrics
    pub static ref SNAPSHOT_VERSION: IntGaugeVec = IntGaugeVec::new(
        Opts::new("arco_snapshot_version", "Current snapshot version"),
        &["tenant_id", "tier"]
    ).unwrap();

    pub static ref SNAPSHOT_SIZE_BYTES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("arco_snapshot_size_bytes", "Snapshot size in bytes"),
        &["tenant_id", "tier", "file"]
    ).unwrap();
}

pub fn register_metrics() {
    REGISTRY.register(Box::new(WRITES_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(WRITE_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(READS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(READ_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(LOCK_ACQUISITIONS.clone())).unwrap();
    REGISTRY.register(Box::new(LOCK_WAIT_TIME.clone())).unwrap();
    REGISTRY.register(Box::new(COMPACTION_RUNS.clone())).unwrap();
    REGISTRY.register(Box::new(EVENTS_COMPACTED.clone())).unwrap();
    REGISTRY.register(Box::new(COMPACTION_LAG.clone())).unwrap();
    REGISTRY.register(Box::new(SNAPSHOT_VERSION.clone())).unwrap();
    REGISTRY.register(Box::new(SNAPSHOT_SIZE_BYTES.clone())).unwrap();
}

// Helper for timing operations
pub struct Timer {
    histogram: HistogramVec,
    labels: Vec<String>,
    start: std::time::Instant,
}

impl Timer {
    pub fn new(histogram: &HistogramVec, labels: &[&str]) -> Self {
        Self {
            histogram: histogram.clone(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let labels: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        self.histogram
            .with_label_values(&labels)
            .observe(self.start.elapsed().as_secs_f64());
    }
}
```

### 7.3 Health Checks

```rust
// arco-api/src/health.rs

use axum::{routing::get, Router, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    status: String,
    version: String,
    checks: Vec<HealthCheck>,
}

#[derive(Serialize)]
pub struct HealthCheck {
    name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

pub fn health_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        checks: vec![],
    })
}

async fn liveness() -> &'static str {
    "OK"
}

async fn readiness(State(state): State<AppState>) -> Result<&'static str, StatusCode> {
    // Check storage connectivity
    let storage_ok = state.storage()
        .head("_health_check")
        .await
        .map(|_| true)
        .unwrap_or(true); // NotFound is OK

    if storage_ok {
        Ok("OK")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}
```

---

## 8. Performance Optimization

### 8.1 Parquet Optimization

```rust
// arco-catalog/src/parquet/writer.rs

use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::basic::{Compression, Encoding};

pub fn optimized_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        // Use Parquet 2.0 for better encoding
        .set_writer_version(WriterVersion::PARQUET_2_0)

        // Snappy compression (good balance of speed/size)
        .set_compression(Compression::SNAPPY)

        // Row group size (128MB default)
        .set_max_row_group_size(128 * 1024 * 1024)

        // Page size (1MB for good column chunk sizes)
        .set_data_page_size(1024 * 1024)

        // Dictionary encoding for string columns
        .set_dictionary_enabled(true)
        .set_max_dictionary_page_size(1024 * 1024)

        // Statistics for predicate pushdown
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)

        .build()
}

// Column-specific settings
pub fn column_specific_properties(schema: &Schema) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY);

    for field in schema.fields() {
        let col_path = parquet::schema::types::ColumnPath::new(vec![field.name().to_string()]);

        match field.data_type() {
            // Use DELTA_BINARY_PACKED for integers (good for IDs, counts)
            DataType::Int32 | DataType::Int64 => {
                builder = builder.set_column_encoding(col_path.clone(), Encoding::DELTA_BINARY_PACKED);
            }
            // Use dictionary encoding for low-cardinality strings
            DataType::Utf8 if is_low_cardinality(field.name()) => {
                builder = builder.set_column_dictionary_enabled(col_path.clone(), true);
            }
            // Use DELTA_LENGTH_BYTE_ARRAY for high-cardinality strings
            DataType::Utf8 => {
                builder = builder.set_column_encoding(col_path.clone(), Encoding::DELTA_LENGTH_BYTE_ARRAY);
            }
            _ => {}
        }
    }

    builder.build()
}

fn is_low_cardinality(column_name: &str) -> bool {
    matches!(
        column_name,
        "format" | "status" | "transformation_type" | "check_type" | "trigger_type" | "pii_type" | "sensitivity"
    )
}
```

### 8.2 Read Optimization

```rust
// arco-catalog/src/reader/optimized.rs

use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions;

/// Optimized reader with projection pushdown and predicate pushdown
pub struct OptimizedReader {
    ctx: SessionContext,
    storage: Arc<TenantStorage>,
}

impl OptimizedReader {
    pub async fn new(storage: Arc<TenantStorage>) -> Result<Self, ArcoError> {
        let ctx = SessionContext::new();

        // Configure for optimal Parquet reading
        let config = ctx.state().config().clone();
        let config = config
            .with_parquet_pushdown_filters(true)
            .with_parquet_reorder_filters(true)
            .with_parquet_pruning(true);

        let ctx = SessionContext::with_config(config);

        Ok(Self { ctx, storage })
    }

    /// Read with projection (only specified columns)
    pub async fn read_tables_projected(
        &self,
        columns: &[&str],
        filter: Option<Expr>,
    ) -> Result<Vec<RecordBatch>, ArcoError> {
        let manifest = self.read_manifest().await?;
        let path = format!("{}/{}", manifest.core.snapshot_path, "tables.parquet");

        let df = self.ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await?
            .select_columns(columns)?;

        let df = if let Some(f) = filter {
            df.filter(f)?
        } else {
            df
        };

        Ok(df.collect().await?)
    }

    /// Batch read multiple files in parallel
    pub async fn read_catalog_batch(&self) -> Result<CatalogBatch, ArcoError> {
        let manifest = self.read_manifest().await?;
        let snapshot_path = &manifest.core.snapshot_path;

        // Read all files in parallel
        let (tables, columns, lineage) = tokio::try_join!(
            self.read_parquet(&format!("{}/tables.parquet", snapshot_path)),
            self.read_parquet(&format!("{}/columns.parquet", snapshot_path)),
            self.read_parquet(&format!("{}/lineage_edges.parquet", snapshot_path)),
        )?;

        Ok(CatalogBatch { tables, columns, lineage })
    }

    async fn read_parquet(&self, path: &str) -> Result<Vec<RecordBatch>, ArcoError> {
        let df = self.ctx.read_parquet(path, ParquetReadOptions::default()).await?;
        Ok(df.collect().await?)
    }
}

#[derive(Debug)]
pub struct CatalogBatch {
    pub tables: Vec<RecordBatch>,
    pub columns: Vec<RecordBatch>,
    pub lineage: Vec<RecordBatch>,
}
```

### 8.3 Caching Strategy

```rust
// arco-catalog/src/cache.rs

use lru::LruCache;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct ManifestCache {
    cache: Mutex<LruCache<String, CachedManifest>>,
    ttl: Duration,
}

struct CachedManifest {
    manifest: CatalogManifest,
    cached_at: Instant,
}

impl ManifestCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(capacity).unwrap())),
            ttl,
        }
    }

    pub fn get(&self, tenant_id: &str) -> Option<CatalogManifest> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(cached) = cache.get(tenant_id) {
            if cached.cached_at.elapsed() < self.ttl {
                return Some(cached.manifest.clone());
            }
            // Expired, remove
            cache.pop(tenant_id);
        }

        None
    }

    pub fn put(&self, tenant_id: &str, manifest: CatalogManifest) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(
            tenant_id.to_string(),
            CachedManifest {
                manifest,
                cached_at: Instant::now(),
            },
        );
    }

    pub fn invalidate(&self, tenant_id: &str) {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(tenant_id);
    }
}

// Snapshot cache for frequently accessed tables
pub struct SnapshotCache {
    tables: Mutex<LruCache<String, CachedTables>>,
    ttl: Duration,
}

struct CachedTables {
    tables: Vec<Table>,
    version: u64,
    cached_at: Instant,
}

impl SnapshotCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            tables: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(capacity).unwrap())),
            ttl,
        }
    }

    pub fn get_tables(&self, tenant_id: &str, current_version: u64) -> Option<Vec<Table>> {
        let mut cache = self.tables.lock().unwrap();

        if let Some(cached) = cache.get(tenant_id) {
            // Check version and TTL
            if cached.version == current_version && cached.cached_at.elapsed() < self.ttl {
                return Some(cached.tables.clone());
            }
        }

        None
    }

    pub fn put_tables(&self, tenant_id: &str, version: u64, tables: Vec<Table>) {
        let mut cache = self.tables.lock().unwrap();
        cache.put(
            tenant_id.to_string(),
            CachedTables {
                tables,
                version,
                cached_at: Instant::now(),
            },
        );
    }
}
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

```rust
// arco-catalog/src/writer/tier1_test.rs

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::LocalBackend;
    use tempfile::TempDir;

    async fn setup() -> (TempDir, Arc<TenantStorage>) {
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(LocalBackend::new(tmp.path()));
        let storage = Arc::new(TenantStorage::new(backend, "test-tenant"));
        (tmp, storage)
    }

    #[tokio::test]
    async fn test_register_table() {
        let (_tmp, storage) = setup().await;
        let writer = CoreCatalogWriter::new(storage.clone(), "test-writer");
        let registry = TableRegistry::new(Arc::new(writer));

        let table = registry.register_table(
            "default",
            "orders",
            "gs://bucket/orders",
            TableFormat::Delta,
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "INT64".to_string(),
                    is_nullable: false,
                    description: None,
                    pii_type: None,
                    sensitivity: None,
                },
            ],
        ).await.unwrap();

        assert_eq!(table.namespace, "default");
        assert_eq!(table.name, "orders");
    }

    #[tokio::test]
    async fn test_concurrent_writes_fail() {
        let (_tmp, storage) = setup().await;

        // Two writers trying to acquire lock
        let writer1 = CoreCatalogWriter::new(storage.clone(), "writer-1");
        let writer2 = CoreCatalogWriter::new(storage.clone(), "writer-2");

        // First writer acquires lock
        let lock1 = writer1.lock.acquire().await.unwrap();

        // Second writer should fail (with short timeout for test)
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            writer2.lock.acquire(),
        ).await;

        assert!(result.is_err() || matches!(result.unwrap(), Err(LockError::Timeout)));

        // Release first lock
        writer1.lock.release(&lock1).await.unwrap();
    }

    #[tokio::test]
    async fn test_table_already_exists() {
        let (_tmp, storage) = setup().await;
        let writer = CoreCatalogWriter::new(storage.clone(), "test-writer");
        let registry = TableRegistry::new(Arc::new(writer));

        // First registration succeeds
        registry.register_table(
            "default",
            "orders",
            "gs://bucket/orders",
            TableFormat::Delta,
            vec![],
        ).await.unwrap();

        // Second registration fails
        let result = registry.register_table(
            "default",
            "orders",
            "gs://bucket/orders2",
            TableFormat::Delta,
            vec![],
        ).await;

        assert!(matches!(result, Err(WriteError::TableExists(_))));
    }
}
```

### 9.2 Integration Tests

```rust
// tests/integration/catalog_flow_test.rs

use arco_catalog::{CoreCatalogWriter, EventWriter, EventCompactor, CatalogReader};
use arco_core::storage::GcsBackend;

#[tokio::test]
#[ignore] // Run with: cargo test --ignored
async fn test_full_catalog_flow() {
    let bucket = std::env::var("TEST_BUCKET").expect("TEST_BUCKET required");
    let tenant_id = format!("test-{}", Uuid::new_v4());

    let backend = Arc::new(GcsBackend::new(&bucket, "").await.unwrap());
    let storage = Arc::new(TenantStorage::new(backend, &tenant_id));

    // 1. Register a table (Tier 1)
    let writer = CoreCatalogWriter::new(storage.clone(), "test-writer");
    let table = writer.write("test", |snapshot| {
        let table = Table::new("default", "orders", "gs://bucket/orders", TableFormat::Delta);
        let op = snapshot.add_table(table.clone());
        Ok((table, vec![op]))
    }).await.unwrap();

    // 2. Emit partition events (Tier 2)
    let event_writer = EventWriter::new(
        storage.clone(),
        EventSource {
            source_type: "test".to_string(),
            execution_id: Some("exec-1".to_string()),
            task_name: None,
        },
    );

    event_writer.emit(CatalogEvent::PartitionCreated {
        table_id: table.id.0.to_string(),
        partition_id: "part-1".to_string(),
        partition_values: [("date".to_string(), "2025-01-15".to_string())].into(),
    }).await.unwrap();

    event_writer.partition_completed("part-1", Some(1000), Some(50000), Some(1500)).await.unwrap();

    // 3. Run compaction
    let compactor = EventCompactor::new(storage.clone());
    let result = compactor.compact().await.unwrap();
    assert!(result.events_processed >= 2);

    // 4. Read and verify
    let reader = CatalogReader::new(storage.clone()).await.unwrap();

    let tables = reader.list_tables().await.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "orders");

    // 5. Cleanup
    // ... delete test tenant data
}
```

### 9.3 Load Tests

```rust
// tests/load/concurrent_writes.rs

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let storage = rt.block_on(async {
        // Setup storage with pre-populated data
        setup_test_catalog().await
    });

    let reader = CatalogReader::new(storage.clone());

    c.bench_function("list_tables_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                reader.list_tables().await.unwrap()
            })
        });
    });

    let mut group = c.benchmark_group("concurrent_reads");
    for concurrency in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            concurrency,
            |b, &n| {
                b.iter(|| {
                    rt.block_on(async {
                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let reader = reader.clone();
                                tokio::spawn(async move {
                                    reader.list_tables().await.unwrap()
                                })
                            })
                            .collect();

                        futures::future::join_all(handles).await
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_concurrent_reads);
criterion_main!(benches);
```

---

## 10. Migration & Upgrades

### 10.1 Schema Evolution

```rust
// arco-catalog/src/migration/schema.rs

/// Schema versions for catalog files
pub const SCHEMA_VERSION: u32 = 1;

/// Migration from one schema version to another
pub trait Migration {
    fn from_version(&self) -> u32;
    fn to_version(&self) -> u32;
    fn migrate(&self, data: &mut CatalogSnapshot) -> Result<(), MigrationError>;
}

pub struct MigrationRunner {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRunner {
    pub fn new() -> Self {
        Self {
            migrations: vec![
                // Add migrations here as schema evolves
                // Box::new(MigrationV1ToV2),
            ],
        }
    }

    pub fn migrate_to_latest(&self, snapshot: &mut CatalogSnapshot, from_version: u32) -> Result<(), MigrationError> {
        let mut current = from_version;

        while current < SCHEMA_VERSION {
            let migration = self.migrations
                .iter()
                .find(|m| m.from_version() == current)
                .ok_or(MigrationError::NoPath { from: current, to: SCHEMA_VERSION })?;

            migration.migrate(snapshot)?;
            current = migration.to_version();
        }

        Ok(())
    }
}

// Example migration
pub struct MigrationV1ToV2;

impl Migration for MigrationV1ToV2 {
    fn from_version(&self) -> u32 { 1 }
    fn to_version(&self) -> u32 { 2 }

    fn migrate(&self, data: &mut CatalogSnapshot) -> Result<(), MigrationError> {
        // Example: Add new field with default value
        for table in &mut data.tables {
            if table.properties.get("migrated").is_none() {
                table.properties.insert("migrated".to_string(), "v2".to_string());
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("No migration path from version {from} to {to}")]
    NoPath { from: u32, to: u32 },

    #[error("Migration failed: {0}")]
    Failed(String),
}
```

### 10.2 Deployment Strategy

```yaml
# Deployment process for Arco updates

# 1. Deploy new compactor (handles both old and new events)
# 2. Deploy new API (backward compatible)
# 3. Run migration job if needed
# 4. Verify health
# 5. Remove backward compatibility code in next release

# Cloud Build - arco-deploy.yaml
steps:
  # Build images
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/arco-api:$TAG_NAME', '-f', 'Dockerfile.arco-api', '.']

  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/arco-compactor:$TAG_NAME', '-f', 'Dockerfile.compactor', '.']

  # Push images
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/arco-api:$TAG_NAME']

  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/arco-compactor:$TAG_NAME']

  # Deploy compactor first (processes events)
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
      - 'functions'
      - 'deploy'
      - 'arco-compactor-$_ENV'
      - '--gen2'
      - '--runtime=provided'
      - '--source=gs://$_DEPLOY_BUCKET/arco-compactor-$TAG_NAME.zip'

  # Wait for compactor to be healthy
  - name: 'gcr.io/cloud-builders/gcloud'
    entrypoint: 'bash'
    args:
      - '-c'
      - 'sleep 30 && gcloud functions describe arco-compactor-$_ENV --gen2 --format="value(state)"'

  # Deploy API
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
      - 'run'
      - 'deploy'
      - 'arco-api-$_ENV'
      - '--image=gcr.io/$PROJECT_ID/arco-api:$TAG_NAME'
      - '--region=$_REGION'

  # Verify deployment
  - name: 'gcr.io/cloud-builders/gcloud'
    entrypoint: 'bash'
    args:
      - '-c'
      - 'curl -f https://arco-api-$_ENV-xxxxx-uc.a.run.app/health'

substitutions:
  _ENV: 'prod'
  _REGION: 'us-central1'
  _DEPLOY_BUCKET: 'arco-deploy-prod'

timeout: '1200s'
```

### 10.3 Rollback Procedure

```bash
#!/bin/bash
# scripts/rollback.sh

set -e

ENV=${1:-prod}
VERSION=${2}

if [ -z "$VERSION" ]; then
    echo "Usage: ./rollback.sh <env> <version>"
    exit 1
fi

echo "Rolling back Arco in $ENV to version $VERSION"

# 1. Rollback API first
gcloud run services update-traffic arco-api-$ENV \
    --to-revisions=arco-api-$ENV-$VERSION=100 \
    --region=us-central1

# 2. Rollback compactor
gcloud functions deploy arco-compactor-$ENV \
    --gen2 \
    --source=gs://arco-deploy-$ENV/arco-compactor-$VERSION.zip

# 3. Verify
curl -f https://arco-api-$ENV-xxxxx-uc.a.run.app/health

echo "Rollback complete"
```

---

*End of Part 2: Operations & Integration*

*Combined with Part 1, this provides a complete technical reference for implementing Arco.*
