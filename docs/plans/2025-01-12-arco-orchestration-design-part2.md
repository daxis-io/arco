# Servo Orchestration Design — Part 2

> Operational Architecture, Runtime Mechanics, and Production Hardening

**Status:** Draft
**Date:** 2025-01-12
**Authors:** Architecture Team
**Companion:** [Part 1 - Core Architecture](./2025-01-12-servo-orchestration-design.md)

---

## Executive Summary

This document extends the Servo core design with operational details required for production deployment:

- **Build & Artifact Lifecycle** — How Python code becomes deployable containers
- **Infrastructure** — Database pooling, message size handling, connection management
- **Worker Runtime** — Heartbeats, secret resolution, code loading, execution lifecycle
- **Scheduler & Dispatcher** — The control loop that moves tasks through states
- **Fairness & Quotas** — Multi-tenant resource allocation and starvation prevention
- **Quality Framework** — Pre/post checks, gating semantics, quarantine
- **Backfill Orchestration** — Multi-run backfills with pause/resume
- **Error Handling** — Classification, retry policies, structured errors
- **Observability** — Metrics, traces, alerts, and correlation
- **Security & RBAC** — Authentication, authorization, and trust boundaries
- **Testing Strategy** — Unit, integration, property, and load testing
- **Deployment Architecture** — GCP infrastructure and Terraform modules

---

## Table of Contents

1. [Build & Artifact Lifecycle](#1-build--artifact-lifecycle)
2. [Infrastructure & Connectivity](#2-infrastructure--connectivity)
3. [Worker Runtime](#3-worker-runtime)
4. [Scheduler & Dispatcher Loop](#4-scheduler--dispatcher-loop)
5. [Fairness & Quota Enforcement](#5-fairness--quota-enforcement)
6. [Quality Framework](#6-quality-framework)
7. [Backfill Orchestration](#7-backfill-orchestration)
8. [Error Handling](#8-error-handling)
9. [Observability](#9-observability)
10. [Security & RBAC](#10-security--rbac)
11. [Testing Strategy](#11-testing-strategy)
12. [Deployment Architecture](#12-deployment-architecture)
13. [API Versioning & Evolution](#13-api-versioning--evolution)
14. [Additional Storage Schema](#14-additional-storage-schema)
15. [Canonical Serialization Rules](#15-canonical-serialization-rules)
16. [Output Commit Protocol](#16-output-commit-protocol)
17. [Provider Adapters](#17-provider-adapters)
18. [API Standards](#18-api-standards)
19. [Cancellation Semantics](#19-cancellation-semantics)
20. [Projection Rebuild Strategy](#20-projection-rebuild-strategy)
21. [Supply Chain Security](#21-supply-chain-security)
22. [Cost Attribution](#22-cost-attribution)

**Appendices**

- [Appendix A: Canonical String Encodings](#appendix-a-canonical-string-encodings)
- [Appendix B: ADR Template](#appendix-b-adr-template)
- [Appendix C: Production Readiness Checklist](#appendix-c-production-readiness-checklist)
- [Appendix D: Glossary](#appendix-d-glossary)
- [Appendix E: Operational Runbook](#appendix-e-operational-runbook)

---

## 1. Build & Artifact Lifecycle

### 1.1 The Problem

Users write Python code with `@asset` decorators. This code must:
1. Be packaged into a deployable artifact
2. Be versioned and content-addressable
3. Run in serverless workers (Cloud Run)

Users should **not** need to write Dockerfiles.

### 1.2 Build Strategy: Serverless Build Service

**Decision:** CLI uploads source → Cloud Build packages → Artifact Registry stores

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Developer  │────▶│  servo CLI  │────▶│ Cloud Build │────▶│  Artifact   │
│  Workspace  │     │             │     │             │     │  Registry   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │                   │
       │ @asset code       │ Upload src.zip    │ Build image       │ Store image
       │                   │ + requirements    │ w/ base worker    │ gcr.io/...
       └───────────────────┴───────────────────┴───────────────────┘
```

### 1.3 CLI Build Flow

```bash
# User runs deploy
servo deploy

# CLI does:
# 1. Scan for @asset decorated functions
# 2. Generate manifest.json
# 3. Package source code
# 4. Upload to staging bucket
# 5. Trigger build (optional)
# 6. Register with control plane
```

### 1.4 Artifact Types

| Type | Use Case | Contents |
|------|----------|----------|
| **Source Archive** | Development, small projects | `src.zip` + `requirements.txt` |
| **Container Image** | Production, custom deps | Full Docker image with code baked in |

### 1.5 Protobuf Additions

```protobuf
message CodeArtifact {
  string uri = 1;               // gs://bucket/artifacts/abc123.zip OR gcr.io/project/worker:abc123
  string digest = 2;            // sha256:...
  ArtifactType type = 3;

  CodeEntrypoint entrypoint = 10;
  repeated string requirements = 11;  // For source archives
}

enum ArtifactType {
  ARTIFACT_TYPE_UNSPECIFIED = 0;
  ARTIFACT_TYPE_SOURCE_ARCHIVE = 1;   // ZIP with Python source
  ARTIFACT_TYPE_CONTAINER_IMAGE = 2;  // Full container
}

message CodeEntrypoint {
  string module = 1;      // e.g., "my_project.assets.users"
  string function = 2;    // e.g., "user_metrics"
}

// DeployRequest can accept either inline manifest or URI reference
message DeployRequest {
  TenantId tenant_id = 1;
  WorkspaceId workspace_id = 2;

  oneof manifest_source {
    AssetManifest manifest = 10;      // Inline (small manifests)
    string manifest_uri = 11;         // GCS reference (large manifests)
  }

  oneof artifact_source {
    string artifact_uri = 20;         // Pre-built artifact
    SourceUpload source_upload = 21;  // Source to build
  }

  DeployOptions options = 30;
}

message SourceUpload {
  string upload_uri = 1;              // Presigned GCS URL for upload
  string requirements_hash = 2;       // Hash of requirements.txt
}
```

### 1.6 Build Process Detail

**For Source Archive:**
```python
# Worker startup (source archive mode)
async def load_artifact(artifact: CodeArtifact):
    if artifact.type == ARTIFACT_TYPE_SOURCE_ARCHIVE:
        # 1. Download and verify
        archive = await download_gcs(artifact.uri)
        assert sha256(archive) == artifact.digest

        # 2. Extract to temp directory
        extract_dir = extract_zip(archive, "/tmp/code")

        # 3. Install requirements (cached by digest)
        if artifact.requirements:
            pip_install(artifact.requirements, cache_key=artifact.digest)

        # 4. Add to Python path
        sys.path.insert(0, extract_dir)

        # 5. Import module
        return importlib.import_module(artifact.entrypoint.module)
```

**For Container Image:**
```dockerfile
# Base worker image (maintained by platform team)
FROM python:3.11-slim

# Install common dependencies
RUN pip install polars pandas pyarrow servo-worker

# Copy user code (baked in at build time)
COPY src/ /app/src/
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Worker entrypoint
ENTRYPOINT ["python", "-m", "servo.worker"]
```

### 1.7 CLI Commands

```bash
# Build artifact locally (for testing)
servo build --output ./dist/

# Deploy with build
servo deploy --build

# Deploy pre-built artifact
servo deploy --artifact gs://bucket/artifacts/v1.2.3.zip

# Validate manifest without deploying
servo deploy --dry-run
```

---

## 2. Infrastructure & Connectivity

### 2.1 Database Connection Management

**Problem:** Serverless workers scaling to 1000+ instances would exhaust Postgres connection limits.

**Solution:** Workers **never** connect to Postgres directly. All state updates flow through presigned callback URLs to the Control Plane API.

```
┌──────────────┐                    ┌──────────────┐
│   Worker 1   │─── callback URL ──▶│              │
├──────────────┤                    │  Control     │     ┌──────────────┐
│   Worker 2   │─── callback URL ──▶│  Plane       │────▶│  PgBouncer   │────▶ Postgres
├──────────────┤                    │  (Rust)      │     │ (20 conns)   │
│   Worker N   │─── callback URL ──▶│              │     └──────────────┘
└──────────────┘                    └──────────────┘
                                         │
                                    Fixed pool
                                    (10 conns)
```

### 2.2 Connection Pooling

**Control Plane (Rust):**
```rust
// sqlx connection pool configuration
let pool = PgPoolOptions::new()
    .max_connections(10)          // Per instance
    .min_connections(2)           // Keep warm
    .acquire_timeout(Duration::from_secs(3))
    .idle_timeout(Duration::from_secs(600))
    .connect(&database_url)
    .await?;
```

**PgBouncer Configuration (transaction mode):**
```ini
[databases]
servo = host=postgres port=5432 dbname=servo

[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 20
reserve_pool_size = 5
reserve_pool_timeout = 3
```

**Connection Retry Logic:**

```rust
/// Acquire connection with exponential backoff retry
async fn get_connection_with_retry(pool: &PgPool) -> Result<PoolConnection<Postgres>> {
    let mut attempts = 0;
    loop {
        match pool.acquire().await {
            Ok(conn) => return Ok(conn),
            Err(e) if attempts < 3 => {
                attempts += 1;
                let backoff = Duration::from_millis(100 * 2_u64.pow(attempts));
                tracing::warn!(
                    attempt = attempts,
                    backoff_ms = backoff.as_millis(),
                    error = %e,
                    "Connection acquire failed, retrying"
                );
                tokio::time::sleep(backoff).await;
            }
            Err(e) => return Err(Error::Database(e.to_string())),
        }
    }
}
```

### 2.3 Message Size Handling

**Problem:** gRPC default limit is 4MB. Large manifests (5000+ assets) or plans exceed this.

**Solution:** Large payloads use GCS references, not inline data.

| Payload | Threshold | Strategy |
|---------|-----------|----------|
| `AssetManifest` | > 1MB | Upload to GCS, pass `manifest_uri` |
| `ExecutionPlan` | > 1MB | Store in GCS, return `plan_uri` in response |
| `TaskRequest` | Always < 64KB | Inline (task-specific subset of plan) |
| Event stream | Unbounded | Pagination with `resume_token` |

**Implementation:**
```rust
impl DeployService {
    async fn deploy(&self, req: DeployRequest) -> Result<DeployResponse> {
        // Load manifest from either source
        let manifest = match req.manifest_source {
            Some(ManifestSource::Manifest(m)) => m,
            Some(ManifestSource::ManifestUri(uri)) => {
                self.storage.download_json(&uri).await?
            }
            None => return Err(Status::invalid_argument("manifest required")),
        };

        // Validate size constraints
        if manifest.assets.len() > 10_000 {
            return Err(Status::invalid_argument("max 10,000 assets per manifest"));
        }

        // ... rest of deploy logic
    }
}
```

### 2.4 Presigned URL Generation

Workers receive presigned URLs for callbacks, avoiding the need for worker-side authentication:

```rust
impl CallbackUrlGenerator {
    fn generate_callbacks(&self, task: &Task) -> WorkerCallbacks {
        let expires_at = Utc::now() + Duration::hours(2);
        let base_path = format!(
            "/v1/tenants/{}/runs/{}/tasks/{}/callbacks",
            task.tenant_id, task.run_id, task.task_id
        );

        WorkerCallbacks {
            status_url: self.sign_url(&format!("{}/status", base_path), expires_at),
            events_url: self.sign_url(&format!("{}/events", base_path), expires_at),
            heartbeat_url: self.sign_url(&format!("{}/heartbeat", base_path), expires_at),
            urls_expire_at: expires_at.into(),
        }
    }

    fn sign_url(&self, path: &str, expires_at: DateTime<Utc>) -> String {
        let signature = hmac_sha256(
            &self.signing_key,
            format!("{}:{}", path, expires_at.timestamp()),
        );
        format!(
            "{}{}?expires={}&sig={}",
            self.base_url, path, expires_at.timestamp(), base64_url_encode(&signature)
        )
    }
}
```

---

## 3. Worker Runtime

### 3.1 Worker Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Worker Lifecycle                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│   │ Receive  │──▶│ Validate │──▶│  Load    │──▶│ Execute  │            │
│   │ Envelope │   │  Auth    │   │  Code    │   │ Function │            │
│   └──────────┘   └──────────┘   └──────────┘   └──────────┘            │
│                                                      │                   │
│                                                      ▼                   │
│                                              ┌──────────┐               │
│                                              │   Run    │               │
│                                              │  Checks  │               │
│                                              └──────────┘               │
│                                                      │                   │
│                                                      ▼                   │
│                                              ┌──────────┐               │
│                                              │  Report  │               │
│                                              │  Result  │               │
│                                              └──────────┘               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Worker Implementation

```python
# python/arco/servo/worker/executor.py

import asyncio
import importlib
import hashlib
from datetime import datetime, timedelta
from typing import Optional

from servo._proto.servo.v1 import TaskEnvelope, TaskRequest, TaskResult, TaskOutcome
from servo.worker.callbacks import CallbackClient
from servo.worker.checks import CheckRunner

class WorkerExecutor:
    """Main worker execution loop."""

    def __init__(self):
        self.callback_client = CallbackClient()
        self.check_runner = CheckRunner()
        self.heartbeat_task: Optional[asyncio.Task] = None

    async def execute(self, envelope: TaskEnvelope) -> TaskResult:
        """Execute a task from the envelope."""
        request = envelope.body.request
        started_at = datetime.utcnow()

        try:
            # 1. Validate authentication
            self._validate_auth(envelope)

            # 2. Start heartbeat loop
            self.heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(request.callbacks.heartbeat_url)
            )

            # 3. Report RUNNING status
            await self.callback_client.report_status(
                request.callbacks.status_url,
                "running",
                attempt=request.attempt,
            )

            # 4. Load code artifact
            module = await self._load_artifact(request.artifact)
            func = getattr(module, request.artifact.entrypoint.function)

            # 5. Prepare execution context
            ctx = AssetContext(
                tenant_id=request.tenant_id.value,
                run_id=request.run_id.value,
                task_id=request.task_id.value,
                partition_key=dict(request.partition_key.dimensions),
                callbacks=request.callbacks,
                variables=dict(request.variables),
                secrets=await self._resolve_secrets(request.secrets),
            )

            # 6. Load inputs
            inputs = await self._load_inputs(request.inputs)

            # 7. Execute user function
            output = await self._execute_with_timeout(
                func, ctx, inputs, timeout=request.timeout.ToTimedelta()
            )

            # 8. Run quality checks
            check_results = await self.check_runner.run_checks(
                output.data, request.checks
            )
            gating_decision = self._compute_gating(check_results)

            # 9. Write output (if not quarantined)
            materialization = None
            if gating_decision != GatingDecision.QUARANTINE:
                materialization = await self._write_output(
                    output, request.output
                )

            return TaskResult(
                task_id=request.task_id,
                run_id=request.run_id,
                tenant_id=request.tenant_id,
                idempotency_key=request.idempotency_key,
                attempt=request.attempt,
                outcome=TaskOutcome.TASK_OUTCOME_SUCCEEDED,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                materialization=materialization,
                check_results=check_results,
                gating_decision=gating_decision,
            )

        except TimeoutError:
            return self._build_error_result(
                request, started_at, TaskOutcome.TASK_OUTCOME_TIMED_OUT,
                "Task exceeded timeout"
            )
        except Exception as e:
            return self._build_error_result(
                request, started_at, TaskOutcome.TASK_OUTCOME_FAILED,
                str(e), traceback=traceback.format_exc()
            )
        finally:
            if self.heartbeat_task:
                self.heartbeat_task.cancel()

    def _validate_auth(self, envelope: TaskEnvelope):
        """Verify HMAC signature on envelope."""
        auth = envelope.auth

        # Check expiry
        if auth.expires_at.ToDatetime() < datetime.utcnow():
            raise AuthenticationError("Token expired")

        # Verify signature
        expected_sig = hmac_sha256(
            get_signing_key(auth.key_id),
            self._canonical_bytes(envelope),
        )
        if not hmac.compare_digest(auth.signature, expected_sig):
            raise AuthenticationError("Invalid signature")

    async def _heartbeat_loop(self, url: str):
        """Send heartbeats every 30 seconds."""
        while True:
            await asyncio.sleep(30)
            try:
                await self.callback_client.send_heartbeat(url)
            except Exception as e:
                # Log but don't fail - control plane will eventually time out
                logger.warning(f"Heartbeat failed: {e}")

    async def _resolve_secrets(
        self, secret_refs: dict[str, SecretRef]
    ) -> dict[str, str]:
        """Resolve secret references to values at runtime."""
        secrets = {}
        for name, ref in secret_refs.items():
            if ref.provider == "gcp-secret-manager":
                secrets[name] = await gcp_secret_manager.access(ref.path)
            elif ref.provider == "env":
                secrets[name] = os.environ[ref.path]
            else:
                raise ValueError(f"Unknown secret provider: {ref.provider}")
        return secrets
```

### 3.3 Heartbeat Protocol

```
┌──────────┐                              ┌──────────────┐
│  Worker  │                              │ Control Plane│
└────┬─────┘                              └──────┬───────┘
     │                                           │
     │  POST /heartbeat (every 30s)              │
     │──────────────────────────────────────────▶│
     │                                           │ Update tasks.last_heartbeat
     │                          200 OK           │
     │◀──────────────────────────────────────────│
     │                                           │
     │  ... task executing ...                   │
     │                                           │
     │  POST /heartbeat                          │
     │──────────────────────────────────────────▶│
     │                                           │
     │                                           │
     │  === Worker crashes ===                   │
     │                                           │
     │                                           │
     │                              ┌────────────┴────────────┐
     │                              │ Scheduler detects       │
     │                              │ heartbeat_timeout (5m)  │
     │                              │ → Mark FAILED           │
     │                              │ → Maybe retry           │
     │                              └─────────────────────────┘
```

**Heartbeat Configuration:**
```rust
struct HeartbeatConfig {
    /// How often workers should send heartbeats
    interval: Duration,           // Default: 30s

    /// How long before a missing heartbeat marks task failed
    timeout: Duration,            // Default: 5m (10 missed heartbeats)

    /// Grace period for network jitter
    grace_period: Duration,       // Default: 10s
}
```

### 3.4 Secret Resolution

**Principle:** Secrets are resolved at runtime by the worker, never stored in events or passed through the control plane.

```protobuf
message SecretRef {
  string provider = 1;    // "gcp-secret-manager", "aws-secrets-manager", "env"
  string path = 2;        // "projects/123/secrets/api-key/versions/latest"
  string version = 3;     // Optional: pin to specific version
}
```

**Resolution Flow:**
```
1. Control plane includes SecretRef in TaskRequest (NOT the value)
2. Worker receives TaskRequest with SecretRef
3. Worker uses Workload Identity to access Secret Manager
4. Secret value resolved at execution time, held only in memory
5. Secret value never logged, never in events, never persisted
```

### 3.5 Graceful Shutdown

Workers must handle SIGTERM gracefully to avoid task failures during deployments or scale-down:

```python
class WorkerExecutor:
    def __init__(self):
        self.shutting_down = False
        self.current_task: Optional[TaskRequest] = None

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown on SIGTERM/SIGINT."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.shutting_down = True

        # If currently executing a task, report interruption
        if self.current_task:
            asyncio.create_task(self._report_interrupted())

    async def _report_interrupted(self):
        """Report current task as interrupted for retry."""
        if self.current_task:
            await self.callback_client.report_status(
                self.current_task.callbacks.status_url,
                TaskOutcome.INTERRUPTED,
                reason="worker_shutdown",
                retry_eligible=True,
            )

    async def run(self):
        """Main worker loop."""
        while not self.shutting_down:
            task = await self.receive_task()
            if task:
                self.current_task = task
                await self.execute(task)
                self.current_task = None

        # Cancel heartbeat task on shutdown
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

        logger.info("Graceful shutdown complete")
```

**Shutdown Sequence:**
1. Cloud Run sends SIGTERM (10 second grace period by default)
2. Worker stops accepting new tasks
3. If task in progress, reports INTERRUPTED status (eligible for retry)
4. Cancels heartbeat loop
5. Exits cleanly

**Cloud Run Configuration:**
```yaml
spec:
  containers:
    - name: worker
      # Extend grace period for long-running tasks
      terminationGracePeriodSeconds: 300
```

---

## 4. Scheduler & Dispatcher Loop

### 4.1 Overview

The scheduler is a continuous loop that:
1. Evaluates task dependencies → moves PENDING → READY
2. Acquires quota → moves READY → QUEUED
3. Dispatches to Cloud Tasks → moves QUEUED → DISPATCHED
4. Detects zombies → marks RUNNING → FAILED
5. Processes retry timers → moves RETRY_WAIT → READY

### 4.2 Scheduler Deployment Model

**Decision: Always-On Scheduler (MVP)**

The scheduler runs as a dedicated Cloud Run service with `min_instances=1`. This is a deliberate trade-off:

| Model | Latency | Cost | Complexity |
|-------|---------|------|------------|
| **Always-On (MVP)** | Low (sub-second) | ~$50/month | Low |
| Tick-Based | Higher (seconds) | Lower | Medium |
| Event-Driven | Lowest | Lowest | High |

**Rationale:**
- Scheduler is lightweight (CPU-bound, no heavy I/O)
- Sub-second scheduling latency improves user experience
- Workers still scale to zero (where the real cost is)
- Simplifies leader election (single instance)

**Future Optimization Path:**
```
Phase 1 (MVP): Always-on scheduler, workers scale to zero
Phase 2: Tick-based scheduler via Cloud Scheduler (if cost is a concern)
Phase 3: Event-driven with Pub/Sub triggers (if scale demands)
```

**Leader Election (HA):**

```rust
impl Scheduler {
    pub async fn run_with_leader_election(&mut self) -> Result<()> {
        let mut lease = LeaderLease::new(&self.store, "servo-scheduler").await?;

        loop {
            if !lease.is_leader().await? {
                lease.wait_for_leadership().await?;
                continue;
            }

            lease.renew().await?;
            self.tick().await?;
        }
    }
}
```

**LeaderLease Implementation (Postgres-based):**

```rust
/// Distributed lease using Postgres advisory locks
pub struct LeaderLease {
    pool: PgPool,
    lock_key: i64,
    holder_id: String,
    lease_duration: Duration,
    last_renewal: Option<Instant>,
}

impl LeaderLease {
    pub async fn new(pool: &PgPool, name: &str) -> Result<Self> {
        // Convert name to stable lock key
        let lock_key = hash_to_i64(name);
        let holder_id = Uuid::new_v4().to_string();

        Ok(Self {
            pool: pool.clone(),
            lock_key,
            holder_id,
            lease_duration: Duration::from_secs(30),
            last_renewal: None,
        })
    }

    pub async fn is_leader(&self) -> Result<bool> {
        // Try to acquire advisory lock (non-blocking)
        let result: (bool,) = sqlx::query_as(
            "SELECT pg_try_advisory_lock($1)"
        )
        .bind(self.lock_key)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0)
    }

    pub async fn wait_for_leadership(&self) -> Result<()> {
        // Blocking acquire - waits until lock is available
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(self.lock_key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn renew(&mut self) -> Result<()> {
        // Postgres advisory locks auto-renew while connection is held
        // Just update our tracking
        self.last_renewal = Some(Instant::now());
        Ok(())
    }

    pub async fn release(&self) -> Result<()> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.lock_key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

impl Drop for LeaderLease {
    fn drop(&mut self) {
        // Advisory locks are automatically released when connection closes
        // This handles crash scenarios automatically
    }
}
```

**Why Postgres Advisory Locks:**
- No additional infrastructure (Redis, etcd, Zookeeper)
- Automatic release on connection drop (handles crashes)
- Simple, battle-tested primitive
- Works with existing connection pool

### 4.3 Scheduler Implementation

```rust
// crates/servo-runtime/src/scheduler.rs

pub struct Scheduler {
    store: Arc<dyn EventStore>,
    dispatcher: Arc<Dispatcher>,
    quota_manager: Arc<QuotaManager>,
    fairness: FairnessScheduler,
    config: SchedulerConfig,
}

impl Scheduler {
    pub async fn run(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.tick_interval);

        loop {
            interval.tick().await;

            // Process each phase
            self.evaluate_dependencies().await?;
            self.schedule_ready_tasks().await?;
            self.detect_zombies().await?;
            self.process_retry_timers().await?;
        }
    }

    /// Phase 1: Evaluate dependencies, move PENDING → READY
    async fn evaluate_dependencies(&self) -> Result<()> {
        let pending_tasks = self.store.find_tasks_by_state(TaskState::Pending).await?;

        for task in pending_tasks {
            let deps_satisfied = self.check_dependencies(&task).await?;

            if deps_satisfied {
                self.transition_task(
                    &task,
                    TaskState::Ready,
                    TransitionReason::DependenciesSatisfied,
                ).await?;
            }
        }

        Ok(())
    }

    /// Phase 2: Acquire quota, dispatch READY → QUEUED → DISPATCHED
    async fn schedule_ready_tasks(&self) -> Result<()> {
        // Get ready tasks grouped by tenant
        let ready_by_tenant = self.store.find_ready_tasks_by_tenant().await?;

        // Apply fairness scheduling
        while let Some(tenant_id) = self.fairness.select_next_tenant(&ready_by_tenant) {
            let tasks = ready_by_tenant.get(&tenant_id).unwrap();

            for task in tasks {
                // Try to acquire quota
                match self.quota_manager.try_acquire(&tenant_id).await {
                    Ok(permit) => {
                        // Move to QUEUED
                        self.transition_task(
                            task,
                            TaskState::Queued,
                            TransitionReason::QuotaAcquired,
                        ).await?;

                        // Dispatch to Cloud Tasks
                        let envelope = self.build_envelope(task).await?;
                        self.dispatcher.dispatch(envelope).await?;

                        // Move to DISPATCHED
                        self.transition_task(
                            task,
                            TaskState::Dispatched,
                            TransitionReason::Dispatched,
                        ).await?;
                    }
                    Err(QuotaExhausted) => {
                        // Leave in READY, try next tenant
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Phase 3: Detect zombie tasks (no heartbeat)
    async fn detect_zombies(&self) -> Result<()> {
        let running_tasks = self.store.find_tasks_by_state(TaskState::Running).await?;
        let now = Utc::now();

        for task in running_tasks {
            let heartbeat_deadline = task.last_heartbeat
                .unwrap_or(task.started_at.unwrap())
                + self.config.heartbeat_timeout;

            if now > heartbeat_deadline {
                // Zombie detected
                let retriable = task.attempt < task.max_attempts;

                if retriable {
                    self.transition_task(
                        &task,
                        TaskState::RetryWait,
                        TransitionReason::HeartbeatTimeout,
                    ).await?;
                } else {
                    self.transition_task(
                        &task,
                        TaskState::Failed,
                        TransitionReason::HeartbeatTimeout,
                    ).await?;
                }
            }
        }

        Ok(())
    }

    /// Phase 4: Process retry timers
    async fn process_retry_timers(&self) -> Result<()> {
        let retry_tasks = self.store.find_tasks_by_state(TaskState::RetryWait).await?;
        let now = Utc::now();

        for task in retry_tasks {
            if let Some(retry_not_before) = task.retry_not_before {
                if now >= retry_not_before {
                    self.transition_task(
                        &task,
                        TaskState::Ready,
                        TransitionReason::RetryTimerExpired,
                    ).await?;
                }
            }
        }

        Ok(())
    }

    /// Check if all dependencies are satisfied
    async fn check_dependencies(&self, task: &Task) -> Result<bool> {
        let run = self.store.get_run(&task.run_id).await?;
        let plan = self.store.get_plan(&run.plan_id).await?;

        let planned_task = plan.spec.tasks
            .iter()
            .find(|t| t.task_id == task.task_id)
            .ok_or_else(|| Error::TaskNotInPlan)?;

        for dep_id in &planned_task.depends_on {
            let dep_task = self.store.get_task(dep_id).await?;

            match dep_task.state {
                TaskState::Succeeded => continue,
                TaskState::Failed | TaskState::Skipped | TaskState::Cancelled => {
                    // Upstream failed - skip this task
                    self.transition_task(
                        task,
                        TaskState::Skipped,
                        TransitionReason::UpstreamFailed,
                    ).await?;
                    return Ok(false);
                }
                _ => return Ok(false), // Still waiting
            }
        }

        Ok(true)
    }
}
```

### 4.3 Dispatcher

```rust
// crates/servo-runtime/src/dispatcher.rs

pub struct Dispatcher {
    cloud_tasks: CloudTasksClient,
    callback_generator: CallbackUrlGenerator,
    config: DispatcherConfig,
}

impl Dispatcher {
    pub async fn dispatch(&self, task: &Task) -> Result<()> {
        // Build TaskRequest
        let request = self.build_task_request(task).await?;

        // Generate presigned callback URLs
        let callbacks = self.callback_generator.generate_callbacks(task);

        // Wrap in envelope with auth
        let envelope = TaskEnvelope {
            contract_version: "1.0".to_string(),
            auth: self.sign_envelope(&request),
            trace: self.extract_trace_context(),
            tenant_id: task.tenant_id.clone(),
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            body: Some(Body::Request(request)),
        };

        // Serialize to JSON
        let payload = serde_json::to_vec(&envelope)?;

        // Create Cloud Tasks task
        let cloud_task = CloudTask {
            http_request: HttpRequest {
                url: self.config.worker_url.clone(),
                http_method: HttpMethod::Post,
                body: payload,
                headers: hashmap! {
                    "Content-Type" => "application/json",
                    "X-Servo-Task-Id" => task.task_id.clone(),
                },
            },
            schedule_time: None, // Execute immediately
            dispatch_deadline: Some(self.config.dispatch_timeout),
        };

        // Submit to Cloud Tasks
        self.cloud_tasks
            .create_task(&self.config.queue_path, cloud_task)
            .await?;

        Ok(())
    }
}
```

---

## 5. Fairness & Quota Enforcement

### 5.1 Fairness Algorithm: Deficit Round-Robin

Multi-tenant fairness ensures no single tenant monopolizes resources:

```rust
// crates/servo-runtime/src/fairness.rs

pub struct FairnessScheduler {
    /// Deficit counter per tenant (higher = more starved)
    tenant_deficits: HashMap<TenantId, f64>,

    /// Weight per tenant (default 1.0, higher = more resources)
    tenant_weights: HashMap<TenantId, f64>,
}

impl FairnessScheduler {
    /// Select next tenant to schedule, returns None if no ready tenants
    pub fn select_next_tenant(
        &mut self,
        ready_by_tenant: &HashMap<TenantId, Vec<Task>>,
    ) -> Option<TenantId> {
        let ready_tenants: Vec<_> = ready_by_tenant.keys().collect();

        if ready_tenants.is_empty() {
            return None;
        }

        // Select tenant with highest deficit (most starved)
        let selected = ready_tenants
            .iter()
            .max_by(|a, b| {
                let deficit_a = self.tenant_deficits.get(*a).unwrap_or(&0.0);
                let deficit_b = self.tenant_deficits.get(*b).unwrap_or(&0.0);
                deficit_a.partial_cmp(deficit_b).unwrap()
            })
            .copied()?;

        // Update deficits
        let selected_weight = self.tenant_weights
            .get(&selected)
            .copied()
            .unwrap_or(1.0);

        // Decrease selected tenant's deficit
        *self.tenant_deficits.entry(selected.clone()).or_insert(0.0) -= 1.0;

        // Increase all other tenants' deficits proportional to weight
        for tenant in ready_tenants {
            if tenant != &selected {
                let weight = self.tenant_weights.get(tenant).copied().unwrap_or(1.0);
                *self.tenant_deficits.entry(tenant.clone()).or_insert(0.0) +=
                    weight / selected_weight;
            }
        }

        Some(selected.clone())
    }
}
```

### 5.2 Quota Types

```rust
pub struct TenantQuotas {
    /// Maximum concurrent tasks in RUNNING state
    pub max_concurrent_tasks: u32,          // Default: 100

    /// Maximum concurrent runs
    pub max_concurrent_runs: u32,           // Default: 10

    /// Maximum tasks queued (waiting for workers)
    pub max_queued_tasks: u32,              // Default: 1000

    /// Maximum runs per day (rate limiting)
    pub max_daily_runs: Option<u32>,        // Default: None (unlimited)

    /// Maximum run duration before timeout
    pub max_run_duration: Duration,         // Default: 24h

    /// Maximum tasks per run
    pub max_tasks_per_run: u32,             // Default: 10000

    /// Maximum backfill concurrency (runs)
    pub max_backfill_concurrency: u32,      // Default: 5
}
```

### 5.3 Quota Enforcement Points

| Quota | Enforcement Point | Effect |
|-------|-------------------|--------|
| `max_concurrent_tasks` | Scheduler (READY → QUEUED) | Task stays READY |
| `max_concurrent_runs` | TriggerRun API | Request rejected |
| `max_queued_tasks` | Scheduler (READY → QUEUED) | Task stays READY |
| `max_daily_runs` | TriggerRun API | Request rejected |
| `max_run_duration` | Run state machine | Run marked TIMED_OUT |
| `max_tasks_per_run` | Plan validation | Deploy rejected |
| `max_backfill_concurrency` | Backfill service | Backfill chunks wait |

### 5.4 Quota Manager

```rust
pub struct QuotaManager {
    store: Arc<dyn Store>,
    quotas: HashMap<TenantId, TenantQuotas>,
}

impl QuotaManager {
    pub async fn try_acquire(&self, tenant_id: &TenantId) -> Result<QuotaPermit, QuotaExhausted> {
        let quotas = self.quotas.get(tenant_id).unwrap_or(&DEFAULT_QUOTAS);

        // Check concurrent tasks
        let running_count = self.store
            .count_tasks_by_state(tenant_id, TaskState::Running)
            .await?;

        if running_count >= quotas.max_concurrent_tasks as i64 {
            return Err(QuotaExhausted::ConcurrentTasks);
        }

        // Check queued tasks
        let queued_count = self.store
            .count_tasks_by_state(tenant_id, TaskState::Queued)
            .await?;

        if queued_count >= quotas.max_queued_tasks as i64 {
            return Err(QuotaExhausted::QueuedTasks);
        }

        Ok(QuotaPermit { tenant_id: tenant_id.clone() })
    }
}
```

---

## 6. Quality Framework

### 6.1 Check Types

```protobuf
message CheckDefinition {
  string name = 1;
  CheckType type = 2;
  Severity severity = 3;           // INFO, WARNING, ERROR, CRITICAL
  CheckPhase phase = 4;            // PRE or POST

  oneof spec {
    RowCountCheck row_count = 10;
    NotNullCheck not_null = 11;
    UniqueCheck unique = 12;
    FreshnessCheck freshness = 13;
    SchemaCheck schema = 14;
    CustomSqlCheck custom_sql = 15;
    CustomPythonCheck custom_python = 16;
  }
}

enum CheckPhase {
  CHECK_PHASE_UNSPECIFIED = 0;
  CHECK_PHASE_PRE = 1;            // Before execution (validate inputs)
  CHECK_PHASE_POST = 2;           // After execution (validate outputs)
}

message RowCountCheck {
  optional int64 min = 1;
  optional int64 max = 2;
  optional double min_ratio = 3;  // vs previous materialization
}

message FreshnessCheck {
  string timestamp_column = 1;
  google.protobuf.Duration max_age = 2;
}
```

### 6.2 Check Execution

```python
# python/arco/servo/worker/checks.py

class CheckRunner:
    """Execute quality checks on data."""

    async def run_checks(
        self,
        data: Any,  # DataFrame, path, etc.
        check_specs: list[CheckSpec],
    ) -> list[CheckResult]:
        results = []

        for spec in check_specs:
            try:
                result = await self._run_check_with_timeout(data, spec)
                results.append(result)

                # Fail fast on CRITICAL
                if not result.passed and spec.severity == Severity.CRITICAL:
                    break

            except Exception as e:
                results.append(CheckResult(
                    check_name=spec.name,
                    passed=False,
                    severity=spec.severity,
                    error=str(e),
                ))

        return results

    async def _run_check_with_timeout(
        self,
        data: Any,
        spec: CheckSpec,
    ) -> CheckResult:
        """Run check with configurable timeout."""
        timeout_seconds = spec.timeout_seconds or 300  # Default 5 minutes

        try:
            return await asyncio.wait_for(
                self._run_check(data, spec),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            return CheckResult(
                check_name=spec.name,
                passed=False,
                severity=spec.severity,
                error=f"Check timed out after {timeout_seconds}s",
                metadata={"timeout": True},
            )

    async def _run_check(self, data: Any, spec: CheckSpec) -> CheckResult:
        if spec.type == CheckType.ROW_COUNT:
            return self._check_row_count(data, spec.row_count)
        elif spec.type == CheckType.NOT_NULL:
            return self._check_not_null(data, spec.not_null)
        elif spec.type == CheckType.UNIQUE:
            return self._check_unique(data, spec.unique)
        elif spec.type == CheckType.CUSTOM_SQL:
            return await self._check_custom_sql(data, spec.custom_sql)
        elif spec.type == CheckType.CUSTOM_PYTHON:
            return await self._check_custom_python(data, spec.custom_python)
        else:
            raise ValueError(f"Unknown check type: {spec.type}")

    def _check_row_count(self, data: Any, spec: RowCountCheck) -> CheckResult:
        count = len(data) if hasattr(data, '__len__') else data.count()

        passed = True
        if spec.min is not None and count < spec.min:
            passed = False
        if spec.max is not None and count > spec.max:
            passed = False

        return CheckResult(
            check_name="row_count",
            passed=passed,
            actual_value=count,
            expected_min=spec.min,
            expected_max=spec.max,
        )
```

### 6.3 Gating Semantics

```python
def compute_gating_decision(results: list[CheckResult]) -> GatingDecision:
    """
    Determine how check results affect downstream execution.

    Gating Decision Matrix:
    ┌──────────┬────────────┬─────────────────────────────────────┐
    │ Severity │ If Failed  │ Effect                              │
    ├──────────┼────────────┼─────────────────────────────────────┤
    │ CRITICAL │ BLOCK      │ Task fails, downstream skipped      │
    │ ERROR    │ WARN       │ Task succeeds, flag set on run      │
    │ WARNING  │ ALLOW      │ Task succeeds, warning logged       │
    │ INFO     │ ALLOW      │ Task succeeds, info logged          │
    └──────────┴────────────┴─────────────────────────────────────┘
    """
    critical_failed = any(
        r for r in results
        if not r.passed and r.severity == Severity.CRITICAL
    )
    error_failed = any(
        r for r in results
        if not r.passed and r.severity == Severity.ERROR
    )

    if critical_failed:
        return GatingDecision.BLOCK
    elif error_failed:
        return GatingDecision.WARN
    else:
        return GatingDecision.ALLOW
```

### 6.4 Quarantine Flow

When gating decision is `QUARANTINE` (configurable per-asset):

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Quarantine Flow                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. Check fails with BLOCK + quarantine_enabled=true                   │
│                                                                          │
│   2. Output written to quarantine path:                                 │
│      gs://bucket/assets/{asset_id}/quarantine/{run_id}/data.parquet    │
│                                                                          │
│   3. Downstream tasks see PREVIOUS GOOD materialization                 │
│      (not the quarantined output)                                       │
│                                                                          │
│   4. Alert sent to asset owners                                         │
│                                                                          │
│   5. Manual review required:                                            │
│      - servo quarantine approve {asset_id} {run_id}  → Promote output   │
│      - servo quarantine reject {asset_id} {run_id}   → Delete output    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Backfill Orchestration

### 7.1 Backfill Model

Large backfills are split into multiple runs for manageability:

```protobuf
service BackfillService {
  rpc CreateBackfill(CreateBackfillRequest) returns (CreateBackfillResponse);
  rpc GetBackfill(GetBackfillRequest) returns (GetBackfillResponse);
  rpc ListBackfills(ListBackfillsRequest) returns (ListBackfillsResponse);
  rpc PauseBackfill(PauseBackfillRequest) returns (PauseBackfillResponse);
  rpc ResumeBackfill(ResumeBackfillRequest) returns (ResumeBackfillResponse);
  rpc CancelBackfill(CancelBackfillRequest) returns (CancelBackfillResponse);
}

message Backfill {
  BackfillId id = 1;
  TenantId tenant_id = 2;
  WorkspaceId workspace_id = 3;

  repeated AssetKey targets = 10;
  PartitionRange range = 11;

  BackfillState state = 20;
  BackfillProgress progress = 21;
  BackfillConfig config = 22;

  repeated RunId run_ids = 30;      // Child runs

  google.protobuf.Timestamp created_at = 40;
  google.protobuf.Timestamp started_at = 41;
  google.protobuf.Timestamp completed_at = 42;
}

enum BackfillState {
  BACKFILL_STATE_UNSPECIFIED = 0;
  BACKFILL_STATE_PENDING = 1;
  BACKFILL_STATE_RUNNING = 2;
  BACKFILL_STATE_PAUSED = 3;
  BACKFILL_STATE_SUCCEEDED = 4;
  BACKFILL_STATE_FAILED = 5;
  BACKFILL_STATE_CANCELLED = 6;
}

message BackfillProgress {
  int64 total_partitions = 1;
  int64 completed_partitions = 2;
  int64 failed_partitions = 3;
  int64 skipped_partitions = 4;
  int64 pending_partitions = 5;
}

message BackfillConfig {
  int32 chunk_size = 1;             // Partitions per run (default: 100)
  int32 max_concurrent_runs = 2;    // Parallel runs (default: 3)
  bool stop_on_failure = 3;         // Halt backfill on first failure
  FailureThreshold failure_threshold = 4;  // e.g., "10%" or "100 partitions"
}
```

### 7.2 Backfill Chunking

```
Backfill: 365 days × 10 tenants = 3,650 partitions

With chunk_size=100, max_concurrent_runs=3:

┌─────────────────────────────────────────────────────────────────────────┐
│ Time ──────────────────────────────────────────────────────────────────▶│
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│ Run 1:  [Chunk 1: 100 partitions] ████████████                          │
│ Run 2:  [Chunk 2: 100 partitions] ████████████                          │
│ Run 3:  [Chunk 3: 100 partitions] ████████████                          │
│                                           │                              │
│                                    Run 1 completes                       │
│                                           ▼                              │
│ Run 4:  [Chunk 4: 100 partitions]         ████████████                  │
│                                                                          │
│ ... continues until all 37 runs complete ...                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Backfill CLI

```bash
# Create backfill
servo backfill create user_metrics \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-size 50 \
  --max-parallel 5

# Monitor progress
servo backfill status bf-abc123

# Pause/resume
servo backfill pause bf-abc123
servo backfill resume bf-abc123

# Retry failed partitions only
servo backfill retry bf-abc123 --failed-only

# Cancel
servo backfill cancel bf-abc123
```

---

## 8. Error Handling

### 8.1 Error Categories

| Category | Examples | Retriable | Default Action |
|----------|----------|-----------|----------------|
| **Transient** | Network timeout, rate limit, 503 | Yes | Exponential backoff |
| **Infrastructure** | OOM, disk full, crash | Maybe | Alert + maybe retry |
| **User Code** | Python exception, assertion | No* | Fail task |
| **Input** | Missing upstream, bad schema | No | Skip or fail |
| **Configuration** | Invalid asset definition | No | Block deploy |
| **Timeout** | Exceeded deadline | Yes | Retry with same timeout |
| **Cancelled** | User requested cancellation | No | Mark cancelled |

*User code errors can be marked retriable via `@asset(retry_on=[ValueError])`

### 8.2 Retry Policy

```protobuf
message RetryPolicy {
  uint32 max_attempts = 1;              // Default: 3
  google.protobuf.Duration initial_delay = 2;   // Default: 1m
  google.protobuf.Duration max_delay = 3;       // Default: 1h
  double backoff_multiplier = 4;        // Default: 2.0

  repeated string retry_on_errors = 10; // Error types to retry
  repeated string no_retry_on_errors = 11; // Error types to NOT retry
}
```

**Backoff Calculation:**
```rust
fn compute_retry_delay(attempt: u32, policy: &RetryPolicy) -> Duration {
    let delay_secs = policy.initial_delay.as_secs_f64()
        * policy.backoff_multiplier.powi((attempt - 1) as i32);

    Duration::from_secs_f64(delay_secs.min(policy.max_delay.as_secs_f64()))
}

// Example: initial=1m, multiplier=2, max=1h
// Attempt 1: fail → wait 1m
// Attempt 2: fail → wait 2m
// Attempt 3: fail → wait 4m
// Attempt 4: fail → wait 8m
// ...
// Attempt 7+: fail → wait 1h (capped)
```

### 8.3 Structured Errors

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct ServoError {
    /// Error code for programmatic handling
    pub code: ErrorCode,

    /// Human-readable message
    pub message: String,

    /// Structured details for debugging
    pub details: Option<serde_json::Value>,

    /// Whether this error is retriable
    pub retriable: bool,

    /// Suggested action
    pub suggested_action: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ErrorCode {
    // Transient (retriable)
    NetworkError,
    RateLimited,
    ServiceUnavailable,
    Timeout,

    // Infrastructure
    OutOfMemory,
    DiskFull,
    WorkerCrash,

    // User code
    UserCodeException,
    ImportError,
    CheckFailed,

    // Input
    MissingInput,
    SchemaViolation,
    DataCorruption,

    // Configuration
    InvalidManifest,
    InvalidPartition,
    CycleDetected,

    // System
    InternalError,
    NotImplemented,
}
```

### 8.4 Error Propagation

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Error Propagation                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│   Worker Exception                                                        │
│        │                                                                  │
│        ▼                                                                  │
│   TaskResult { outcome: FAILED, error: ServoError { ... } }              │
│        │                                                                  │
│        ▼                                                                  │
│   Control Plane receives callback                                         │
│        │                                                                  │
│        ├── If retriable && attempt < max_attempts                        │
│        │       → Emit TaskRetrying event                                 │
│        │       → State: RUNNING → RETRY_WAIT                             │
│        │       → Schedule retry after backoff                            │
│        │                                                                  │
│        └── Else (terminal failure)                                       │
│                → Emit TaskFailed event                                   │
│                → State: RUNNING → FAILED                                 │
│                → Mark dependent tasks: → SKIPPED                         │
│                → Update run state if all tasks terminal                  │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Observability

### 9.1 Metrics (Prometheus)

```yaml
# Task metrics
servo_task_total:
  type: counter
  labels: [tenant_id, workspace_id, asset_key, state, outcome]
  help: "Total task state transitions"

servo_task_duration_seconds:
  type: histogram
  labels: [tenant_id, asset_key]
  buckets: [1, 5, 15, 60, 300, 900, 3600]
  help: "Task execution duration"

servo_task_queue_wait_seconds:
  type: histogram
  labels: [tenant_id]
  buckets: [0.1, 0.5, 1, 5, 15, 60, 300]
  help: "Time from READY to RUNNING"

servo_task_attempts_total:
  type: counter
  labels: [tenant_id, asset_key, attempt_number]
  help: "Task attempts distribution"

# Run metrics
servo_run_total:
  type: counter
  labels: [tenant_id, workspace_id, trigger_type, state]
  help: "Total run state transitions"

servo_run_duration_seconds:
  type: histogram
  labels: [tenant_id, workspace_id]
  buckets: [60, 300, 900, 3600, 7200, 14400, 28800]
  help: "Run execution duration"

# Queue metrics
servo_queue_depth:
  type: gauge
  labels: [tenant_id, state]
  help: "Number of tasks in each state"

# Quota metrics
servo_quota_usage_ratio:
  type: gauge
  labels: [tenant_id, quota_type]
  help: "Current quota utilization (0-1)"

# Quality metrics
servo_check_total:
  type: counter
  labels: [tenant_id, asset_key, check_name, passed]
  help: "Quality check executions"

# Worker metrics
servo_worker_cold_start_seconds:
  type: histogram
  labels: [artifact_type]
  buckets: [0.5, 1, 2, 5, 10, 30, 60]
  help: "Worker cold start time"
```

### 9.2 Tracing (OpenTelemetry)

```
Trace: run-abc123
│
├── Span: servo.plan_run (2ms)
│   └── attributes: { tenant_id, workspace_id, trigger_type }
│
├── Span: servo.start_run (5ms)
│   └── attributes: { run_id, plan_id, tasks_count }
│
├── Span: servo.scheduler_tick (50ms)
│   ├── Span: evaluate_dependencies (10ms)
│   ├── Span: schedule_ready_tasks (30ms)
│   └── Span: detect_zombies (10ms)
│
├── Span: servo.dispatch_task (15ms)
│   └── attributes: { task_id, asset_key, partition_key }
│
├── Span: servo.worker.execute (45s)  [worker process]
│   ├── Span: load_artifact (2s)
│   ├── Span: load_inputs (5s)
│   ├── Span: execute_function (35s)
│   │   └── [user code spans if instrumented]
│   ├── Span: run_checks (2s)
│   │   ├── Span: check.row_count (100ms)
│   │   └── Span: check.not_null (200ms)
│   └── Span: write_output (1s)
│
└── Span: servo.finalize_run (10ms)
    └── attributes: { outcome, tasks_succeeded, tasks_failed }
```

**Trace Context Propagation:**
```rust
// Control plane injects trace context into TaskEnvelope
impl TraceContext {
    fn from_current_span() -> Self {
        let span = tracing::Span::current();
        TraceContext {
            traceparent: extract_traceparent(&span),
            tracestate: extract_tracestate(&span),
            baggage: extract_baggage(&span),
        }
    }
}

// Worker extracts and continues trace
fn continue_trace(trace: &TraceContext) -> tracing::Span {
    let parent_context = parse_traceparent(&trace.traceparent);
    tracing::info_span!(
        "servo.worker.execute",
        parent: parent_context,
        task_id = %task_id,
    )
}
```

### 9.3 Logging

**Structured Log Format:**
```json
{
  "timestamp": "2025-01-15T10:30:00.123Z",
  "level": "INFO",
  "message": "Task state transition",
  "target": "servo_runtime::scheduler",
  "tenant_id": "tenant-123",
  "workspace_id": "ws-456",
  "run_id": "run-789",
  "task_id": "task-abc",
  "asset_key": "analytics/user_metrics",
  "partition_key": {"date": "2025-01-15"},
  "from_state": "PENDING",
  "to_state": "READY",
  "reason": "DependenciesSatisfied",
  "trace_id": "abc123def456",
  "span_id": "789xyz"
}
```

### 9.4 Alerting Rules

```yaml
groups:
  - name: servo_critical
    rules:
      # High failure rate
      - alert: ServoHighTaskFailureRate
        expr: |
          rate(servo_task_total{outcome="FAILED"}[5m])
          / rate(servo_task_total[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High task failure rate for {{ $labels.tenant_id }}"

      # Stuck tasks (no state change)
      - alert: ServoStuckTasks
        expr: |
          servo_queue_depth{state="RUNNING"} > 0
          and changes(servo_task_total{state="RUNNING"}[30m]) == 0
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Tasks stuck in RUNNING state"

      # Queue backup
      - alert: ServoQueueBacklog
        expr: servo_queue_depth{state="READY"} > 1000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Large backlog of READY tasks"

      # Worker cold starts
      - alert: ServoSlowColdStarts
        expr: |
          histogram_quantile(0.95,
            rate(servo_worker_cold_start_seconds_bucket[5m])
          ) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "P95 worker cold start exceeds 30s"

      # Quota exhaustion
      - alert: ServoQuotaExhausted
        expr: servo_quota_usage_ratio > 0.95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Quota nearly exhausted for {{ $labels.tenant_id }}"
```

### 9.5 SLI/SLO Definitions

Define Service Level Indicators (SLIs) and Objectives (SLOs) for operational contracts:

```yaml
# slo.yaml - SLO definitions
slos:
  - name: task_success_rate
    description: "Percentage of tasks that succeed (excluding skipped)"
    target: 99.5%
    window: 7d
    sli:
      query: |
        sum(rate(servo_task_total{outcome="SUCCEEDED"}[7d]))
        / sum(rate(servo_task_total{outcome!="SKIPPED"}[7d]))
    alert_burn_rate:
      fast: 14.4  # Alert if burning 14.4x budget in 1h
      slow: 6.0   # Alert if burning 6x budget in 6h

  - name: run_latency_p99
    description: "99th percentile run duration"
    target: 2h
    window: 7d
    sli:
      query: |
        histogram_quantile(0.99,
          sum(rate(servo_run_duration_seconds_bucket[7d])) by (le)
        )

  - name: api_availability
    description: "API success rate (non-5xx responses)"
    target: 99.9%
    window: 30d
    sli:
      query: |
        sum(rate(http_requests_total{status!~"5.."}[30d]))
        / sum(rate(http_requests_total[30d]))

  - name: scheduling_latency_p95
    description: "Time from READY to DISPATCHED (p95)"
    target: 5s
    window: 7d
    sli:
      query: |
        histogram_quantile(0.95,
          sum(rate(servo_scheduling_latency_seconds_bucket[7d])) by (le)
        )

  - name: heartbeat_reliability
    description: "Percentage of tasks with healthy heartbeats"
    target: 99.9%
    window: 7d
    sli:
      query: |
        1 - (
          sum(rate(servo_task_total{outcome="FAILED", reason="heartbeat_timeout"}[7d]))
          / sum(rate(servo_task_total[7d]))
        )
```

**SLO Dashboard Queries:**

| SLO | Current | Target | Error Budget |
|-----|---------|--------|--------------|
| Task Success | `query:task_success_sli` | 99.5% | `query:task_success_budget` |
| Run Latency P99 | `query:run_latency_p99` | 2h | N/A |
| API Availability | `query:api_availability_sli` | 99.9% | `query:api_budget` |

---

## 10. Security & RBAC

### 10.1 Authentication

| Component | Method | Details |
|-----------|--------|---------|
| **External API** | OIDC / API Key | User-facing requests |
| **Worker → Control Plane** | Presigned URLs | Short-lived, task-specific |
| **Control Plane → Worker** | HMAC signature | Envelope authentication |
| **Internal services** | mTLS | Service mesh (optional) |

### 10.2 Authorization (RBAC)

```protobuf
enum Permission {
  PERMISSION_UNSPECIFIED = 0;

  // Asset management
  PERMISSION_ASSETS_READ = 10;
  PERMISSION_ASSETS_DEPLOY = 11;
  PERMISSION_ASSETS_DELETE = 12;

  // Run management
  PERMISSION_RUNS_READ = 20;
  PERMISSION_RUNS_TRIGGER = 21;
  PERMISSION_RUNS_CANCEL = 22;
  PERMISSION_RUNS_RETRY = 23;

  // Backfill management
  PERMISSION_BACKFILLS_CREATE = 30;
  PERMISSION_BACKFILLS_MANAGE = 31;

  // Admin
  PERMISSION_QUOTAS_MANAGE = 90;
  PERMISSION_ADMIN = 100;
}

message Role {
  string name = 1;
  repeated Permission permissions = 2;
}
```

**Predefined Roles:**

| Role | Permissions | Use Case |
|------|-------------|----------|
| `viewer` | `*_READ` | Dashboards, monitoring |
| `operator` | `viewer` + `RUNS_TRIGGER`, `RUNS_CANCEL` | On-call, manual runs |
| `developer` | `operator` + `ASSETS_DEPLOY`, `BACKFILLS_*` | Deploy and backfill |
| `admin` | All | Full control |

### 10.3 Worker Authentication

```rust
/// Verify HMAC signature on incoming TaskEnvelope
fn verify_envelope(envelope: &TaskEnvelope) -> Result<()> {
    let auth = &envelope.auth;

    // 1. Check expiry
    if auth.expires_at < Utc::now() {
        return Err(AuthError::Expired);
    }

    // 2. Get signing key by key_id (enables rotation)
    let key = get_signing_key(&auth.key_id)?;

    // 3. Compute expected signature over canonical bytes
    let canonical = canonical_bytes(envelope);
    let expected = hmac_sha256(&key, &canonical);

    // 4. Constant-time comparison
    if !constant_time_eq(&auth.signature, &expected) {
        return Err(AuthError::InvalidSignature);
    }

    Ok(())
}
```

### 10.4 Key Rotation

```rust
struct SigningKeyManager {
    keys: HashMap<String, SigningKey>,
    current_key_id: String,
}

impl SigningKeyManager {
    /// Sign with current key
    fn sign(&self, data: &[u8]) -> AuthMaterial {
        let key = &self.keys[&self.current_key_id];
        AuthMaterial {
            key_id: self.current_key_id.clone(),
            signature: hmac_sha256(&key.secret, data),
            expires_at: Utc::now() + Duration::hours(2),
            nonce: generate_nonce(),
        }
    }

    /// Verify with any valid key (supports rotation)
    fn verify(&self, auth: &AuthMaterial, data: &[u8]) -> Result<()> {
        let key = self.keys.get(&auth.key_id)
            .ok_or(AuthError::UnknownKey)?;

        if key.expires_at < Utc::now() {
            return Err(AuthError::KeyExpired);
        }

        // ... verify signature
    }
}
```

### 10.5 Secrets Policy

**Invariants:**
1. Secrets **never** stored in events, logs, or task payloads
2. `SecretRef` contains only provider + path, never values
3. Workers resolve secrets at runtime via Workload Identity
4. Audit log records secret access (path only, not values)
5. Secrets never appear in error messages or stack traces

---

## 11. Testing Strategy

### 11.1 Test Pyramid

```
                    ┌─────────────┐
                    │    E2E      │  Few, slow, high confidence
                    │   Tests     │
                    └─────────────┘
               ┌────────────────────────┐
               │   Integration Tests    │  Cross-boundary
               │   (API, Storage, Queue)│
               └────────────────────────┘
          ┌─────────────────────────────────┐
          │         Unit Tests              │  Fast, isolated
          │ (Planner, Scheduler, StateMachine)│
          └─────────────────────────────────┘
```

### 11.2 Unit Tests

```rust
// crates/servo-planner/src/planner_test.rs

#[test]
fn test_plan_is_deterministic() {
    let manifest = load_fixture("complex_dag.json");
    let snapshot = MetadataSnapshot::default();

    let plan1 = Planner::new().plan(&manifest, &snapshot);
    let plan2 = Planner::new().plan(&manifest, &snapshot);

    assert_eq!(plan1.fingerprint, plan2.fingerprint);
    assert_eq!(plan1.spec, plan2.spec);
}

#[test]
fn test_cycle_detection() {
    let manifest = ManifestBuilder::new()
        .asset("a", deps: ["b"])
        .asset("b", deps: ["c"])
        .asset("c", deps: ["a"])  // Cycle!
        .build();

    let result = Planner::new().plan(&manifest, &snapshot);

    assert!(matches!(result, Err(PlanError::CycleDetected(_))));
}

#[test]
fn test_partition_expansion() {
    let manifest = ManifestBuilder::new()
        .asset("metrics", partitions: DailyPartition("date"))
        .build();

    let request = ExecutionRequest {
        partition_range: PartitionRange::date_range("2025-01-01", "2025-01-03"),
    };

    let plan = Planner::new().plan_with_request(&manifest, &snapshot, &request)?;

    assert_eq!(plan.spec.tasks.len(), 3);  // 3 days
}
```

### 11.3 Integration Tests

```rust
// tests/integration/scheduler_test.rs

#[tokio::test]
async fn test_full_run_lifecycle() {
    let harness = TestHarness::new().await;

    // Deploy manifest
    let manifest = load_fixture("simple_dag.json");
    harness.deploy(manifest).await.unwrap();

    // Trigger run
    let run_id = harness.trigger_run("asset_c", None).await.unwrap();

    // Wait for completion (with timeout)
    let run = harness
        .wait_for_terminal_state(run_id, Duration::from_secs(60))
        .await
        .unwrap();

    // Verify
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.tasks_succeeded, 3);  // a → b → c
    assert_eq!(run.tasks_failed, 0);
}

#[tokio::test]
async fn test_partial_failure_continues() {
    let harness = TestHarness::new().await;
    harness.deploy(load_fixture("diamond_dag.json")).await.unwrap();

    // Inject failure for task "b"
    harness.inject_failure("b", "simulated failure");

    // Trigger run (a → b, c → d)
    let run_id = harness.trigger_run("d", None).await.unwrap();
    let run = harness.wait_for_terminal(run_id).await.unwrap();

    // "a" succeeded, "b" failed, "c" succeeded, "d" skipped (dep on b failed)
    assert_eq!(run.state, RunState::Failed);
    assert_eq!(run.tasks_succeeded, 2);  // a, c
    assert_eq!(run.tasks_failed, 1);      // b
    assert_eq!(run.tasks_skipped, 1);     // d
}
```

### 11.4 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    /// Any valid manifest produces a valid plan
    #[test]
    fn plan_always_valid(manifest in arbitrary_manifest()) {
        let result = Planner::new().plan(&manifest, &MetadataSnapshot::default());

        match result {
            Ok(plan) => {
                // Plan has expected properties
                prop_assert!(!plan.fingerprint.is_empty());
                prop_assert!(plan.spec.tasks.len() <= manifest.assets.len() * 100);
            }
            Err(PlanError::CycleDetected(_)) => {
                // Cycles are a valid rejection
            }
            Err(e) => {
                prop_assert!(false, "Unexpected error: {:?}", e);
            }
        }
    }

    /// State machine transitions are always valid
    #[test]
    fn state_transitions_valid(
        initial in arbitrary_task_state(),
        event in arbitrary_task_event(),
    ) {
        let result = StateMachine::transition(initial, &event);

        match result {
            Ok(next) => {
                // Verify transition is in allowed set
                prop_assert!(ALLOWED_TRANSITIONS.contains(&(initial, next)));
            }
            Err(TransitionError::Invalid) => {
                // Invalid transitions are rejected
                prop_assert!(!ALLOWED_TRANSITIONS.contains(&(initial, event.target_state())));
            }
        }
    }
}
```

### 11.5 Load Tests

```yaml
# k6 load test configuration
scenarios:
  steady_state:
    executor: constant-vus
    vus: 50
    duration: 10m
    exec: triggerAndWaitRun

  burst:
    executor: ramping-vus
    startVUs: 0
    stages:
      - duration: 30s, target: 100
      - duration: 1m, target: 100
      - duration: 30s, target: 0
    exec: triggerRun

thresholds:
  http_req_duration: [p(95) < 500]
  http_req_failed: [rate < 0.01]
  servo_run_duration: [p(95) < 300000]  # 5 minutes
```

---

## 12. Deployment Architecture

### 12.1 GCP Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            GCP Project                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Cloud Load Balancer                       │   │
│  │                        (HTTPS, API Gateway)                      │   │
│  └────────────────────────────────┬────────────────────────────────┘   │
│                                   │                                     │
│                                   ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         Cloud Run                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │   │
│  │  │  API        │  │  Scheduler  │  │  Worker (auto-scaled)   │  │   │
│  │  │  (2+ inst)  │  │  (1 inst)   │  │  (0-100 instances)      │  │   │
│  │  └──────┬──────┘  └──────┬──────┘  └────────────┬────────────┘  │   │
│  └─────────┼────────────────┼──────────────────────┼───────────────┘   │
│            │                │                      │                    │
│            └────────────────┼──────────────────────┘                    │
│                             │                                           │
│                             ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Cloud SQL                                 │   │
│  │  ┌─────────────┐    ┌─────────────┐                             │   │
│  │  │  PgBouncer  │───▶│  PostgreSQL │                             │   │
│  │  │  (Sidecar)  │    │  (HA, 2CPU) │                             │   │
│  │  └─────────────┘    └─────────────┘                             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐     │
│  │  Cloud Tasks    │  │  Cloud Storage  │  │  Secret Manager     │     │
│  │  (Task Queue)   │  │  (Artifacts)    │  │  (Secrets)          │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘     │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐     │
│  │  Cloud          │  │  Artifact       │  │  Cloud Monitoring   │     │
│  │  Scheduler      │  │  Registry       │  │  + Logging          │     │
│  │  (Cron)         │  │  (Containers)   │  │                     │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Terraform Modules

```hcl
# terraform/modules/servo/main.tf

module "servo" {
  source = "./modules/servo"

  project_id   = var.project_id
  region       = "us-central1"
  environment  = "production"

  # Database
  db_instance_name = "servo-postgres"
  db_tier          = "db-custom-2-8192"
  db_ha_enabled    = true
  db_backup_enabled = true

  # API Service
  api_min_instances = 2
  api_max_instances = 10
  api_cpu           = "1"
  api_memory        = "1Gi"

  # Scheduler Service
  scheduler_min_instances = 1
  scheduler_max_instances = 1
  scheduler_cpu           = "1"
  scheduler_memory        = "2Gi"

  # Worker Service
  worker_min_instances = 0
  worker_max_instances = 100
  worker_cpu           = "2"
  worker_memory        = "4Gi"
  worker_timeout       = 3600  # 1 hour max

  # Storage
  artifact_bucket     = "servo-artifacts-${var.project_id}"

  # Networking
  vpc_connector = google_vpc_access_connector.main.id
}
```

### 12.3 Resource Sizing

| Component | CPU | Memory | Min Instances | Max Instances |
|-----------|-----|--------|---------------|---------------|
| API | 1 | 1Gi | 2 | 10 |
| Scheduler | 1 | 2Gi | 1 | 1 |
| Worker | 2 | 4Gi | 0 | 100 |
| PostgreSQL | 2 | 8Gi | 1 (HA: 2) | - |
| PgBouncer | 0.5 | 256Mi | 1 | - |

### 12.4 Environment Promotion Strategy

**Environment Hierarchy:**

| Environment | Purpose | Promotion Trigger | Data |
|-------------|---------|-------------------|------|
| `dev` | Development testing | Automatic on PR merge | Synthetic |
| `staging` | Integration testing | Manual approval | Anonymized prod copy |
| `production` | Live traffic | Manual approval + canary | Real |

**Promotion Pipeline:**

```yaml
# .github/workflows/deploy.yaml
name: Deploy

on:
  push:
    branches: [main]
  workflow_dispatch:
    inputs:
      environment:
        required: true
        type: choice
        options: [staging, production]

jobs:
  deploy-dev:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - run: ./deploy.sh dev

  deploy-staging:
    if: github.event.inputs.environment == 'staging'
    runs-on: ubuntu-latest
    environment: staging  # Requires approval
    steps:
      - uses: actions/checkout@v4
      - run: ./deploy.sh staging
      - run: ./integration-tests.sh staging

  deploy-production:
    if: github.event.inputs.environment == 'production'
    runs-on: ubuntu-latest
    environment: production  # Requires approval
    steps:
      - uses: actions/checkout@v4
      - run: ./deploy.sh production --canary 10
      - run: ./monitor-canary.sh 15m
      - run: ./deploy.sh production --promote
```

**Canary Deployment Process:**

```
1. Deploy new version to 10% of workers (--canary 10)
2. Monitor error rate for 15 minutes
3. If error_rate < 1%: promote to 100%
4. If error_rate >= 1%: automatic rollback

Metrics watched during canary:
- servo_task_total{outcome="FAILED"} / servo_task_total
- http_request_duration_seconds (p95)
- servo_worker_errors_total
```

**Rollback Procedure:**

```bash
# Immediate rollback to previous version
./deploy.sh production --rollback

# Rollback to specific version
./deploy.sh production --rollback v1.2.3

# Rollback with traffic drain (graceful)
./deploy.sh production --rollback --drain-timeout 5m
```

---

## 13. API Versioning & Evolution

### 13.1 Versioning Strategy

```
Package naming:  servo.v1, servo.v2
Service naming:  servo.v1.ExecutionService
HTTP paths:      /v1/runs, /v2/runs
```

### 13.2 Compatibility Rules

| Change Type | Backward Compatible? | Action |
|-------------|---------------------|--------|
| Add field | Yes | Safe |
| Add enum value | Yes | Safe |
| Add RPC method | Yes | Safe |
| Remove field | No | Deprecate first, remove after 12 months |
| Change field type | No | Never (add new field instead) |
| Rename field | No | Never (add alias if needed) |
| Remove enum value | No | Deprecate, never remove |

### 13.3 Proto Evolution Policy

```protobuf
// Deprecated fields use reserved numbers and names
message SomeMessage {
  string current_field = 1;

  reserved 2, 3;  // Previously used field numbers
  reserved "old_field", "legacy_field";  // Previously used names

  // New fields always get new numbers
  string new_field = 10;
}

// Deprecated RPCs include deprecation notice
service SomeService {
  // Deprecated: Use NewMethod instead. Will be removed 2026-01-01.
  rpc OldMethod(OldRequest) returns (OldResponse) {
    option deprecated = true;
  }

  rpc NewMethod(NewRequest) returns (NewResponse);
}
```

### 13.4 Buf Configuration

```yaml
# buf.yaml
version: v1
name: buf.build/arco/servo
breaking:
  use:
    - FILE  # Strictest breaking change detection
lint:
  use:
    - DEFAULT
    - COMMENTS
  except:
    - PACKAGE_VERSION_SUFFIX
```

---

## 14. Additional Storage Schema

These tables complement the core schema from Part 1:

```sql
-- Heartbeat tracking (separate from events for performance)
CREATE TABLE task_heartbeats (
    task_id         TEXT PRIMARY KEY REFERENCES tasks(task_id),
    tenant_id       TEXT NOT NULL,
    last_heartbeat  TIMESTAMPTZ NOT NULL,
    worker_id       TEXT,
    worker_metadata JSONB
);

CREATE INDEX idx_heartbeats_tenant_time
    ON task_heartbeats(tenant_id, last_heartbeat);

-- Asset state tracking (latest materialization + health)
CREATE TABLE asset_states (
    tenant_id               TEXT NOT NULL,
    workspace_id            TEXT NOT NULL,
    asset_key               TEXT NOT NULL,

    -- Latest successful materialization
    last_materialization_id TEXT,
    last_materialization_at TIMESTAMPTZ,
    last_partition_key      JSONB,

    -- Health metrics
    consecutive_failures    INT NOT NULL DEFAULT 0,
    last_failure_at         TIMESTAMPTZ,
    last_success_at         TIMESTAMPTZ,

    -- Quality summary
    last_check_passed       BOOLEAN,
    last_gating_decision    TEXT,

    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, workspace_id, asset_key)
);

-- Materialization history
CREATE TABLE materializations (
    materialization_id  TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    asset_key           TEXT NOT NULL,
    partition_key       JSONB,

    run_id              TEXT NOT NULL,
    task_id             TEXT NOT NULL,

    output_uri          TEXT NOT NULL,
    output_format       TEXT NOT NULL,
    row_count           BIGINT,
    byte_size           BIGINT,
    schema_hash         TEXT,

    -- Quality
    checks_passed       INT NOT NULL DEFAULT 0,
    checks_failed       INT NOT NULL DEFAULT 0,
    gating_decision     TEXT NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL,

    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX idx_materializations_asset
    ON materializations(tenant_id, workspace_id, asset_key, created_at DESC);

-- Idempotency keys (API-level deduplication)
CREATE TABLE idempotency_keys (
    tenant_id       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,

    request_hash    TEXT NOT NULL,
    response        BYTEA,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX idx_idempotency_expires
    ON idempotency_keys(expires_at);

-- Backfill tracking
CREATE TABLE backfills (
    backfill_id     TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,

    targets         JSONB NOT NULL,      -- Array of asset keys
    partition_range JSONB NOT NULL,

    state           TEXT NOT NULL,

    -- Progress
    total_partitions      INT NOT NULL,
    completed_partitions  INT NOT NULL DEFAULT 0,
    failed_partitions     INT NOT NULL DEFAULT 0,
    skipped_partitions    INT NOT NULL DEFAULT 0,

    -- Config
    chunk_size            INT NOT NULL DEFAULT 100,
    max_concurrent_runs   INT NOT NULL DEFAULT 3,

    created_at      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,

    last_event_seq  BIGINT NOT NULL
);

CREATE TABLE backfill_runs (
    backfill_id     TEXT NOT NULL REFERENCES backfills(backfill_id),
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    chunk_index     INT NOT NULL,

    PRIMARY KEY (backfill_id, run_id)
);

-- Quotas configuration
CREATE TABLE tenant_quotas (
    tenant_id               TEXT PRIMARY KEY,

    max_concurrent_tasks    INT NOT NULL DEFAULT 100,
    max_concurrent_runs     INT NOT NULL DEFAULT 10,
    max_queued_tasks        INT NOT NULL DEFAULT 1000,
    max_daily_runs          INT,
    max_run_duration_secs   INT NOT NULL DEFAULT 86400,
    max_tasks_per_run       INT NOT NULL DEFAULT 10000,
    max_backfill_concurrency INT NOT NULL DEFAULT 5,

    -- Fairness weight (1.0 = normal, 2.0 = double priority)
    fairness_weight         FLOAT NOT NULL DEFAULT 1.0,

    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- RLS for new tables
ALTER TABLE task_heartbeats ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE materializations ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE backfills ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_quotas ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_heartbeats
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation ON asset_states
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation ON materializations
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation ON idempotency_keys
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation ON backfills
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation ON tenant_quotas
    USING (tenant_id = current_setting('app.tenant_id'));

-- Schema migration tracking
CREATE TABLE schema_migrations (
    version         BIGINT PRIMARY KEY,
    name            TEXT NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum        TEXT NOT NULL,            -- SHA256 of migration file
    execution_time  INTERVAL,                 -- How long migration took
    applied_by      TEXT NOT NULL DEFAULT current_user
);

-- Record each migration
-- Example: INSERT INTO schema_migrations VALUES (20250112001, 'initial_schema', NOW(), 'sha256:abc...', '2.3s', 'deploy-sa');

-- Verify no gaps in migrations
CREATE OR REPLACE FUNCTION verify_migration_sequence()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.version != (SELECT COALESCE(MAX(version), 0) + 1 FROM schema_migrations WHERE version < NEW.version) + 1 THEN
        -- Allow gaps but log warning (some migrations may be env-specific)
        RAISE WARNING 'Migration % may have gaps in sequence', NEW.version;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_migration_sequence
    BEFORE INSERT ON schema_migrations
    FOR EACH ROW EXECUTE FUNCTION verify_migration_sequence();
```

### 14.4 Migration Best Practices

**Migration File Naming:**
```
migrations/
├── 20250112001_initial_schema.sql
├── 20250112002_add_heartbeat_table.sql
├── 20250115001_add_materialization_index.sql
└── 20250120001_add_cost_tracking.sql
```

**Migration Template:**
```sql
-- Migration: 20250115001_add_materialization_index
-- Description: Add index for materialization lookups by asset
-- Reversible: Yes

-- Up
CREATE INDEX CONCURRENTLY idx_materializations_asset
    ON materializations(tenant_id, asset_key, created_at DESC);

-- Down (for rollback)
-- DROP INDEX CONCURRENTLY idx_materializations_asset;
```

**Migration Runner (Rust):**
```rust
pub async fn run_migrations(pool: &PgPool, migrations_dir: &Path) -> Result<()> {
    let applied: HashSet<i64> = sqlx::query_scalar(
        "SELECT version FROM schema_migrations"
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let mut pending: Vec<Migration> = read_migration_files(migrations_dir)?
        .into_iter()
        .filter(|m| !applied.contains(&m.version))
        .collect();

    pending.sort_by_key(|m| m.version);

    for migration in pending {
        let start = Instant::now();

        let mut tx = pool.begin().await?;
        sqlx::raw_sql(&migration.up_sql).execute(&mut *tx).await?;

        sqlx::query(
            "INSERT INTO schema_migrations (version, name, checksum, execution_time)
             VALUES ($1, $2, $3, $4)"
        )
        .bind(migration.version)
        .bind(&migration.name)
        .bind(&migration.checksum)
        .bind(start.elapsed().as_secs_f64())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        tracing::info!(
            version = migration.version,
            name = migration.name,
            duration_ms = start.elapsed().as_millis(),
            "Applied migration"
        );
    }

    Ok(())
}
```

---

## 15. Canonical Serialization Rules

### 15.1 The Problem

Several parts of Servo rely on deterministic serialization:
- **Plan fingerprints** — Same inputs must produce same hash
- **Envelope signatures** — HMAC must verify across languages
- **Idempotency keys** — Must be reproducible
- **Partition key comparisons** — Must be stable for indexing

Protobuf `map<>` fields have **non-deterministic iteration order**. If you serialize and hash without canonicalization, you get "same logical data, different bytes."

### 15.2 Canonical JSON Rules

All JSON serialization for hashing/signing uses **Canonical JSON**:

```
1. Keys sorted alphabetically (lexicographic UTF-8)
2. No whitespace between tokens
3. No trailing commas
4. Numbers without leading zeros or trailing decimal zeros
5. UTF-8 encoded
6. No BOM
```

**Example:**
```json
// Input (non-canonical)
{
  "tenant": "acme",
  "date": "2025-01-15"
}

// Output (canonical)
{"date":"2025-01-15","tenant":"acme"}
```

### 15.3 Implementation

**Rust:**
```rust
use serde::Serialize;
use serde_json::ser::{Formatter, Serializer};

pub fn canonical_json<T: Serialize>(value: &T) -> Result<String> {
    // Use serde_json with sorted keys
    let mut buf = Vec::new();
    let mut ser = Serializer::with_formatter(&mut buf, CanonicalFormatter);
    value.serialize(&mut ser)?;
    Ok(String::from_utf8(buf)?)
}

// For maps specifically, convert to sorted Vec first
pub fn canonical_partition_key(key: &PartitionKey) -> String {
    let mut pairs: Vec<_> = key.dimensions.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    let sorted: BTreeMap<_, _> = pairs.into_iter().collect();
    serde_json::to_string(&sorted).unwrap()
}
```

**Python:**
```python
import json

def canonical_json(obj: dict) -> str:
    """Serialize to canonical JSON (sorted keys, no whitespace)."""
    return json.dumps(obj, sort_keys=True, separators=(',', ':'))

def canonical_partition_key(partition_key: dict[str, str]) -> str:
    """Canonical string for partition key."""
    return canonical_json(partition_key)
```

### 15.4 Fingerprint Computation

```rust
pub fn compute_plan_fingerprint(spec: &ExecutionPlanSpec) -> String {
    // 1. Convert spec to canonical JSON (NOT protobuf bytes)
    let canonical = canonical_json(spec)?;

    // 2. Hash with SHA-256
    let hash = sha256(canonical.as_bytes());

    // 3. Return hex-encoded
    hex::encode(hash)
}
```

### 15.5 Envelope Signature

```rust
pub fn sign_envelope(envelope: &TaskEnvelope, key: &[u8]) -> AuthMaterial {
    // 1. Create copy WITHOUT signature field
    let mut for_signing = envelope.clone();
    for_signing.auth = None;

    // 2. Serialize to canonical JSON
    let canonical = canonical_json(&for_signing)?;

    // 3. Compute HMAC-SHA256
    let signature = hmac_sha256(key, canonical.as_bytes());

    AuthMaterial {
        key_id: current_key_id(),
        signature,
        expires_at: Utc::now() + Duration::hours(2),
        nonce: generate_nonce(),
    }
}

pub fn verify_envelope(envelope: &TaskEnvelope, key: &[u8]) -> Result<()> {
    let auth = envelope.auth.as_ref().ok_or(AuthError::Missing)?;

    // 1. Create copy WITHOUT signature field
    let mut for_signing = envelope.clone();
    for_signing.auth = None;

    // 2. Serialize to canonical JSON
    let canonical = canonical_json(&for_signing)?;

    // 3. Compute expected signature
    let expected = hmac_sha256(key, canonical.as_bytes());

    // 4. Constant-time comparison
    if !constant_time_eq(&auth.signature, &expected) {
        return Err(AuthError::InvalidSignature);
    }

    Ok(())
}
```

### 15.6 Testing Canonicalization

```rust
#[test]
fn test_partition_key_canonical() {
    // Different insertion order, same canonical output
    let mut key1 = HashMap::new();
    key1.insert("date", "2025-01-15");
    key1.insert("tenant", "acme");

    let mut key2 = HashMap::new();
    key2.insert("tenant", "acme");
    key2.insert("date", "2025-01-15");

    assert_eq!(
        canonical_partition_key(&key1),
        canonical_partition_key(&key2)
    );
    assert_eq!(
        canonical_partition_key(&key1),
        r#"{"date":"2025-01-15","tenant":"acme"}"#
    );
}
```

---

## 16. Output Commit Protocol

### 16.1 The Problem

Workers write outputs to object storage. If Cloud Tasks retries or a worker is re-invoked:
- Duplicate writes can corrupt data
- Partial writes can leave invalid state
- Quarantined outputs might accidentally become "latest"

We need **exactly-once effects** for materializations.

### 16.2 Materialization States

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Materialization State Machine                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐                                                          │
│   │  STAGED  │  Worker writes to attempt-scoped staging path            │
│   └────┬─────┘                                                          │
│        │ checks pass                                                     │
│        ▼                                                                 │
│   ┌───────────┐                                                         │
│   │ COMMITTED │  Worker writes commit marker, reports to control plane  │
│   └────┬──────┘                                                         │
│        │ control plane accepts                                           │
│        ▼                                                                 │
│   ┌──────────┐                                                          │
│   │ PROMOTED │  Control plane updates "latest" pointer                  │
│   └──────────┘                                                          │
│                                                                          │
│   ┌─────────────┐                                                       │
│   │ QUARANTINED │  Checks failed, output isolated for review            │
│   └─────────────┘                                                       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 16.3 Storage Layout

```
gs://bucket/assets/{asset_id}/
├── latest.json                          # Pointer to current good version
├── materializations/
│   ├── {materialization_id}/
│   │   ├── data.parquet                 # The actual data
│   │   ├── _metadata.json               # Row count, schema hash, etc.
│   │   └── _commit.json                 # Commit marker (atomic)
│   └── ...
├── staging/
│   └── {run_id}/{task_id}/attempt={N}/
│       ├── data.parquet                 # In-progress write
│       └── _metadata.json               # Partial metadata
└── quarantine/
    └── {run_id}/
        ├── data.parquet                 # Failed output
        └── _reason.json                 # Why it was quarantined
```

### 16.4 Commit Protocol

```python
# Worker commit protocol
async def commit_output(
    ctx: AssetContext,
    output: AssetOutput,
    check_results: list[CheckResult],
) -> MaterializationInfo:
    staging_path = f"staging/{ctx.run_id}/{ctx.task_id}/attempt={ctx.attempt}"
    final_path = f"materializations/{ctx.materialization_id}"

    # 1. Write to staging (idempotent - same content, same path)
    await write_parquet(f"{staging_path}/data.parquet", output.data)

    # 2. Compute metadata
    metadata = {
        "row_count": len(output.data),
        "schema_hash": hash_schema(output.data.schema),
        "byte_size": output.data.nbytes,
        "created_at": datetime.utcnow().isoformat(),
    }
    await write_json(f"{staging_path}/_metadata.json", metadata)

    # 3. Run checks (if not already done)
    gating_decision = compute_gating_decision(check_results)

    if gating_decision == GatingDecision.QUARANTINE:
        # Move to quarantine, don't commit
        await move_to_quarantine(staging_path, ctx.run_id, check_results)
        return MaterializationInfo(
            state=MaterializationState.QUARANTINED,
            path=f"quarantine/{ctx.run_id}",
        )

    # 4. Atomic move from staging to final (copy + delete)
    await copy_recursive(staging_path, final_path)

    # 5. Write commit marker (atomic - signals completion)
    commit_marker = {
        "materialization_id": ctx.materialization_id,
        "task_id": ctx.task_id,
        "run_id": ctx.run_id,
        "attempt": ctx.attempt,
        "committed_at": datetime.utcnow().isoformat(),
        "metadata": metadata,
    }
    await write_json(f"{final_path}/_commit.json", commit_marker)

    # 6. Delete staging (cleanup)
    await delete_recursive(staging_path)

    return MaterializationInfo(
        state=MaterializationState.COMMITTED,
        path=final_path,
        metadata=metadata,
    )
```

### 16.5 Control Plane Promotion

```rust
/// Control plane promotes committed materialization to "latest"
async fn promote_materialization(
    &self,
    task: &Task,
    result: &TaskResult,
) -> Result<()> {
    let mat = result.materialization.as_ref()
        .ok_or(Error::NoMaterialization)?;

    // 1. Verify commit marker exists (worker actually committed)
    let commit_path = format!("{}/{}/_commit.json", mat.path, mat.materialization_id);
    if !self.storage.exists(&commit_path).await? {
        return Err(Error::NotCommitted);
    }

    // 2. Check gating decision allows promotion
    if result.gating_decision == GatingDecision::BLOCK {
        return Err(Error::GatingBlocked);
    }

    // 3. Update latest pointer (atomic overwrite)
    let latest = LatestPointer {
        materialization_id: mat.materialization_id.clone(),
        path: mat.path.clone(),
        partition_key: task.partition_key.clone(),
        promoted_at: Utc::now(),
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
    };

    let latest_path = format!(
        "assets/{}/partitions/{}/latest.json",
        task.asset_key,
        canonical_partition_key(&task.partition_key),
    );

    self.storage.write_json(&latest_path, &latest).await?;

    // 4. Update materializations table
    self.store.update_materialization_state(
        &mat.materialization_id,
        MaterializationState::PROMOTED,
    ).await?;

    Ok(())
}
```

### 16.6 Idempotency Guarantees

| Operation | Idempotency Key | Behavior on Duplicate |
|-----------|-----------------|----------------------|
| Worker write to staging | `{run_id}/{task_id}/attempt={N}` | Overwrites (same content) |
| Commit marker write | `{materialization_id}` | Fails if exists (expected) |
| Task result callback | `{task_id}:{attempt}` | Dedupe by idempotency_key |
| Materialization record | `materialization_id` (PK) | Insert conflict = no-op |
| Latest pointer update | N/A (last-write-wins) | Overwrites (correct) |

### 16.7 Downstream Input Resolution

```rust
/// Resolve inputs for a task, using "latest committed" or specific version
async fn resolve_inputs(
    &self,
    task: &PlannedTask,
) -> Result<Vec<ResolvedInput>> {
    let mut inputs = Vec::new();

    for input_ref in &task.inputs {
        let resolved = match &input_ref.version {
            // Specific version requested
            Some(version) => {
                let path = format!(
                    "assets/{}/materializations/{}/data.parquet",
                    input_ref.asset_key, version.materialization_id,
                );
                ResolvedInput { path, version: version.clone() }
            }
            // Use latest committed
            None => {
                let latest_path = format!(
                    "assets/{}/partitions/{}/latest.json",
                    input_ref.asset_key,
                    canonical_partition_key(&input_ref.partition_key),
                );
                let latest: LatestPointer = self.storage.read_json(&latest_path).await?;
                ResolvedInput {
                    path: format!("{}/data.parquet", latest.path),
                    version: Version {
                        materialization_id: latest.materialization_id,
                        run_id: latest.run_id,
                    },
                }
            }
        };
        inputs.push(resolved);
    }

    Ok(inputs)
}
```

---

## 17. Provider Adapters

### 17.1 Abstraction Layers

Servo is GCP-first but designed for multi-cloud. Provider adapters abstract infrastructure:

```rust
// Core adapter traits

#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn enqueue(&self, task: QueueTask) -> Result<()>;
    async fn delete(&self, task_id: &str) -> Result<()>;
    async fn get_stats(&self) -> Result<QueueStats>;
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn read(&self, path: &str) -> Result<Bytes>;
    async fn write(&self, path: &str, data: Bytes) -> Result<()>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn exists(&self, path: &str) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn presign_upload(&self, path: &str, expires: Duration) -> Result<String>;
    async fn presign_download(&self, path: &str, expires: Duration) -> Result<String>;
}

#[async_trait]
pub trait SecretsProvider: Send + Sync {
    async fn get_secret(&self, path: &str, version: Option<&str>) -> Result<String>;
}

#[async_trait]
pub trait IdentityProvider: Send + Sync {
    async fn verify_token(&self, token: &str) -> Result<Identity>;
    async fn get_service_token(&self) -> Result<String>;
}

#[async_trait]
pub trait BuildService: Send + Sync {
    async fn trigger_build(&self, config: BuildConfig) -> Result<BuildId>;
    async fn get_build_status(&self, build_id: &BuildId) -> Result<BuildStatus>;
}
```

### 17.2 GCP Implementations (Default)

| Adapter | GCP Service | Notes |
|---------|-------------|-------|
| `TaskQueue` | Cloud Tasks | Default queue implementation |
| `BlobStore` | Cloud Storage (GCS) | Uses `object_store` crate |
| `SecretsProvider` | Secret Manager | Workload Identity auth |
| `IdentityProvider` | Cloud IAM + OIDC | Firebase Auth for users |
| `BuildService` | Cloud Build | Buildpacks support |

### 17.3 Alternative Implementations (Future)

| Adapter | AWS | Self-Hosted |
|---------|-----|-------------|
| `TaskQueue` | SQS | RabbitMQ / Redis |
| `BlobStore` | S3 | MinIO |
| `SecretsProvider` | Secrets Manager | Vault |
| `IdentityProvider` | Cognito | Keycloak |
| `BuildService` | CodeBuild | Kaniko |

### 17.4 Provider Configuration

```rust
#[derive(Debug, Deserialize)]
pub struct ProviderConfig {
    pub task_queue: TaskQueueConfig,
    pub blob_store: BlobStoreConfig,
    pub secrets: SecretsConfig,
    pub identity: IdentityConfig,
    pub build: BuildConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum TaskQueueConfig {
    #[serde(rename = "cloud-tasks")]
    CloudTasks { project: String, location: String, queue: String },

    #[serde(rename = "sqs")]
    Sqs { queue_url: String, region: String },

    #[serde(rename = "memory")]
    InMemory,  // For testing
}
```

### 17.5 Dependency Injection

```rust
pub struct ServoRuntime {
    pub task_queue: Arc<dyn TaskQueue>,
    pub blob_store: Arc<dyn BlobStore>,
    pub secrets: Arc<dyn SecretsProvider>,
    pub identity: Arc<dyn IdentityProvider>,
    pub build: Arc<dyn BuildService>,
}

impl ServoRuntime {
    pub fn from_config(config: &ProviderConfig) -> Result<Self> {
        Ok(Self {
            task_queue: match &config.task_queue {
                TaskQueueConfig::CloudTasks { .. } => Arc::new(CloudTasksQueue::new(config)?),
                TaskQueueConfig::Sqs { .. } => Arc::new(SqsQueue::new(config)?),
                TaskQueueConfig::InMemory => Arc::new(InMemoryQueue::new()),
            },
            // ... other providers
        })
    }
}
```

---

## 18. API Standards

### 18.1 Error Model

**gRPC Errors:**
```protobuf
// Standard gRPC status codes + rich details
message ErrorDetail {
  string code = 1;              // Machine-readable: "QUOTA_EXCEEDED"
  string message = 2;           // Human-readable description
  map<string, string> metadata = 3;  // Additional context

  // For validation errors
  repeated FieldViolation field_violations = 10;

  // For rate limiting
  google.protobuf.Duration retry_after = 20;
}

message FieldViolation {
  string field = 1;
  string description = 2;
}
```

**HTTP Gateway (RFC 7807 Problem Details):**
```json
{
  "type": "https://servo.arco.dev/errors/quota-exceeded",
  "title": "Quota Exceeded",
  "status": 429,
  "detail": "Tenant 'acme' has exceeded max_concurrent_runs quota (10)",
  "instance": "/v1/runs/trigger",
  "tenant_id": "acme",
  "quota_type": "max_concurrent_runs",
  "current": 10,
  "limit": 10,
  "retry_after": 300
}
```

### 18.2 Pagination

**Cursor-Based Pagination:**
```protobuf
message ListRunsRequest {
  TenantId tenant_id = 1;
  WorkspaceId workspace_id = 2;

  // Pagination
  int32 page_size = 10;         // Max 100, default 20
  string page_token = 11;       // Opaque cursor

  // Filtering
  RunStateFilter state_filter = 20;
  google.protobuf.Timestamp created_after = 21;
  google.protobuf.Timestamp created_before = 22;

  // Sorting (stable sort key required)
  string order_by = 30;         // "created_at desc" (default)
}

message ListRunsResponse {
  repeated Run runs = 1;
  string next_page_token = 2;   // Empty if no more pages
  int32 total_count = 3;        // Optional, expensive to compute
}
```

**Page Token Encoding:**
```rust
#[derive(Serialize, Deserialize)]
struct PageToken {
    // Sort key value (for stable cursor)
    sort_value: String,
    // Unique ID (for ties)
    last_id: String,
    // Timestamp (for expiry, anti-tampering)
    created_at: DateTime<Utc>,
}

fn encode_page_token(token: &PageToken) -> String {
    let json = serde_json::to_vec(token).unwrap();
    let encrypted = aes_encrypt(&json, &PAGE_TOKEN_KEY);
    base64_url_encode(&encrypted)
}
```

### 18.3 Rate Limiting

**Headers:**
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1705320000
Retry-After: 60
```

**Limits by Endpoint:**

| Endpoint | Limit | Window | Scope |
|----------|-------|--------|-------|
| `TriggerRun` | 100/min | Per tenant | Write |
| `ListRuns` | 1000/min | Per tenant | Read |
| `StreamRunEvents` | 10 concurrent | Per tenant | Stream |
| `Deploy` | 10/min | Per workspace | Write |

### 18.4 Idempotency

**Mutation Endpoints:**
```protobuf
message TriggerRunRequest {
  // Required for idempotency
  string idempotency_key = 100;  // Client-generated UUID

  // ... other fields
}
```

**Behavior:**
1. First request: Process normally, store response with key
2. Duplicate request (same key): Return stored response
3. Key expiry: 24 hours

**Implementation:**
```rust
async fn trigger_run(&self, req: TriggerRunRequest) -> Result<TriggerRunResponse> {
    // Check idempotency cache
    if let Some(cached) = self.idempotency.get(&req.idempotency_key).await? {
        // Verify request hash matches (prevent key reuse with different params)
        if cached.request_hash != hash_request(&req) {
            return Err(Status::invalid_argument("Idempotency key reused with different request"));
        }
        return Ok(cached.response);
    }

    // Process request
    let response = self.do_trigger_run(req.clone()).await?;

    // Store for idempotency
    self.idempotency.put(
        &req.idempotency_key,
        IdempotencyEntry {
            request_hash: hash_request(&req),
            response: response.clone(),
            expires_at: Utc::now() + Duration::hours(24),
        },
    ).await?;

    Ok(response)
}
```

---

## 19. Cancellation Semantics

### 19.1 Cancellation Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Cancellation Flow                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  User calls CancelRun                                                    │
│       │                                                                  │
│       ▼                                                                  │
│  Run state: RUNNING → CANCELLING                                        │
│       │                                                                  │
│       ├─────────────────────────────────────────────────────────┐       │
│       │                                                         │       │
│       ▼                                                         ▼       │
│  Tasks in PLANNED/PENDING/READY:              Tasks in RUNNING:         │
│  → Mark CANCELLED immediately                 → Set cancel flag         │
│                                               → Worker checks on         │
│       │                                         next heartbeat          │
│       │                                               │                 │
│       │                                               ▼                 │
│       │                                       Worker sees cancel        │
│       │                                       → Graceful shutdown       │
│       │                                       → Report CANCELLED        │
│       │                                               │                 │
│       └───────────────────────────────────────────────┘                 │
│                               │                                          │
│                               ▼                                          │
│                      All tasks terminal?                                │
│                               │                                          │
│                               ▼                                          │
│                  Run state: CANCELLING → CANCELLED                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 19.2 Task State Transitions on Cancel

| Current State | Action | New State |
|---------------|--------|-----------|
| PLANNED | Mark cancelled | CANCELLED |
| PENDING | Mark cancelled | CANCELLED |
| READY | Mark cancelled | CANCELLED |
| QUEUED | Delete from queue + mark | CANCELLED |
| DISPATCHED | Wait for worker | CANCELLED (via worker) |
| RUNNING | Signal worker | CANCELLED (via worker) |
| RETRY_WAIT | Mark cancelled | CANCELLED |

### 19.3 Worker Cancellation Protocol

```python
# Worker checks for cancellation on heartbeat response
async def _heartbeat_loop(self, url: str):
    while True:
        await asyncio.sleep(30)
        try:
            response = await self.callback_client.send_heartbeat(url)

            # Check for cancellation signal
            if response.get("should_cancel"):
                logger.info("Received cancellation signal")
                raise CancellationRequested()

        except CancellationRequested:
            # Graceful shutdown
            await self._graceful_shutdown()
            raise

async def _graceful_shutdown(self):
    """Clean up on cancellation."""
    # 1. Stop any ongoing work (if possible)
    self.cancelled = True

    # 2. Clean up staging artifacts
    if self.current_staging_path:
        await delete_recursive(self.current_staging_path)

    # 3. Report cancellation
    await self.callback_client.report_status(
        self.callbacks.status_url,
        "cancelled",
        reason="user_requested",
    )
```

### 19.4 Handling Late Results

If a worker finishes after cancellation was requested:

```rust
async fn handle_task_result(&self, result: TaskResult) -> Result<()> {
    let task = self.store.get_task(&result.task_id).await?;
    let run = self.store.get_run(&task.run_id).await?;

    // Check if run is cancelling/cancelled
    if run.state == RunState::Cancelling || run.state == RunState::Cancelled {
        // Accept the result but don't promote materialization
        self.store.update_task(
            &task.task_id,
            TaskUpdate {
                state: TaskState::Cancelled,
                outcome: Some(TaskOutcome::Cancelled),
                // Store result for debugging but mark as cancelled
                result_metadata: Some(json!({
                    "late_result": true,
                    "original_outcome": result.outcome,
                })),
            },
        ).await?;

        // Don't promote materialization to latest
        return Ok(());
    }

    // Normal result handling
    self.process_result(result).await
}
```

### 19.5 Force Cancel

For stuck tasks (no heartbeat, not responding):

```rust
async fn force_cancel_task(&self, task_id: &TaskId) -> Result<()> {
    let task = self.store.get_task(task_id).await?;

    // Only force-cancel if stuck
    if task.state != TaskState::Running {
        return Err(Error::InvalidState("Can only force-cancel RUNNING tasks"));
    }

    let last_heartbeat = task.last_heartbeat.unwrap_or(task.started_at.unwrap());
    if Utc::now() - last_heartbeat < Duration::minutes(5) {
        return Err(Error::TooSoon("Task has recent heartbeat, use regular cancel"));
    }

    // Force transition
    self.transition_task(
        &task,
        TaskState::Cancelled,
        TransitionReason::ForceCancelled,
    ).await?;

    // Try to delete from queue (best effort)
    let _ = self.task_queue.delete(&task.task_id.value).await;

    Ok(())
}
```

---

## 20. Projection Rebuild Strategy

### 20.1 When to Rebuild

Projection rebuilds are needed for:
- Schema changes to projection tables
- Bug fixes in projection logic
- Data corruption recovery
- Adding new projections

### 20.2 Rebuild Modes

| Mode | Downtime | Data Loss | Use Case |
|------|----------|-----------|----------|
| **Offline** | Yes | No | Schema migrations, urgent fixes |
| **Online (Shadow)** | No | No | Normal upgrades |

### 20.3 Offline Rebuild

```bash
# 1. Stop scheduler (prevent new events)
servo admin scheduler stop

# 2. Drain running tasks (wait for completion)
servo admin tasks drain --timeout 30m

# 3. Run rebuild
servo admin projections rebuild \
  --projection runs \
  --from-event-id 0 \
  --batch-size 10000 \
  --checkpoint-interval 1000

# 4. Verify
servo admin projections verify --projection runs

# 5. Restart scheduler
servo admin scheduler start
```

### 20.4 Online Rebuild (Shadow Tables)

```rust
/// Online projection rebuild using shadow tables
async fn rebuild_projection_online(
    &self,
    projection: &str,
) -> Result<()> {
    // 1. Create shadow table
    let shadow_table = format!("{}_shadow", projection);
    self.store.create_shadow_table(projection, &shadow_table).await?;

    // 2. Start dual-write (new events go to both tables)
    self.store.enable_dual_write(projection, &shadow_table).await?;

    // 3. Backfill shadow from event log
    let mut last_event_id = 0;
    loop {
        let events = self.store
            .get_events_after(last_event_id, 10000)
            .await?;

        if events.is_empty() {
            break;
        }

        for event in &events {
            self.apply_event_to_projection(&shadow_table, event).await?;
            last_event_id = event.global_position;
        }

        // Checkpoint progress
        self.store.update_rebuild_progress(projection, last_event_id).await?;
    }

    // 4. Verify shadow matches primary (sample check)
    self.verify_projection_consistency(projection, &shadow_table).await?;

    // 5. Atomic swap
    self.store.swap_tables(projection, &shadow_table).await?;

    // 6. Disable dual-write, drop old table
    self.store.disable_dual_write(projection).await?;
    self.store.drop_table(&shadow_table).await?;

    Ok(())
}
```

### 20.5 Rebuild CLI

```bash
# List projections and their status
servo admin projections list

# Start online rebuild
servo admin projections rebuild \
  --projection runs \
  --mode online \
  --verify

# Check rebuild progress
servo admin projections rebuild-status --projection runs

# Abort rebuild (if needed)
servo admin projections rebuild-abort --projection runs

# Manual verification
servo admin projections verify \
  --projection runs \
  --sample-size 1000
```

### 20.6 Safety Rails

```rust
struct RebuildConfig {
    /// Maximum events to process per batch
    batch_size: usize,

    /// How often to checkpoint progress
    checkpoint_interval: usize,

    /// Abort if error rate exceeds this
    max_error_rate: f64,

    /// Verify checksum every N events
    verify_interval: usize,

    /// Enable resume from checkpoint on failure
    resumable: bool,
}
```

---

## 21. Supply Chain Security

### 21.1 Dependency Management

**Python (uv/pip-tools):**
```bash
# requirements.in - direct dependencies
polars>=0.20.0
pyarrow>=14.0.0

# requirements.txt - locked with hashes
polars==0.20.3 \
    --hash=sha256:abc123...
pyarrow==14.0.2 \
    --hash=sha256:def456...
```

**Rust (Cargo.lock):**
```toml
# Cargo.lock is committed and verified
[[package]]
name = "tokio"
version = "1.35.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "abc123..."
```

### 21.2 Vulnerability Scanning

```yaml
# CI pipeline
security-scan:
  steps:
    # Python
    - run: pip-audit --strict --require-hashes -r requirements.txt

    # Rust
    - run: cargo audit

    # Container
    - run: trivy image --severity HIGH,CRITICAL $IMAGE

    # SBOM generation
    - run: syft $IMAGE -o spdx-json > sbom.json
```

### 21.3 Container Signing

```bash
# Sign with cosign
cosign sign --key cosign.key gcr.io/project/servo-worker:v1.0.0

# Verify signature in deployment
cosign verify --key cosign.pub gcr.io/project/servo-worker:v1.0.0
```

### 21.4 Build Provenance (SLSA)

```yaml
# Cloud Build config with provenance
steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-t', '$_IMAGE', '.']
    env:
      - 'DOCKER_BUILDKIT=1'

options:
  requestedVerifyOption: VERIFIED
  # Generates SLSA provenance attestation
```

### 21.5 User Code Isolation

```dockerfile
# Worker container - minimal attack surface
FROM python:3.11-slim

# Non-root user
RUN useradd -m -u 1000 worker
USER worker

# Read-only filesystem (except /tmp)
# Set at runtime via Cloud Run

# No shell access in production
RUN rm /bin/sh /bin/bash

# Minimal packages
RUN pip install --no-cache-dir servo-worker
```

---

## 22. Cost Attribution

### 22.1 Cost Model

```sql
-- Cost dimensions
CREATE TABLE task_costs (
    task_id             TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    asset_key           TEXT NOT NULL,

    -- Compute
    cpu_seconds         FLOAT NOT NULL,
    memory_gb_seconds   FLOAT NOT NULL,
    gpu_seconds         FLOAT,

    -- Storage
    input_bytes_read    BIGINT NOT NULL,
    output_bytes_written BIGINT NOT NULL,

    -- Network
    network_egress_bytes BIGINT NOT NULL,

    -- Derived cost (USD)
    compute_cost_usd    DECIMAL(10, 6) NOT NULL,
    storage_cost_usd    DECIMAL(10, 6) NOT NULL,
    network_cost_usd    DECIMAL(10, 6) NOT NULL,
    total_cost_usd      DECIMAL(10, 6) NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_task_costs_tenant_date
    ON task_costs(tenant_id, created_at);
CREATE INDEX idx_task_costs_asset
    ON task_costs(tenant_id, asset_key, created_at);
```

### 22.2 Cost Computation

```rust
pub fn compute_task_cost(metrics: &TaskMetrics, pricing: &Pricing) -> TaskCost {
    // Compute cost
    let cpu_cost = metrics.cpu_seconds * pricing.cpu_per_second;
    let memory_cost = metrics.memory_gb_seconds * pricing.memory_gb_per_second;
    let gpu_cost = metrics.gpu_seconds.unwrap_or(0.0) * pricing.gpu_per_second;
    let compute_cost = cpu_cost + memory_cost + gpu_cost;

    // Storage cost
    let storage_cost =
        (metrics.input_bytes_read + metrics.output_bytes_written) as f64
        / 1_000_000_000.0  // Convert to GB
        * pricing.storage_per_gb;

    // Network cost (egress only)
    let network_cost =
        metrics.network_egress_bytes as f64
        / 1_000_000_000.0
        * pricing.network_egress_per_gb;

    TaskCost {
        compute_cost_usd: compute_cost,
        storage_cost_usd: storage_cost,
        network_cost_usd: network_cost,
        total_cost_usd: compute_cost + storage_cost + network_cost,
    }
}
```

### 22.3 Pricing Configuration

```yaml
# pricing.yaml
compute:
  cpu_per_second: 0.000024      # $0.0864/hour
  memory_gb_per_second: 0.0000025  # $0.009/hour
  gpu_per_second: 0.00035       # $1.26/hour (T4)

storage:
  storage_per_gb: 0.02          # GCS standard
  storage_per_gb_month: 0.02

network:
  egress_per_gb: 0.12           # Inter-region

# Tenant overrides (for enterprise pricing)
tenant_overrides:
  enterprise-tenant:
    cpu_per_second: 0.000020    # 20% discount
```

### 22.4 Cost Aggregation

```sql
-- Daily cost by tenant
SELECT
    tenant_id,
    DATE(created_at) as date,
    SUM(total_cost_usd) as daily_cost,
    COUNT(*) as task_count,
    SUM(cpu_seconds) as total_cpu_seconds
FROM task_costs
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY tenant_id, DATE(created_at)
ORDER BY tenant_id, date;

-- Cost by asset (for showback)
SELECT
    tenant_id,
    asset_key,
    SUM(total_cost_usd) as monthly_cost,
    AVG(total_cost_usd) as avg_task_cost
FROM task_costs
WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tenant_id, asset_key
ORDER BY monthly_cost DESC;
```

### 22.5 Cost Alerts

```yaml
# Alerting rules
groups:
  - name: servo_cost
    rules:
      - alert: HighTenantCost
        expr: |
          sum by (tenant_id) (
            increase(servo_task_cost_usd_total[24h])
          ) > 100
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Tenant {{ $labels.tenant_id }} cost exceeds $100/day"

      - alert: CostSpike
        expr: |
          sum by (tenant_id) (increase(servo_task_cost_usd_total[1h]))
          / sum by (tenant_id) (increase(servo_task_cost_usd_total[1h] offset 24h))
          > 3
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "3x cost spike for tenant {{ $labels.tenant_id }}"
```

---

## Appendix A: Canonical String Encodings

### Asset Key

```
Format: {namespace}/{name}
Examples:
  - analytics/user_metrics
  - raw/events
  - gold/revenue_daily
```

### Partition Key

```
Format: JSON object, keys sorted alphabetically
Examples:
  - {"date": "2025-01-15"}
  - {"date": "2025-01-15", "tenant": "acme"}
  - {"region": "us-east-1", "tenant": "acme"}

Canonical serialization (for hashing/comparison):
  - Keys sorted alphabetically
  - No whitespace
  - UTF-8 encoded
```

---

## Appendix B: ADR Template

```markdown
# ADR-NNN: Title

**Status:** Proposed | Accepted | Deprecated | Superseded by ADR-XXX
**Date:** YYYY-MM-DD
**Deciders:** [list of people]

## Context

What is the issue that we're seeing that is motivating this decision?

## Decision

What is the change that we're proposing and/or doing?

## Consequences

What becomes easier or more difficult to do because of this change?

### Positive
- ...

### Negative
- ...

### Neutral
- ...
```

---

## Appendix C: Production Readiness Checklist

Use this checklist before each milestone to verify production readiness.

### C.1 Pre-Launch Checklist

**Infrastructure**

- [ ] All Cloud Run services deployed with appropriate min/max instances
- [ ] Cloud Tasks queues created with correct rate limits and retry policies
- [ ] Postgres database provisioned with appropriate instance size
- [ ] All IAM permissions follow principle of least privilege
- [ ] VPC Service Controls configured for sensitive data
- [ ] Cloud Armor rules in place for API endpoints
- [ ] DNS and load balancer configured with appropriate health checks

**Observability**

- [ ] Structured logging enabled with correlation IDs
- [ ] All metrics exported to Cloud Monitoring
- [ ] Dashboards created for key SLIs (latency, error rate, saturation)
- [ ] Alerting policies defined with appropriate thresholds
- [ ] Distributed tracing enabled end-to-end
- [ ] Log-based metrics for error patterns
- [ ] Uptime checks for external endpoints

**Security**

- [ ] TLS 1.3 enforced on all endpoints
- [ ] API authentication validated (service accounts, user tokens)
- [ ] Row-level security enabled and tested for all tables
- [ ] Secrets stored in Secret Manager (never in code/config)
- [ ] Vulnerability scanning passing in CI
- [ ] Container images signed and verified
- [ ] Audit logging enabled for sensitive operations

**Data Integrity**

- [ ] Event store append-only constraint verified
- [ ] Idempotency keys working end-to-end
- [ ] Projection rebuild tested and documented
- [ ] Backup strategy implemented and tested
- [ ] Point-in-time recovery verified
- [ ] Cross-region replication configured (if required)

**Performance**

- [ ] Load testing completed at 2x expected peak
- [ ] P99 latency meets SLO under load
- [ ] Connection pooling configured appropriately
- [ ] Query plans reviewed for critical paths
- [ ] Indexes verified for common queries
- [ ] Task queue throughput validated

**Operational**

- [ ] Runbooks documented for common failure scenarios
- [ ] On-call rotation established
- [ ] Escalation paths defined
- [ ] Rollback procedure tested
- [ ] Feature flags in place for gradual rollout
- [ ] Chaos engineering tests passed

### C.2 Tenant Onboarding Checklist

- [ ] Tenant ID provisioned in control plane
- [ ] Quota limits configured per tier
- [ ] Worker service account created
- [ ] GCS buckets provisioned with lifecycle policies
- [ ] BigQuery datasets created with appropriate permissions
- [ ] Secret Manager secrets provisioned
- [ ] Monitoring dashboards cloned for tenant

### C.3 Go/No-Go Criteria

| Criterion | Threshold | Current |
|-----------|-----------|---------|
| Unit test coverage | ≥80% | ___ |
| Integration test coverage | ≥60% | ___ |
| P99 API latency | <500ms | ___ |
| Error rate | <0.1% | ___ |
| Successful load test | 2x peak | ___ |
| Security scan | 0 critical/high | ___ |
| Documentation complete | 100% | ___ |
| Runbooks reviewed | All signed off | ___ |

### C.4 Post-Launch Verification

**First Hour**

- [ ] Error rate within normal bounds
- [ ] No unexpected alerts firing
- [ ] First tenant run completed successfully
- [ ] Logs showing expected patterns

**First Day**

- [ ] Full scheduler loop executed multiple times
- [ ] At least one retry scenario handled correctly
- [ ] Quota enforcement working as expected
- [ ] No memory leaks detected

**First Week**

- [ ] All scheduled runs executing on time
- [ ] Backfill functionality verified
- [ ] Quality gates triggered and working
- [ ] Cost attribution data accurate

---

## Appendix D: Glossary

| Term | Definition |
|------|------------|
| **Asset** | A logical data artifact with defined inputs, computation, and outputs. Assets form the nodes of the DAG. |
| **Attempt** | A single execution try of a task. Failed attempts may be retried up to `max_attempts`. |
| **Backfill** | Executing an asset for historical partitions, typically with controlled parallelism. |
| **Check** | A quality assertion that runs before or after task execution (pre-check, post-check). |
| **Code Version** | An immutable snapshot of deployed user code, identified by `CodeVersionId`. |
| **Control Plane** | Rust services that manage orchestration: planner, scheduler, API. |
| **Data Plane** | Python workers that execute user code in serverless containers. |
| **DAG** | Directed Acyclic Graph of asset dependencies defining execution order. |
| **Envelope** | Wrapper around task payloads containing auth, routing, and trace context. |
| **Event** | Immutable fact recorded in the event store. Source of truth for all state. |
| **Execution Plan** | Deterministic specification of tasks to execute, their order, and dependencies. |
| **Fingerprint** | Hash of deterministic inputs (code, config) used for change detection. |
| **Gating Decision** | Result of quality checks: ALLOW, WARN, BLOCK, or QUARANTINE. |
| **Heartbeat** | Periodic signal from worker indicating task is still alive. |
| **Idempotency Key** | Unique identifier ensuring an operation executes exactly once. |
| **Manifest** | Declaration of assets, dependencies, and schedules from user code. |
| **Materialization** | A successfully completed execution of an asset partition, producing output. |
| **Metadata Snapshot** | Point-in-time capture of catalog state used during planning. |
| **Outbox** | Pattern ensuring events are reliably published to external systems. |
| **Partition** | A slice of an asset's data, typically by time or tenant dimension. |
| **Plan** | See Execution Plan. |
| **Projection** | Materialized view built from events for efficient querying. |
| **Quarantine** | Isolation of failed outputs pending investigation. |
| **Quota** | Per-tenant limits on concurrent tasks, API calls, or storage. |
| **Run** | A single execution of an execution plan, containing multiple tasks. |
| **Scheduler** | Component that evaluates task readiness and dispatches to queues. |
| **Stage** | Topological level in the DAG (0 = roots, 1 = depends on stage 0, etc.). |
| **Task** | A single unit of work: one asset + one partition + one attempt. |
| **Tenant** | An isolated customer account with dedicated quotas and data. |
| **Worker** | Cloud Run instance executing user Python code for a task. |
| **Workspace** | A deployment environment within a tenant (e.g., dev, staging, prod). |

---

## Appendix E: Operational Runbook

### E.1 Common Issues

#### High Task Failure Rate

**Alert:** `ServoHighTaskFailureRate` firing

**Symptoms:**
- Error rate > 5% for sustained period
- Multiple tenants affected

**Diagnosis:**

```sql
-- Find failing tasks by error type
SELECT
    asset_key,
    error_message,
    COUNT(*) as failures,
    COUNT(DISTINCT tenant_id) as affected_tenants
FROM tasks
WHERE state = 'FAILED'
  AND completed_at > NOW() - INTERVAL '1 hour'
GROUP BY asset_key, error_message
ORDER BY failures DESC
LIMIT 10;

-- Check if specific code version is problematic
SELECT
    code_version_id,
    COUNT(*) FILTER (WHERE state = 'FAILED') as failures,
    COUNT(*) FILTER (WHERE state = 'SUCCEEDED') as successes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE state = 'FAILED') / COUNT(*), 2) as failure_pct
FROM tasks
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY code_version_id
ORDER BY failure_pct DESC;
```

**Resolution:**
- **Transient errors (network, timeout):** Usually self-healing via retries. Monitor for 15 minutes.
- **User code errors:** Contact asset owner with error details. Consider pausing affected assets.
- **Infrastructure errors:** Check Cloud Run logs, Postgres health, Cloud Tasks queue.

#### Stuck Tasks (Zombie Detection)

**Alert:** `ServoStuckTasks` firing

**Symptoms:**
- Tasks in RUNNING state > 1 hour with no recent heartbeat
- Run progress stalled

**Diagnosis:**

```sql
-- Find zombie tasks
SELECT
    task_id,
    run_id,
    asset_key,
    started_at,
    last_heartbeat,
    NOW() - last_heartbeat as heartbeat_age,
    worker_id
FROM tasks
WHERE state = 'RUNNING'
  AND last_heartbeat < NOW() - INTERVAL '10 minutes'
ORDER BY last_heartbeat ASC;

-- Check if scheduler is running
SELECT
    event_type,
    MAX(timestamp) as last_seen
FROM events
WHERE stream_type = 'scheduler'
  AND timestamp > NOW() - INTERVAL '1 hour'
GROUP BY event_type;
```

**Resolution:**
1. **Scheduler should auto-fail these** — verify scheduler is running and processing
2. **If scheduler stuck:** Restart scheduler service via Cloud Run console
3. **Manual intervention (last resort):**
```sql
-- Force-fail zombie tasks
UPDATE tasks
SET state = 'FAILED',
    outcome = 'TIMED_OUT',
    error_message = 'Manual intervention: heartbeat timeout',
    completed_at = NOW()
WHERE state = 'RUNNING'
  AND last_heartbeat < NOW() - INTERVAL '30 minutes';
```

#### Queue Backup

**Alert:** `ServoQueueBacklog` firing

**Symptoms:**
- Tasks stuck in READY state
- High queue depth in Cloud Tasks

**Diagnosis:**

```bash
# Check Cloud Tasks queue depth
gcloud tasks queues describe servo-tasks --location=us-central1

# Check quota utilization
curl -s localhost:9090/api/v1/query \
  --data-urlencode 'query=servo_quota_usage_ratio' | jq
```

```sql
-- Tasks waiting for quota
SELECT
    tenant_id,
    COUNT(*) as ready_tasks,
    MIN(created_at) as oldest_waiting
FROM tasks
WHERE state = 'READY'
GROUP BY tenant_id
ORDER BY ready_tasks DESC;
```

**Resolution:**
1. **If quota exhausted:** Temporarily increase quota for affected tenant
2. **If workers not scaling:** Check Cloud Run autoscaling settings, increase max instances
3. **If specific tenant:** Check tenant fairness weight, may need rebalancing

#### Database Connection Issues

**Alert:** `ServoDBConnectionErrors` firing

**Symptoms:**
- Connection timeouts in logs
- High latency on API calls

**Diagnosis:**

```bash
# Check PgBouncer stats
psql -h pgbouncer -p 6432 -U admin pgbouncer -c "SHOW POOLS;"
psql -h pgbouncer -p 6432 -U admin pgbouncer -c "SHOW CLIENTS;"

# Check Postgres connections
psql -h postgres -U admin servo -c "
SELECT state, COUNT(*)
FROM pg_stat_activity
WHERE datname = 'servo'
GROUP BY state;"
```

**Resolution:**
1. **If pool exhausted:** Increase `default_pool_size` in PgBouncer
2. **If connections leaking:** Check for long-running transactions, restart affected service
3. **If Postgres overloaded:** Scale up instance, add read replicas

### E.2 Maintenance Procedures

#### Rolling Restart

```bash
# Restart control plane services (zero-downtime)
gcloud run services update servo-api --region=us-central1 \
  --update-env-vars="RESTART_TRIGGER=$(date +%s)"

gcloud run services update servo-scheduler --region=us-central1 \
  --update-env-vars="RESTART_TRIGGER=$(date +%s)"
```

#### Database Maintenance

```sql
-- Vacuum and analyze (run during low-traffic window)
VACUUM ANALYZE events;
VACUUM ANALYZE tasks;
VACUUM ANALYZE runs;

-- Check table bloat
SELECT
    relname,
    pg_size_pretty(pg_total_relation_size(relid)) as total_size,
    pg_size_pretty(pg_relation_size(relid)) as table_size,
    pg_size_pretty(pg_indexes_size(relid)) as index_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 10;
```

#### Projection Rebuild

```bash
# Trigger projection rebuild (if projections diverged from events)
servo-admin projection rebuild --projection=runs --from-sequence=0 --dry-run
servo-admin projection rebuild --projection=runs --from-sequence=0 --confirm
```

### E.3 Emergency Procedures

#### Circuit Breaker - Stop All Processing

```bash
# Emergency stop: pause all task dispatch
gcloud tasks queues pause servo-tasks --location=us-central1

# When ready to resume
gcloud tasks queues resume servo-tasks --location=us-central1
```

#### Tenant Isolation

```sql
-- Temporarily disable a problematic tenant
UPDATE tenant_quotas
SET max_concurrent_tasks = 0,
    max_queued_tasks = 0
WHERE tenant_id = 'problematic-tenant';

-- Re-enable after investigation
UPDATE tenant_quotas
SET max_concurrent_tasks = 50,
    max_queued_tasks = 200
WHERE tenant_id = 'problematic-tenant';
```

#### Rollback Deployment

```bash
# List recent revisions
gcloud run revisions list --service=servo-api --region=us-central1

# Rollback to previous revision
gcloud run services update-traffic servo-api \
  --region=us-central1 \
  --to-revisions=servo-api-00042-abc=100
```

### E.4 Escalation Matrix

| Severity | Response Time | Escalation Path |
|----------|---------------|-----------------|
| P1 (Critical) | 15 min | On-call → Team Lead → Engineering Director |
| P2 (High) | 1 hour | On-call → Team Lead |
| P3 (Medium) | 4 hours | On-call (next business day OK) |
| P4 (Low) | 24 hours | Ticket queue |

**P1 Criteria:**
- Complete service outage
- Data corruption detected
- Security breach suspected

**P2 Criteria:**
- Degraded performance affecting >50% of tenants
- Single critical tenant impacted
- Failed deployments

---

*End of Part 2 Design Document*
